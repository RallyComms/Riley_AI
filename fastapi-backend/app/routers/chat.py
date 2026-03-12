import asyncio
import logging
import re
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Depends, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

from app.core.config import get_settings
from app.core.personas import get_persona_context  # CRITICAL: Preserve for Board Demo
from app.dependencies.auth import verify_clerk_token, check_tenant_membership
from app.dependencies.graph_dep import get_graph, get_graph_optional
from app.services.genai_client import get_genai_client
from app.services.graph import GraphService
from app.services.qdrant import vector_service
from app.services.rerank import rerank_candidates

logger = logging.getLogger(__name__)

def _extract_user_id_from_session(session_id: str) -> str:
    """Extract user_id from session_id format: session_{tenantId}_{userId}_{timestamp}
    
    Args:
        session_id: Session ID in format session_{tenantId}_{userId}_{timestamp}
        
    Returns:
        User ID extracted from session_id, or "anonymous" if parsing fails
    """
    try:
        if session_id and session_id.startswith("session_"):
            parts = session_id.split("_")
            if len(parts) >= 3:
                return parts[2]  # user_id is the third part
    except Exception:
        pass
    return "anonymous"


router = APIRouter()


class ChatRequest(BaseModel):
    query: str = Field(..., max_length=2000, description="User query text (max 2000 characters)")
    tenant_id: str = Field(..., max_length=50, description="Tenant/client identifier (max 50 characters)")
    mode: Literal["fast", "deep"] = "fast"  # UI retrieval depth mode
    session_id: Optional[str] = Field(None, description="Optional session ID for chat memory")
    user_display_name: Optional[str] = Field(None, description="User's display name for personalization (username, firstName, or email)")


class ChatResponse(BaseModel):
    response: str
    model_used: str
    sources_count: int
    sources: List[Dict[str, str]] = Field(default_factory=list)


class RileyIndexRecentUpload(BaseModel):
    filename: str
    file_type: str
    ingestion_status: str
    upload_date: str


class RileyIndexSummaryResponse(BaseModel):
    total_documents: int
    indexed_count: int
    processing_count: int
    failed_count: int
    low_text_count: int
    ocr_needed_count: int
    counts_by_file_type: Dict[str, int]
    recent_uploads: List[RileyIndexRecentUpload] = Field(default_factory=list)


class MessageSchema(BaseModel):
    """Schema for a chat message in history."""
    role: str = Field(..., description="Message role: 'user' or 'model'")
    content: str = Field(..., description="Message content text")


class RenameRequest(BaseModel):
    """Schema for renaming a chat session."""
    title: str = Field(..., max_length=200, description="New title for the session (max 200 characters)")


class RileyConversationResponse(BaseModel):
    id: str
    title: str
    project_id: Optional[str] = None
    last_message: Optional[str] = None
    last_message_at: Optional[str] = None
    created_at: Optional[str] = None


class RileyConversationsListResponse(BaseModel):
    conversations: List[RileyConversationResponse]


class CreateRileyConversationRequest(BaseModel):
    tenant_id: str = Field(..., max_length=50, description="Tenant/client identifier (or 'global')")
    title: Optional[str] = Field(None, max_length=200, description="Optional conversation title")


class RileyConversationMessageSchema(BaseModel):
    role: str = Field(..., description="Message role: 'user' or 'model'")
    content: str = Field(..., description="Message content text")


class RileyConversationMessagesResponse(BaseModel):
    messages: List[RileyConversationMessageSchema]


class CreateRileyConversationMessageRequest(BaseModel):
    tenant_id: str = Field(..., max_length=50, description="Tenant/client identifier (or 'global')")
    role: Literal["user", "model"] = Field(..., description="Message role")
    content: str = Field(..., max_length=2000, description="Message content text")


class RileyProjectResponse(BaseModel):
    id: str
    name: str
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class RileyProjectsListResponse(BaseModel):
    projects: List[RileyProjectResponse]


class CreateRileyProjectRequest(BaseModel):
    tenant_id: str = Field(..., max_length=50, description="Tenant/client identifier (or 'global')")
    name: str = Field(..., max_length=200, description="Project name")


class UpdateRileyProjectRequest(BaseModel):
    name: str = Field(..., max_length=200, description="Project name")


class AssignRileyConversationProjectRequest(BaseModel):
    tenant_id: str = Field(..., max_length=50, description="Tenant/client identifier (or 'global')")
    project_id: Optional[str] = Field(None, description="Project ID or null to unassign")


def embed_query_text(text: str) -> List[float]:
    """Embed the query text using Gemini text embeddings.
    
    Uses the new google-genai SDK via the shared client.
    This function is synchronous and should be run in a threadpool.
    """
    settings = get_settings()
    model_name = settings.EMBEDDING_MODEL
    
    try:
        # Get client (lazy initialization)
        client = get_genai_client()
        # Use the shared client to generate embedding
        response = client.models.embed_content(
            model=model_name,
            contents=text
        )
        
        # Extract embedding vector from response
        if not response.embeddings or len(response.embeddings) == 0:
            raise RuntimeError(
                f"Embedding response did not contain any embeddings for model '{model_name}'."
            )
        
        embedding = response.embeddings[0].values
        if not isinstance(embedding, list):
            raise RuntimeError(
                f"Embedding response did not contain a valid vector for model '{model_name}'."
            )
        
        return embedding
    except RuntimeError:
        # Re-raise RuntimeError as-is
        raise
    except Exception as exc:
        error_msg = (
            f"Query embedding failed using model '{model_name}': {exc}"
        )
        raise RuntimeError(error_msg) from exc


async def _embed_query_text(content: str) -> List[float]:
    """Embed the query text using Gemini text embeddings with retry logic.
    
    Wraps embed_query_text in a threadpool for async execution.
    """
    settings = get_settings()
    model_name = settings.EMBEDDING_MODEL

    try:
        # Run blocking GenAI call in threadpool to avoid blocking event loop
        return await run_in_threadpool(embed_query_text, content)
    except RuntimeError:
        # Re-raise RuntimeError as-is
        raise
    except Exception as exc:
        error_msg = (
            f"Query embedding failed using model '{model_name}': {exc}"
        )
        raise RuntimeError(error_msg) from exc


def _get_text_for_rag(payload: Dict[str, Any], settings: Any) -> str:
    """Extract appropriate text for RAG context from a payload.
    
    STANDARDIZED BEHAVIOR:
    - For images with valid OCR: returns FULL ocr_text (already capped at OCR_MAX_CHARS, not truncated to 1000)
    - For non-images: returns FULL content field (capped at MAX_CHARS_TOTAL=20k during ingestion)
    - Falls back to content_preview only if content is missing (backward compatibility)
    
    For images with OCR, uses OCR text if:
    - ai_enabled == True
    - ocr_status == "complete"
    - ocr_confidence >= OCR_MIN_CONFIDENCE
    
    Args:
        payload: Qdrant point payload dictionary
        settings: Application settings (for OCR_MIN_CONFIDENCE)
        
    Returns:
        Text content to use in RAG context (full text, not truncated to 1000 chars)
    """
    # Check if this is an image with valid OCR
    file_type = payload.get("file_type") or payload.get("type", "")
    is_image = file_type.lower() in ("png", "jpg", "jpeg", "webp", "tiff")
    
    if is_image:
        ai_enabled = payload.get("ai_enabled", False)
        ocr_status = payload.get("ocr_status", "not_requested")
        ocr_confidence = payload.get("ocr_confidence")
        ocr_text = payload.get("ocr_text")
        
        # Use OCR text if all conditions are met (return FULL OCR text, already capped at OCR_MAX_CHARS)
        # DO NOT truncate to 1000 chars here - use the full OCR text for better RAG context
        if (ai_enabled and 
            ocr_status == "complete" and 
            ocr_text and
            (ocr_confidence is None or ocr_confidence >= settings.OCR_MIN_CONFIDENCE)):
            return ocr_text  # Return full OCR text (already capped at OCR_MAX_CHARS=8000)
    
    # Default: use FULL content field (not preview) for RAG
    # content field stores full extracted text (capped at MAX_CHARS_TOTAL=20k during ingestion)
    # content_preview is only for UI display (first 1000 chars)
    # Fall back to content_preview only if content is missing (backward compatibility)
    return payload.get("content") or payload.get("content_preview") or "No preview available."


def _format_rag_context(
    private_results: List[Dict[str, Any]], 
    global_results: List[Dict[str, Any]],
    graph_results: str = "",
    file_manifest: Optional[List[str]] = None
) -> str:
    """Format retrieved documents into a context string for the LLM with citations.
    
    Includes both vector search results, graph search results, and file manifest.
    Uses OCR text for images when appropriate (ai_enabled=True, ocr_status=complete, confidence>=threshold).
    """
    from app.core.config import get_settings
    settings = get_settings()
    
    context_parts: List[str] = []

    # Format File Manifest (Available files in the campaign)
    if file_manifest and len(file_manifest) > 0:
        context_parts.append("### 📂 FILE MANIFEST (Assets in this Campaign)")
        context_parts.append("The following files are available in the secure vault. If the user asks about one, acknowledge it exists:")
        for filename in file_manifest:
            context_parts.append(f"- {filename}")
        context_parts.append("")  # Blank line

    # Format Graph Results (Golden Set - Structured Campaigns)
    if graph_results:
        context_parts.append("### 🕸️ KNOWLEDGE GRAPH HITS")
        context_parts.append(graph_results)
        context_parts.append("")  # Blank line

    # Format Internal Campaign Data (Private Results - Vectors)
    if private_results:
        context_parts.append("### 📄 DOCUMENT ARCHIVE (Campaign-Specific)")
        for result in private_results:
            payload = result.get("payload", {})
            filename = payload.get("filename", "Unknown Document")
            # Use _get_text_for_rag which returns full content (not preview) for RAG context
            # For images: returns full ocr_text if OCR is complete and allowed
            # For non-images: returns full content field (capped at MAX_CHARS_TOTAL)
            rag_text = _get_text_for_rag(payload, settings)

            context_parts.append(
                f"[[Source: {filename}]]\n"
                f"Content: {rag_text}\n"
            )

    # Format Firm-Wide Knowledge (Global Results - Vectors)
    if global_results:
        context_parts.append("\n### 📄 DOCUMENT ARCHIVE (Firm-Wide)")
        for result in global_results:
            payload = result.get("payload", {})
            filename = payload.get("filename", "Unknown Document")
            # Use _get_text_for_rag which returns full content (not preview) for RAG context
            # For images: returns full ocr_text if OCR is complete and allowed
            # For non-images: returns full content field (capped at MAX_CHARS_TOTAL)
            rag_text = _get_text_for_rag(payload, settings)

            context_parts.append(
                f"[[Source: {filename}]]\n"
                f"Content: {rag_text}\n"
            )

    return "\n".join(context_parts)


def _result_location(payload: Dict[str, Any]) -> str:
    location_type = payload.get("location_type")
    location_value = payload.get("location_value")
    if location_type and location_value:
        return f"{location_type}:{location_value}"
    for key in ("page", "page_number", "slide", "slide_number", "section"):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return f"{key}:{value}"
    section_path = payload.get("section_path")
    if section_path:
        return str(section_path)
    chunk_index = payload.get("chunk_index")
    if chunk_index is not None:
        return f"chunk:{chunk_index}"
    return "unknown"


def _result_chunk_id(result: Dict[str, Any]) -> str:
    payload = result.get("payload", {}) or {}
    chunk_id = payload.get("chunk_id")
    if chunk_id:
        return str(chunk_id)
    parent = payload.get("parent_file_id")
    idx = payload.get("chunk_index")
    if parent is not None and idx is not None:
        return f"{parent}::chunk::{idx}"
    return str(result.get("id", ""))


def _build_sources_payload(
    private_results: List[Dict[str, Any]],
    global_results: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    seen: set[str] = set()
    sources: List[Dict[str, str]] = []
    for result in [*private_results, *global_results]:
        payload = result.get("payload", {}) or {}
        cid = _result_chunk_id(result)
        if cid in seen:
            continue
        seen.add(cid)
        filename = str(payload.get("filename") or "Unknown Document")
        sources.append(
            {
                "id": cid,
                "filename": filename,
                "location": _result_location(payload),
            }
        )
    return sources


def _validate_and_sanitize_quotes(
    response_text: str,
    private_results: List[Dict[str, Any]],
    global_results: List[Dict[str, Any]],
) -> str:
    """Ensure quoted spans are exact substrings from retrieved context."""
    settings = get_settings()
    corpus_parts: List[str] = []
    for result in [*private_results, *global_results]:
        payload = result.get("payload", {}) or {}
        raw_chunk = payload.get("raw_text")
        if raw_chunk:
            corpus_parts.append(str(raw_chunk))
        corpus_parts.append(_get_text_for_rag(payload, settings))
    corpus = "\n".join(corpus_parts)
    if not corpus.strip():
        return response_text

    invalid_found = False

    def _replace_invalid(match: Any) -> str:
        nonlocal invalid_found
        opening = match.group(1)
        quoted = match.group(2)
        closing = match.group(3)
        if quoted in corpus:
            return f"{opening}{quoted}{closing}"
        invalid_found = True
        # Keep semantic content but remove hallucinated quote marks.
        return quoted

    sanitized = re.sub(r'(["“])([^"\n”]{3,})(["”])', _replace_invalid, response_text)
    if invalid_found:
        sanitized = (
            f"{sanitized}\n\n"
            "Note: Quote not found in sources; re-run with more context."
        )
    return sanitized


def _get_riley_model_name(*, deep: bool) -> str:
    settings = get_settings()
    return settings.RILEY_DEEP_MODEL if deep else settings.RILEY_MODEL


def _extract_openai_response_text(response_json: Dict[str, Any]) -> str:
    output_text = response_json.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text
    output = response_json.get("output")
    if not isinstance(output, list):
        return ""
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for segment in content:
            if not isinstance(segment, dict):
                continue
            text = segment.get("text")
            if isinstance(text, str) and text.strip():
                return text
    return ""


async def _generate_riley_answer_openai(*, prompt: str, model_name: str, timeout_seconds: int) -> str:
    try:
        import httpx  # type: ignore
    except ImportError as exc:
        raise RuntimeError("httpx dependency is not installed") from exc

    settings = get_settings()
    if not settings.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")

    payload = {
        "model": model_name,
        "input": prompt,
    }
    timeout = httpx.Timeout(timeout_seconds)
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
        text = _extract_openai_response_text(response.json())
        if not text:
            raise RuntimeError("OpenAI response did not contain text output")
        return text


def _candidate_id(result: Dict[str, Any]) -> str:
    payload = result.get("payload", {}) or {}
    chunk_id = payload.get("chunk_id")
    if chunk_id:
        return str(chunk_id)
    parent = payload.get("parent_file_id")
    idx = payload.get("chunk_index")
    if parent is not None and idx is not None:
        return f"{parent}::chunk::{idx}"
    return str(result.get("id", ""))


def _apply_rerank_order(
    private_results: List[Dict[str, Any]],
    global_results: List[Dict[str, Any]],
    ranked_ids: List[str],
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if not ranked_ids:
        return private_results, global_results

    by_id: Dict[str, tuple[str, Dict[str, Any]]] = {}
    for item in private_results:
        by_id[_candidate_id(item)] = ("private", item)
    for item in global_results:
        by_id[_candidate_id(item)] = ("global", item)

    ordered_private: List[Dict[str, Any]] = []
    ordered_global: List[Dict[str, Any]] = []
    used: set[str] = set()
    for candidate_id in ranked_ids:
        if candidate_id in used:
            continue
        entry = by_id.get(candidate_id)
        if not entry:
            continue
        used.add(candidate_id)
        scope, item = entry
        if scope == "private":
            ordered_private.append(item)
        else:
            ordered_global.append(item)

    # Preserve stability for any remaining items not returned by reranker.
    for item in private_results:
        cid = _candidate_id(item)
        if cid not in used:
            ordered_private.append(item)
    for item in global_results:
        cid = _candidate_id(item)
        if cid not in used:
            ordered_global.append(item)

    return ordered_private, ordered_global


@router.post("/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    background_tasks: BackgroundTasks,
    http_request: Request,
    deep: bool = Query(False, description="Enable deep research mode"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: Optional[GraphService] = Depends(get_graph_optional)
) -> ChatResponse:
    """Chat endpoint with RAG and model routing for tenant-isolated queries.
    
    Supports two modes:
    - Global Mode (tenant_id == "global"): Searches ONLY Global Knowledge Base (Tier 1)
    - Campaign Mode (tenant_id != "global"): Searches Client Silo (Tier 2) + Global (Tier 1)
    
    SECURITY: Tenant membership is enforced after parsing the request body.
    """
    # Verify tenant membership (tenant_id comes from request body)
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id, http_request)
    
    # Log user_id + tenant_id for every request
    print(f"Chat request: user_id={user_id}, tenant_id={request.tenant_id}")
    
    settings = get_settings()
    rerank_candidates_limit = max(1, int(settings.RERANK_CANDIDATES))
    rerank_top_k = max(1, int(settings.RERANK_TOP_K))
    target_private_limit = 40 if request.mode == "deep" else 10
    target_global_limit = 20 if request.mode == "deep" else 5
    if deep:
        target_private_limit = max(target_private_limit, 80)
        target_global_limit = max(target_global_limit, 40)
        rerank_candidates_limit = max(rerank_candidates_limit, 100)
        rerank_top_k = max(rerank_top_k, 20)
    if request.tenant_id == "global":
        target_private_limit = 0
        target_global_limit = 40 if request.mode == "deep" else 20
        if deep:
            target_global_limit = max(target_global_limit, 80)

    # Step A: Embed the query
    try:
        query_vector = await _embed_query_text(request.query)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=f"Embedding failed: {exc}") from exc

    # Step B: Parallel Hybrid RAG - Graph + Vector Search
    private_results: List[Dict[str, Any]] = []
    global_results: List[Dict[str, Any]] = []
    graph_results = ""
    file_manifest: List[str] = []  # File manifest for Campaign Mode
    
    if request.tenant_id == "global":
        # Global Mode: Search TIER_1 for Firm Documents (files with is_global=True) + Graph
        # DO NOT search Tier 2 in global mode - Firm Documents live in Tier 1
        try:
            # Parallel execution: Graph and Vector searches
            tasks = []
            if graph:
                tasks.append(graph.search_campaigns_fuzzy(request.query))
            
            # Search Tier 1 with is_global=True filter for Firm Documents
            global_filter = Filter(
                must=[
                    FieldCondition(
                        key="is_global",
                        match=MatchValue(value=True),
                    )
                ],
                must_not=[
                    FieldCondition(
                        key="record_type",
                        match=MatchValue(value="file"),
                    )
                ],
            )

            vector_limit = max(40 if request.mode == "deep" else 20, rerank_candidates_limit) if settings.RERANK_ENABLED else (40 if request.mode == "deep" else 20)
            if settings.HYBRID_SEARCH_ENABLED:
                vector_task = vector_service.hybrid_search_research(
                    collection_name=settings.QDRANT_COLLECTION_TIER_1,
                    query_text=request.query,
                    query_embedding=query_vector,
                    tenant_filter=global_filter,
                    limit=vector_limit,
                )
            else:
                vector_task = vector_service.search_global(
                    collection_name=settings.QDRANT_COLLECTION_TIER_1,
                    query_vector=query_vector,
                    limit=vector_limit,
                    filter=global_filter,  # SECURITY: Always filter by is_global=True
                )
            tasks.append(vector_task)
            
            # Execute in parallel
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Extract results
            if graph and len(results) > 0:
                graph_result = results[0]
                if isinstance(graph_result, str):
                    graph_results = graph_result
                else:
                    graph_results = ""
            else:
                graph_results = ""
            
            vector_result = results[-1] if results else []
            if isinstance(vector_result, list):
                global_results = vector_result
            else:
                global_results = []
        except Exception as exc:
            raise HTTPException(
                status_code=500, detail=f"Global search failed: {exc}"
            ) from exc
    else:
        # Campaign Mode: Search Tier 2 (Client Silo) + Tier 1 (Global) + Graph + File Manifest
        private_limit = target_private_limit
        global_limit = target_global_limit
        if settings.RERANK_ENABLED:
            private_limit = max(private_limit, rerank_candidates_limit)
            global_limit = max(global_limit, rerank_candidates_limit)

        # Fetch File Manifest (list of available files in the campaign)
        file_manifest: List[str] = []
        try:
            file_list = await vector_service.list_tenant_files(
                collection_name=settings.QDRANT_COLLECTION_TIER_2,
                tenant_id=request.tenant_id,
                limit=50,
            )
            # Extract just the filenames
            file_manifest = [file.get("filename", "Unknown") for file in file_list if file.get("filename")]
        except Exception as exc:
            # If file listing fails, continue without manifest
            print(f"Warning: Failed to fetch file manifest: {exc}")
            file_manifest = []

        # Parallel execution: Graph and Vector searches
        tasks = []
        if graph:
            tasks.append(graph.search_campaigns_fuzzy(request.query))
        
        # Campaign Mode: Search Tier 2 with client_id + ai_enabled filters.
        private_filter = Filter(
            must=[
                FieldCondition(
                    key="client_id",
                    match=MatchValue(value=request.tenant_id),
                ),
                FieldCondition(
                    key="ai_enabled",
                    match=MatchValue(value=True),
                ),
            ],
            must_not=[
                FieldCondition(
                    key="record_type",
                    match=MatchValue(value="file"),
                )
            ],
        )
        if settings.HYBRID_SEARCH_ENABLED:
            tasks.append(
                vector_service.hybrid_search_research(
                    collection_name=settings.QDRANT_COLLECTION_TIER_2,
                    query_text=request.query,
                    query_embedding=query_vector,
                    tenant_filter=private_filter,
                    limit=private_limit,
                )
            )
        else:
            tasks.append(vector_service.search_silo(
                collection_name=settings.QDRANT_COLLECTION_TIER_2,
                query_vector=query_vector,
                tenant_id=request.tenant_id,
                limit=private_limit,
                require_ai_enabled=True,  # CRITICAL: Only retrieve files with AI Memory enabled
            ))
        
        # Search Tier 1 with is_global=True filter (never unfiltered)
        global_filter = Filter(
            must=[
                FieldCondition(
                    key="is_global",
                    match=MatchValue(value=True),
                )
            ],
            must_not=[
                FieldCondition(
                    key="record_type",
                    match=MatchValue(value="file"),
                )
            ],
        )
        if settings.HYBRID_SEARCH_ENABLED:
            tasks.append(
                vector_service.hybrid_search_research(
                    collection_name=settings.QDRANT_COLLECTION_TIER_1,
                    query_text=request.query,
                    query_embedding=query_vector,
                    tenant_filter=global_filter,
                    limit=global_limit,
                )
            )
        else:
            tasks.append(vector_service.search_global(
                collection_name=settings.QDRANT_COLLECTION_TIER_1,
                query_vector=query_vector,
                limit=global_limit,
                filter=global_filter,  # SECURITY: Always filter by is_global=True
            ))
        
        # Execute all searches in parallel
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Extract results (handle exceptions gracefully)
            if graph and len(results) > 0:
                graph_result = results[0]
                if isinstance(graph_result, str):
                    graph_results = graph_result
                else:
                    graph_results = ""
            else:
                graph_results = ""
            
            private_result = results[1] if len(results) > 1 else []
            if isinstance(private_result, list):
                private_results = private_result
            else:
                private_results = []
            
            global_result = results[2] if len(results) > 2 else []
            if isinstance(global_result, list):
                global_results = global_result
            else:
                global_results = []
        except Exception as exc:
            # If parallel execution fails, try sequential as fallback
            if graph:
                try:
                    graph_results = await graph.search_campaigns_fuzzy(request.query)
                except:
                    graph_results = ""
            try:
                private_filter = Filter(
                    must=[
                        FieldCondition(
                            key="client_id",
                            match=MatchValue(value=request.tenant_id),
                        ),
                        FieldCondition(
                            key="ai_enabled",
                            match=MatchValue(value=True),
                        ),
                    ],
                    must_not=[
                        FieldCondition(
                            key="record_type",
                            match=MatchValue(value="file"),
                        )
                    ],
                )
                if settings.HYBRID_SEARCH_ENABLED:
                    private_results = await vector_service.hybrid_search_research(
                        collection_name=settings.QDRANT_COLLECTION_TIER_2,
                        query_text=request.query,
                        query_embedding=query_vector,
                        tenant_filter=private_filter,
                        limit=private_limit,
                    )
                else:
                    private_results = await vector_service.search_silo(
                        collection_name=settings.QDRANT_COLLECTION_TIER_2,
                        query_vector=query_vector,
                        tenant_id=request.tenant_id,
                        limit=private_limit,
                        require_ai_enabled=True,  # CRITICAL: Only retrieve files with AI Memory enabled
                    )
            except:
                private_results = []
            try:
                global_filter = Filter(
                    must=[
                        FieldCondition(
                            key="is_global",
                            match=MatchValue(value=True),
                        )
                    ],
                    must_not=[
                        FieldCondition(
                            key="record_type",
                            match=MatchValue(value="file"),
                        )
                    ],
                )
                if settings.HYBRID_SEARCH_ENABLED:
                    global_results = await vector_service.hybrid_search_research(
                        collection_name=settings.QDRANT_COLLECTION_TIER_1,
                        query_text=request.query,
                        query_embedding=query_vector,
                        tenant_filter=global_filter,
                        limit=global_limit,
                    )
                else:
                    global_results = await vector_service.search_global(
                        collection_name=settings.QDRANT_COLLECTION_TIER_1,
                        query_vector=query_vector,
                        limit=global_limit,
                        filter=global_filter,  # SECURITY: Always filter by is_global=True
                    )
            except:
                global_results = []

    # Step B2: Optional LLM reranking over hybrid candidates.
    if settings.RERANK_ENABLED:
        combined_candidates = private_results + global_results
        ranked_ids = await rerank_candidates(
            query=request.query,
            candidates=combined_candidates,
            top_k=rerank_top_k,
        )
        if ranked_ids:
            private_results, global_results = _apply_rerank_order(
                private_results=private_results,
                global_results=global_results,
                ranked_ids=ranked_ids,
            )
            private_results = private_results[:rerank_top_k]
            remaining = max(0, rerank_top_k - len(private_results))
            global_results = global_results[:remaining]
        else:
            # Fallback to existing hybrid order if reranking fails.
            private_results = private_results[:target_private_limit]
            global_results = global_results[:target_global_limit]
    else:
        # Keep standard retrieval caps when reranking is disabled.
        private_results = private_results[:target_private_limit]
        global_results = global_results[:target_global_limit]

    # Step C: Fetch chat history if session_id provided
    chat_history: List[Dict[str, str]] = []
    if request.session_id and graph:
        try:
            # Use authenticated user identity for scope enforcement.
            # Parsing from session_id is unreliable for IDs containing underscores (e.g., Clerk IDs).
            user_id = current_user.get("id", "unknown")
            chat_history = await graph.get_chat_history(
                session_id=request.session_id,
                tenant_id=request.tenant_id,
                user_id=user_id,
                limit=6
            )
        except Exception as exc:
            chat_history = []

    # Step D: Format context (includes Graph + Vector results + File Manifest)
    has_context = (
        (private_results and len(private_results) > 0) or
        (global_results and len(global_results) > 0) or
        (graph_results and len(graph_results) > 0) or
        (file_manifest and len(file_manifest) > 0)
    )
    context = _format_rag_context(private_results, global_results, graph_results, file_manifest)
    total_sources = len(private_results) + len(global_results) + (1 if graph_results else 0)
    source_items = _build_sources_payload(private_results, global_results)

    # Step E: Build system prompt with persona injection and formatting rules
    normal_mode_instruction = """NORMAL MODE INSTRUCTION:
- Answer directly and get to the point.
- Use retrieved evidence efficiently; prioritize the highest-signal facts.
- Stay strategic, but do not be excessively long unless the user asks for deeper detail.
- Give a clear recommendation with rationale and concrete next steps."""
    deep_mode_instruction = """DEEP RESEARCH MODE INSTRUCTION:
- Think broadly across campaign and global sources before concluding.
- Compare sources explicitly and call out contradictions, gaps, and recurring patterns.
- Surface strategic implications, second-order effects, and risk/opportunity tradeoffs.
- Ask clarifying questions when key constraints or decision criteria are missing.
- Produce a thorough, memo-style response when appropriate."""
    mode_instruction = deep_mode_instruction if deep else normal_mode_instruction

    # Get persona context (CRITICAL for Board Demo)
    persona_context = get_persona_context()

    empty_context_instruction = ""
    if not has_context:
        empty_context_instruction = """
SOURCE LIMITATION: Retrieved campaign/global evidence is limited for this turn.
Be transparent about that limitation, avoid unsupported certainty, and provide provisional strategy guidance.
Ask focused clarifying questions to help retrieve or validate what is missing.
"""

    # Get user display name for personalization (fallback to "there" if not provided)
    user_display_name = request.user_display_name or "there"
    
    system_prompt = f"""You are Riley, a senior campaign strategist and decision partner at RALLY.

PERSONALITY AND LEADERSHIP STANDARD:
- Be brilliant, kind, firm, and practical.
- Be supportive and empathetic, but never sycophantic.
- Do not optimize for pleasing the user.
- Be direct when ideas are weak, unsupported, risky, or strategically misaligned.
- Challenge weak assumptions and contradictions clearly and respectfully.

STRATEGIC OPERATING MODE:
- Do not just summarize documents. Synthesize them into strategic direction.
- Proactively surface implications for narrative, messaging, audience/persona fit, positioning, opposition framing, risks, and opportunities.
- Detect patterns, gaps, conflicts, and missing evidence quickly.
- Ask clarifying questions whenever strategy is ambiguous or key constraints are missing.
- Do not pretend certainty when evidence is incomplete.

SOURCE HIERARCHY AND GROUNDING:
- Use campaign materials first.
- Use global knowledge base as secondary context when relevant.
- Distinguish clearly between:
  1) what the documents explicitly say,
  2) your inference/analysis,
  3) your strategic recommendation.
- Ground factual claims in retrieved sources when applicable using `[[Source: Filename.ext]]`.
- If evidence is limited or absent, state that plainly and continue with provisional guidance.
- If asked for direct quotes, only provide exact source-grounded quotes. Never invent or paraphrase as a quote.

STRATEGIC PERSONA RECOGNITION:
Refer to these Strategic Archetypes: {persona_context}
If the user references a persona (for example, Passive Patty or Skeptical Sam), tailor analysis and recommendations to their motivations, barriers, and message requirements.

RESPONSE STYLE:
- Executive-caliber, intelligent, and actionable.
- Concise by default; expand into a detailed strategic memo when depth is requested.
- Confident but not arrogant. Never gushy. Never flattering without substance.
- Write naturally, not robotically.

RESPONSE STRUCTURE:
- Start with a direct answer.
- Then include clear sections:
  - `### Evidence from Sources`
  - `### Strategic Analysis`
  - `### Recommendation`
- When uncertainty is material, add:
  - `### Clarifying Questions`
- Use bullet points for lists, tradeoffs, and action steps.

FILE MANIFEST AWARENESS:
- If a file appears in FILE MANIFEST but details were not retrieved, say you can see the file and ask for a narrower retrieval target.
- Never claim a listed file does not exist.

{empty_context_instruction}
{mode_instruction}

Context Data:
{context if has_context else "[No specific data found in files yet]"}
"""

    # Format chat history
    history_context = ""
    if chat_history:
        history_context = "\n\n=== Previous Conversation ===\n"
        for msg in chat_history:
            role_label = "User" if msg["role"] == "user" else "Riley"
            history_context += f"{role_label}: {msg['content']}\n"
    
    system_prompt += history_context
    system_prompt += f"\nUser Question: {request.query}"

    # Step F: Generate response using Riley primary provider (OpenAI).
    model_name = _get_riley_model_name(deep=deep)
    provider = (settings.RILEY_PROVIDER or "openai").strip().lower()

    try:
        if provider != "openai":
            raise RuntimeError(f"Unsupported Riley provider: {provider}")
        response_text = await _generate_riley_answer_openai(
            prompt=system_prompt,
            model_name=model_name,
            timeout_seconds=max(5, int(settings.RILEY_TIMEOUT_SECONDS)),
        )
        response_text = _validate_and_sanitize_quotes(response_text, private_results, global_results)
    except Exception as exc:
        logger.warning(
            "riley_answer_generation_failed provider=%s model=%s tenant=%s error_type=%s",
            provider,
            model_name,
            request.tenant_id,
            type(exc).__name__,
        )
        raise HTTPException(
            status_code=503,
            detail="Riley is temporarily unavailable. Please try again in a moment.",
        ) from exc

    # Step G: Save messages to Neo4j using background tasks (non-blocking)
    if request.session_id and graph:
        # Use authenticated user identity for scope enforcement.
        user_id = current_user.get("id", "unknown")
        
        background_tasks.add_task(
            graph.save_message,
            request.session_id,
            "user",
            request.query,
            request.tenant_id,
            user_id,
        )
        background_tasks.add_task(
            graph.save_message,
            request.session_id,
            "model",
            response_text,
            request.tenant_id,
            user_id,
        )

    return ChatResponse(
        response=response_text,
        model_used=model_name,
        sources_count=total_sources,
        sources=source_items,
    )


@router.get("/riley/index-summary", response_model=RileyIndexSummaryResponse)
async def get_riley_index_summary(
    http_request: Request,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
) -> RileyIndexSummaryResponse:
    """Campaign/global document indexing summary used for Riley trust visibility."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)

    settings = get_settings()
    collection_name = (
        settings.QDRANT_COLLECTION_TIER_1
        if tenant_id == "global"
        else settings.QDRANT_COLLECTION_TIER_2
    )
    summary = await vector_service.get_index_summary(
        collection_name=collection_name,
        tenant_id=tenant_id,
    )
    return RileyIndexSummaryResponse(**summary)


@router.get("/riley/conversations", response_model=RileyConversationsListResponse)
async def list_riley_conversations(
    http_request: Request,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyConversationsListResponse:
    """List persisted Riley conversations for the current user and tenant."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)

    conversations = await graph.list_riley_conversations(
        tenant_id=tenant_id,
        user_id=user_id,
        limit=50,
    )
    return RileyConversationsListResponse(
        conversations=[RileyConversationResponse(**conv) for conv in conversations]
    )


@router.post("/riley/conversations", response_model=RileyConversationResponse)
async def create_riley_conversation(
    http_request: Request,
    request: CreateRileyConversationRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyConversationResponse:
    """Create a new persisted Riley conversation shell."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id, http_request)

    conversation = await graph.create_riley_conversation(
        tenant_id=request.tenant_id,
        user_id=user_id,
        title=request.title,
    )
    return RileyConversationResponse(
        id=conversation["id"],
        title=conversation.get("title") or "New Conversation",
        project_id=conversation.get("project_id"),
        created_at=conversation.get("created_at"),
    )


@router.get("/riley/conversations/{conversation_id}/messages", response_model=RileyConversationMessagesResponse)
async def get_riley_conversation_messages(
    http_request: Request,
    conversation_id: str,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyConversationMessagesResponse:
    """Get messages for a persisted Riley conversation."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)

    messages = await graph.get_riley_conversation_messages(
        session_id=conversation_id,
        tenant_id=tenant_id,
        user_id=user_id,
        limit=100,
    )
    return RileyConversationMessagesResponse(
        messages=[RileyConversationMessageSchema(**msg) for msg in messages]
    )


@router.post("/riley/conversations/{conversation_id}/messages")
async def add_riley_conversation_message(
    http_request: Request,
    conversation_id: str,
    request: CreateRileyConversationMessageRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, str]:
    """Append a message to a persisted Riley conversation."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id, http_request)

    await graph.append_riley_conversation_message(
        session_id=conversation_id,
        tenant_id=request.tenant_id,
        user_id=user_id,
        role=request.role,
        content=request.content,
    )
    return {"status": "ok"}


@router.get("/riley/projects", response_model=RileyProjectsListResponse)
async def list_riley_projects(
    http_request: Request,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyProjectsListResponse:
    """List Riley projects for the current user and tenant scope."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)

    projects = await graph.list_riley_projects(
        tenant_id=tenant_id,
        user_id=user_id,
        limit=100,
    )
    return RileyProjectsListResponse(projects=[RileyProjectResponse(**project) for project in projects])


@router.post("/riley/projects", response_model=RileyProjectResponse)
async def create_riley_project(
    http_request: Request,
    request: CreateRileyProjectRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyProjectResponse:
    """Create a Riley project for the current user and tenant scope."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id, http_request)

    project = await graph.create_riley_project(
        tenant_id=request.tenant_id,
        user_id=user_id,
        name=request.name,
    )
    return RileyProjectResponse(**project)


@router.patch("/riley/projects/{project_id}", response_model=RileyProjectResponse)
async def update_riley_project(
    http_request: Request,
    project_id: str,
    request: UpdateRileyProjectRequest,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyProjectResponse:
    """Rename a Riley project in scope."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)

    project = await graph.update_riley_project(
        project_id=project_id,
        tenant_id=tenant_id,
        user_id=user_id,
        name=request.name,
    )
    return RileyProjectResponse(**project)


@router.delete("/riley/projects/{project_id}")
async def delete_riley_project(
    http_request: Request,
    project_id: str,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, str]:
    """Delete a Riley project and unassign its conversations."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)

    await graph.delete_riley_project(
        project_id=project_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    return {"status": "ok"}


@router.patch("/riley/conversations/{conversation_id}/project")
async def assign_riley_conversation_project(
    http_request: Request,
    conversation_id: str,
    request: AssignRileyConversationProjectRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Optional[str]]:
    """Assign or clear the project for a Riley conversation."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id, http_request)

    updated = await graph.assign_riley_conversation_project(
        session_id=conversation_id,
        tenant_id=request.tenant_id,
        user_id=user_id,
        project_id=request.project_id,
    )
    return {"id": updated["id"], "project_id": updated.get("project_id")}


@router.delete("/riley/conversations/{conversation_id}")
async def delete_riley_conversation(
    http_request: Request,
    conversation_id: str,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, str]:
    """Delete a Riley conversation and its persisted messages."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)

    await graph.delete_riley_conversation(
        session_id=conversation_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    return {"status": "ok"}


@router.patch("/riley/conversations/{conversation_id}", response_model=RileyConversationResponse)
async def rename_riley_conversation(
    http_request: Request,
    conversation_id: str,
    request: RenameRequest,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyConversationResponse:
    """Rename a persisted Riley conversation."""
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)

    updated_title = await graph.update_session_title(
        session_id=conversation_id,
        tenant_id=tenant_id,
        user_id=user_id,
        title=request.title,
    )
    return RileyConversationResponse(
        id=conversation_id,
        title=updated_title,
    )


@router.get("/chat/history/{session_id}", response_model=List[MessageSchema])
async def get_chat_session_history(
    session_id: str,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    graph: GraphService = Depends(get_graph)
) -> List[MessageSchema]:
    """Retrieve chat history for a specific session.
    
    SECURITY: Requires tenant_id to enforce scope isolation.
    Only returns messages for the specified tenant and user.
    """
    try:
        # Extract user_id from session_id (format: session_{tenantId}_{userId}_{timestamp})
        user_id = _extract_user_id_from_session(session_id)
        history = await graph.get_chat_history(
            session_id=session_id,
            tenant_id=tenant_id,
            user_id=user_id,
            limit=10
        )
        return [MessageSchema(**msg) for msg in history]
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to get chat history: {exc}"
        ) from exc


@router.delete("/chat/history/{session_id}")
async def clear_chat_history(
    session_id: str,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    graph: GraphService = Depends(get_graph)
) -> Dict[str, str]:
    """Clear chat history for a session.
    
    Deletes all messages and the session node from Neo4j.
    This endpoint matches the frontend path: DELETE /api/v1/chat/history/{session_id}
    
    SECURITY: Requires tenant_id to enforce scope isolation.
    Only deletes sessions for the specified tenant and user.
    """
    try:
        # Extract user_id from session_id (format: session_{tenantId}_{userId}_{timestamp})
        user_id = _extract_user_id_from_session(session_id)
        await graph.clear_chat_history(
            session_id=session_id,
            tenant_id=tenant_id,
            user_id=user_id
        )
        return {
            "status": "success",
            "message": f"Chat history cleared for session {session_id}"
        }
    except Exception as exc:
        print(f"Error clearing chat history: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to clear chat history: {str(exc)}"
        ) from exc


@router.patch("/sessions/{session_id}")
async def rename_session(
    session_id: str,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    request: RenameRequest = ...,
    graph: GraphService = Depends(get_graph)
) -> Dict[str, str]:
    """Rename a chat session.
    
    Updates the title of an existing chat session in Neo4j.
    
    SECURITY: Requires tenant_id to enforce scope isolation.
    Only updates sessions for the specified tenant and user.
    
    Args:
        session_id: Unique identifier for the chat session
        tenant_id: Tenant/client identifier for scope isolation
        request: RenameRequest containing the new title
        
    Returns:
        Dictionary with status and updated title
    """
    try:
        # Extract user_id from session_id (format: session_{tenantId}_{userId}_{timestamp})
        user_id = _extract_user_id_from_session(session_id)
        updated_title = await graph.update_session_title(
            session_id=session_id,
            tenant_id=tenant_id,
            user_id=user_id,
            title=request.title
        )
        return {
            "status": "success",
            "title": updated_title
        }
    except ValueError as exc:
        raise HTTPException(
            status_code=404, detail=str(exc)
        ) from exc
    except Exception as exc:
        print(f"Error renaming session {session_id}: {exc}")
        raise HTTPException(
            status_code=500, detail=f"Failed to rename session: {str(exc)}"
        ) from exc
