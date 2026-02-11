import asyncio
from typing import Any, Dict, List, Literal, Optional

import google.generativeai as genai  # Still needed for GenerativeModel (chat responses)
from typing import Dict
from fastapi import APIRouter, BackgroundTasks, HTTPException, Query, Depends, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from qdrant_client.http.models import Filter, FieldCondition, MatchValue
from tenacity import retry, stop_after_attempt, wait_exponential

from app.core.config import get_settings
from app.core.personas import get_persona_context  # CRITICAL: Preserve for Board Demo
from app.dependencies.auth import verify_clerk_token, check_tenant_membership
from app.dependencies.graph_dep import get_graph, get_graph_optional
from app.services.genai_client import get_genai_client
from app.services.graph import GraphService
from app.services.qdrant import vector_service


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
    mode: Literal["fast", "deep"] = "fast"  # 'fast' = Gemini 2.5 Flash, 'deep' = Gemini 2.5 Pro
    session_id: Optional[str] = Field(None, description="Optional session ID for chat memory")
    user_display_name: Optional[str] = Field(None, description="User's display name for personalization (username, firstName, or email)")


class ChatResponse(BaseModel):
    response: str
    model_used: str
    sources_count: int


class MessageSchema(BaseModel):
    """Schema for a chat message in history."""
    role: str = Field(..., description="Message role: 'user' or 'model'")
    content: str = Field(..., description="Message content text")


class RenameRequest(BaseModel):
    """Schema for renaming a chat session."""
    title: str = Field(..., max_length=200, description="New title for the session (max 200 characters)")


def _get_embedding_model() -> None:
    """Configure the Google Generative AI client with the current API key.
    
    NOTE: This is still needed for GenerativeModel (chat responses).
    Embeddings now use the shared genai_client.
    """
    settings = get_settings()
    if not settings.GOOGLE_API_KEY:
        raise RuntimeError("GOOGLE_API_KEY is not configured.")

    genai.configure(api_key=settings.GOOGLE_API_KEY)


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


def _get_model_name(mode: Literal["fast", "deep"]) -> str:
    """Return the Gemini 2.5 model name based on the mode."""
    if mode == "fast":
        return "gemini-2.5-flash"
    return "gemini-2.5-pro"


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def _generate_content_with_retry(model: genai.GenerativeModel, prompt: str) -> Any:
    """Synchronous wrapper for model.generate_content with retry logic."""
    return model.generate_content(prompt)


@router.post("/chat", response_model=ChatResponse)
async def chat(
    request: ChatRequest,
    background_tasks: BackgroundTasks,
    http_request: Request,
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
                ]
            )
            
            vector_task = vector_service.search_global(
                collection_name=settings.QDRANT_COLLECTION_TIER_1,
                query_vector=query_vector,
                limit=40 if request.mode == "deep" else 20,
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
        private_limit = 40 if request.mode == "deep" else 10
        global_limit = 20 if request.mode == "deep" else 5

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
        
        # Campaign Mode: Search Tier 2 with client_id filter AND ai_enabled=true
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
            ]
        )
        
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
                    ]
                )
                global_results = await vector_service.search_global(
                    collection_name=settings.QDRANT_COLLECTION_TIER_1,
                    query_vector=query_vector,
                    limit=global_limit,
                    filter=global_filter,  # SECURITY: Always filter by is_global=True
                )
            except:
                global_results = []

    # Step C: Fetch chat history if session_id provided
    chat_history: List[Dict[str, str]] = []
    if request.session_id and graph:
        try:
            # Extract user_id from session_id (format: session_{tenantId}_{userId}_{timestamp})
            user_id = _extract_user_id_from_session(request.session_id)
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

    # Step E: Build system prompt with persona injection and formatting rules
    mode_instruction = (
        "Provide comprehensive strategic analysis with deep insights and connections."
    ) if request.mode == "deep" else (
        "Provide a concise, actionable summary."
    )

    # Get persona context (CRITICAL for Board Demo)
    persona_context = get_persona_context()

    empty_context_instruction = ""
    if not has_context:
        empty_context_instruction = """
IMPORTANT: The context below is empty. Do NOT say "I don't know" or "I don't have information."
Instead, say: "I'm looking through the files, but I don't see that specific detail yet. However, based on standard RALLY strategy, we usually..." 
Provide helpful guidance based on campaign strategy best practices, even if the specific data isn't in the files.
"""

    # Get user display name for personalization (fallback to "there" if not provided)
    user_display_name = request.user_display_name or "there"
    
    system_prompt = f"""You are Riley, a Senior Strategist at RALLY.
You speak in a **High-Bandwidth, Low-Density** style.

YOUR ROLE:
You work alongside {user_display_name} in a Campaign Workspace setting. You're warm, professional, confident, and proactive. You think strategically and connect dots that others might miss.

YOUR TONE:
Confident, collaborative, and punchy. Use bolding to highlight **key metrics** or **names**.
- Use "We" instead of "You" (e.g., "We should consider..." not "You should consider...")
- Speak like a trusted colleague who's been in the trenches
- Get straight to the point—no fluff

STRATEGIC PERSONA RECOGNITION (CRITICAL):
Refer to this list of Strategic Archetypes: {persona_context}
Treat them as VIP audiences. If the user mentions a Persona (like "Passive Patty" or "Skeptical Sam"), analyze their specific psychographics, motivations, and messaging needs. These personas represent distinct audience segments with unique characteristics.

CITATION PROTOCOL:
- **ALWAYS cite your sources explicitly** using the format: `[[Source: Filename.pdf]]`
- When you reference information from the context, include the source citation inline
- Example: "According to the Q3 report [[Source: Q3_Report_2024.pdf]], we saw a 15% increase..."
- **If the answer is NOT in the context provided, explicitly admit it**: "I don't see that specific information in the available documents. However, based on standard RALLY strategy..."

FORMATTING RULES (NON-NEGOTIABLE - SCANNABLE OUTPUT):
1. **SHORT HEADINGS WITH EMOJIS:** Always use `###` (h3) for section headers with required emojis. Keep header text concise (3-5 words max).
   - Examples: `### 🧠 Strategic Context`, `### 🔎 Key Insights`, `### 🚀 Next Moves`
2. **BULLET LISTS REQUIRED:** Key Insights and Next Moves MUST be formatted as bullet lists, not paragraphs:
   - Correct: `### 🔎 Key Insights\n- **Insight 1:** Description.\n- **Insight 2:** Description.`
   - Incorrect: `### Key Insights\nParagraph format with multiple sentences...`
3. **AGGRESSIVE SPACING:** You MUST put a blank line between every single paragraph, list item, and section.
4. **REAL LISTS:** When listing items (like attributes, steps, or insights), you MUST use Markdown bullet points:
   - Correct: `- **Attribute Name:** The description.`
   - Incorrect: `Attribute Name: The description.`
5. **INDENTATION:** Never write a list item without the `- ` prefix.
6. **AVOID DENSE PARAGRAPHS:** Keep paragraphs to 1-2 sentences max. Use bullet lists instead of long paragraphs unless the user specifically asks for detailed prose.
7. **STRUCTURE RULE:** Use Markdown Headers (###) for sections. ALWAYS put TWO blank lines between sections.

RESPONSE TEMPLATE (MANDATORY FORMAT):
EVERY response MUST follow this exact structure. No exceptions.

Start with a direct answer (1-2 sentences max).

Then ALWAYS include these three sections in this exact order:

### 🧠 Strategic Context
1-2 sentences explaining why this matters and connections to campaign goals.

### 🔎 Key Insights
MUST be formatted as a bullet list:
- **Insight 1:** Description with citation `[[Source: ...]]` if applicable.
- **Insight 2:** Description with citation `[[Source: ...]]` if applicable.
- **Insight 3:** Description with citation `[[Source: ...]]` if applicable.

If no documents were retrieved, include ONE bullet: `- No documents retrieved from the archive for this query.`

### 🚀 Next Moves
MUST be formatted as a bullet list:
- **Action 1:** Specific actionable recommendation.
- **Action 2:** Specific actionable recommendation.
- **Action 3:** Specific actionable recommendation.

CRITICAL FORMATTING RULES:
- Use EXACTLY these emojis: 🧠 for Strategic Context, 🔎 for Key Insights, 🚀 for Next Moves
- Put a blank line (`\n\n`) between EVERY section
- Strategic Context: 1-2 sentences only (no dense paragraphs)
- Key Insights and Next Moves: ALWAYS bullet lists, never paragraphs
- If you have no insights, still include the section with: `- No specific insights available.`

PLACEHOLDER TOKEN PROHIBITION:
- **NEVER output placeholder tokens** like `[User Name]`, `[Filename]`, or `[[Source: ...]]` unless you are actually citing a real source from the context.
- Only use `[[Source: Filename.pdf]]` when you are referencing information that was actually retrieved from the Document Archive.
- If no sources were used, do not include citation placeholders.

DATA SOURCES (TWO MEMORY BANKS):
You have access to two memory banks:

1. **🕸️ KNOWLEDGE GRAPH (The Golden Set):** Structured campaign data from Neo4j. This contains campaign names, descriptions, and relationships. Use this to identify project names and campaign structures.

2. **📄 DOCUMENT ARCHIVE (The Archive):** Raw documents from vector search. This contains detailed content, messaging, research, and tactical information. Use this for specific details, quotes, and evidence.

**CRITICAL INSTRUCTION:** 
- If the Graph is empty, rely entirely on the Archive.
- If the Graph has results, use it to identify campaign names, then use the Archive for details.
- Always cite sources from the Archive using `[[Source: Filename.pdf]]`.

**FILE MANIFEST AWARENESS:**
- If the user asks about a file listed in the **FILE MANIFEST**, acknowledge that you see it.
- If the specific content wasn't retrieved in the **DOCUMENT ARCHIVE** section, tell the user: "I see [Filename] in the vault. Ask me a specific question about its contents so I can retrieve the details."
- Never claim a file doesn't exist if it's in the FILE MANIFEST.

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

    # Step F: Generate response (Non-blocking with threadpool)
    model_name = _get_model_name(request.mode)
    _get_embedding_model()  # Ensure API key is configured

    try:
        model = genai.GenerativeModel(model_name)
        # Run blocking GenAI call in threadpool with retry logic to avoid blocking event loop
        response = await run_in_threadpool(
            _generate_content_with_retry,
            model,
            system_prompt,
        )
        
        # Extract text from response
        response_text = ""
        if hasattr(response, "text"):
            response_text = response.text
        elif hasattr(response, "candidates") and response.candidates:
            candidate = response.candidates[0]
            if hasattr(candidate, "content") and hasattr(candidate.content, "parts"):
                response_text = " ".join(
                    part.text for part in candidate.content.parts if hasattr(part, "text")
                )
        
        if not response_text:
            raise RuntimeError("Failed to extract text from model response.")

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Model generation failed ({model_name}): {exc}",
        ) from exc

    # Step G: Save messages to Neo4j using background tasks (non-blocking)
    if request.session_id and graph:
        # Extract user_id from session_id (format: session_{tenantId}_{userId}_{timestamp})
        user_id = _extract_user_id_from_session(request.session_id)
        
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
