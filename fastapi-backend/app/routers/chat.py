import asyncio
import json
import logging
import re
import time
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
from app.services.provider_fallback import (
    classify_gemini_generation_failure,
    classify_openai_generation_failure,
    generate_text_with_gemini_with_usage,
)
from app.services.qdrant import vector_service
from app.services.rerank import rerank_candidates_with_metrics
from app.services.analytics_contract import infer_provider_from_model
from app.services.pricing_registry import estimate_text_generation_cost
from app.services.token_utils import estimate_tokens

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
    # Accept legacy "normal" mode from older frontend builds and treat it as "fast".
    mode: Literal["fast", "deep", "normal"] = "fast"
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
    ocr_processed_count: int
    vision_processed_count: int
    partial_count: int
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


def _safe_json_loads(raw: Any, default: Any) -> Any:
    if not isinstance(raw, str) or not raw.strip():
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


async def _load_parent_document_intelligence(
    *,
    collection_name: str,
    results: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    parent_ids: List[str] = []
    seen: set[str] = set()
    for result in results:
        payload = result.get("payload", {}) or {}
        parent_id = str(payload.get("parent_file_id") or "").strip()
        if not parent_id or parent_id in seen:
            continue
        seen.add(parent_id)
        parent_ids.append(parent_id)
    if not parent_ids:
        return []
    try:
        parent_points = await vector_service.client.retrieve(
            collection_name=collection_name,
            ids=parent_ids,
            with_payload=True,
            with_vectors=False,
        )
    except Exception:
        return []
    docs: List[Dict[str, Any]] = []
    for point in parent_points:
        payload = point.payload or {}
        if str(payload.get("analysis_status") or "").lower() != "complete":
            continue
        docs.append(payload)
    return docs


def _build_doc_intel_context_block(doc_intel_items: List[Dict[str, Any]]) -> str:
    if not doc_intel_items:
        return ""
    lines: List[str] = ["### 🧠 SYNTHESIZED DOCUMENT INTELLIGENCE"]
    for item in doc_intel_items[:10]:
        filename = str(item.get("filename") or "Unknown")
        short_summary = str(item.get("doc_summary_short") or "").strip()
        themes = item.get("key_themes") or []
        tones = item.get("tone_labels") or []
        framings = item.get("framing_labels") or []
        opportunities = item.get("strategic_opportunities") or []
        risks = item.get("persuasion_risks") or []
        fidelity = str(item.get("analysis_fidelity_level") or "unknown").strip()
        exec_mode = str(item.get("analysis_execution_mode") or "unknown").strip()
        chunks_cov = item.get("analysis_chunks_coverage_ratio")
        chars_cov = item.get("analysis_chars_coverage_ratio")
        bands_total = int(item.get("analysis_total_bands") or 0)
        bands_analyzed = int(item.get("analysis_analyzed_bands") or 0)
        band_cov = item.get("analysis_band_coverage_ratio")
        validation_status = str(item.get("analysis_validation_status") or "").strip()
        validation_note = str(item.get("analysis_validation_note") or "").strip()
        contradiction_count = int(item.get("analysis_contradiction_count") or 0)
        failed_bands = int(item.get("analysis_failed_bands_count") or 0)
        high_signal_cov = item.get("analysis_high_signal_band_coverage_ratio")
        appendix_required = bool(item.get("analysis_appendix_required"))
        appendix_covered = bool(item.get("analysis_appendix_covered"))
        lines.append(f"[[Document Intelligence: {filename}]]")
        lines.append(f"- fidelity: {fidelity}")
        lines.append(f"- execution_mode: {exec_mode}")
        if bands_total > 0:
            if band_cov is not None:
                lines.append(
                    f"- band_coverage: {bands_analyzed}/{bands_total} ({float(band_cov or 0.0):.2%})"
                )
            else:
                lines.append(f"- band_coverage: {bands_analyzed}/{bands_total}")
        if chunks_cov is not None or chars_cov is not None:
            lines.append(
                f"- coverage: chunks={float(chunks_cov or 0.0):.2%}, chars={float(chars_cov or 0.0):.2%}"
            )
        if validation_status:
            lines.append(f"- validation_status: {validation_status}")
        if validation_note:
            lines.append(f"- validation_note: {validation_note}")
        if contradiction_count > 0:
            lines.append(f"- intra_document_tensions: {contradiction_count}")
        if failed_bands > 0:
            lines.append(f"- failed_bands: {failed_bands}")
        if high_signal_cov is not None:
            lines.append(f"- high_signal_coverage: {float(high_signal_cov or 0.0):.2%}")
        if appendix_required:
            lines.append(f"- appendix_coverage: {'covered' if appendix_covered else 'missing'}")
        if short_summary:
            lines.append(f"- summary: {short_summary}")
        if themes:
            lines.append(f"- themes: {', '.join(str(x) for x in themes[:5])}")
        if tones:
            lines.append(f"- tone: {', '.join(str(x) for x in tones[:5])}")
        if framings:
            lines.append(f"- framing: {', '.join(str(x) for x in framings[:5])}")
        if opportunities:
            lines.append(f"- opportunities: {' | '.join(str(x) for x in opportunities[:3])}")
        if risks:
            lines.append(f"- risks: {' | '.join(str(x) for x in risks[:3])}")
        lines.append("")
    return "\n".join(lines).strip()


def _build_campaign_intel_context_block(snapshot: Optional[Dict[str, Any]]) -> str:
    if not snapshot:
        return ""
    dominant_narratives = list(snapshot.get("dominant_narratives") or [])[:8]
    opportunities = list(snapshot.get("strategic_opportunities") or [])[:8]
    risks = list(snapshot.get("strategic_risks") or [])[:8]
    contradictions = _safe_json_loads(snapshot.get("contradiction_tensions_json"), [])
    sentiment_distribution = _safe_json_loads(snapshot.get("sentiment_distribution_json"), {})
    tone_distribution = _safe_json_loads(snapshot.get("tone_distribution_json"), {})
    framing_distribution = _safe_json_loads(snapshot.get("framing_distribution_json"), {})
    lines: List[str] = ["### 🌐 CAMPAIGN-WIDE INTELLIGENCE"]
    lines.append(f"- snapshot_version: {snapshot.get('version')}")
    docs_total = int(snapshot.get("docs_total") or 0)
    docs_analyzed = int(snapshot.get("docs_analyzed") or 0)
    docs_failed = int(snapshot.get("docs_failed") or 0)
    coverage_ratio = float(snapshot.get("doc_intel_coverage_ratio") or 0.0)
    completeness_status = str(snapshot.get("input_completeness_status") or "").strip().lower()
    completeness_note = str(snapshot.get("input_completeness_note") or "").strip()
    quality_status = str(snapshot.get("input_quality_status") or "").strip().lower()
    quality_note = str(snapshot.get("input_quality_note") or "").strip()
    degraded_docs = int(snapshot.get("doc_intel_degraded_docs") or 0)
    full_fidelity_docs = int(snapshot.get("doc_intel_full_fidelity_docs") or 0)
    if docs_total > 0:
        lines.append(
            f"- input_coverage: analyzed {docs_analyzed}/{docs_total} indexed docs "
            f"(coverage={coverage_ratio:.2%}, missing={docs_failed})"
        )
    if completeness_status:
        lines.append(f"- input_completeness_status: {completeness_status}")
    if completeness_note:
        lines.append(f"- input_completeness_note: {completeness_note}")
    if docs_analyzed > 0:
        lines.append(
            f"- input_fidelity_mix: full_fidelity_docs={full_fidelity_docs}, degraded_docs={degraded_docs}"
        )
    if quality_status:
        lines.append(f"- input_quality_status: {quality_status}")
    if quality_note:
        lines.append(f"- input_quality_note: {quality_note}")
    if completeness_status in {"partial", "none"}:
        lines.append(
            "- caution: campaign intelligence is based on incomplete document-intelligence inputs; "
            "treat strategic synthesis as provisional."
        )
    if quality_status in {"mixed_fidelity", "degraded_fidelity"}:
        lines.append(
            "- caution: some document-intelligence inputs were reduced-context; avoid false precision and "
            "validate high-impact claims against raw evidence."
        )
    if dominant_narratives:
        lines.append(f"- dominant_narratives: {', '.join(str(x) for x in dominant_narratives)}")
    if opportunities:
        lines.append(f"- strategic_opportunities: {' | '.join(str(x) for x in opportunities[:5])}")
    if risks:
        lines.append(f"- strategic_risks: {' | '.join(str(x) for x in risks[:5])}")
    if isinstance(sentiment_distribution, dict) and sentiment_distribution:
        lines.append(f"- sentiment_distribution: {sentiment_distribution}")
    if isinstance(tone_distribution, dict) and tone_distribution:
        lines.append(f"- tone_distribution: {tone_distribution}")
    if isinstance(framing_distribution, dict) and framing_distribution:
        lines.append(f"- framing_distribution: {framing_distribution}")
    if isinstance(contradictions, list) and contradictions:
        summaries = [
            str(item.get("contradiction_summary") or "").strip()
            for item in contradictions[:4]
            if isinstance(item, dict) and str(item.get("contradiction_summary") or "").strip()
        ]
        if summaries:
            lines.append(f"- contradiction_tensions: {' | '.join(summaries)}")
    return "\n".join(lines).strip()


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
        logger.debug("chat_quote_sanitization_removed_invalid_quotes")
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


def _extract_openai_usage(response_json: Dict[str, Any]) -> Dict[str, int]:
    usage = response_json.get("usage")
    if not isinstance(usage, dict):
        return {}
    prompt_tokens = usage.get("prompt_tokens")
    completion_tokens = usage.get("completion_tokens")
    input_tokens = usage.get("input_tokens")
    output_tokens = usage.get("output_tokens")
    normalized_input = input_tokens if isinstance(input_tokens, (int, float)) else prompt_tokens
    normalized_output = output_tokens if isinstance(output_tokens, (int, float)) else completion_tokens
    total_tokens = usage.get("total_tokens")
    parsed: Dict[str, int] = {}
    if isinstance(prompt_tokens, (int, float)):
        parsed["prompt_tokens"] = int(prompt_tokens)
    if isinstance(completion_tokens, (int, float)):
        parsed["completion_tokens"] = int(completion_tokens)
    if isinstance(normalized_input, (int, float)):
        parsed["input_tokens"] = int(normalized_input)
    if isinstance(normalized_output, (int, float)):
        parsed["output_tokens"] = int(normalized_output)
    if isinstance(total_tokens, (int, float)):
        parsed["total_tokens"] = int(total_tokens)
    return parsed


async def _generate_riley_answer_openai(
    *, prompt: str, model_name: str, timeout_seconds: int
) -> tuple[str, Dict[str, int]]:
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
        response_payload = response.json()
        text = _extract_openai_response_text(response_payload)
        if not text:
            raise RuntimeError("OpenAI response did not contain text output")
        return text, _extract_openai_usage(response_payload)


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


def _has_exact_usage(usage: Dict[str, Any]) -> bool:
    return isinstance(usage.get("input_tokens"), int) and isinstance(usage.get("output_tokens"), int)


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
    normalized_mode: Literal["fast", "deep"] = "deep" if request.mode == "deep" else "fast"
    rerank_candidates_limit = max(1, int(settings.RERANK_CANDIDATES))
    rerank_top_k = max(1, int(settings.RERANK_TOP_K))
    target_private_limit = 40 if normalized_mode == "deep" else 10
    target_global_limit = 20 if normalized_mode == "deep" else 5
    if deep:
        target_private_limit = max(target_private_limit, 80)
        target_global_limit = max(target_global_limit, 40)
        rerank_candidates_limit = max(rerank_candidates_limit, 100)
        rerank_top_k = max(rerank_top_k, 20)
    if request.tenant_id == "global":
        target_private_limit = 0
        target_global_limit = 40 if normalized_mode == "deep" else 20
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

            vector_limit = max(40 if normalized_mode == "deep" else 20, rerank_candidates_limit) if settings.RERANK_ENABLED else (40 if normalized_mode == "deep" else 20)
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
        rerank_result = await rerank_candidates_with_metrics(
            query=request.query,
            candidates=combined_candidates,
            top_k=rerank_top_k,
        )
        ranked_ids = rerank_result.get("ranked_ids")
        if graph:
            try:
                user_id = current_user.get("id", "unknown")
                await graph.append_analytics_event(
                    event_id=f"rerank:{request.tenant_id}:{request.session_id or 'ad-hoc'}:{int(time.time() * 1000)}",
                    source_event_type_raw="rerank_completed"
                    if rerank_result.get("status") == "succeeded"
                    else "rerank_failed",
                    source_entity="Reranker",
                    campaign_id=request.tenant_id,
                    user_id=user_id,
                    actor_user_id=user_id,
                    object_id=request.session_id,
                    status="succeeded"
                    if rerank_result.get("status") == "succeeded"
                    else "failed",
                    provider=str(rerank_result.get("provider") or ""),
                    model=str(rerank_result.get("model") or ""),
                    latency_ms=int(rerank_result.get("latency_ms") or 0),
                    cost_estimate_usd=(
                        float(rerank_result.get("cost_estimate_usd"))
                        if rerank_result.get("cost_estimate_usd") is not None
                        else None
                    ),
                    pricing_version=str(rerank_result.get("pricing_version") or "cost-accounting-v1"),
                    cost_confidence=str(rerank_result.get("cost_confidence") or "proxy_only"),
                    metadata={
                        "service": "rerank",
                        "input_tokens": int(rerank_result.get("input_tokens") or rerank_result.get("estimated_input_tokens") or 0),
                        "output_tokens": int(rerank_result.get("output_tokens") or 0),
                        "total_tokens": int(
                            rerank_result.get("total_tokens") or rerank_result.get("estimated_input_tokens") or 0
                        ),
                        "estimated_input_tokens": int(rerank_result.get("estimated_input_tokens") or 0),
                        "candidate_count_in": int(rerank_result.get("candidate_count_in") or 0),
                        "candidate_count_sent": int(rerank_result.get("candidate_count_sent") or 0),
                        "candidate_count_out": int(rerank_result.get("candidate_count_out") or 0),
                        "requests_count": 1,
                        "error_type": rerank_result.get("error_type"),
                    },
                )
            except Exception:
                pass
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

    # Step D: Format context (includes Graph + Vector results + File Manifest + intelligence artifacts)
    has_context = (
        (private_results and len(private_results) > 0) or
        (global_results and len(global_results) > 0) or
        (graph_results and len(graph_results) > 0) or
        (file_manifest and len(file_manifest) > 0)
    )
    context = _format_rag_context(private_results, global_results, graph_results, file_manifest)
    intelligence_blocks: List[str] = []
    try:
        private_doc_intel = await _load_parent_document_intelligence(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            results=private_results,
        )
    except Exception:
        private_doc_intel = []
    try:
        global_doc_intel = await _load_parent_document_intelligence(
            collection_name=settings.QDRANT_COLLECTION_TIER_1,
            results=global_results,
        )
    except Exception:
        global_doc_intel = []
    all_doc_intel = [*private_doc_intel, *global_doc_intel]
    doc_intel_block = _build_doc_intel_context_block(all_doc_intel)
    if doc_intel_block:
        intelligence_blocks.append(doc_intel_block)

    campaign_snapshot: Optional[Dict[str, Any]] = None
    if graph:
        try:
            campaign_snapshot = await graph.get_latest_riley_campaign_intelligence_snapshot(
                tenant_id=request.tenant_id
            )
        except Exception:
            campaign_snapshot = None
    campaign_block = _build_campaign_intel_context_block(campaign_snapshot)
    if campaign_block:
        intelligence_blocks.append(campaign_block)

    if intelligence_blocks:
        context = f"{context}\n\n" + "\n\n".join(intelligence_blocks) if context else "\n\n".join(intelligence_blocks)
        has_context = True
    total_sources = len(private_results) + len(global_results) + (1 if graph_results else 0)
    source_items = _build_sources_payload(private_results, global_results)

    # Step E: Build system prompt with persona injection and formatting rules
    normal_mode_instruction = """NORMAL MODE INSTRUCTION:
- Answer like a trusted strategic advisor in a real conversation.
- Be clear and direct without sounding formal or memo-like by default.
- Keep it concise when the ask is simple.
- Use retrieved evidence efficiently; prioritize the highest-signal facts.
- Use document/campaign intelligence artifacts when available to speed synthesis, but verify with source evidence.
- Give practical next steps when helpful, but do not force a heavy structure unless the task needs it."""
    deep_mode_instruction = """DEEP RESEARCH MODE INSTRUCTION:
- Think broadly across campaign and global sources before concluding.
- Use campaign intelligence artifacts by default and reconcile them with raw evidence.
- Use document intelligence summaries to accelerate synthesis, then validate with chunk evidence.
- Compare sources explicitly and call out contradictions, gaps, and recurring patterns.
- Surface strategic implications, second-order effects, and risk/opportunity tradeoffs.
- Ask clarifying questions when key constraints or decision criteria are missing.
- Be thorough and structured when needed, but stay grounded and human rather than overly polished or corporate."""
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
- Be warm, sharp, grounded, and practical.
- Be supportive and empathetic, but never sycophantic or flattering without substance.
- Prioritize truth and strategic usefulness over agreeableness.
- Be direct without being harsh.
- Challenge weak assumptions, contradictions, blind spots, and risks clearly and respectfully.
- Be willing to say no or redirect when the user is off-track.
- Sound intelligent without sounding pompous.
- Stay calm and human; never robotic.

STRATEGIC OPERATING MODE:
- Do not just summarize documents. Synthesize them into strategic direction.
- Proactively surface implications for narrative, messaging, audience/persona fit, positioning, opposition framing, risks, and opportunities.
- Detect patterns, gaps, conflicts, and missing evidence quickly.
- Ask clarifying questions whenever strategy is ambiguous or key constraints are missing.
- Do not pretend certainty when evidence is incomplete.
- Do not manufacture confidence when evidence is weak.

SOURCE HIERARCHY AND GROUNDING:
- Use campaign materials first.
- Use global knowledge base as secondary context when relevant.
- Use synthesized document intelligence and campaign intelligence as an analysis layer, not a substitute for evidence.
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
- Conversational, clear, and advisor-like.
- Concise by default; expand structure only when the task actually needs it.
- Confident but not arrogant; warm but not gushy.
- Avoid defaulting to management-consultant or formal memo voice unless explicitly asked for a memo/brief.
- Write naturally, not robotically.

### RESPONSE CONTROL RULES
1) If the user asks a simple question:
   - respond simply
   - do NOT default to structured sections
   - do NOT expand unnecessarily
2) Do NOT default to memo or report-style writing unless explicitly requested.
3) If the user asks for ideas or quick input:
   - respond conversationally
   - avoid headers unless needed
   - avoid over-structuring
4) Only use structured sections when:
   - the task is explicitly analytical
   - or the user asks for a report, memo, or breakdown
5) If the user's request is weak, unclear, or strategically flawed:
   - point it out directly
   - explain why
   - redirect to a stronger approach
6) Do not agree just to be agreeable.
   - prioritize correctness and usefulness over tone
7) Keep answers proportional:
   - short question -> short answer
   - deep question -> structured answer
8) Avoid corporate or consulting tone unless explicitly requested.
   - default tone should feel like a sharp, practical advisor, not a formal memo

RESPONSE STRUCTURE:
- Start with a direct answer.
- Do NOT default to formal headers for simple or conversational asks.
- Use structured sections only when the task explicitly calls for analytical structure
  (for example: analysis, breakdown, report, memo, or brief).
- If structured sections are warranted, use:
  - `### Evidence from Sources`
  - `### Synthesized Intelligence`
  - `### Strategic Recommendation`
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

    # Step F: Generate response using Riley primary provider (Gemini) with OpenAI fallback.
    use_deep_model = bool(deep or normalized_mode == "deep")
    primary_model = _get_riley_model_name(deep=use_deep_model)
    fallback_model = settings.RILEY_OPENAI_FALLBACK_MODEL
    response_text: Optional[str] = None
    last_error: Optional[Exception] = None
    generation_model_used = primary_model
    generation_started = time.perf_counter()
    generation_usage: Dict[str, int] = {}
    generation_usage_is_exact = False
    try:
        response_text, gemini_usage = await generate_text_with_gemini_with_usage(
            prompt=system_prompt,
            model_name=primary_model,
            timeout_seconds=max(5, int(settings.RILEY_TIMEOUT_SECONDS)),
        )
        gemini_usage_exact = _has_exact_usage(gemini_usage)
        generation_usage = {
            "input_tokens": int(gemini_usage.get("input_tokens") or estimate_tokens(system_prompt)),
            "output_tokens": int(gemini_usage.get("output_tokens") or estimate_tokens(response_text)),
            "total_tokens": int(
                gemini_usage.get("total_tokens")
                or (
                    int(gemini_usage.get("input_tokens") or estimate_tokens(system_prompt))
                    + int(gemini_usage.get("output_tokens") or estimate_tokens(response_text))
                )
            ),
        }
        if gemini_usage_exact and "prompt_tokens" not in generation_usage:
            generation_usage["prompt_tokens"] = int(generation_usage["input_tokens"])
            generation_usage["completion_tokens"] = int(generation_usage["output_tokens"])
        generation_usage_is_exact = gemini_usage_exact
        generation_model_used = primary_model
    except Exception as exc:
        last_error = exc
        failure = classify_gemini_generation_failure(exc)
        logger.warning(
            "gemini_generation_failed subsystem=%s tenant_id=%s primary_provider=%s fallback_provider=%s primary_model=%s fallback_model=%s error_type=%s http_status=%s provider_error_code=%s provider_error_type=%s fallback_eligible=%s",
            "chat",
            request.tenant_id,
            "gemini",
            "openai",
            primary_model,
            fallback_model,
            failure.error_type,
            failure.http_status,
            failure.provider_error_code,
            failure.provider_error_type,
            failure.fallback_eligible,
        )
        if failure.fallback_eligible:
            logger.warning(
                "gemini_generation_fallback_to_openai subsystem=%s tenant_id=%s primary_provider=%s fallback_provider=%s primary_model=%s fallback_model=%s error_type=%s http_status=%s provider_error_code=%s provider_error_type=%s fallback_eligible=%s",
                "chat",
                request.tenant_id,
                "gemini",
                "openai",
                primary_model,
                fallback_model,
                failure.error_type,
                failure.http_status,
                failure.provider_error_code,
                failure.provider_error_type,
                True,
            )
            try:
                response_text, openai_usage = await _generate_riley_answer_openai(
                    prompt=system_prompt,
                    model_name=fallback_model,
                    timeout_seconds=max(5, int(settings.RILEY_TIMEOUT_SECONDS)),
                )
                generation_usage = {
                    "input_tokens": int(openai_usage.get("input_tokens") or estimate_tokens(system_prompt)),
                    "output_tokens": int(openai_usage.get("output_tokens") or estimate_tokens(response_text)),
                    "total_tokens": int(
                        openai_usage.get("total_tokens")
                        or (
                            int(openai_usage.get("input_tokens") or estimate_tokens(system_prompt))
                            + int(openai_usage.get("output_tokens") or estimate_tokens(response_text))
                        )
                    ),
                }
                if isinstance(openai_usage.get("prompt_tokens"), int):
                    generation_usage["prompt_tokens"] = int(openai_usage["prompt_tokens"])
                if isinstance(openai_usage.get("completion_tokens"), int):
                    generation_usage["completion_tokens"] = int(openai_usage["completion_tokens"])
                generation_usage_is_exact = _has_exact_usage(openai_usage)
                generation_model_used = fallback_model
                logger.info(
                    "provider_fallback_succeeded subsystem=%s tenant_id=%s primary_provider=%s fallback_provider=%s primary_model=%s fallback_model=%s",
                    "chat",
                    request.tenant_id,
                    "gemini",
                    "openai",
                    primary_model,
                    fallback_model,
                )
            except Exception as fallback_exc:
                last_error = fallback_exc
                fallback_failure = classify_openai_generation_failure(fallback_exc)
                logger.warning(
                    "provider_fallback_failed subsystem=%s tenant_id=%s primary_provider=%s fallback_provider=%s primary_model=%s fallback_model=%s error_type=%s http_status=%s provider_error_code=%s provider_error_type=%s",
                    "chat",
                    request.tenant_id,
                    "gemini",
                    "openai",
                    primary_model,
                    fallback_model,
                    fallback_failure.error_type,
                    fallback_failure.http_status,
                    fallback_failure.provider_error_code,
                    fallback_failure.provider_error_type,
                )

    if response_text is None:
        logger.warning(
            "riley_answer_generation_failed provider=%s model=%s tenant=%s error_type=%s",
            "gemini",
            primary_model,
            request.tenant_id,
            type(last_error).__name__ if last_error else "UnknownError",
        )
        raise HTTPException(
            status_code=503,
            detail="Riley is temporarily unavailable. Please try again in a moment.",
        ) from last_error

    if not generation_usage:
        input_tokens = estimate_tokens(system_prompt)
        output_tokens = estimate_tokens(response_text)
        generation_usage = {
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
        }
    generation_latency_ms = int((time.perf_counter() - generation_started) * 1000)
    generation_provider = infer_provider_from_model(generation_model_used) or "unknown"
    cost_meta = estimate_text_generation_cost(
        service="chat_generation",
        provider=generation_provider,
        model=generation_model_used,
        input_tokens=int(generation_usage.get("input_tokens") or 0),
        output_tokens=int(generation_usage.get("output_tokens") or 0),
        usage_is_exact=generation_usage_is_exact,
    )
    if graph:
        try:
            user_id = current_user.get("id", "unknown")
            await graph.append_analytics_event(
                event_id=f"chat_generation:{request.session_id or 'ad-hoc'}:{int(time.time() * 1000)}",
                source_event_type_raw="chat_generation_completed",
                source_entity="ChatGeneration",
                campaign_id=request.tenant_id,
                user_id=user_id,
                actor_user_id=user_id,
                object_id=request.session_id,
                status="succeeded",
                provider=generation_provider,
                model=generation_model_used,
                latency_ms=generation_latency_ms,
                cost_estimate_usd=(
                    float(cost_meta["cost_estimate_usd"])
                    if cost_meta.get("cost_estimate_usd") is not None
                    else None
                ),
                pricing_version=str(cost_meta.get("pricing_version") or "cost-accounting-v1"),
                cost_confidence=str(cost_meta.get("cost_confidence") or "proxy_only"),
                metadata={
                    "service": "chat_generation",
                    "input_tokens": int(generation_usage.get("input_tokens") or 0),
                    "output_tokens": int(generation_usage.get("output_tokens") or 0),
                    "total_tokens": int(generation_usage.get("total_tokens") or 0),
                    "prompt_tokens": int(generation_usage.get("prompt_tokens") or generation_usage.get("input_tokens") or 0),
                    "completion_tokens": int(
                        generation_usage.get("completion_tokens") or generation_usage.get("output_tokens") or 0
                    ),
                    "requests_count": 1,
                    "used_fallback_model": generation_model_used == fallback_model,
                },
            )
        except Exception:
            pass

    response_text = _validate_and_sanitize_quotes(response_text, private_results, global_results)

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
        model_used=generation_model_used,
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
    http_request: Request,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> List[MessageSchema]:
    """Retrieve chat history for a specific session.
    
    SECURITY: Requires tenant_id to enforce scope isolation.
    Only returns messages for the specified tenant and user.
    """
    try:
        user_id = current_user.get("id", "unknown")
        await check_tenant_membership(user_id, tenant_id, http_request)
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
    http_request: Request,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, str]:
    """Clear chat history for a session.
    
    Deletes all messages and the session node from Neo4j.
    This endpoint matches the frontend path: DELETE /api/v1/chat/history/{session_id}
    
    SECURITY: Requires tenant_id to enforce scope isolation.
    Only deletes sessions for the specified tenant and user.
    """
    try:
        user_id = current_user.get("id", "unknown")
        await check_tenant_membership(user_id, tenant_id, http_request)
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
    http_request: Request,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    request: RenameRequest = ...,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
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
        user_id = current_user.get("id", "unknown")
        await check_tenant_membership(user_id, tenant_id, http_request)
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
