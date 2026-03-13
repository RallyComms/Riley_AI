import asyncio
import json
import logging
import re
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi.concurrency import run_in_threadpool
from google.api_core.exceptions import AlreadyExists
from google.cloud import tasks_v2
from qdrant_client.http.models import FieldCondition, Filter, MatchValue

from app.core.config import get_settings
from app.services.graph import GraphService
from app.services.qdrant import vector_service

logger = logging.getLogger(__name__)


def _build_worker_payload(
    *,
    job_id: str,
    file_id: str,
    collection_name: str,
    tenant_id: str,
    is_global: bool,
) -> Dict[str, Any]:
    return {
        "job_id": job_id,
        "file_id": file_id,
        "collection_name": collection_name,
        "tenant_id": tenant_id,
        "is_global": bool(is_global),
    }


async def _enqueue_doc_intel_cloud_task(payload: Dict[str, Any]) -> None:
    settings = get_settings()
    if not settings.RILEY_DOC_INTEL_USE_CLOUD_TASKS:
        return
    if not settings.GCP_PROJECT_ID or not settings.RILEY_DOC_INTEL_WORKER_URL:
        raise RuntimeError(
            "Doc intelligence Cloud Tasks is enabled but GCP_PROJECT_ID/RILEY_DOC_INTEL_WORKER_URL is missing"
        )

    def _create_task_sync() -> None:
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(
            settings.GCP_PROJECT_ID,
            settings.RILEY_DOC_INTEL_TASKS_LOCATION,
            settings.RILEY_DOC_INTEL_TASKS_QUEUE,
        )
        task_name = f"{parent}/tasks/{payload['job_id']}"
        headers = {"Content-Type": "application/json"}
        if settings.RILEY_DOC_INTEL_WORKER_TOKEN:
            headers["X-Riley-Doc-Intel-Worker-Token"] = settings.RILEY_DOC_INTEL_WORKER_TOKEN
        http_request: Dict[str, Any] = {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": settings.RILEY_DOC_INTEL_WORKER_URL,
            "headers": headers,
            "body": json.dumps(payload).encode("utf-8"),
        }
        if settings.RILEY_DOC_INTEL_TASKS_SERVICE_ACCOUNT_EMAIL:
            http_request["oidc_token"] = {
                "service_account_email": settings.RILEY_DOC_INTEL_TASKS_SERVICE_ACCOUNT_EMAIL,
                "audience": settings.RILEY_DOC_INTEL_WORKER_URL,
            }
        try:
            client.create_task(request={"parent": parent, "task": {"name": task_name, "http_request": http_request}})
        except AlreadyExists:
            return

    await run_in_threadpool(_create_task_sync)


def _extract_openai_output_text(response_json: Dict[str, Any]) -> str:
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


def _extract_json_object(text: str) -> Dict[str, Any]:
    raw = (text or "").strip()
    if not raw:
        raise RuntimeError("Document intelligence model returned empty output")
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if not match:
        raise RuntimeError("Document intelligence output did not include a JSON object")
    parsed = json.loads(match.group(0))
    if not isinstance(parsed, dict):
        raise RuntimeError("Document intelligence JSON output shape was invalid")
    return parsed


def _normalize_string_list(value: Any, *, max_items: int = 20, max_len: int = 280) -> List[str]:
    if not isinstance(value, list):
        return []
    cleaned: List[str] = []
    for item in value:
        text = str(item or "").strip()
        if not text:
            continue
        cleaned.append(text[:max_len])
        if len(cleaned) >= max_items:
            break
    return cleaned


def _normalize_analysis(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "doc_summary_short": str(raw.get("doc_summary_short") or "").strip()[:1200],
        "doc_summary_long": str(raw.get("doc_summary_long") or "").strip()[:6000],
        "key_themes": _normalize_string_list(raw.get("key_themes"), max_items=20, max_len=180),
        "key_entities": _normalize_string_list(raw.get("key_entities"), max_items=30, max_len=180),
        "sentiment_overall": str(raw.get("sentiment_overall") or "").strip()[:80],
        "tone_labels": _normalize_string_list(raw.get("tone_labels"), max_items=16, max_len=80),
        "framing_labels": _normalize_string_list(raw.get("framing_labels"), max_items=18, max_len=90),
        "audience_implications": _normalize_string_list(raw.get("audience_implications"), max_items=16, max_len=280),
        "persuasion_risks": _normalize_string_list(raw.get("persuasion_risks"), max_items=16, max_len=280),
        "strategic_opportunities": _normalize_string_list(raw.get("strategic_opportunities"), max_items=16, max_len=280),
        "tone_profile": str(raw.get("tone_profile") or "").strip()[:2000],
        "framing_profile": str(raw.get("framing_profile") or "").strip()[:2000],
        "strategic_notes": str(raw.get("strategic_notes") or "").strip()[:6000],
        "major_claims_or_evidence": _normalize_string_list(
            raw.get("major_claims_or_evidence"),
            max_items=25,
            max_len=380,
        ),
    }


def _build_doc_intel_prompt(
    *,
    filename: str,
    file_type: str,
    context_text: str,
) -> str:
    return f"""You are Riley's document intelligence pipeline.
Analyze the provided indexed document content and produce structured intelligence.
Use only information from the provided source material. Do not fabricate details.
If uncertain, be explicit.
Keep analysis grounded to document evidence and rhetorical signals from the text itself.
Do not rely on prior political assumptions not present in the source.

Document:
- filename: {filename}
- file_type: {file_type}

Return valid JSON with exactly these keys:
{{
  "doc_summary_short": "2-4 sentence summary",
  "doc_summary_long": "detailed paragraph or short memo",
  "key_themes": ["theme1", "theme2"],
  "key_entities": ["actor1", "actor2"],
  "sentiment_overall": "positive|mixed|negative|neutral",
  "tone_labels": ["urgency", "attack", "optimism", "pessimism", "institutional", "populist", "fear", "trust", "competence", "coalition", "opposition"],
  "framing_labels": ["economic_anxiety", "law_and_order", "anti_elite", "stability", "change", "values", "threat", "credibility"],
  "audience_implications": ["how key audiences may interpret this framing"],
  "persuasion_risks": ["ways this language could backfire or limit persuasion"],
  "strategic_opportunities": ["practical campaign opportunities implied by the rhetoric"],
  "tone_profile": "tone analysis",
  "framing_profile": "framing analysis",
  "strategic_notes": "strategic implications and opportunities/risks",
  "major_claims_or_evidence": ["claim/evidence 1", "claim/evidence 2"]
}}

Source material begins:
{context_text}
"""


async def _call_openai_document_intel(
    *,
    prompt: str,
    model_name: str,
    timeout_seconds: int,
) -> Dict[str, Any]:
    settings = get_settings()
    if not settings.OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY is not configured")
    try:
        import httpx  # type: ignore
    except Exception as exc:
        raise RuntimeError("httpx dependency is not installed") from exc

    schema: Dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "doc_summary_short",
            "doc_summary_long",
            "key_themes",
            "key_entities",
            "sentiment_overall",
            "tone_labels",
            "framing_labels",
            "audience_implications",
            "persuasion_risks",
            "strategic_opportunities",
            "tone_profile",
            "framing_profile",
            "strategic_notes",
            "major_claims_or_evidence",
        ],
        "properties": {
            "doc_summary_short": {"type": "string"},
            "doc_summary_long": {"type": "string"},
            "key_themes": {"type": "array", "items": {"type": "string"}},
            "key_entities": {"type": "array", "items": {"type": "string"}},
            "sentiment_overall": {"type": "string"},
            "tone_labels": {"type": "array", "items": {"type": "string"}},
            "framing_labels": {"type": "array", "items": {"type": "string"}},
            "audience_implications": {"type": "array", "items": {"type": "string"}},
            "persuasion_risks": {"type": "array", "items": {"type": "string"}},
            "strategic_opportunities": {"type": "array", "items": {"type": "string"}},
            "tone_profile": {"type": "string"},
            "framing_profile": {"type": "string"},
            "strategic_notes": {"type": "string"},
            "major_claims_or_evidence": {"type": "array", "items": {"type": "string"}},
        },
    }
    payload = {
        "model": model_name,
        "input": prompt,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "document_intelligence",
                "strict": True,
                "schema": schema,
            }
        },
    }
    timeout = httpx.Timeout(max(10, int(timeout_seconds)))
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
        raw_text = _extract_openai_output_text(response.json())
    return _extract_json_object(raw_text)


async def _load_document_context(
    *,
    collection_name: str,
    file_id: str,
    max_chunks_override: Optional[int] = None,
    max_chars_override: Optional[int] = None,
) -> Tuple[Dict[str, Any], str, int, int, Dict[str, Any]]:
    settings = get_settings()
    parent_points = await vector_service.client.retrieve(
        collection_name=collection_name,
        ids=[file_id],
        with_payload=True,
        with_vectors=False,
    )
    if not parent_points:
        raise RuntimeError(f"Parent file record not found for file_id={file_id}")
    parent_payload = parent_points[0].payload or {}

    chunk_filter = Filter(
        must=[
            FieldCondition(key="parent_file_id", match=MatchValue(value=file_id)),
            FieldCondition(key="record_type", match=MatchValue(value="chunk")),
        ]
    )
    chunks: List[Any] = []
    offset = None
    while True:
        try:
            scroll_result = await vector_service.client.scroll(
                collection_name=collection_name,
                scroll_filter=chunk_filter,
                limit=200,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            batch = scroll_result[0]
        except Exception:
            fallback_filter = Filter(
                must=[FieldCondition(key="parent_file_id", match=MatchValue(value=file_id))]
            )
            scroll_result = await vector_service.client.scroll(
                collection_name=collection_name,
                scroll_filter=fallback_filter,
                limit=200,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            batch = [
                point
                for point in scroll_result[0]
                if (point.payload or {}).get("record_type") == "chunk"
            ]
        if not batch:
            break
        chunks.extend(batch)
        offset = scroll_result[1]
        if offset is None:
            break

    chunk_payloads = [point.payload or {} for point in chunks]
    chunk_payloads.sort(
        key=lambda payload: (
            0 if str(payload.get("chunk_type") or "") == "macro" else 1,
            int(payload.get("chunk_index") or 0),
        )
    )

    max_chunks = max(10, int(max_chunks_override or settings.RILEY_DOC_INTEL_MAX_CHUNKS))
    max_chars = max(4000, int(max_chars_override or settings.RILEY_DOC_INTEL_MAX_CONTEXT_CHARS))

    candidate_blocks: List[Dict[str, Any]] = []
    total_chars_available = 0
    ocr_available = False
    vision_available = False
    for payload in chunk_payloads:
        text = str(payload.get("content") or payload.get("text") or "").strip()
        if not text:
            continue
        chunk_type = str(payload.get("chunk_type") or "chunk")
        chunk_index = str(payload.get("chunk_index") or "")
        location_type = str(payload.get("location_type") or "location")
        location_value = str(payload.get("location_value") or "").strip()
        section_path = str(payload.get("section_path") or "").strip()
        meta_bits = [f"{chunk_type}#{chunk_index}"]
        if location_value:
            meta_bits.append(f"{location_type}:{location_value}")
        if section_path:
            meta_bits.append(f"section:{section_path}")
        block = f"[{' | '.join(meta_bits)}]\n{text}"
        lower_text = text.lower()
        has_ocr = bool(payload.get("ocr_text_present")) or "[ocr]" in lower_text
        has_vision = bool(payload.get("has_visual_content")) or bool(payload.get("vision_caption")) or "[vision]" in lower_text
        if has_ocr:
            ocr_available = True
        if has_vision:
            vision_available = True
        candidate_blocks.append(
            {
                "payload": payload,
                "block": block,
                "length": len(block),
                "chunk_type": chunk_type,
                "has_ocr": has_ocr,
                "has_vision": has_vision,
                "text": text,
            }
        )
        total_chars_available += len(block)

    total_chunks_available = len(candidate_blocks)
    if not candidate_blocks:
        fallback_text = str(
            parent_payload.get("cleaned_content")
            or parent_payload.get("content")
            or parent_payload.get("raw_content")
            or ""
        ).strip()
        if not fallback_text:
            raise RuntimeError("Indexed document had no usable content for analysis")
        clipped = fallback_text[:max_chars]
        stats = {
            "total_chunks_available": 0,
            "chunks_analyzed": 1,
            "total_chars_available": len(fallback_text),
            "chars_analyzed": len(clipped),
            "chunks_coverage_ratio": 1.0,
            "chars_coverage_ratio": round(len(clipped) / max(1, len(fallback_text)), 4),
            "selection_strategy": "fallback_parent_content",
            "selection_applied": True,
            "ocr_content_available": False,
            "vision_content_available": False,
            "ocr_content_included": False,
            "vision_content_included": False,
        }
        return parent_payload, f"[document]\n{clipped}", 1, len(clipped), stats

    def _signal_score(item: Dict[str, Any], idx: int, total: int) -> float:
        payload = item["payload"]
        text = str(item.get("text") or "")
        lower = text.lower()
        score = 0.0
        if item.get("chunk_type") == "macro":
            score += 1.5
        if item.get("has_ocr"):
            score += 1.0
        if item.get("has_vision"):
            score += 1.0
        if bool(payload.get("major_claims_or_evidence")):
            score += 1.0
        if any(token in lower for token in ["recommend", "risk", "opportunit", "strategy", "framing", "tone", "narrative"]):
            score += 1.2
        if re.search(r"\b\d+(?:\.\d+)?%?\b", text):
            score += 0.8
        if any(token in lower for token in ["table", "chart", "graph", "poll", "budget", "turnout"]):
            score += 0.9
        # Lightly prefer distributed anchors (beginning/middle/end) for full-document coverage.
        if total > 1:
            rel = idx / max(1, total - 1)
            score += 0.25 if (rel < 0.18 or 0.42 <= rel <= 0.58 or rel > 0.82) else 0.0
        return score

    # Start from full-order inclusion; only switch to evidence-preserving selection if needed.
    selected_indices: List[int] = []
    chars_used = 0
    for idx, item in enumerate(candidate_blocks):
        if len(selected_indices) >= max_chunks:
            break
        length = int(item.get("length") or 0)
        if chars_used + length > max_chars:
            break
        selected_indices.append(idx)
        chars_used += length

    selection_strategy = "full_order"
    selection_applied = len(selected_indices) < total_chunks_available

    if selection_applied:
        selection_strategy = "evidence_preserving_selection"
        # Preserve broad coverage first: beginning/middle/end + quartiles.
        anchor_positions = [0, 0.25, 0.5, 0.75, 1.0]
        anchor_indices = {
            min(total_chunks_available - 1, max(0, int(round((total_chunks_available - 1) * pos))))
            for pos in anchor_positions
        }
        scored_items: List[Tuple[float, int]] = []
        for idx, item in enumerate(candidate_blocks):
            scored_items.append((_signal_score(item, idx, total_chunks_available), idx))
            if item.get("has_ocr") or item.get("has_vision"):
                anchor_indices.add(idx)

        chosen: set[int] = set()
        chosen_chars = 0
        # First, add anchors in natural order.
        for idx in sorted(anchor_indices):
            if len(chosen) >= max_chunks:
                break
            length = int(candidate_blocks[idx].get("length") or 0)
            if chosen_chars + length > max_chars:
                continue
            chosen.add(idx)
            chosen_chars += length

        # Then fill by strongest signal.
        for _, idx in sorted(scored_items, key=lambda x: x[0], reverse=True):
            if len(chosen) >= max_chunks:
                break
            if idx in chosen:
                continue
            length = int(candidate_blocks[idx].get("length") or 0)
            if chosen_chars + length > max_chars:
                continue
            chosen.add(idx)
            chosen_chars += length

        selected_indices = sorted(chosen)
        chars_used = chosen_chars

    lines = [candidate_blocks[idx]["block"] for idx in selected_indices]
    used_chunks = len(selected_indices)
    included_items = [candidate_blocks[idx] for idx in selected_indices]
    ocr_included = any(bool(item.get("has_ocr")) for item in included_items)
    vision_included = any(bool(item.get("has_vision")) for item in included_items)
    if not lines:
        fallback_text = str(
            parent_payload.get("cleaned_content")
            or parent_payload.get("content")
            or parent_payload.get("raw_content")
            or ""
        ).strip()
        if not fallback_text:
            raise RuntimeError("Indexed document had no usable content for analysis")
        clipped = fallback_text[:max_chars]
        lines = [f"[document]\n{clipped}"]
        chars_used = len(clipped)
        used_chunks = 1
        selection_strategy = "fallback_parent_content"
        selection_applied = True

    stats = {
        "total_chunks_available": total_chunks_available,
        "chunks_analyzed": used_chunks,
        "total_chars_available": total_chars_available,
        "chars_analyzed": chars_used,
        "chunks_coverage_ratio": round(used_chunks / max(1, total_chunks_available), 4),
        "chars_coverage_ratio": round(chars_used / max(1, total_chars_available), 4),
        "selection_strategy": selection_strategy,
        "selection_applied": bool(selection_applied),
        "ocr_content_available": bool(ocr_available),
        "vision_content_available": bool(vision_available),
        "ocr_content_included": bool(ocr_included),
        "vision_content_included": bool(vision_included),
    }
    return parent_payload, "\n\n".join(lines), used_chunks, chars_used, stats


def _is_retryable_doc_intel_error(exc: Exception) -> bool:
    name = type(exc).__name__.lower()
    message = str(exc).lower()
    if "timeout" in name:
        return True
    if "ratelimit" in name or "toomanyrequests" in name:
        return True
    return (
        "readtimeout" in message
        or "read timed out" in message
        or "timed out" in message
        or "temporarily unavailable" in message
        or "status code: 429" in message
        or "status code: 500" in message
        or "status code: 502" in message
        or "status code: 503" in message
        or "status code: 504" in message
    )


async def _enqueue_campaign_intel_refresh_if_needed(
    *,
    settings: Any,
    graph: Optional[GraphService],
    tenant_id: str,
    is_global: bool,
    trigger_source: str,
    file_id: str,
) -> None:
    if not (settings.RILEY_CAMPAIGN_INTEL_ENABLED and graph is not None and not is_global):
        return
    try:
        from app.services.campaign_intelligence import enqueue_campaign_intelligence_job

        await enqueue_campaign_intelligence_job(
            graph=graph,
            tenant_id=tenant_id,
            requested_by_user_id=None,
            trigger_source=trigger_source,
        )
    except Exception as campaign_exc:
        logger.warning(
            "campaign_intel_enqueue_failed file_id=%s tenant=%s trigger=%s error=%s",
            file_id,
            tenant_id,
            trigger_source,
            campaign_exc,
        )


async def _best_effort_graph_upsert(
    *,
    graph: Optional[GraphService],
    file_id: str,
    tenant_id: str,
    is_global: bool,
    status: str,
    analysis_started_at: Optional[str] = None,
    analysis_completed_at: Optional[str] = None,
    analysis_error: Optional[str] = None,
    doc_summary_short: Optional[str] = None,
    doc_summary_long: Optional[str] = None,
    key_themes: Optional[List[str]] = None,
    key_entities: Optional[List[str]] = None,
    sentiment_overall: Optional[str] = None,
    tone_labels: Optional[List[str]] = None,
    framing_labels: Optional[List[str]] = None,
    audience_implications: Optional[List[str]] = None,
    persuasion_risks: Optional[List[str]] = None,
    strategic_opportunities: Optional[List[str]] = None,
    tone_profile: Optional[str] = None,
    framing_profile: Optional[str] = None,
    strategic_notes: Optional[str] = None,
    major_claims_or_evidence: Optional[List[str]] = None,
    source_chunk_count: Optional[int] = None,
    source_char_count: Optional[int] = None,
    analysis_fidelity_level: Optional[str] = None,
    analysis_retry_count_used: Optional[int] = None,
    analysis_selection_strategy: Optional[str] = None,
    analysis_context_reduction_applied: Optional[bool] = None,
    chunks_coverage_ratio: Optional[float] = None,
    chars_coverage_ratio: Optional[float] = None,
    ocr_content_included: Optional[bool] = None,
    vision_content_included: Optional[bool] = None,
) -> None:
    if graph is None:
        return
    try:
        await graph.upsert_riley_document_intelligence(
            file_id=file_id,
            tenant_id=tenant_id,
            is_global=is_global,
            status=status,
            analysis_started_at=analysis_started_at,
            analysis_completed_at=analysis_completed_at,
            analysis_error=analysis_error,
            doc_summary_short=doc_summary_short,
            doc_summary_long=doc_summary_long,
            key_themes=key_themes,
            key_entities=key_entities,
            sentiment_overall=sentiment_overall,
            tone_labels=tone_labels,
            framing_labels=framing_labels,
            audience_implications=audience_implications,
            persuasion_risks=persuasion_risks,
            strategic_opportunities=strategic_opportunities,
            tone_profile=tone_profile,
            framing_profile=framing_profile,
            strategic_notes=strategic_notes,
            major_claims_or_evidence=major_claims_or_evidence,
            source_chunk_count=source_chunk_count,
            source_char_count=source_char_count,
            analysis_fidelity_level=analysis_fidelity_level,
            analysis_retry_count_used=analysis_retry_count_used,
            analysis_selection_strategy=analysis_selection_strategy,
            analysis_context_reduction_applied=analysis_context_reduction_applied,
            chunks_coverage_ratio=chunks_coverage_ratio,
            chars_coverage_ratio=chars_coverage_ratio,
            ocr_content_included=ocr_content_included,
            vision_content_included=vision_content_included,
        )
    except Exception as exc:
        logger.warning(
            "doc_intel_graph_upsert_failed file_id=%s tenant=%s status=%s error=%s",
            file_id,
            tenant_id,
            status,
            exc,
        )


async def enqueue_document_intelligence_job(
    *,
    collection_name: str,
    file_id: str,
    tenant_id: str,
    is_global: bool,
) -> str:
    settings = get_settings()
    if not settings.RILEY_DOC_INTEL_ENABLED:
        return ""

    job_id = str(uuid.uuid4())
    now = datetime.now().isoformat()
    payload = _build_worker_payload(
        job_id=job_id,
        file_id=file_id,
        collection_name=collection_name,
        tenant_id=tenant_id,
        is_global=is_global,
    )
    await vector_service.client.set_payload(
        collection_name=collection_name,
        payload={
            "analysis_job_id": job_id,
            "analysis_job_status": "queued",
            "analysis_job_created_at": now,
            "analysis_job_started_at": None,
            "analysis_job_completed_at": None,
            "analysis_status": "queued",
            "analysis_error": None,
            "analysis_completed_at": None,
            "analysis_fidelity_level": "pending",
            "analysis_selection_strategy": None,
            "analysis_context_reduction_applied": None,
            "analysis_queue_provider": (
                "cloud_tasks" if settings.RILEY_DOC_INTEL_USE_CLOUD_TASKS else "inline"
            ),
        },
        points=[file_id],
    )
    try:
        if settings.RILEY_DOC_INTEL_USE_CLOUD_TASKS:
            await _enqueue_doc_intel_cloud_task(payload)
        else:
            asyncio.create_task(
                run_document_intelligence_job(
                    job_id=job_id,
                    file_id=file_id,
                    collection_name=collection_name,
                    tenant_id=tenant_id,
                    is_global=is_global,
                    graph=None,
                )
            )
    except Exception as exc:
        completed_at = datetime.now().isoformat()
        error_name = type(exc).__name__
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={
                "analysis_status": "failed",
                "analysis_error": f"Failed to enqueue document intelligence job: {error_name}",
                "analysis_completed_at": completed_at,
                "analysis_job_status": "failed",
                "analysis_job_completed_at": completed_at,
            },
            points=[file_id],
        )
        raise
    return job_id


async def run_document_intelligence_job(
    *,
    job_id: str,
    file_id: str,
    collection_name: str,
    tenant_id: str,
    is_global: bool,
    graph: Optional[GraphService],
) -> None:
    settings = get_settings()
    started_at = datetime.now().isoformat()
    await vector_service.client.set_payload(
        collection_name=collection_name,
        payload={
            "analysis_status": "processing",
            "analysis_error": None,
            "analysis_job_status": "processing",
            "analysis_job_started_at": started_at,
            "analysis_started_at": started_at,
            "analysis_fidelity_level": "pending",
        },
        points=[file_id],
    )
    await _best_effort_graph_upsert(
        graph=graph,
        file_id=file_id,
        tenant_id=tenant_id,
        is_global=is_global,
        status="processing",
        analysis_started_at=started_at,
        analysis_error=None,
    )

    try:
        retry_attempts = max(0, int(settings.RILEY_DOC_INTEL_RETRY_ATTEMPTS))
        max_attempts = retry_attempts + 1
        backoff_seconds = max(0.25, float(settings.RILEY_DOC_INTEL_RETRY_BACKOFF_SECONDS))
        max_timeout_seconds = max(
            int(settings.RILEY_DOC_INTEL_TIMEOUT_SECONDS),
            int(getattr(settings, "RILEY_DOC_INTEL_MAX_TIMEOUT_SECONDS", settings.RILEY_DOC_INTEL_TIMEOUT_SECONDS)),
        )
        # Intelligence-first degradation ladder:
        # - Keep full context for first two attempts.
        # - Then step down gradually, preserving broad evidence coverage.
        # - Avoid severe shrink in normal operation; heavy reduction is last resort only.
        attempt_plan = [
            {"chunks_scale": 1.00, "chars_scale": 1.00, "timeout_scale": 1.00, "fidelity": "full"},
            {"chunks_scale": 1.00, "chars_scale": 1.00, "timeout_scale": 1.45, "fidelity": "full"},
            {"chunks_scale": 0.95, "chars_scale": 0.95, "timeout_scale": 1.85, "fidelity": "lightly_reduced"},
            {"chunks_scale": 0.90, "chars_scale": 0.90, "timeout_scale": 2.30, "fidelity": "lightly_reduced"},
            {"chunks_scale": 0.85, "chars_scale": 0.85, "timeout_scale": 2.90, "fidelity": "moderately_reduced"},
            {"chunks_scale": 0.80, "chars_scale": 0.80, "timeout_scale": 3.60, "fidelity": "moderately_reduced"},
            {"chunks_scale": 0.72, "chars_scale": 0.72, "timeout_scale": 4.20, "fidelity": "heavily_reduced"},
        ]
        base_max_chunks = max(10, int(settings.RILEY_DOC_INTEL_MAX_CHUNKS))
        base_max_chars = max(4000, int(settings.RILEY_DOC_INTEL_MAX_CONTEXT_CHARS))
        base_timeout = max(60, int(settings.RILEY_DOC_INTEL_TIMEOUT_SECONDS))

        raw_output: Optional[Dict[str, Any]] = None
        parent_payload: Dict[str, Any] = {}
        source_chunk_count = 0
        source_char_count = 0
        context_stats: Dict[str, Any] = {}
        filename = file_id
        file_type = "unknown"
        last_exc: Optional[Exception] = None
        retry_count_used = 0
        attempt_count_used = 0
        used_timeout_seconds = base_timeout
        used_fidelity_level = "unknown"

        for attempt_idx in range(max_attempts):
            plan = attempt_plan[min(attempt_idx, len(attempt_plan) - 1)]
            attempt_max_chunks = max(10, int(round(base_max_chunks * float(plan["chunks_scale"]))))
            attempt_max_chars = max(4000, int(round(base_max_chars * float(plan["chars_scale"]))))
            used_timeout_seconds = min(
                max_timeout_seconds,
                max(base_timeout, int(round(base_timeout * float(plan["timeout_scale"])))),
            )
            parent_payload, context_text, source_chunk_count, source_char_count, context_stats = await _load_document_context(
                collection_name=collection_name,
                file_id=file_id,
                max_chunks_override=attempt_max_chunks,
                max_chars_override=attempt_max_chars,
            )
            used_fidelity_level = str(plan["fidelity"])
            filename = str(parent_payload.get("filename") or file_id)
            file_type = str(parent_payload.get("file_type") or parent_payload.get("type") or "unknown")
            prompt = _build_doc_intel_prompt(
                filename=filename,
                file_type=file_type,
                context_text=context_text,
            )
            try:
                raw_output = await _call_openai_document_intel(
                    prompt=prompt,
                    model_name=settings.RILEY_DOC_INTEL_MODEL,
                    timeout_seconds=used_timeout_seconds,
                )
                attempt_count_used = attempt_idx + 1
                retry_count_used = attempt_idx
                break
            except Exception as exc:
                last_exc = exc
                should_retry = _is_retryable_doc_intel_error(exc) and attempt_idx < retry_attempts
                logger.warning(
                    "doc_intel_attempt_failed file_id=%s tenant=%s job_id=%s attempt=%s/%s retryable=%s "
                    "error_type=%s source_chunks=%s source_chars=%s max_chunks=%s max_chars=%s timeout_s=%s fidelity=%s selection=%s",
                    file_id,
                    tenant_id,
                    job_id,
                    attempt_idx + 1,
                    max_attempts,
                    should_retry,
                    type(exc).__name__,
                    source_chunk_count,
                    source_char_count,
                    attempt_max_chunks,
                    attempt_max_chars,
                    used_timeout_seconds,
                    used_fidelity_level,
                    context_stats.get("selection_strategy"),
                )
                if not should_retry:
                    raise
                await asyncio.sleep(backoff_seconds * (2 ** attempt_idx))

        if raw_output is None:
            raise RuntimeError(f"Document intelligence retries exhausted: {type(last_exc).__name__ if last_exc else 'UnknownError'}")

        normalized = _normalize_analysis(raw_output)
        chunks_coverage_ratio = float(context_stats.get("chunks_coverage_ratio") or 0.0)
        chars_coverage_ratio = float(context_stats.get("chars_coverage_ratio") or 0.0)
        if chunks_coverage_ratio >= 0.98 and chars_coverage_ratio >= 0.98:
            analysis_fidelity_level = "full"
        elif chunks_coverage_ratio >= 0.88 and chars_coverage_ratio >= 0.88:
            analysis_fidelity_level = "lightly_reduced"
        elif chunks_coverage_ratio >= 0.72 and chars_coverage_ratio >= 0.72:
            analysis_fidelity_level = "moderately_reduced"
        else:
            analysis_fidelity_level = "heavily_reduced"

        completed_at = datetime.now().isoformat()
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={
                "doc_summary_short": normalized["doc_summary_short"],
                "doc_summary_long": normalized["doc_summary_long"],
                "key_themes": normalized["key_themes"],
                "key_entities": normalized["key_entities"],
                "sentiment_overall": normalized["sentiment_overall"],
                "tone_labels": normalized["tone_labels"],
                "framing_labels": normalized["framing_labels"],
                "audience_implications": normalized["audience_implications"],
                "persuasion_risks": normalized["persuasion_risks"],
                "strategic_opportunities": normalized["strategic_opportunities"],
                "tone_profile": normalized["tone_profile"],
                "framing_profile": normalized["framing_profile"],
                "strategic_notes": normalized["strategic_notes"],
                "major_claims_or_evidence": normalized["major_claims_or_evidence"],
                "analysis_status": "complete",
                "analysis_completed_at": completed_at,
                "analysis_error": None,
                "analysis_model": settings.RILEY_DOC_INTEL_MODEL,
                "analysis_job_status": "complete",
                "analysis_job_completed_at": completed_at,
                "analysis_source_chunk_count": source_chunk_count,
                "analysis_source_char_count": source_char_count,
                "analysis_total_chunks_available": int(context_stats.get("total_chunks_available") or source_chunk_count),
                "analysis_total_chars_available": int(context_stats.get("total_chars_available") or source_char_count),
                "analysis_chunks_coverage_ratio": chunks_coverage_ratio,
                "analysis_chars_coverage_ratio": chars_coverage_ratio,
                "analysis_fidelity_level": analysis_fidelity_level,
                "analysis_retry_count_used": retry_count_used,
                "analysis_attempt_count_used": max(1, attempt_count_used),
                "analysis_timeout_seconds_used": used_timeout_seconds,
                "analysis_selection_strategy": str(context_stats.get("selection_strategy") or "unknown"),
                "analysis_context_reduction_applied": bool(context_stats.get("selection_applied")),
                "analysis_ocr_content_available": bool(context_stats.get("ocr_content_available")),
                "analysis_vision_content_available": bool(context_stats.get("vision_content_available")),
                "analysis_ocr_content_included": bool(context_stats.get("ocr_content_included")),
                "analysis_vision_content_included": bool(context_stats.get("vision_content_included")),
            },
            points=[file_id],
        )
        await _best_effort_graph_upsert(
            graph=graph,
            file_id=file_id,
            tenant_id=tenant_id,
            is_global=is_global,
            status="complete",
            analysis_started_at=started_at,
            analysis_completed_at=completed_at,
            analysis_error=None,
            doc_summary_short=normalized["doc_summary_short"],
            doc_summary_long=normalized["doc_summary_long"],
            key_themes=normalized["key_themes"],
            key_entities=normalized["key_entities"],
            sentiment_overall=normalized["sentiment_overall"],
            tone_labels=normalized["tone_labels"],
            framing_labels=normalized["framing_labels"],
            audience_implications=normalized["audience_implications"],
            persuasion_risks=normalized["persuasion_risks"],
            strategic_opportunities=normalized["strategic_opportunities"],
            tone_profile=normalized["tone_profile"],
            framing_profile=normalized["framing_profile"],
            strategic_notes=normalized["strategic_notes"],
            major_claims_or_evidence=normalized["major_claims_or_evidence"],
            source_chunk_count=source_chunk_count,
            source_char_count=source_char_count,
            analysis_fidelity_level=analysis_fidelity_level,
            analysis_retry_count_used=retry_count_used,
            analysis_selection_strategy=str(context_stats.get("selection_strategy") or "unknown"),
            analysis_context_reduction_applied=bool(context_stats.get("selection_applied")),
            chunks_coverage_ratio=chunks_coverage_ratio,
            chars_coverage_ratio=chars_coverage_ratio,
            ocr_content_included=bool(context_stats.get("ocr_content_included")),
            vision_content_included=bool(context_stats.get("vision_content_included")),
        )
        await _enqueue_campaign_intel_refresh_if_needed(
            settings=settings,
            graph=graph,
            tenant_id=tenant_id,
            is_global=is_global,
            trigger_source="doc_intel_complete",
            file_id=file_id,
        )
        logger.info(
            "doc_intel_complete file_id=%s tenant=%s chunks=%s/%s chars=%s/%s fidelity=%s selection=%s "
            "ocr_included=%s vision_included=%s model=%s timeout_s=%s retries=%s",
            file_id,
            tenant_id,
            source_chunk_count,
            int(context_stats.get("total_chunks_available") or source_chunk_count),
            source_char_count,
            int(context_stats.get("total_chars_available") or source_char_count),
            analysis_fidelity_level,
            context_stats.get("selection_strategy"),
            bool(context_stats.get("ocr_content_included")),
            bool(context_stats.get("vision_content_included")),
            settings.RILEY_DOC_INTEL_MODEL,
            used_timeout_seconds,
            retry_count_used,
        )
    except Exception as exc:
        completed_at = datetime.now().isoformat()
        error_message = f"{type(exc).__name__}: {exc}"
        logger.exception(
            "doc_intel_failed file_id=%s tenant=%s job_id=%s error=%s timeout_s=%s retries=%s max_chunks=%s max_chars=%s",
            file_id,
            tenant_id,
            job_id,
            exc,
            int(settings.RILEY_DOC_INTEL_TIMEOUT_SECONDS),
            max(0, int(settings.RILEY_DOC_INTEL_RETRY_ATTEMPTS)),
            int(settings.RILEY_DOC_INTEL_MAX_CHUNKS),
            int(settings.RILEY_DOC_INTEL_MAX_CONTEXT_CHARS),
        )
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={
                "analysis_status": "failed",
                "analysis_completed_at": completed_at,
                "analysis_error": error_message[:2000],
                "analysis_job_status": "failed",
                "analysis_job_completed_at": completed_at,
                "analysis_fidelity_level": "failed",
                "analysis_selection_strategy": "none",
            },
            points=[file_id],
        )
        await _best_effort_graph_upsert(
            graph=graph,
            file_id=file_id,
            tenant_id=tenant_id,
            is_global=is_global,
            status="failed",
            analysis_started_at=started_at,
            analysis_completed_at=completed_at,
            analysis_error=error_message[:2000],
        )
        await _enqueue_campaign_intel_refresh_if_needed(
            settings=settings,
            graph=graph,
            tenant_id=tenant_id,
            is_global=is_global,
            trigger_source="doc_intel_failed",
            file_id=file_id,
        )
