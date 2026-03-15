import asyncio
import hashlib
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


async def _load_document_chunks(
    *,
    collection_name: str,
    file_id: str,
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
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
                limit=256,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
        except Exception as exc:
            if "record_type" not in str(exc).lower():
                raise
            legacy_filter = Filter(
                must=[
                    FieldCondition(key="parent_file_id", match=MatchValue(value=file_id)),
                ]
            )
            scroll_result = await vector_service.client.scroll(
                collection_name=collection_name,
                scroll_filter=legacy_filter,
                limit=256,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )
            page_points = [
                p for p in scroll_result[0]
                if str((p.payload or {}).get("record_type") or "chunk").lower() != "file"
            ]
            chunks.extend(page_points)
            offset = scroll_result[1]
            if offset is None:
                break
            continue

        page_points = scroll_result[0]
        chunks.extend(page_points)
        offset = scroll_result[1]
        if offset is None:
            break

    payloads = [dict(point.payload or {}) for point in chunks]
    payloads.sort(key=lambda payload: int(payload.get("chunk_index", 0)))
    return dict(parent_payload), payloads


def _build_doc_profile(
    *,
    chunk_payloads: List[Dict[str, Any]],
    file_type: str,
) -> Dict[str, Any]:
    section_labels: set[str] = set()
    location_labels: set[str] = set()
    numeric_locations: set[int] = set()
    total_chars = 0
    ocr_present = False
    vision_present = False
    for payload in chunk_payloads:
        text = str(payload.get("content") or payload.get("text") or "").strip()
        if not text:
            continue
        total_chars += len(text)
        section = str(payload.get("section_path") or "").strip()
        if section:
            section_labels.add(section)
        loc_val = str(payload.get("location_value") or "").strip()
        if loc_val:
            location_labels.add(loc_val)
            if loc_val.isdigit():
                numeric_locations.add(int(loc_val))
        lowered = text.lower()
        if bool(payload.get("ocr_text_present")) or "[ocr]" in lowered:
            ocr_present = True
        if bool(payload.get("has_visual_content")) or bool(payload.get("vision_caption")) or "[vision]" in lowered:
            vision_present = True
    return {
        "chunk_count": len(chunk_payloads),
        "char_count": total_chars,
        "page_or_slide_count": len(numeric_locations),
        "section_count": len(section_labels),
        "location_count": len(location_labels),
        "ocr_present": ocr_present,
        "vision_present": vision_present,
        "file_type": (file_type or "").lower(),
    }


def _route_doc_intel_mode(profile: Dict[str, Any], settings: Any) -> str:
    if not bool(getattr(settings, "RILEY_DOC_INTEL_MULTIPASS_ENABLED", True)):
        return "single_pass"
    chunk_count = int(profile.get("chunk_count") or 0)
    char_count = int(profile.get("char_count") or 0)
    page_count = int(profile.get("page_or_slide_count") or 0)
    section_count = int(profile.get("section_count") or 0)
    multimodal_complex = bool(profile.get("ocr_present")) or bool(profile.get("vision_present"))
    file_type = str(profile.get("file_type") or "")
    complex_file = file_type in {"pdf", "pptx", "ppt"}

    if chunk_count >= int(settings.RILEY_DOC_INTEL_MULTIPASS_MIN_CHUNKS):
        return "multi_pass"
    if char_count >= int(settings.RILEY_DOC_INTEL_MULTIPASS_MIN_CHARS):
        return "multi_pass"
    if page_count >= int(settings.RILEY_DOC_INTEL_MULTIPASS_MIN_PAGES):
        return "multi_pass"
    if section_count >= 24:
        return "multi_pass"
    if multimodal_complex and chunk_count >= 28:
        return "multi_pass"
    if complex_file and chunk_count >= 40:
        return "multi_pass"
    return "single_pass"


def _band_id(file_id: str, band_index: int) -> str:
    digest = hashlib.sha1(f"{file_id}:{band_index}".encode("utf-8")).hexdigest()[:10]
    return f"band_{band_index + 1}_{digest}"


def _build_analysis_bands(
    *,
    file_id: str,
    chunk_payloads: List[Dict[str, Any]],
    settings: Any,
) -> List[Dict[str, Any]]:
    if not chunk_payloads:
        return []
    target_chunks = max(10, int(settings.RILEY_DOC_INTEL_MULTIPASS_BAND_TARGET_CHUNKS))
    max_chunks = max(target_chunks, int(settings.RILEY_DOC_INTEL_MULTIPASS_BAND_MAX_CHUNKS))
    target_chars = max(5000, int(settings.RILEY_DOC_INTEL_MULTIPASS_BAND_TARGET_CHARS))
    max_chars = max(target_chars, int(settings.RILEY_DOC_INTEL_MULTIPASS_BAND_MAX_CHARS))

    appendix_tokens = ("appendix", "annex", "supplement", "references", "methodology", "endnotes")
    high_signal_tokens = (
        "recommend", "strategy", "strategic", "risk", "opportunit", "tradeoff", "budget",
        "turnout", "persuad", "message", "framing", "narrative", "target", "audience",
        "poll", "survey", "claim", "evidence", "counter", "attack", "contrast",
    )

    prepared: List[Dict[str, Any]] = []
    for payload in chunk_payloads:
        text = str(payload.get("content") or payload.get("text") or "").strip()
        if not text:
            continue
        section_path = str(payload.get("section_path") or "").strip()
        location_value = str(payload.get("location_value") or "").strip()
        loc_numeric = int(location_value) if location_value.isdigit() else None
        lowered = f"{section_path}\n{text}".lower()
        has_appendix_signal = any(tok in lowered for tok in appendix_tokens)
        high_signal_score = sum(1 for tok in high_signal_tokens if tok in lowered)
        prepared.append(
            {
                "payload": payload,
                "text": text,
                "section_path": section_path,
                "location_value": location_value,
                "loc_numeric": loc_numeric,
                "has_appendix_signal": has_appendix_signal,
                "high_signal_score": high_signal_score,
                "chunk_index": int(payload.get("chunk_index") or 0),
            }
        )
    if not prepared:
        return []

    unique_sections = {
        item["section_path"] for item in prepared
        if item["section_path"]
    }
    structure_rich = len(unique_sections) >= 4

    bands: List[Dict[str, Any]] = []
    current_items: List[Dict[str, Any]] = []
    current_chars = 0

    def _close_band() -> None:
        nonlocal current_items, current_chars
        if not current_items:
            return
        band_index = len(bands)
        current_chunks = [item["payload"] for item in current_items]
        loc_vals = [str(c.get("location_value") or "").strip() for c in current_chunks if str(c.get("location_value") or "").strip()]
        loc_numeric = [int(v) for v in loc_vals if v.isdigit()]
        sections = sorted(
            {
                str(c.get("section_path") or "").strip()
                for c in current_chunks
                if str(c.get("section_path") or "").strip()
            }
        )
        chunk_indices = [int(item.get("chunk_index") or 0) for item in current_items]
        ocr_included = any(
            bool(c.get("ocr_text_present")) or "[ocr]" in str(c.get("content") or c.get("text") or "").lower()
            for c in current_chunks
        )
        vision_included = any(
            bool(c.get("has_visual_content")) or bool(c.get("vision_caption")) or "[vision]" in str(c.get("content") or c.get("text") or "").lower()
            for c in current_chunks
        )
        evidence_snippets: List[str] = []
        for pos in [0, len(current_items) // 2, max(0, len(current_items) - 1)]:
            if pos < 0 or pos >= len(current_chunks):
                continue
            text = str(current_items[pos]["text"]).strip()
            if text:
                evidence_snippets.append(text[:320])
        high_signal_band = sum(int(item.get("high_signal_score") or 0) for item in current_items) >= 3

        bands.append(
            {
                "band_id": _band_id(file_id, band_index),
                "band_index": band_index,
                "location_type": str(current_chunks[0].get("location_type") or "segment"),
                "location_start": min(loc_numeric) if loc_numeric else (loc_vals[0] if loc_vals else None),
                "location_end": max(loc_numeric) if loc_numeric else (loc_vals[-1] if loc_vals else None),
                "section_labels": sections[:10],
                "chunk_count": len(current_chunks),
                "char_count": current_chars,
                "ocr_included": ocr_included,
                "vision_included": vision_included,
                "has_appendix_signal": any(
                    bool(item.get("has_appendix_signal"))
                    for item in current_items
                ),
                "high_signal_band": high_signal_band,
                "high_signal_score": sum(int(item.get("high_signal_score") or 0) for item in current_items),
                "chunk_index_start": min(chunk_indices) if chunk_indices else None,
                "chunk_index_end": max(chunk_indices) if chunk_indices else None,
                "evidence_snippets": evidence_snippets[:3],
                "chunks": current_chunks,
            }
        )
        current_items = []
        current_chars = 0

    prev_section = ""
    prev_loc = None
    for item in prepared:
        cur_section = str(item["section_path"] or "")
        cur_loc = item["loc_numeric"] if item["loc_numeric"] is not None else item["location_value"]
        adding_chars = len(item["text"])
        current_count = len(current_items)
        section_changed = bool(cur_section and prev_section and cur_section != prev_section)
        location_changed = cur_loc != prev_loc
        section_boundary_ready = (
            structure_rich
            and section_changed
            and current_count >= max(6, target_chunks // 2)
            and current_chars >= max(2200, target_chars // 2)
        )
        page_locality_boundary = (
            location_changed
            and current_count >= max(8, target_chunks // 2)
            and current_chars >= max(2600, target_chars // 2)
        )
        overflow = (
            current_count >= max_chunks
            or (current_chars + adding_chars) > max_chars
        )
        if current_items and (section_boundary_ready or page_locality_boundary or overflow):
            _close_band()
        current_items.append(item)
        current_chars += adding_chars
        prev_section = cur_section or prev_section
        prev_loc = cur_loc
    _close_band()

    # Merge tiny neighboring bands to reduce arbitrary cuts.
    compacted: List[Dict[str, Any]] = []
    for band in bands:
        if not compacted:
            compacted.append(band)
            continue
        is_tiny = int(band.get("chunk_count") or 0) < 5 or int(band.get("char_count") or 0) < 1600
        prev = compacted[-1]
        same_section = bool(set(band.get("section_labels") or []).intersection(set(prev.get("section_labels") or [])))
        if is_tiny and same_section:
            prev_chunks = list(prev.get("chunks") or [])
            new_chunks = list(band.get("chunks") or [])
            merged_chunks = [*prev_chunks, *new_chunks]
            prev["chunks"] = merged_chunks
            prev["chunk_count"] = len(merged_chunks)
            prev["char_count"] = int(prev.get("char_count") or 0) + int(band.get("char_count") or 0)
            prev["ocr_included"] = bool(prev.get("ocr_included")) or bool(band.get("ocr_included"))
            prev["vision_included"] = bool(prev.get("vision_included")) or bool(band.get("vision_included"))
            prev["has_appendix_signal"] = bool(prev.get("has_appendix_signal")) or bool(band.get("has_appendix_signal"))
            prev["high_signal_band"] = bool(prev.get("high_signal_band")) or bool(band.get("high_signal_band"))
            prev["high_signal_score"] = int(prev.get("high_signal_score") or 0) + int(band.get("high_signal_score") or 0)
            prev["evidence_snippets"] = list(dict.fromkeys([*(prev.get("evidence_snippets") or []), *(band.get("evidence_snippets") or [])]))[:4]
            prev["chunk_index_end"] = band.get("chunk_index_end")
            prev["location_end"] = band.get("location_end")
            continue
        compacted.append(band)
    bands = compacted

    total_bands = len(bands)
    all_chunk_indices = [int(item.get("chunk_index") or 0) for item in prepared]
    doc_min_idx = min(all_chunk_indices) if all_chunk_indices else 0
    doc_max_idx = max(all_chunk_indices) if all_chunk_indices else 1
    for idx, band in enumerate(bands):
        band["band_index"] = idx
        band["band_id"] = _band_id(file_id, idx)
        start_idx = int(band.get("chunk_index_start") or doc_min_idx)
        end_idx = int(band.get("chunk_index_end") or start_idx)
        rel_center = ((start_idx + end_idx) / 2 - doc_min_idx) / max(1, (doc_max_idx - doc_min_idx))
        if total_bands <= 2:
            zone = "full_span"
        elif rel_center < 0.33:
            zone = "beginning"
        elif rel_center < 0.66:
            zone = "middle"
        else:
            zone = "end"
        band["coverage_zone"] = zone

    # Ensure appendix/end matter remains explicit when present.
    if any(item.get("has_appendix_signal") for item in prepared):
        appendix_bands = [band for band in bands if bool(band.get("has_appendix_signal"))]
        if not appendix_bands and bands:
            bands[-1]["has_appendix_signal"] = True
            bands[-1]["coverage_zone"] = "end"

    return bands


def _build_band_context_text(
    *,
    band: Dict[str, Any],
    max_chars: int,
) -> str:
    lines: List[str] = []
    chars_used = 0
    for payload in band.get("chunks", []):
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
        if chars_used + len(block) > max_chars:
            break
        lines.append(block)
        chars_used += len(block)
    return "\n\n".join(lines).strip()


def _build_band_prompt(
    *,
    filename: str,
    file_type: str,
    band: Dict[str, Any],
    context_text: str,
) -> str:
    return f"""You are Riley's document intelligence pipeline.
Analyze this specific section band of a larger campaign document.
Use only the supplied evidence for this band.

Document:
- filename: {filename}
- file_type: {file_type}
- band_id: {band.get("band_id")}
- band_index: {band.get("band_index")}
- coverage_zone: {band.get("coverage_zone")}
- location_type: {band.get("location_type")}
- location_start: {band.get("location_start")}
- location_end: {band.get("location_end")}
- section_labels: {band.get("section_labels")}

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

Band source material:
{context_text}
"""


def _build_multipass_synthesis_prompt(
    *,
    filename: str,
    file_type: str,
    band_artifacts: List[Dict[str, Any]],
    tensions: List[Dict[str, Any]],
    coverage_metadata: Dict[str, Any],
    modality_metadata: Dict[str, Any],
    failed_bands: List[Dict[str, Any]],
) -> str:
    evidence_packet = []
    for band in band_artifacts:
        evidence_packet.append(
            {
                "band_id": band.get("band_id"),
                "band_index": band.get("band_index"),
                "coverage_zone": band.get("coverage_zone"),
                "location_start": band.get("location_start"),
                "location_end": band.get("location_end"),
                "section_labels": band.get("section_labels"),
                "high_signal_band": band.get("high_signal_band"),
                "evidence_snippets": list(band.get("evidence_snippets") or [])[:4],
                "major_claims_or_evidence": list(band.get("major_claims_or_evidence") or [])[:6],
                "key_themes": list(band.get("key_themes") or [])[:6],
                "tone_labels": list(band.get("tone_labels") or [])[:6],
                "framing_labels": list(band.get("framing_labels") or [])[:6],
                "audience_implications": list(band.get("audience_implications") or [])[:4],
                "persuasion_risks": list(band.get("persuasion_risks") or [])[:4],
                "strategic_opportunities": list(band.get("strategic_opportunities") or [])[:4],
                "strategic_notes": str(band.get("strategic_notes") or "")[:1200],
            }
        )
    return f"""You are Riley's whole-document intelligence synthesis pipeline.
Synthesize a complete strategic understanding of the document from multi-band analyses.
Do not just stitch summaries. Build a coherent whole-document interpretation.

Document:
- filename: {filename}
- file_type: {file_type}
- analyzed_band_count: {len(band_artifacts)}

Required synthesis:
- what the document is trying to do overall
- strongest recurring themes and narrative arc
- strongest evidence and most consequential claims
- strategic coherence or incoherence across sections
- major vulnerabilities and risks
- strategic opportunities and how to use this document

You MUST use all of the following as first-class inputs:
1) structured band artifacts
2) raw evidence snippets from each band
3) contradiction/tension artifacts
4) coverage metadata (including weak spots)
5) modality metadata (OCR/vision inclusion)

Do NOT rely primarily on band summary fields. Ground the synthesis in evidence snippets and claims.
If coverage is partial or there are failed bands, explicitly reflect reduced confidence in strategic conclusions.

Band evidence packet JSON:
{json.dumps(evidence_packet, ensure_ascii=False)}

Intra-document tensions JSON:
{json.dumps(tensions, ensure_ascii=False)}

Coverage metadata JSON:
{json.dumps(coverage_metadata, ensure_ascii=False)}

Modality metadata JSON:
{json.dumps(modality_metadata, ensure_ascii=False)}

Failed bands JSON:
{json.dumps(failed_bands, ensure_ascii=False)}

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
"""


def _detect_intra_document_tensions(
    band_artifacts: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if len(band_artifacts) < 2:
        return []

    tensions: List[Dict[str, Any]] = []

    def _new_tension(kind: str, summary: str, why: str, implication: str, involved: List[Dict[str, Any]]) -> Dict[str, Any]:
        key = "|".join(str(item.get("band_id") or "") for item in involved[:6])
        cid = f"intra_{hashlib.sha1(f'{kind}:{key}'.encode('utf-8')).hexdigest()[:10]}"
        evidence_refs = []
        for item in involved[:4]:
            bid = str(item.get("band_id") or "")
            for snippet in list(item.get("evidence_snippets") or [])[:2]:
                evidence_refs.append(f"{bid}: {str(snippet)[:220]}")
        return {
            "contradiction_id": cid,
            "involved_band_ids": [str(item.get("band_id") or "") for item in involved[:8]],
            "topic": kind,
            "contradiction_summary": summary,
            "why_it_matters": why,
            "strategic_implication": implication,
            "supporting_evidence_refs": evidence_refs[:10],
        }

    def _labels(band: Dict[str, Any], key: str) -> set[str]:
        return {str(x).lower().strip() for x in (band.get(key) or []) if str(x).strip()}

    def _text_blob(band: Dict[str, Any]) -> str:
        parts = [
            str(band.get("band_summary_short") or ""),
            str(band.get("band_summary_long") or ""),
            str(band.get("strategic_notes") or ""),
            " ".join(str(x) for x in (band.get("major_claims_or_evidence") or [])),
            " ".join(str(x) for x in (band.get("strategic_opportunities") or [])),
            " ".join(str(x) for x in (band.get("persuasion_risks") or [])),
            " ".join(str(x) for x in (band.get("audience_implications") or [])),
        ]
        return " ".join(parts).lower()

    optimistic = [b for b in band_artifacts if "optimism" in _labels(b, "tone_labels")]
    threat = [
        b for b in band_artifacts
        if ("fear" in _labels(b, "tone_labels"))
        or ("threat" in _labels(b, "framing_labels"))
        or ("alarm" in _text_blob(b))
    ]
    if optimistic and threat:
        tensions.append(
            _new_tension(
                "optimism_vs_threat_posture",
                "Document shifts between optimistic-forward and threat-heavy framing across sections.",
                "Mixed emotional posture can confuse message hierarchy.",
                "Define where positive mobilization vs threat framing is intentionally used.",
                [optimistic[0], threat[0]],
            )
        )

    institutional = [b for b in band_artifacts if "institutional" in _labels(b, "tone_labels")]
    populist = [b for b in band_artifacts if "populist" in _labels(b, "tone_labels")]
    if institutional and populist:
        tensions.append(
            _new_tension(
                "institutional_vs_populist_frame",
                "Sections alternate between institutional governance and populist anti-elite framing.",
                "Competing frames can weaken narrative coherence.",
                "Establish a dominant frame and explicit bridge language.",
                [institutional[0], populist[0]],
            )
        )

    persuadable = [b for b in band_artifacts if "persuad" in _text_blob(b)]
    base = [b for b in band_artifacts if "base" in _text_blob(b) or "core supporter" in _text_blob(b)]
    if persuadable and base:
        tensions.append(
            _new_tension(
                "audience_assumption_conflict",
                "Different bands assume different primary audiences (persuadable vs base).",
                "Audience mismatch can produce contradictory targeting decisions.",
                "Split audience lanes and enforce a shared narrative backbone.",
                [persuadable[0], base[0]],
            )
        )

    # Recommendation vs evidence mismatch: strategic directives with weak evidence references.
    rec_heavy = [
        b for b in band_artifacts
        if len(list(b.get("strategic_opportunities") or [])) >= 2 or "recommend" in _text_blob(b)
    ]
    evidence_light = [
        b for b in band_artifacts
        if len(list(b.get("major_claims_or_evidence") or [])) <= 1 and len(list(b.get("evidence_snippets") or [])) <= 1
    ]
    if rec_heavy and evidence_light:
        tensions.append(
            _new_tension(
                "recommendation_evidence_mismatch",
                "Some bands make strong recommendations without commensurate evidence detail.",
                "Weak evidence traceability undermines confidence in tactical prescriptions.",
                "Tie each recommendation to explicit evidence and avoid unsupported certainty.",
                [rec_heavy[0], evidence_light[0]],
            )
        )

    # Priority conflicts across sections using theme buckets.
    economy = [b for b in band_artifacts if any(tok in _text_blob(b) for tok in ["econom", "cost", "jobs", "inflation", "tax"])]
    security = [b for b in band_artifacts if any(tok in _text_blob(b) for tok in ["crime", "safety", "security", "border", "law and order"])]
    governance = [b for b in band_artifacts if any(tok in _text_blob(b) for tok in ["institut", "govern", "process", "oversight", "integrity"])]
    if economy and security and governance:
        tensions.append(
            _new_tension(
                "priority_conflict",
                "Different sections prioritize competing strategic agendas (economy, security, governance).",
                "Unresolved priority competition can fragment campaign focus.",
                "Define a primary priority stack and map each to audience/channel context.",
                [economy[0], security[0], governance[0]],
            )
        )

    # Narrative arc break: beginning vs end theme/stance divergence.
    beginning = [b for b in band_artifacts if str(b.get("coverage_zone") or "") == "beginning"]
    ending = [b for b in band_artifacts if str(b.get("coverage_zone") or "") == "end"]
    if beginning and ending:
        begin_themes = _labels(beginning[0], "key_themes")
        end_themes = _labels(ending[-1], "key_themes")
        begin_sent = str(beginning[0].get("sentiment_overall") or "").lower()
        end_sent = str(ending[-1].get("sentiment_overall") or "").lower()
        if (begin_themes and end_themes and not begin_themes.intersection(end_themes)) or (begin_sent and end_sent and begin_sent != end_sent):
            tensions.append(
                _new_tension(
                    "narrative_arc_break",
                    "The narrative stance or dominant themes shift sharply from beginning to end.",
                    "Arc breaks can signal incoherent storytelling or unresolved strategic pivots.",
                    "Introduce explicit transition logic that reconciles early framing with late conclusions.",
                    [beginning[0], ending[-1]],
                )
            )

    # Message discipline drift: framing labels disperse too broadly across bands.
    framing_sets = [tuple(sorted(_labels(b, "framing_labels"))) for b in band_artifacts if _labels(b, "framing_labels")]
    if len(set(framing_sets)) >= max(3, len(band_artifacts) // 2):
        tensions.append(
            _new_tension(
                "message_discipline_drift",
                "Framing shifts repeatedly across bands without a stable message spine.",
                "Excessive frame drift weakens repetition and persuasion efficiency.",
                "Select one core frame and use secondary frames only as supporting lanes.",
                band_artifacts[:3],
            )
        )

    # Tactical inconsistency: opportunities/risk advice conflict across bands.
    digital_push = [b for b in band_artifacts if any(tok in _text_blob(b) for tok in ["digital", "online", "social", "microtarget"])]
    field_push = [b for b in band_artifacts if any(tok in _text_blob(b) for tok in ["field", "door", "ground", "organizing", "canvass"])]
    cautionary = [b for b in band_artifacts if any(tok in _text_blob(b) for tok in ["avoid", "do not", "risk", "backfire", "overreach"])]
    if digital_push and field_push and cautionary:
        tensions.append(
            _new_tension(
                "tactical_inconsistency",
                "Bands prescribe different tactical execution paths with conflicting risk postures.",
                "Conflicting tactical advice increases execution drift and resource waste.",
                "Set clear sequencing rules and channel-specific guardrails for tactics.",
                [digital_push[0], field_push[0], cautionary[0]],
            )
        )

    return tensions[:12]


def _validate_multipass_quality(
    *,
    bands: List[Dict[str, Any]],
    band_artifacts: List[Dict[str, Any]],
    profile: Dict[str, Any],
    failed_bands: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    total_bands = len(bands)
    analyzed_bands = len(band_artifacts)
    band_ratio = analyzed_bands / max(1, total_bands)
    failed_bands = list(failed_bands or [])
    failed_count = len(failed_bands)
    failed_ratio = failed_count / max(1, total_bands)

    total_chunks = sum(int(b.get("chunk_count") or 0) for b in bands)
    analyzed_chunks = sum(int(b.get("chunk_count") or 0) for b in band_artifacts)
    chunks_ratio = analyzed_chunks / max(1, total_chunks)

    total_chars = sum(int(b.get("char_count") or 0) for b in bands)
    analyzed_chars = sum(int(b.get("char_count") or 0) for b in band_artifacts)
    chars_ratio = analyzed_chars / max(1, total_chars)

    analyzed_zones = {str(b.get("coverage_zone") or "") for b in band_artifacts}
    has_bme = {"beginning", "middle", "end"}.issubset(analyzed_zones) or ("full_span" in analyzed_zones and len(analyzed_zones) == 1)

    appendix_total = sum(1 for b in bands if bool(b.get("has_appendix_signal")))
    appendix_analyzed = sum(1 for b in band_artifacts if bool(b.get("has_appendix_signal")))
    appendix_required = appendix_total > 0
    appendix_covered = appendix_analyzed > 0

    high_signal_total = sum(1 for b in bands if bool(b.get("high_signal_band")))
    high_signal_analyzed = sum(1 for b in band_artifacts if bool(b.get("high_signal_band")))
    high_signal_ratio = (high_signal_analyzed / max(1, high_signal_total)) if high_signal_total > 0 else 1.0

    end_covered = any(str(b.get("coverage_zone") or "") == "end" for b in band_artifacts)

    ocr_needed = bool(profile.get("ocr_present"))
    vision_needed = bool(profile.get("vision_present"))
    ocr_included = any(bool(b.get("ocr_included")) for b in band_artifacts)
    vision_included = any(bool(b.get("vision_included")) for b in band_artifacts)
    reasons: List[str] = []
    if not has_bme:
        reasons.append("missing beginning/middle/end representation")
    if not end_covered:
        reasons.append("late-document coverage missing")
    if appendix_required and not appendix_covered:
        reasons.append("appendix/endmatter not represented")
    if failed_count > 0:
        reasons.append(f"{failed_count} bands failed")
    if ocr_needed and not ocr_included:
        reasons.append("OCR-rich content not included")
    if vision_needed and not vision_included:
        reasons.append("vision-rich content not included")
    if high_signal_total > 0 and high_signal_ratio < 0.85:
        reasons.append("high-signal strategy bands under-covered")

    if (
        band_ratio >= 0.98
        and chunks_ratio >= 0.9
        and chars_ratio >= 0.9
        and has_bme
        and end_covered
        and (not appendix_required or appendix_covered)
        and failed_count == 0
        and high_signal_ratio >= 0.95
        and ((not ocr_needed) or ocr_included)
        and ((not vision_needed) or vision_included)
    ):
        fidelity = "full"
        status = "passed"
    elif (
        band_ratio >= 0.9
        and chunks_ratio >= 0.8
        and chars_ratio >= 0.8
        and has_bme
        and end_covered
        and failed_ratio <= 0.15
        and high_signal_ratio >= 0.8
    ):
        fidelity = "lightly_reduced"
        status = "passed_with_reduction"
    elif band_ratio >= 0.72 and chunks_ratio >= 0.65 and chars_ratio >= 0.65:
        fidelity = "moderately_reduced"
        status = "partial"
    else:
        fidelity = "heavily_reduced"
        status = "insufficient"

    if fidelity == "full":
        note = "Multipass analysis achieved broad structural, modality, and high-signal coverage."
    elif reasons:
        note = " ; ".join(reasons[:6])
    else:
        note = "Coverage is reduced; confidence downgraded."

    return {
        "total_bands": total_bands,
        "analyzed_bands": analyzed_bands,
        "failed_bands_count": failed_count,
        "failed_bands_ratio": round(failed_ratio, 4),
        "failed_band_ids": [str(item.get("band_id") or "") for item in failed_bands[:50]],
        "band_coverage_ratio": round(band_ratio, 4),
        "total_chunks": total_chunks,
        "analyzed_chunks": analyzed_chunks,
        "chunks_coverage_ratio": round(chunks_ratio, 4),
        "total_chars": total_chars,
        "analyzed_chars": analyzed_chars,
        "chars_coverage_ratio": round(chars_ratio, 4),
        "has_beginning_middle_end": has_bme,
        "has_end_coverage": end_covered,
        "appendix_total": appendix_total,
        "appendix_analyzed": appendix_analyzed,
        "appendix_required": appendix_required,
        "appendix_covered": appendix_covered,
        "ocr_included": ocr_included,
        "vision_included": vision_included,
        "high_signal_total": high_signal_total,
        "high_signal_analyzed": high_signal_analyzed,
        "high_signal_ratio": round(high_signal_ratio, 4),
        "fidelity": fidelity,
        "validation_status": status,
        "validation_note": note,
        "validation_reasons": reasons[:8],
    }


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
    analysis_execution_mode: Optional[str] = None,
    total_bands: Optional[int] = None,
    analyzed_bands: Optional[int] = None,
    band_coverage_ratio: Optional[float] = None,
    contradiction_count: Optional[int] = None,
    validation_status: Optional[str] = None,
    validation_note: Optional[str] = None,
    band_artifacts_json: Optional[str] = None,
    intra_document_tensions_json: Optional[str] = None,
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
            analysis_execution_mode=analysis_execution_mode,
            total_bands=total_bands,
            analyzed_bands=analyzed_bands,
            band_coverage_ratio=band_coverage_ratio,
            contradiction_count=contradiction_count,
            validation_status=validation_status,
            validation_note=validation_note,
            band_artifacts_json=band_artifacts_json,
            intra_document_tensions_json=intra_document_tensions_json,
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
            "analysis_execution_mode": "pending",
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
        parent_payload, chunk_payloads = await _load_document_chunks(
            collection_name=collection_name,
            file_id=file_id,
        )
        filename = str(parent_payload.get("filename") or file_id)
        file_type = str(parent_payload.get("file_type") or parent_payload.get("type") or "unknown")
        profile = _build_doc_profile(chunk_payloads=chunk_payloads, file_type=file_type)
        execution_mode = _route_doc_intel_mode(profile, settings)

        if execution_mode == "multi_pass":
            bands = _build_analysis_bands(
                file_id=file_id,
                chunk_payloads=chunk_payloads,
                settings=settings,
            )
            if not bands:
                raise RuntimeError("Multipass selected but no bands were created")

            retry_attempts = max(0, int(settings.RILEY_DOC_INTEL_RETRY_ATTEMPTS))
            max_attempts = retry_attempts + 1
            backoff_seconds = max(0.25, float(settings.RILEY_DOC_INTEL_RETRY_BACKOFF_SECONDS))
            base_timeout = max(60, int(settings.RILEY_DOC_INTEL_TIMEOUT_SECONDS))
            max_timeout_seconds = max(
                base_timeout,
                int(getattr(settings, "RILEY_DOC_INTEL_MAX_TIMEOUT_SECONDS", base_timeout)),
            )
            band_attempt_plan = [
                {"scale": 1.0, "timeout_scale": 1.0},
                {"scale": 1.0, "timeout_scale": 1.35},
                {"scale": 0.95, "timeout_scale": 1.8},
                {"scale": 0.9, "timeout_scale": 2.2},
                {"scale": 0.85, "timeout_scale": 2.8},
            ]

            successful_bands: List[Dict[str, Any]] = []
            failed_bands: List[Dict[str, Any]] = []
            total_retry_count = 0
            total_attempt_count = 0
            max_timeout_used = base_timeout

            for band in bands:
                band_output: Optional[Dict[str, Any]] = None
                last_exc: Optional[Exception] = None
                for attempt_idx in range(max_attempts):
                    plan = band_attempt_plan[min(attempt_idx, len(band_attempt_plan) - 1)]
                    band_chars = max(3000, int(round(int(band.get("char_count") or 3000) * float(plan["scale"]))))
                    timeout_seconds = min(
                        max_timeout_seconds,
                        max(base_timeout, int(round(base_timeout * float(plan["timeout_scale"])))),
                    )
                    max_timeout_used = max(max_timeout_used, timeout_seconds)
                    context_text = _build_band_context_text(
                        band=band,
                        max_chars=band_chars,
                    )
                    prompt = _build_band_prompt(
                        filename=filename,
                        file_type=file_type,
                        band=band,
                        context_text=context_text,
                    )
                    try:
                        raw = await _call_openai_document_intel(
                            prompt=prompt,
                            model_name=settings.RILEY_DOC_INTEL_MODEL,
                            timeout_seconds=timeout_seconds,
                        )
                        normalized = _normalize_analysis(raw)
                        band_output = {
                            "band_id": band.get("band_id"),
                            "band_index": band.get("band_index"),
                            "coverage_zone": band.get("coverage_zone"),
                            "location_type": band.get("location_type"),
                            "location_start": band.get("location_start"),
                            "location_end": band.get("location_end"),
                            "section_labels": band.get("section_labels"),
                            "chunk_count": int(band.get("chunk_count") or 0),
                            "char_count": int(band.get("char_count") or 0),
                            "ocr_included": bool(band.get("ocr_included")),
                            "vision_included": bool(band.get("vision_included")),
                            "has_appendix_signal": bool(band.get("has_appendix_signal")),
                            "evidence_snippets": list(band.get("evidence_snippets") or []),
                            "band_summary_short": normalized["doc_summary_short"],
                            "band_summary_long": normalized["doc_summary_long"][:2000],
                            "key_themes": normalized["key_themes"],
                            "key_entities": normalized["key_entities"],
                            "sentiment_overall": normalized["sentiment_overall"],
                            "tone_labels": normalized["tone_labels"],
                            "framing_labels": normalized["framing_labels"],
                            "audience_implications": normalized["audience_implications"],
                            "persuasion_risks": normalized["persuasion_risks"],
                            "strategic_opportunities": normalized["strategic_opportunities"],
                            "strategic_notes": normalized["strategic_notes"][:2600],
                            "major_claims_or_evidence": normalized["major_claims_or_evidence"],
                            "attempt_count_used": attempt_idx + 1,
                            "timeout_seconds_used": timeout_seconds,
                        }
                        total_retry_count += attempt_idx
                        total_attempt_count += attempt_idx + 1
                        break
                    except Exception as exc:
                        last_exc = exc
                        should_retry = _is_retryable_doc_intel_error(exc) and attempt_idx < retry_attempts
                        logger.warning(
                            "doc_intel_band_attempt_failed file_id=%s band_id=%s attempt=%s/%s retryable=%s error_type=%s",
                            file_id,
                            band.get("band_id"),
                            attempt_idx + 1,
                            max_attempts,
                            should_retry,
                            type(exc).__name__,
                        )
                        if not should_retry:
                            break
                        await asyncio.sleep(backoff_seconds * (2 ** attempt_idx))

                if band_output:
                    successful_bands.append(band_output)
                else:
                    failed_bands.append(
                        {
                            "band_id": band.get("band_id"),
                            "band_index": band.get("band_index"),
                            "coverage_zone": band.get("coverage_zone"),
                            "error_type": type(last_exc).__name__ if last_exc else "UnknownError",
                        }
                    )

            if not successful_bands:
                raise RuntimeError("All multipass band analyses failed")

            tensions = _detect_intra_document_tensions(successful_bands)
            pre_validation = _validate_multipass_quality(
                bands=bands,
                band_artifacts=successful_bands,
                profile=profile,
                failed_bands=failed_bands,
            )
            coverage_metadata = {
                "total_bands": pre_validation.get("total_bands"),
                "analyzed_bands": pre_validation.get("analyzed_bands"),
                "failed_bands_count": pre_validation.get("failed_bands_count"),
                "band_coverage_ratio": pre_validation.get("band_coverage_ratio"),
                "chunks_coverage_ratio": pre_validation.get("chunks_coverage_ratio"),
                "chars_coverage_ratio": pre_validation.get("chars_coverage_ratio"),
                "has_beginning_middle_end": pre_validation.get("has_beginning_middle_end"),
                "has_end_coverage": pre_validation.get("has_end_coverage"),
                "appendix_required": pre_validation.get("appendix_required"),
                "appendix_covered": pre_validation.get("appendix_covered"),
                "high_signal_ratio": pre_validation.get("high_signal_ratio"),
                "validation_reasons_so_far": pre_validation.get("validation_reasons"),
            }
            modality_metadata = {
                "ocr_present_in_document": bool(profile.get("ocr_present")),
                "vision_present_in_document": bool(profile.get("vision_present")),
                "ocr_included_in_analyzed_bands": bool(pre_validation.get("ocr_included")),
                "vision_included_in_analyzed_bands": bool(pre_validation.get("vision_included")),
            }
            synth_prompt = _build_multipass_synthesis_prompt(
                filename=filename,
                file_type=file_type,
                band_artifacts=successful_bands,
                tensions=tensions,
                coverage_metadata=coverage_metadata,
                modality_metadata=modality_metadata,
                failed_bands=failed_bands,
            )
            synth_output: Optional[Dict[str, Any]] = None
            synth_last_exc: Optional[Exception] = None
            synth_timeout = min(max_timeout_seconds, max(base_timeout, int(round(base_timeout * 2.6))))
            max_timeout_used = max(max_timeout_used, synth_timeout)
            for synth_attempt in range(min(4, max_attempts)):
                try:
                    synth_output = await _call_openai_document_intel(
                        prompt=synth_prompt,
                        model_name=settings.RILEY_DOC_INTEL_MODEL,
                        timeout_seconds=synth_timeout,
                    )
                    total_retry_count += synth_attempt
                    total_attempt_count += synth_attempt + 1
                    break
                except Exception as exc:
                    synth_last_exc = exc
                    should_retry = _is_retryable_doc_intel_error(exc) and synth_attempt < min(4, max_attempts) - 1
                    if not should_retry:
                        break
                    await asyncio.sleep(backoff_seconds * (2 ** synth_attempt))
            if synth_output is None:
                raise RuntimeError(
                    f"Multipass synthesis failed: {type(synth_last_exc).__name__ if synth_last_exc else 'UnknownError'}"
                )

            normalized = _normalize_analysis(synth_output)
            validation = _validate_multipass_quality(
                bands=bands,
                band_artifacts=successful_bands,
                profile=profile,
                failed_bands=failed_bands,
            )
            completed_at = datetime.now().isoformat()
            contradictions_count = len(tensions)
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
                    "analysis_execution_mode": "multi_pass",
                    "analysis_total_bands": int(validation["total_bands"]),
                    "analysis_analyzed_bands": int(validation["analyzed_bands"]),
                    "analysis_failed_bands_count": int(validation["failed_bands_count"]),
                    "analysis_band_coverage_ratio": float(validation["band_coverage_ratio"]),
                    "analysis_source_chunk_count": int(validation["analyzed_chunks"]),
                    "analysis_source_char_count": int(validation["analyzed_chars"]),
                    "analysis_total_chunks_available": int(validation["total_chunks"]),
                    "analysis_total_chars_available": int(validation["total_chars"]),
                    "analysis_chunks_coverage_ratio": float(validation["chunks_coverage_ratio"]),
                    "analysis_chars_coverage_ratio": float(validation["chars_coverage_ratio"]),
                    "analysis_fidelity_level": str(validation["fidelity"]),
                    "analysis_final_fidelity_level": str(validation["fidelity"]),
                    "analysis_retry_count_used": total_retry_count,
                    "analysis_attempt_count_used": max(1, total_attempt_count),
                    "analysis_timeout_seconds_used": max_timeout_used,
                    "analysis_selection_strategy": "multi_pass_band_synthesis",
                    "analysis_context_reduction_applied": str(validation["fidelity"]) != "full",
                    "analysis_ocr_content_available": bool(profile.get("ocr_present")),
                    "analysis_vision_content_available": bool(profile.get("vision_present")),
                    "analysis_ocr_content_included": bool(validation["ocr_included"]),
                    "analysis_vision_content_included": bool(validation["vision_included"]),
                    "analysis_contradiction_count": contradictions_count,
                    "analysis_validation_status": str(validation["validation_status"]),
                    "analysis_validation_note": str(validation["validation_note"]),
                    "analysis_validation_reasons_json": json.dumps(validation.get("validation_reasons") or []),
                    "analysis_high_signal_band_coverage_ratio": float(validation.get("high_signal_ratio") or 0.0),
                    "analysis_appendix_required": bool(validation.get("appendix_required")),
                    "analysis_appendix_covered": bool(validation.get("appendix_covered")),
                    "analysis_band_artifacts_json": json.dumps(successful_bands),
                    "analysis_intra_document_tensions_json": json.dumps(tensions),
                    "analysis_failed_bands_json": json.dumps(failed_bands),
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
                source_chunk_count=int(validation["analyzed_chunks"]),
                source_char_count=int(validation["analyzed_chars"]),
                analysis_fidelity_level=str(validation["fidelity"]),
                analysis_retry_count_used=total_retry_count,
                analysis_selection_strategy="multi_pass_band_synthesis",
                analysis_context_reduction_applied=str(validation["fidelity"]) != "full",
                chunks_coverage_ratio=float(validation["chunks_coverage_ratio"]),
                chars_coverage_ratio=float(validation["chars_coverage_ratio"]),
                ocr_content_included=bool(validation["ocr_included"]),
                vision_content_included=bool(validation["vision_included"]),
                analysis_execution_mode="multi_pass",
                total_bands=int(validation["total_bands"]),
                analyzed_bands=int(validation["analyzed_bands"]),
                band_coverage_ratio=float(validation["band_coverage_ratio"]),
                contradiction_count=contradictions_count,
                validation_status=str(validation["validation_status"]),
                validation_note=str(validation["validation_note"]),
                band_artifacts_json=json.dumps(successful_bands),
                intra_document_tensions_json=json.dumps(tensions),
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
                "doc_intel_complete file_id=%s tenant=%s mode=multi_pass bands=%s/%s failed_bands=%s "
                "chunks=%s/%s chars=%s/%s high_signal=%s appendix=%s/%s fidelity=%s",
                file_id,
                tenant_id,
                validation["analyzed_bands"],
                validation["total_bands"],
                validation["failed_bands_count"],
                validation["analyzed_chunks"],
                validation["total_chunks"],
                validation["analyzed_chars"],
                validation["total_chars"],
                validation["high_signal_ratio"],
                int(validation["appendix_analyzed"]),
                int(validation["appendix_total"]),
                validation["fidelity"],
            )
            return

        # Single-pass path (existing behavior preserved).
        retry_attempts = max(0, int(settings.RILEY_DOC_INTEL_RETRY_ATTEMPTS))
        max_attempts = retry_attempts + 1
        backoff_seconds = max(0.25, float(settings.RILEY_DOC_INTEL_RETRY_BACKOFF_SECONDS))
        max_timeout_seconds = max(
            int(settings.RILEY_DOC_INTEL_TIMEOUT_SECONDS),
            int(getattr(settings, "RILEY_DOC_INTEL_MAX_TIMEOUT_SECONDS", settings.RILEY_DOC_INTEL_TIMEOUT_SECONDS)),
        )
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
        source_chunk_count = 0
        source_char_count = 0
        context_stats: Dict[str, Any] = {}
        last_exc: Optional[Exception] = None
        retry_count_used = 0
        attempt_count_used = 0
        used_timeout_seconds = base_timeout
        for attempt_idx in range(max_attempts):
            plan = attempt_plan[min(attempt_idx, len(attempt_plan) - 1)]
            attempt_max_chunks = max(10, int(round(base_max_chunks * float(plan["chunks_scale"]))))
            attempt_max_chars = max(4000, int(round(base_max_chars * float(plan["chars_scale"]))))
            used_timeout_seconds = min(
                max_timeout_seconds,
                max(base_timeout, int(round(base_timeout * float(plan["timeout_scale"])))),
            )
            _, context_text, source_chunk_count, source_char_count, context_stats = await _load_document_context(
                collection_name=collection_name,
                file_id=file_id,
                max_chunks_override=attempt_max_chunks,
                max_chars_override=attempt_max_chars,
            )
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
                "analysis_execution_mode": "single_pass",
                "analysis_total_bands": 1,
                "analysis_analyzed_bands": 1,
                "analysis_band_coverage_ratio": 1.0,
                "analysis_source_chunk_count": source_chunk_count,
                "analysis_source_char_count": source_char_count,
                "analysis_total_chunks_available": int(context_stats.get("total_chunks_available") or source_chunk_count),
                "analysis_total_chars_available": int(context_stats.get("total_chars_available") or source_char_count),
                "analysis_chunks_coverage_ratio": chunks_coverage_ratio,
                "analysis_chars_coverage_ratio": chars_coverage_ratio,
                "analysis_fidelity_level": analysis_fidelity_level,
                "analysis_final_fidelity_level": analysis_fidelity_level,
                "analysis_retry_count_used": retry_count_used,
                "analysis_attempt_count_used": max(1, attempt_count_used),
                "analysis_timeout_seconds_used": used_timeout_seconds,
                "analysis_selection_strategy": str(context_stats.get("selection_strategy") or "unknown"),
                "analysis_context_reduction_applied": bool(context_stats.get("selection_applied")),
                "analysis_ocr_content_available": bool(context_stats.get("ocr_content_available")),
                "analysis_vision_content_available": bool(context_stats.get("vision_content_available")),
                "analysis_ocr_content_included": bool(context_stats.get("ocr_content_included")),
                "analysis_vision_content_included": bool(context_stats.get("vision_content_included")),
                "analysis_contradiction_count": 0,
                "analysis_validation_status": "passed" if analysis_fidelity_level == "full" else "passed_with_reduction",
                "analysis_validation_note": "Single-pass document intelligence completed.",
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
            analysis_execution_mode="single_pass",
            total_bands=1,
            analyzed_bands=1,
            band_coverage_ratio=1.0,
            contradiction_count=0,
            validation_status="passed" if analysis_fidelity_level == "full" else "passed_with_reduction",
            validation_note="Single-pass document intelligence completed.",
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
            "doc_intel_complete file_id=%s tenant=%s mode=single_pass chunks=%s/%s chars=%s/%s fidelity=%s",
            file_id,
            tenant_id,
            source_chunk_count,
            int(context_stats.get("total_chunks_available") or source_chunk_count),
            source_char_count,
            int(context_stats.get("total_chars_available") or source_char_count),
            analysis_fidelity_level,
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
                "analysis_execution_mode": "failed",
                "analysis_validation_status": "failed",
                "analysis_validation_note": "Document intelligence job failed before completion.",
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
            analysis_execution_mode="failed",
            validation_status="failed",
            validation_note="Document intelligence job failed before completion.",
        )
        await _enqueue_campaign_intel_refresh_if_needed(
            settings=settings,
            graph=graph,
            tenant_id=tenant_id,
            is_global=is_global,
            trigger_source="doc_intel_failed",
            file_id=file_id,
        )
