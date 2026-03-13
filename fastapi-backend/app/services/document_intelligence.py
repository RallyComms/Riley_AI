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
) -> Tuple[Dict[str, Any], str, int, int]:
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

    max_chunks = max(10, int(settings.RILEY_DOC_INTEL_MAX_CHUNKS))
    max_chars = max(4000, int(settings.RILEY_DOC_INTEL_MAX_CONTEXT_CHARS))
    lines: List[str] = []
    chars_used = 0
    used_chunks = 0
    for payload in chunk_payloads:
        if used_chunks >= max_chunks:
            break
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
        used_chunks += 1

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

    return parent_payload, "\n\n".join(lines), used_chunks, chars_used


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
        parent_payload, context_text, source_chunk_count, source_char_count = await _load_document_context(
            collection_name=collection_name,
            file_id=file_id,
        )
        filename = str(parent_payload.get("filename") or file_id)
        file_type = str(parent_payload.get("file_type") or parent_payload.get("type") or "unknown")
        prompt = _build_doc_intel_prompt(
            filename=filename,
            file_type=file_type,
            context_text=context_text,
        )
        raw_output = await _call_openai_document_intel(
            prompt=prompt,
            model_name=settings.RILEY_DOC_INTEL_MODEL,
            timeout_seconds=settings.RILEY_DOC_INTEL_TIMEOUT_SECONDS,
        )
        normalized = _normalize_analysis(raw_output)
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
        )
        if (
            settings.RILEY_CAMPAIGN_INTEL_ENABLED
            and graph is not None
            and not is_global
        ):
            try:
                from app.services.campaign_intelligence import enqueue_campaign_intelligence_job

                await enqueue_campaign_intelligence_job(
                    graph=graph,
                    tenant_id=tenant_id,
                    requested_by_user_id=None,
                    trigger_source="doc_indexed",
                )
            except Exception as campaign_exc:
                logger.warning(
                    "campaign_intel_enqueue_failed file_id=%s tenant=%s error=%s",
                    file_id,
                    tenant_id,
                    campaign_exc,
                )
        logger.info(
            "doc_intel_complete file_id=%s tenant=%s chunks=%s chars=%s",
            file_id,
            tenant_id,
            source_chunk_count,
            source_char_count,
        )
    except Exception as exc:
        completed_at = datetime.now().isoformat()
        error_message = f"{type(exc).__name__}: {exc}"
        logger.exception(
            "doc_intel_failed file_id=%s tenant=%s job_id=%s error=%s",
            file_id,
            tenant_id,
            job_id,
            exc,
        )
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={
                "analysis_status": "failed",
                "analysis_completed_at": completed_at,
                "analysis_error": error_message[:2000],
                "analysis_job_status": "failed",
                "analysis_job_completed_at": completed_at,
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
