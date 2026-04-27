import hmac
import logging
import time
import uuid
from typing import Dict, Optional

from fastapi import APIRouter, Header, Request
from pydantic import BaseModel

from app.core.config import get_settings
from app.services.ingestion import IngestionPermanentFailure, run_ingestion_job
from app.services.analytics_contract import infer_provider_from_model
from app.services.pricing_registry import estimate_single_unit_cost, estimate_worker_runtime_cost
from app.services.qdrant import vector_service


router = APIRouter()
logger = logging.getLogger(__name__)


class IngestionWorkerRequest(BaseModel):
    job_id: str
    file_id: str
    collection_name: str


async def _emit_worker_failure_event(
    request: Request,
    *,
    event_key: str,
    reason: str,
    payload: Optional[IngestionWorkerRequest],
    include_file_context: bool,
) -> None:
    graph = getattr(request.app.state, "graph", None)
    if graph is None:
        return
    try:
        await graph.append_analytics_event(
            event_id=f"{event_key}:{int(time.time() * 1000)}:{uuid.uuid4().hex[:8]}",
            source_event_type_raw="worker_failed",
            source_entity="IngestionWorker",
            campaign_id=None,
            object_id=(payload.file_id if (payload and include_file_context) else None),
            status="failed",
            metadata={
                "service": "ingestion_worker",
                "worker": "ingestion_worker",
                "severity": "critical",
                "failure_event_key": event_key,
                "failure_reason": reason,
                "permanent_failure": True,
                "job_id": (payload.job_id if payload else None),
                "collection_name": (payload.collection_name if payload else None),
                "unverified_payload_file_id": (
                    payload.file_id if payload and not include_file_context else None
                ),
                "error_message": reason,
            },
        )
    except Exception:
        pass


@router.post("/internal/ingestion/run")
async def run_ingestion_worker(
    payload: IngestionWorkerRequest,
    request: Request,
    x_ingestion_worker_token: Optional[str] = Header(None),
) -> Dict[str, str]:
    """Cloud Tasks worker endpoint for durable ingestion execution."""
    started = time.perf_counter()
    settings = get_settings()
    expected_token = settings.INGESTION_WORKER_TOKEN
    if not expected_token:
        event_key = "ingestion_worker_token_misconfigured"
        await _emit_worker_failure_event(
            request,
            event_key=event_key,
            reason="INGESTION_WORKER_TOKEN is not configured; worker auth cannot be validated.",
            payload=payload,
            include_file_context=False,
        )
        logger.error(
            "ingestion_worker_token_misconfigured reason=worker_token_not_configured job_id=%s file_id=%s collection=%s",
            payload.job_id,
            payload.file_id,
            payload.collection_name,
        )
        return {"status": "permanent_failure", "reason": "worker_token_not_configured"}

    if not x_ingestion_worker_token or not hmac.compare_digest(
        x_ingestion_worker_token, expected_token
    ):
        event_key = "ingestion_worker_auth_permanent_failure"
        await _emit_worker_failure_event(
            request,
            event_key=event_key,
            reason="Worker token header missing or invalid; rejecting ingestion worker request.",
            payload=payload,
            include_file_context=False,
        )
        logger.error(
            "ingestion_worker_auth_permanent_failure reason=invalid_worker_token job_id=%s file_id=%s collection=%s",
            payload.job_id,
            payload.file_id,
            payload.collection_name,
        )
        return {"status": "permanent_failure", "reason": "invalid_worker_token"}

    try:
        await run_ingestion_job(
            job_id=payload.job_id,
            file_id=payload.file_id,
            collection_name=payload.collection_name,
        )
        graph = getattr(request.app.state, "graph", None)
        if graph is not None:
            try:
                settings = get_settings()
                point_records = await vector_service.client.retrieve(
                    collection_name=payload.collection_name,
                    ids=[payload.file_id],
                    with_payload=True,
                    with_vectors=False,
                )
                point_payload = (point_records[0].payload or {}) if point_records else {}
                embedding_model = str(point_payload.get("embedding_model") or settings.EMBEDDING_MODEL)
                embedding_tokens = int(point_payload.get("embedding_tokens_estimate") or 0)
                embedded_cost = point_payload.get("embedding_cost_estimate_usd")
                pricing_meta = estimate_single_unit_cost(
                    service="embedding",
                    provider=infer_provider_from_model(embedding_model) or "google_gemini",
                    model=embedding_model,
                    unit_type="input_token_1k",
                    quantity=embedding_tokens,
                )
                await graph.append_analytics_event(
                    event_id=f"embedding_indexing_completed:{payload.file_id}:{payload.job_id}",
                    source_event_type_raw="embedding_indexing_completed",
                    source_entity="IngestionWorker",
                    campaign_id=(
                        point_payload.get("client_id")
                        or ("global" if bool(point_payload.get("is_global")) else None)
                    ),
                    object_id=payload.file_id,
                    status=str(point_payload.get("ingestion_status") or "unknown"),
                    provider=infer_provider_from_model(embedding_model) or "google_gemini",
                    model=embedding_model,
                    latency_ms=int(point_payload.get("processing_duration_ms") or 0),
                    cost_estimate_usd=(
                        float(embedded_cost)
                        if embedded_cost is not None
                        else (
                            float(pricing_meta["cost_estimate_usd"])
                            if pricing_meta.get("cost_estimate_usd") is not None
                            else None
                        )
                    ),
                    pricing_version=str(pricing_meta.get("pricing_version") or "cost-accounting-v1"),
                    cost_confidence="estimated_units",
                    metadata={
                        "service": "embedding",
                        "input_tokens": embedding_tokens,
                        "output_tokens": 0,
                        "total_tokens": embedding_tokens,
                        "requests_count": max(1, int(point_payload.get("chunk_count") or 0)),
                        "chunk_count": int(point_payload.get("chunk_count") or 0),
                        "file_id": payload.file_id,
                        "job_id": payload.job_id,
                    },
                )
                if bool(point_payload.get("ocr_enabled")) and str(point_payload.get("ocr_status") or "").lower() == "complete":
                    file_type = str(point_payload.get("file_type") or point_payload.get("type") or "").lower()
                    ingestion_decision = point_payload.get("ingestion_decision") if isinstance(point_payload.get("ingestion_decision"), dict) else {}
                    pages_processed = int(
                        ingestion_decision.get("nonempty_page_count")
                        or ingestion_decision.get("total_page_count")
                        or 0
                    )
                    images_processed = 1 if file_type in {"png", "jpg", "jpeg", "webp", "tiff"} else 0
                    ocr_quantity = pages_processed if pages_processed > 0 else max(1, images_processed)
                    ocr_unit_type = "page" if pages_processed > 0 else "request"
                    ocr_cost = estimate_single_unit_cost(
                        service="ocr",
                        provider="google_cloud_vision",
                        model="document_text_detection",
                        unit_type=ocr_unit_type,
                        quantity=ocr_quantity,
                    )
                    await graph.append_analytics_event(
                        event_id=f"ocr_completed:ingestion:{payload.file_id}:{payload.job_id}",
                        source_event_type_raw="ocr_completed",
                        source_entity="IngestionWorker",
                        campaign_id=(
                            point_payload.get("client_id")
                            or ("global" if bool(point_payload.get("is_global")) else None)
                        ),
                        object_id=payload.file_id,
                        status="succeeded",
                        provider="google_cloud_vision",
                        model="document_text_detection",
                        cost_estimate_usd=(
                            float(ocr_cost["cost_estimate_usd"])
                            if ocr_cost.get("cost_estimate_usd") is not None
                            else None
                        ),
                        pricing_version=str(ocr_cost.get("pricing_version") or "cost-accounting-v1"),
                        cost_confidence="estimated_units",
                        metadata={
                            "service": "ocr",
                            "pages_processed": pages_processed,
                            "images_processed": images_processed,
                            "requests_count": 1,
                            "ocr_confidence": point_payload.get("ocr_confidence"),
                            "file_id": payload.file_id,
                            "job_id": payload.job_id,
                        },
                    )
            except Exception:
                pass
        if graph is not None:
            try:
                duration_ms = int((time.perf_counter() - started) * 1000)
                runtime_cost = estimate_worker_runtime_cost(duration_ms=duration_ms)
                await graph.append_analytics_event(
                    event_id=f"worker_runtime_completed:ingestion:{payload.job_id}:{payload.file_id}",
                    source_event_type_raw="worker_runtime_completed",
                    source_entity="IngestionWorker",
                    object_id=payload.job_id,
                    status="succeeded",
                    provider="cloud_run",
                    model="default_worker_profile",
                    latency_ms=duration_ms,
                    cost_estimate_usd=(
                        float(runtime_cost["cost_estimate_usd"])
                        if runtime_cost.get("cost_estimate_usd") is not None
                        else None
                    ),
                    pricing_version=str(runtime_cost.get("pricing_version") or "cost-accounting-v1"),
                    cost_confidence="proxy_only",
                    metadata={
                        "service": "worker_runtime",
                        "duration_ms": duration_ms,
                        "worker": "ingestion_worker",
                        "requests_count": 1,
                        "job_id": payload.job_id,
                    },
                )
            except Exception:
                pass
    except IngestionPermanentFailure as exc:
        point_payload: Dict[str, object] = {}
        try:
            point_records = await vector_service.client.retrieve(
                collection_name=payload.collection_name,
                ids=[payload.file_id],
                with_payload=True,
                with_vectors=False,
            )
            point_payload = (point_records[0].payload or {}) if point_records else {}
        except Exception:
            point_payload = {}
        logger.error(
            "ingestion_permanent_failure reason=%s job_id=%s file_id=%s collection=%s",
            getattr(exc, "reason", "permanent_failure"),
            payload.job_id,
            payload.file_id,
            payload.collection_name,
        )
        graph = getattr(request.app.state, "graph", None)
        if graph is not None:
            try:
                await graph.append_analytics_event(
                    event_id=f"ingestion_failed:{payload.file_id}:{payload.job_id}:{getattr(exc, 'reason', 'permanent_failure')}",
                    source_event_type_raw="ingestion_failed",
                    source_entity="IngestionWorker",
                    campaign_id=None,
                    object_id=payload.file_id,
                    status="failed",
                    metadata={
                        "worker": "ingestion_worker",
                        "job_id": payload.job_id,
                        "collection_name": payload.collection_name,
                        "failure_reason": str(getattr(exc, "reason", "permanent_failure")),
                        "error_type": "IngestionPermanentFailure",
                        "permanent_failure": True,
                        "error_message": str(point_payload.get("ingestion_error") or "")[:500] or None,
                        "ingestion_failure_reason": (
                            str(point_payload.get("ingestion_failure_reason") or "").strip() or None
                        ),
                        "ingestion_previous_failure_reason": (
                            str(point_payload.get("ingestion_previous_failure_reason") or "").strip() or None
                        ),
                        "ingestion_previous_error": (
                            str(point_payload.get("ingestion_previous_error") or "").strip()[:500] or None
                        ),
                    },
                )
            except Exception:
                pass
        return {"status": "permanent_failure", "reason": getattr(exc, "reason", "permanent_failure")}
    except Exception as exc:
        logger.warning(
            "ingestion_retryable_failure job_id=%s file_id=%s collection=%s error_type=%s error=%s",
            payload.job_id,
            payload.file_id,
            payload.collection_name,
            type(exc).__name__,
            exc,
        )
        graph = getattr(request.app.state, "graph", None)
        if graph is not None:
            try:
                await graph.append_analytics_event(
                    event_id=f"ingestion_failed:{payload.file_id}:{payload.job_id}:{type(exc).__name__}",
                    source_event_type_raw="ingestion_failed",
                    source_entity="IngestionWorker",
                    campaign_id=None,
                    object_id=payload.file_id,
                    status="failed",
                    metadata={
                        "worker": "ingestion_worker",
                        "job_id": payload.job_id,
                        "collection_name": payload.collection_name,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc)[:240],
                    },
                )
            except Exception:
                pass
        logger.exception(
            "ingestion_worker_request_failed job_id=%s file_id=%s collection=%s error=%s",
            payload.job_id,
            payload.file_id,
            payload.collection_name,
            exc,
        )
        raise
    return {"status": "ok"}
