import hmac
import logging
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.core.config import get_settings
from app.services.ingestion import run_ingestion_job
from app.services.analytics_contract import infer_provider_from_model
from app.services.pricing_registry import estimate_single_unit_cost
from app.services.qdrant import vector_service


router = APIRouter()
logger = logging.getLogger(__name__)


class IngestionWorkerRequest(BaseModel):
    job_id: str
    file_id: str
    collection_name: str


@router.post("/internal/ingestion/run")
async def run_ingestion_worker(
    payload: IngestionWorkerRequest,
    request: Request,
    x_ingestion_worker_token: Optional[str] = Header(None),
) -> Dict[str, str]:
    """Cloud Tasks worker endpoint for durable ingestion execution."""
    settings = get_settings()
    expected_token = settings.INGESTION_WORKER_TOKEN
    if not expected_token:
        raise HTTPException(status_code=500, detail="INGESTION_WORKER_TOKEN is not configured")

    if not x_ingestion_worker_token or not hmac.compare_digest(
        x_ingestion_worker_token, expected_token
    ):
        raise HTTPException(status_code=401, detail="Invalid ingestion worker token")

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
            except Exception:
                pass
    except Exception as exc:
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
