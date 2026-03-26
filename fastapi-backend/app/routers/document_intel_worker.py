import hmac
import logging
import time
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.core.config import get_settings
from app.dependencies.graph_dep import get_graph_optional
from app.services.document_intelligence import run_document_intelligence_job
from app.services.graph import GraphService
from app.services.pricing_registry import estimate_worker_runtime_cost

router = APIRouter()
logger = logging.getLogger(__name__)


class RileyDocIntelWorkerRequest(BaseModel):
    job_id: str
    file_id: str
    collection_name: str
    tenant_id: str
    is_global: bool = False


@router.post("/internal/document-intelligence/run")
async def run_riley_document_intelligence_worker(
    payload: RileyDocIntelWorkerRequest,
    request: Request,
    x_riley_doc_intel_worker_token: Optional[str] = Header(None),
) -> Dict[str, str]:
    """Cloud Tasks worker endpoint for durable document intelligence execution."""
    started = time.perf_counter()
    settings = get_settings()
    expected_token = settings.RILEY_DOC_INTEL_WORKER_TOKEN
    if not expected_token:
        raise HTTPException(status_code=500, detail="RILEY_DOC_INTEL_WORKER_TOKEN is not configured")

    if not x_riley_doc_intel_worker_token or not hmac.compare_digest(
        x_riley_doc_intel_worker_token, expected_token
    ):
        raise HTTPException(status_code=401, detail="Invalid document intelligence worker token")

    graph: Optional[GraphService] = await get_graph_optional(request)
    try:
        await run_document_intelligence_job(
            job_id=payload.job_id,
            file_id=payload.file_id,
            collection_name=payload.collection_name,
            tenant_id=payload.tenant_id,
            is_global=payload.is_global,
            graph=graph,
        )
        if graph is not None:
            try:
                duration_ms = int((time.perf_counter() - started) * 1000)
                runtime_cost = estimate_worker_runtime_cost(duration_ms=duration_ms)
                await graph.append_analytics_event(
                    event_id=f"worker_runtime_completed:document_intel:{payload.job_id}:{payload.file_id}",
                    source_event_type_raw="worker_runtime_completed",
                    source_entity="DocumentIntelligenceWorker",
                    campaign_id=payload.tenant_id,
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
                        "worker": "document_intel_worker",
                        "requests_count": 1,
                        "job_id": payload.job_id,
                        "file_id": payload.file_id,
                    },
                )
            except Exception:
                pass
    except Exception as exc:
        logger.exception(
            "document_intel_worker_request_failed job_id=%s file_id=%s collection=%s tenant=%s error=%s",
            payload.job_id,
            payload.file_id,
            payload.collection_name,
            payload.tenant_id,
            exc,
        )
        raise

    return {"status": "ok"}
