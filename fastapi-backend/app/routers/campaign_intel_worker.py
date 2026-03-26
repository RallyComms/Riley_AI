import hmac
import logging
import time
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.core.config import get_settings
from app.dependencies.graph_dep import get_graph
from app.services.campaign_intelligence import run_campaign_intelligence_job
from app.services.graph import GraphService
from app.services.pricing_registry import estimate_worker_runtime_cost

router = APIRouter()
logger = logging.getLogger(__name__)


class RileyCampaignIntelWorkerRequest(BaseModel):
    job_id: str


@router.post("/internal/campaign-intelligence/run")
async def run_riley_campaign_intelligence_worker(
    payload: RileyCampaignIntelWorkerRequest,
    request: Request,
    x_riley_campaign_intel_worker_token: Optional[str] = Header(None),
) -> Dict[str, str]:
    """Cloud Tasks worker endpoint for durable campaign intelligence aggregation."""
    started = time.perf_counter()
    settings = get_settings()
    expected_token = settings.RILEY_CAMPAIGN_INTEL_WORKER_TOKEN
    if not expected_token:
        raise HTTPException(status_code=500, detail="RILEY_CAMPAIGN_INTEL_WORKER_TOKEN is not configured")

    if not x_riley_campaign_intel_worker_token or not hmac.compare_digest(
        x_riley_campaign_intel_worker_token, expected_token
    ):
        raise HTTPException(status_code=401, detail="Invalid campaign intelligence worker token")

    graph: GraphService = await get_graph(request)
    try:
        await run_campaign_intelligence_job(job_id=payload.job_id, graph=graph)
        try:
            duration_ms = int((time.perf_counter() - started) * 1000)
            runtime_cost = estimate_worker_runtime_cost(duration_ms=duration_ms)
            await graph.append_analytics_event(
                event_id=f"worker_runtime_completed:campaign_intel:{payload.job_id}",
                source_event_type_raw="worker_runtime_completed",
                source_entity="CampaignIntelligenceWorker",
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
                    "worker": "campaign_intel_worker",
                    "requests_count": 1,
                    "job_id": payload.job_id,
                },
            )
        except Exception:
            pass
    except Exception as exc:
        logger.exception(
            "campaign_intel_worker_request_failed job_id=%s error=%s",
            payload.job_id,
            exc,
        )
        raise

    return {"status": "ok"}
