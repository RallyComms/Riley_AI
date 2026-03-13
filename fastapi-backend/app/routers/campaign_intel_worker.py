import hmac
import logging
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.core.config import get_settings
from app.dependencies.graph_dep import get_graph
from app.services.campaign_intelligence import run_campaign_intelligence_job
from app.services.graph import GraphService

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
    except Exception as exc:
        logger.exception(
            "campaign_intel_worker_request_failed job_id=%s error=%s",
            payload.job_id,
            exc,
        )
        raise

    return {"status": "ok"}
