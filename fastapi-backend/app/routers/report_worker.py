import hmac
import logging
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.core.config import get_settings
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService
from app.services.riley_reports import run_report_job

router = APIRouter()
logger = logging.getLogger(__name__)


class RileyReportWorkerRequest(BaseModel):
    report_job_id: str


@router.post("/internal/reports/run")
async def run_riley_report_worker(
    payload: RileyReportWorkerRequest,
    request: Request,
    x_riley_report_worker_token: Optional[str] = Header(None),
) -> Dict[str, str]:
    """Cloud Tasks worker endpoint for durable Riley report execution."""
    settings = get_settings()
    expected_token = settings.RILEY_REPORT_WORKER_TOKEN
    if not expected_token:
        raise HTTPException(status_code=500, detail="RILEY_REPORT_WORKER_TOKEN is not configured")

    if not x_riley_report_worker_token or not hmac.compare_digest(
        x_riley_report_worker_token, expected_token
    ):
        raise HTTPException(status_code=401, detail="Invalid report worker token")

    graph: GraphService = await get_graph(request)
    try:
        logger.info("report_worker_execution_started report_job_id=%s", payload.report_job_id)
        await run_report_job(report_job_id=payload.report_job_id, graph=graph)
        logger.info("report_worker_execution_completed report_job_id=%s", payload.report_job_id)
    except Exception as exc:
        logger.exception(
            "riley_report_worker_request_failed report_job_id=%s error_type=%s error_message=%s",
            payload.report_job_id,
            type(exc).__name__,
            str(exc)[:240],
        )
        raise
    return {"status": "ok"}
