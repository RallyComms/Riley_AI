import hmac
import logging
import time
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.core.config import get_settings
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService
from app.services.pricing_registry import estimate_worker_runtime_cost
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
    started = time.perf_counter()
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
        try:
            duration_ms = int((time.perf_counter() - started) * 1000)
            runtime_cost = estimate_worker_runtime_cost(duration_ms=duration_ms)
            await graph.append_analytics_event(
                event_id=f"worker_runtime_completed:report:{payload.report_job_id}",
                source_event_type_raw="worker_runtime_completed",
                source_entity="ReportWorker",
                object_id=payload.report_job_id,
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
                    "worker": "report_worker",
                    "requests_count": 1,
                    "report_job_id": payload.report_job_id,
                },
            )
        except Exception:
            pass
    except Exception as exc:
        try:
            await graph.append_analytics_event(
                event_id=f"worker_failed:report:{payload.report_job_id}",
                source_event_type_raw="worker_failed",
                source_entity="ReportWorker",
                object_id=payload.report_job_id,
                status="failed",
                metadata={
                    "worker": "report_worker",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:240],
                },
            )
        except Exception:
            pass
        logger.exception(
            "riley_report_worker_request_failed report_job_id=%s error_type=%s error_message=%s",
            payload.report_job_id,
            type(exc).__name__,
            str(exc)[:240],
        )
        raise
    return {"status": "ok"}
