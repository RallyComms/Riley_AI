import logging
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import PlainTextResponse, RedirectResponse, Response
from google.api_core.exceptions import NotFound
from google.cloud import tasks_v2
from pydantic import BaseModel, Field

from app.core.config import get_settings
from app.dependencies.auth import check_tenant_membership, verify_clerk_token
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService
from app.services.riley_reports import create_report_job

router = APIRouter()
logger = logging.getLogger(__name__)


class CreateRileyReportRequest(BaseModel):
    tenant_id: str = Field(..., max_length=50)
    query: str = Field(..., min_length=3, max_length=12000)
    conversation_id: Optional[str] = Field(None, max_length=200)
    report_type: str = Field(default="strategy_memo", max_length=100)
    title: Optional[str] = Field(None, max_length=180)
    mode: Literal["normal", "deep"] = "deep"


class RileyReportJobResponse(BaseModel):
    report_job_id: str
    tenant_id: str
    user_id: str
    conversation_id: Optional[str] = None
    report_type: str
    title: str
    status: Literal["queued", "processing", "cancelling", "cancelled", "complete", "failed", "deleted"]
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    cancel_requested_at: Optional[str] = None
    cancelled_at: Optional[str] = None
    deleted_at: Optional[str] = None
    cancellation_reason: Optional[str] = None
    error_message: Optional[str] = None
    output_file_id: Optional[str] = None
    output_url: Optional[str] = None
    summary_text: Optional[str] = None
    report_fidelity_level: Optional[str] = None
    report_context_reduction_applied: Optional[bool] = None
    report_context_strategy: Optional[str] = None
    retrieval_doc_count: Optional[int] = None
    retrieval_chunk_count: Optional[int] = None
    context_chars_included: Optional[int] = None
    generation_model: Optional[str] = None
    generation_attempts_used: Optional[int] = None
    failure_stage: Optional[str] = None
    failure_code: Optional[str] = None
    failure_detail: Optional[str] = None
    query: str
    mode: Literal["normal", "deep"]
    report_body: Optional[str] = None


class RileyReportJobsListResponse(BaseModel):
    jobs: List[RileyReportJobResponse]


class RileyReportLifecycleResponse(BaseModel):
    report_job_id: str
    status: str
    cancellation_reason: Optional[str] = None
    cancel_requested_at: Optional[str] = None
    cancelled_at: Optional[str] = None
    deleted_at: Optional[str] = None


async def _best_effort_delete_report_task(
    *,
    report_job_id: str,
    tenant_id: str,
    user_id: str,
) -> None:
    settings = get_settings()
    if not settings.RILEY_REPORTS_USE_CLOUD_TASKS:
        return
    if not settings.GCP_PROJECT_ID:
        logger.warning(
            "report_task_delete_failed report_job_id=%s tenant_id=%s user_id=%s error_type=ConfigError error_message=GCP_PROJECT_ID missing",
            report_job_id,
            tenant_id,
            user_id,
        )
        return

    queue = settings.RILEY_REPORTS_TASKS_QUEUE
    location = settings.RILEY_REPORTS_TASKS_LOCATION

    def _delete_task_sync() -> str:
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(settings.GCP_PROJECT_ID, location, queue)
        task_name = f"{parent}/tasks/{report_job_id}"
        logger.info(
            "report_task_delete_attempted report_job_id=%s tenant_id=%s user_id=%s task_name=%s",
            report_job_id,
            tenant_id,
            user_id,
            task_name,
        )
        try:
            client.delete_task(name=task_name)
            return "deleted"
        except NotFound:
            return "missing"

    try:
        result = await run_in_threadpool(_delete_task_sync)
        if result == "deleted":
            logger.info(
                "report_task_delete_succeeded report_job_id=%s tenant_id=%s user_id=%s",
                report_job_id,
                tenant_id,
                user_id,
            )
        else:
            logger.info(
                "report_task_delete_missing report_job_id=%s tenant_id=%s user_id=%s",
                report_job_id,
                tenant_id,
                user_id,
            )
    except Exception as exc:
        logger.warning(
            "report_task_delete_failed report_job_id=%s tenant_id=%s user_id=%s error_type=%s error_message=%s",
            report_job_id,
            tenant_id,
            user_id,
            type(exc).__name__,
            str(exc)[:240],
        )


@router.post("/riley/reports", response_model=RileyReportJobResponse)
async def create_riley_report(
    request: CreateRileyReportRequest,
    http_request: Request,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyReportJobResponse:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id, http_request)
    active_count = await graph.count_active_riley_report_jobs_for_tenant(tenant_id=request.tenant_id)
    if active_count >= 1:
        raise HTTPException(
            status_code=409,
            detail="A report is already running for this campaign. Please wait for it to finish or cancel it before starting another.",
        )
    try:
        job = await create_report_job(
            graph=graph,
            tenant_id=request.tenant_id,
            user_id=user_id,
            query_text=request.query,
            mode=request.mode,
            report_type=request.report_type,
            title=request.title,
            conversation_id=request.conversation_id,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create report job: {type(exc).__name__}",
        ) from exc
    return RileyReportJobResponse(**job)


@router.get("/riley/reports", response_model=RileyReportJobsListResponse)
async def list_riley_reports(
    http_request: Request,
    tenant_id: str = Query(..., max_length=50),
    limit: int = Query(50, ge=1, le=200),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyReportJobsListResponse:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)
    jobs = await graph.list_riley_report_jobs(
        tenant_id=tenant_id,
        user_id=user_id,
        limit=limit,
    )
    return RileyReportJobsListResponse(jobs=[RileyReportJobResponse(**job) for job in jobs])


@router.get("/riley/reports/{report_job_id}", response_model=RileyReportJobResponse)
async def get_riley_report(
    report_job_id: str,
    http_request: Request,
    tenant_id: str = Query(..., max_length=50),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyReportJobResponse:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)
    job = await graph.get_riley_report_job(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    if not job:
        raise HTTPException(status_code=404, detail="Report job not found")
    return RileyReportJobResponse(**job)


@router.get("/riley/reports/{report_job_id}/download", response_model=None)
async def download_riley_report(
    report_job_id: str,
    http_request: Request,
    tenant_id: str = Query(..., max_length=50),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> Response:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)
    job = await graph.get_riley_report_job(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    if not job:
        raise HTTPException(status_code=404, detail="Report job not found")
    if job.get("status") != "complete":
        raise HTTPException(status_code=409, detail="Report is not complete yet")
    output_url = (job.get("output_url") or "").strip()
    if output_url:
        return RedirectResponse(url=output_url, status_code=307)
    report_body = (job.get("report_body") or "").strip()
    if not report_body:
        raise HTTPException(status_code=404, detail="Report artifact is not available")
    return PlainTextResponse(
        content=report_body,
        media_type="text/markdown",
        headers={"Content-Disposition": f'attachment; filename="riley-report-{report_job_id}.md"'},
    )


@router.post("/riley/reports/{report_job_id}/cancel", response_model=RileyReportLifecycleResponse)
async def cancel_riley_report(
    report_job_id: str,
    http_request: Request,
    tenant_id: str = Query(..., max_length=50),
    reason: Optional[str] = Query(None, max_length=200),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyReportLifecycleResponse:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)
    logger.info(
        "report_cancel_requested report_job_id=%s tenant_id=%s user_id=%s",
        report_job_id,
        tenant_id,
        user_id,
    )

    job = await graph.get_riley_report_job(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    if not job:
        raise HTTPException(status_code=404, detail="Report job not found")

    status = str(job.get("status") or "").lower()
    if status in {"complete", "failed", "cancelled", "deleted"}:
        await _best_effort_delete_report_task(
            report_job_id=report_job_id,
            tenant_id=tenant_id,
            user_id=user_id,
        )
        logger.info(
            "report_cancel_marked report_job_id=%s tenant_id=%s user_id=%s status=%s idempotent=true",
            report_job_id,
            tenant_id,
            user_id,
            status,
        )
        return RileyReportLifecycleResponse(
            report_job_id=report_job_id,
            status=status,
            cancellation_reason=job.get("cancellation_reason"),
            cancel_requested_at=job.get("cancel_requested_at"),
            cancelled_at=job.get("cancelled_at"),
            deleted_at=job.get("deleted_at"),
        )

    now = datetime.now().isoformat()
    cancellation_reason = (reason or "user_requested_cancel").strip()[:200]
    await graph.update_riley_report_job(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
        status="cancelled",
        cancel_requested_at=now,
        cancelled_at=now,
        cancellation_reason=cancellation_reason,
    )
    await _best_effort_delete_report_task(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    updated = await graph.get_riley_report_job(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    logger.info(
        "report_cancel_marked report_job_id=%s tenant_id=%s user_id=%s status=cancelled idempotent=false",
        report_job_id,
        tenant_id,
        user_id,
    )
    return RileyReportLifecycleResponse(
        report_job_id=report_job_id,
        status=str((updated or {}).get("status") or "cancelled"),
        cancellation_reason=(updated or {}).get("cancellation_reason"),
        cancel_requested_at=(updated or {}).get("cancel_requested_at"),
        cancelled_at=(updated or {}).get("cancelled_at"),
        deleted_at=(updated or {}).get("deleted_at"),
    )


@router.delete("/riley/reports/{report_job_id}", response_model=RileyReportLifecycleResponse)
async def delete_riley_report(
    report_job_id: str,
    http_request: Request,
    tenant_id: str = Query(..., max_length=50),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyReportLifecycleResponse:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)
    logger.info(
        "report_delete_requested report_job_id=%s tenant_id=%s user_id=%s",
        report_job_id,
        tenant_id,
        user_id,
    )

    job = await graph.get_riley_report_job(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    if not job:
        raise HTTPException(status_code=404, detail="Report job not found")

    status = str(job.get("status") or "").lower()
    if status == "deleted":
        await _best_effort_delete_report_task(
            report_job_id=report_job_id,
            tenant_id=tenant_id,
            user_id=user_id,
        )
        logger.info(
            "report_marked_deleted report_job_id=%s tenant_id=%s user_id=%s idempotent=true",
            report_job_id,
            tenant_id,
            user_id,
        )
        return RileyReportLifecycleResponse(
            report_job_id=report_job_id,
            status="deleted",
            cancellation_reason=job.get("cancellation_reason"),
            cancel_requested_at=job.get("cancel_requested_at"),
            cancelled_at=job.get("cancelled_at"),
            deleted_at=job.get("deleted_at"),
        )

    now = datetime.now().isoformat()
    await graph.update_riley_report_job(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
        status="deleted",
        deleted_at=now,
    )
    await _best_effort_delete_report_task(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    updated = await graph.get_riley_report_job(
        report_job_id=report_job_id,
        tenant_id=tenant_id,
        user_id=user_id,
    )
    logger.info(
        "report_marked_deleted report_job_id=%s tenant_id=%s user_id=%s idempotent=false",
        report_job_id,
        tenant_id,
        user_id,
    )
    return RileyReportLifecycleResponse(
        report_job_id=report_job_id,
        status=str((updated or {}).get("status") or "deleted"),
        cancellation_reason=(updated or {}).get("cancellation_reason"),
        cancel_requested_at=(updated or {}).get("cancel_requested_at"),
        cancelled_at=(updated or {}).get("cancelled_at"),
        deleted_at=(updated or {}).get("deleted_at"),
    )
