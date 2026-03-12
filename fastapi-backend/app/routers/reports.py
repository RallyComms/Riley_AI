from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse, RedirectResponse
from pydantic import BaseModel, Field

from app.dependencies.auth import check_tenant_membership, verify_clerk_token
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService
from app.services.riley_reports import create_report_job

router = APIRouter()


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
    status: Literal["queued", "processing", "complete", "failed"]
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    error_message: Optional[str] = None
    output_file_id: Optional[str] = None
    output_url: Optional[str] = None
    summary_text: Optional[str] = None
    query: str
    mode: Literal["normal", "deep"]
    report_body: Optional[str] = None


class RileyReportJobsListResponse(BaseModel):
    jobs: List[RileyReportJobResponse]


@router.post("/riley/reports", response_model=RileyReportJobResponse)
async def create_riley_report(
    request: CreateRileyReportRequest,
    http_request: Request,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RileyReportJobResponse:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id, http_request)
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


@router.get("/riley/reports/{report_job_id}/download")
async def download_riley_report(
    report_job_id: str,
    http_request: Request,
    tenant_id: str = Query(..., max_length=50),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> RedirectResponse | PlainTextResponse:
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
