import hmac
import logging
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.core.config import get_settings
from app.dependencies.graph_dep import get_graph_optional
from app.services.document_intelligence import run_document_intelligence_job
from app.services.graph import GraphService

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
