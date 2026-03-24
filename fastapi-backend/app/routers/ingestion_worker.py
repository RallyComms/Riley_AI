import hmac
import logging
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel

from app.core.config import get_settings
from app.services.ingestion import run_ingestion_job


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
