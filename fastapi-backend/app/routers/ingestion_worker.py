import hmac
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from app.core.config import get_settings
from app.services.ingestion import run_ingestion_job


router = APIRouter()


class IngestionWorkerRequest(BaseModel):
    job_id: str
    file_id: str
    collection_name: str


@router.post("/internal/ingestion/run")
async def run_ingestion_worker(
    payload: IngestionWorkerRequest,
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

    await run_ingestion_job(
        job_id=payload.job_id,
        file_id=payload.file_id,
        collection_name=payload.collection_name,
    )
    return {"status": "ok"}
