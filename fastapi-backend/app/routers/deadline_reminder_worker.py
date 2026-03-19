import hmac
import logging
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request

from app.core.config import get_settings
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/internal/deadlines/reminders/run")
async def run_deadline_reminder_worker(
    request: Request,
    x_deadline_reminder_worker_token: Optional[str] = Header(None),
) -> Dict[str, int | str]:
    """Periodic worker endpoint to generate deadline reminder events."""
    settings = get_settings()
    if not settings.DEADLINE_REMINDERS_ENABLED:
        return {"status": "disabled", "deadline_reminder_10m": 0, "deadline_happening_now": 0, "total_emitted": 0}

    expected_token = settings.DEADLINE_REMINDER_WORKER_TOKEN
    if not expected_token:
        raise HTTPException(status_code=500, detail="DEADLINE_REMINDER_WORKER_TOKEN is not configured")

    if not x_deadline_reminder_worker_token or not hmac.compare_digest(
        x_deadline_reminder_worker_token, expected_token
    ):
        raise HTTPException(status_code=401, detail="Invalid deadline reminder worker token")

    graph: GraphService = await get_graph(request)
    try:
        summary = await graph.generate_deadline_reminder_events()
        logger.info(
            "deadline_reminder_worker_completed emitted_10m=%s emitted_now=%s total=%s",
            summary.get("deadline_reminder_10m", 0),
            summary.get("deadline_happening_now", 0),
            summary.get("total_emitted", 0),
        )
        return {"status": "ok", **summary}
    except Exception as exc:
        logger.exception(
            "deadline_reminder_worker_failed error_type=%s error_message=%s",
            type(exc).__name__,
            str(exc)[:240],
        )
        raise
