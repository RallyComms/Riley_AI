import hmac
import logging
import time
from datetime import datetime
from typing import Dict, Optional

from fastapi import APIRouter, Header, HTTPException, Request

from app.core.config import get_settings
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService
from app.services.pricing_registry import estimate_worker_runtime_cost

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/internal/deadlines/reminders/run")
async def run_deadline_reminder_worker(
    request: Request,
    x_deadline_reminder_worker_token: Optional[str] = Header(None),
) -> Dict[str, int | str]:
    """Periodic worker endpoint to generate deadline reminder events."""
    started = time.perf_counter()
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
        try:
            duration_ms = int((time.perf_counter() - started) * 1000)
            runtime_cost = estimate_worker_runtime_cost(duration_ms=duration_ms)
            await graph.append_analytics_event(
                event_id=f"worker_runtime_completed:deadline_reminder:{datetime.now().isoformat()}",
                source_event_type_raw="worker_runtime_completed",
                source_entity="DeadlineReminderWorker",
                actor_user_id="system:deadline-reminder",
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
                    "worker": "deadline_reminder_worker",
                    "requests_count": 1,
                    "total_emitted": int(summary.get("total_emitted", 0) or 0),
                },
            )
        except Exception:
            pass
        return {"status": "ok", **summary}
    except Exception as exc:
        try:
            await graph.append_analytics_event(
                event_id=f"worker_failed:deadline_reminder:{summary.get('total_emitted', 0) if isinstance(locals().get('summary'), dict) else 0}:{type(exc).__name__}",
                source_event_type_raw="worker_failed",
                source_entity="DeadlineReminderWorker",
                actor_user_id="system:deadline-reminder",
                status="failed",
                metadata={
                    "worker": "deadline_reminder_worker",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:240],
                },
            )
        except Exception:
            pass
        logger.exception(
            "deadline_reminder_worker_failed error_type=%s error_message=%s",
            type(exc).__name__,
            str(exc)[:240],
        )
        raise
