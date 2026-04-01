from datetime import datetime, timezone
import logging
from typing import Optional

from neo4j import AsyncDriver, AsyncGraphDatabase

from app.core.config import get_settings

logger = logging.getLogger(__name__)

GUARDRAIL_USER_MESSAGE = "Riley is temporarily unavailable due to usage limits."
USER_OPERATION_LIMIT_MESSAGE = "You’ve reached your limit for Deep/Reports today."
DEEP_DAILY_LIMIT = 10
REPORTS_MONTHLY_LIMIT = 5

_driver: Optional[AsyncDriver] = None


class LLMCostGuardrailExceeded(RuntimeError):
    """Raised when monthly LLM spend exceeds configured limit."""


class UserOperationLimitExceeded(RuntimeError):
    """Raised when per-user operation quota is exceeded."""


def _get_driver() -> AsyncDriver:
    global _driver
    if _driver is None:
        settings = get_settings()
        _driver = AsyncGraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASSWORD),
        )
    return _driver


async def get_current_month_llm_cost_usd() -> float:
    """Compute current calendar-month LLM spend from analytics events."""
    month_start = datetime.now(timezone.utc).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    query = """
    MATCH (e:AnalyticsEvent)
    WHERE datetime(e.occurred_at) >= datetime($month_start_iso)
      AND e.cost_estimate_usd IS NOT NULL
      AND (
        toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
        OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
        OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
      )
    RETURN coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as total_cost
    """
    driver = _get_driver()
    async with driver.session() as session:
        result = await session.run(query, month_start_iso=month_start.isoformat())
        record = await result.single()
    return float((record or {}).get("total_cost") or 0.0)


async def enforce_monthly_llm_cost_guardrail() -> None:
    """Block LLM usage when monthly spend exceeds configured max."""
    settings = get_settings()
    max_monthly_cost = float(settings.MAX_MONTHLY_COST or 2000.0)
    current_cost = await get_current_month_llm_cost_usd()
    if current_cost > max_monthly_cost:
        logger.error(
            "llm_monthly_cost_guardrail_blocked current_cost_usd=%.4f max_monthly_cost_usd=%.4f",
            current_cost,
            max_monthly_cost,
        )
        raise LLMCostGuardrailExceeded(GUARDRAIL_USER_MESSAGE)


async def _consume_user_quota(*, user_id: str, operation: str, window_key: str, limit: int) -> bool:
    """Atomically consume one quota unit for user+operation+window.

    Returns True when quota unit was consumed, False when already at/over limit.
    """
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id:
        return False
    query = """
    MERGE (q:UserOperationUsage {user_id: $user_id, operation: $operation, window_key: $window_key})
    ON CREATE SET q.count = 0, q.created_at = datetime()
    WITH q
    WHERE coalesce(toInteger(q.count), 0) < $limit
    SET q.count = coalesce(toInteger(q.count), 0) + 1,
        q.updated_at = datetime()
    RETURN q.count as count
    """
    driver = _get_driver()
    async with driver.session() as session:
        result = await session.run(
            query,
            user_id=normalized_user_id,
            operation=operation,
            window_key=window_key,
            limit=max(1, int(limit)),
        )
        record = await result.single()
    return bool(record)


async def enforce_deep_daily_limit(*, user_id: str) -> None:
    """Consume one Deep request for the user, or raise if limit reached."""
    day_key = datetime.now(timezone.utc).date().isoformat()
    allowed = await _consume_user_quota(
        user_id=user_id,
        operation="deep_daily",
        window_key=day_key,
        limit=DEEP_DAILY_LIMIT,
    )
    if not allowed:
        logger.warning(
            "user_operation_limit_exceeded operation=deep_daily user_id=%s window_key=%s limit=%s",
            user_id,
            day_key,
            DEEP_DAILY_LIMIT,
        )
        raise UserOperationLimitExceeded(USER_OPERATION_LIMIT_MESSAGE)


async def enforce_reports_monthly_limit(*, user_id: str) -> None:
    """Consume one Report generation for the user, or raise if limit reached."""
    now = datetime.now(timezone.utc)
    month_key = f"{now.year:04d}-{now.month:02d}"
    allowed = await _consume_user_quota(
        user_id=user_id,
        operation="reports_monthly",
        window_key=month_key,
        limit=REPORTS_MONTHLY_LIMIT,
    )
    if not allowed:
        logger.warning(
            "user_operation_limit_exceeded operation=reports_monthly user_id=%s window_key=%s limit=%s",
            user_id,
            month_key,
            REPORTS_MONTHLY_LIMIT,
        )
        raise UserOperationLimitExceeded(USER_OPERATION_LIMIT_MESSAGE)
