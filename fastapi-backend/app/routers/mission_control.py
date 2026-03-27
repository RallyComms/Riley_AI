import json
import logging
from asyncio import Lock, create_task
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.config import get_settings
from app.dependencies.auth import verify_clerk_token, verify_mission_control_admin
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService

router = APIRouter()
SUPPORTED_TIMEFRAMES = {"24h", "7d", "30d"}
logger = logging.getLogger(__name__)
_rollup_refresh_lock: Lock = Lock()
_last_rollup_refresh_started_at: Optional[datetime] = None


def _resolve_timeframe(timeframe: str) -> Dict[str, Any]:
    tf = str(timeframe or "30d").strip().lower()
    if tf not in SUPPORTED_TIMEFRAMES:
        tf = "30d"
    if tf == "24h":
        return {"timeframe": tf, "window_hours": 24, "window_days": 1, "label": "Last 24h"}
    if tf == "7d":
        return {"timeframe": tf, "window_hours": 24 * 7, "window_days": 7, "label": "Last 7 days"}
    return {"timeframe": "30d", "window_hours": 24 * 30, "window_days": 30, "label": "Last 30 days"}


def _window_start_iso(window_hours: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(hours=max(1, int(window_hours)))).isoformat()


def _window_start_day(window_days: int) -> str:
    start_day = date.today() - timedelta(days=max(1, int(window_days)) - 1)
    return start_day.isoformat()


def _fill_day_series(
    *,
    rows: List[Dict[str, Any]],
    window_days: int,
    value_fields: List[str],
    caster: Any = int,
) -> List[Dict[str, Any]]:
    row_by_day = {str(row.get("day") or ""): row for row in rows if row.get("day")}
    start_day = date.today() - timedelta(days=max(1, int(window_days)) - 1)
    filled: List[Dict[str, Any]] = []
    for idx in range(max(1, int(window_days))):
        day_value = (start_day + timedelta(days=idx)).isoformat()
        source = row_by_day.get(day_value, {})
        item: Dict[str, Any] = {"day": day_value}
        for field in value_fields:
            item[field] = caster(source.get(field) or 0)
        filled.append(item)
    return filled


async def _maybe_schedule_rollup_refresh(graph: GraphService) -> None:
    global _last_rollup_refresh_started_at
    settings = get_settings()
    if not getattr(settings, "MISSION_CONTROL_ROLLUP_AUTO_REFRESH_ENABLED", False):
        return
    refresh_interval_minutes = max(1, int(getattr(settings, "MISSION_CONTROL_ROLLUP_REFRESH_MINUTES", 15)))
    days_back = max(7, int(getattr(settings, "MISSION_CONTROL_ROLLUP_DAYS_BACK", 35)))
    now = datetime.now(timezone.utc)
    async with _rollup_refresh_lock:
        if _last_rollup_refresh_started_at:
            elapsed = (now - _last_rollup_refresh_started_at).total_seconds()
            if elapsed < refresh_interval_minutes * 60:
                return
        _last_rollup_refresh_started_at = now
    create_task(_refresh_rollups_task(graph, days_back))


async def _refresh_rollups_task(graph: GraphService, days_back: int) -> None:
    try:
        await graph.rebuild_analytics_daily_rollups(days_back=days_back)
    except Exception:
        logger.exception("mission_control_rollup_refresh_failed days_back=%s", days_back)


def _safe_campaign_name(campaign_name: Optional[str], campaign_id: Optional[str]) -> str:
    name = str(campaign_name or "").strip()
    cid = str(campaign_id or "").strip()
    if name and name.lower() != "unknown_campaign":
        return name
    if cid and cid.lower() != "unknown_campaign":
        return f"Unresolved Campaign"
    return "Unresolved Campaign"


def _safe_campaign_secondary(campaign_id: Optional[str]) -> str:
    cid = str(campaign_id or "").strip()
    if not cid or cid.lower() == "unknown_campaign":
        return ""
    return cid


def _safe_user_label(
    *,
    display_name: Optional[str],
    username: Optional[str],
    email: Optional[str],
    user_id: Optional[str],
) -> str:
    display = str(display_name or "").strip()
    if display:
        return display
    uname = str(username or "").strip()
    if uname:
        return uname
    em = str(email or "").strip()
    if em:
        prefix = em.split("@")[0].strip()
        if prefix:
            return prefix
        return em
    uid = str(user_id or "").strip()
    if "@" in uid:
        uid_prefix = uid.split("@")[0].strip()
        if uid_prefix:
            return uid_prefix
    return uid or "unknown_user"


def _safe_user_secondary(user_id: Optional[str], primary_label: str) -> str:
    uid = str(user_id or "").strip()
    if not uid or uid == primary_label:
        return ""
    return uid


def _failure_type_label(event_type: str) -> str:
    value = str(event_type or "").strip().lower()
    mapping = {
        "auth_tenant_access_denied": "Tenant Access Denied",
        "worker_failed": "Worker Failed",
        "ingestion_failed": "Ingestion Failed",
        "preview_generation_failed": "Preview Generation Failed",
        "report_generation_attempt_completed": "Report Generation Failed",
        "report_job_status_changed": "Report Job Failed",
    }
    return mapping.get(value, value.replace("_", " ").title() if value else "Unknown Failure")


def _failure_classification(*, event_type: str, worker_name: str, detail: str, metadata: Dict[str, Any]) -> str:
    event = str(event_type or "").strip().lower()
    worker = str(worker_name or "").strip().lower()
    detail_text = str(detail or "").strip().lower()
    error_type = str(metadata.get("error_type") or "").strip().lower()
    if (
        "neo4j" in detail_text
        or ":7687" in detail_text
        or "failed to write data to connection" in detail_text
        or "connection refused" in detail_text
        or "connection reset" in detail_text
        or "temporarily unavailable" in detail_text
        or "timed out" in detail_text
        or "service unavailable" in detail_text
    ):
        return "Database Connectivity"
    if event == "auth_tenant_access_denied":
        return "Auth Failure"
    if "timeout" in error_type or "timeout" in detail_text:
        return "Worker Timeout"
    if "permission" in detail_text or "unauthorized" in detail_text or "forbidden" in detail_text:
        return "Authorization"
    if "deadline_reminder" in worker or "deadlinereminderworker" in worker:
        return "Worker Execution"
    if event in {"worker_failed", "ingestion_failed", "preview_generation_failed"}:
        return "Worker Execution"
    return "Platform Error"


def _failure_scope_campaign_name(campaign_name: Optional[str], campaign_id: Optional[str]) -> str:
    cid = str(campaign_id or "").strip()
    if not cid or cid.lower() in {"unknown_campaign", "global"}:
        return "System / Platform"
    return _safe_campaign_name(campaign_name, campaign_id)


def _failure_scope_campaign_secondary(campaign_id: Optional[str]) -> str:
    cid = str(campaign_id or "").strip()
    if not cid or cid.lower() in {"unknown_campaign", "global"}:
        return ""
    return _safe_campaign_secondary(campaign_id)


def _metadata_json_to_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


@router.get("/mission-control/access")
async def mission_control_access(
    user: Dict = Depends(verify_clerk_token),
) -> Dict[str, bool]:
    """Lightweight Mission Control access probe used by frontend navigation."""
    try:
        await verify_mission_control_admin(user)
        return {"allowed": True}
    except HTTPException as exc:
        if exc.status_code == 403:
            return {"allowed": False}
        raise


async def _single_value(graph: GraphService, query: str, **params: Any) -> Dict[str, Any]:
    async with graph.driver.session() as session:
        result = await session.run(query, **params)
        record = await result.single()
        return dict(record) if record else {}


async def _rows(graph: GraphService, query: str, **params: Any) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    async with graph.driver.session() as session:
        result = await session.run(query, **params)
        async for row in result:
            items.append(dict(row))
    return items


@router.get("/mission-control/overview")
async def mission_control_overview(
    timeframe: str = Query("30d", pattern="^(24h|7d|30d)$"),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    window = _resolve_timeframe(timeframe)
    await _maybe_schedule_rollup_refresh(graph)
    window_start_iso = _window_start_iso(int(window["window_hours"]))
    window_start_day = _window_start_day(int(window["window_days"]))
    today_day = date.today().isoformat()
    active_users = await _single_value(
        graph,
        """
        MATCH (r:AnalyticsDailyUserRollup)
        WHERE r.event_date >= $window_start_day
        WITH coalesce(r.user_id, "") as uid
        WHERE trim(uid) <> ""
          AND uid <> "system:deadline-reminder"
          AND uid <> "unknown_user"
        RETURN count(DISTINCT uid) as value
        """,
        window_start_day=window_start_day,
    )
    active_campaigns = await _single_value(
        graph,
        """
        MATCH (r:AnalyticsDailyCampaignRollup)
        WHERE r.event_date >= $window_start_day
          AND r.campaign_id IS NOT NULL
          AND trim(r.campaign_id) <> ""
          AND r.campaign_id <> "global"
          AND r.campaign_id <> "unknown_campaign"
        RETURN count(DISTINCT r.campaign_id) as value
        """,
        window_start_day=window_start_day,
    )
    activity_metrics = await _single_value(
        graph,
        """
        MATCH (r:AnalyticsDailySystemRollup)
        WHERE r.event_date >= $window_start_day
        RETURN
          coalesce(sum(toInteger(r.chat_events)), 0) as chats,
          coalesce(sum(toInteger(r.report_events)), 0) as reports,
          coalesce(sum(toInteger(r.report_failures)), 0) as failed_reports,
          coalesce(sum(toFloat(r.latency_sum_ms)), 0.0) as latency_sum_ms,
          coalesce(sum(toInteger(r.latency_count)), 0) as latency_count
        """,
        window_start_day=window_start_day,
    )
    report_success = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.occurred_at >= $window_start_iso
          AND e.source_entity = "RileyReportJob"
          AND e.status IN ["complete", "failed"]
          AND e.object_id IS NOT NULL
        WITH e.object_id as report_job_id, max(e.occurred_at) as latest_occurred_at
        MATCH (latest:AnalyticsEvent)
        WHERE latest.object_id = report_job_id
          AND latest.occurred_at = latest_occurred_at
          AND latest.source_entity = "RileyReportJob"
          AND latest.status IN ["complete", "failed"]
        RETURN
          count(DISTINCT report_job_id) as total,
          count(DISTINCT CASE WHEN latest.status = "complete" THEN report_job_id ELSE NULL END) as success
        """,
        window_start_iso=window_start_iso,
    )
    month_cost = await _single_value(
        graph,
        """
        MATCH (r:AnalyticsDailySystemRollup)
        WHERE r.event_date >= $month_start_day
          AND r.event_date <= $today_day
        RETURN coalesce(sum(toFloat(r.total_cost_estimate_usd)), 0.0) as value
        """,
        month_start_day=date.today().replace(day=1).isoformat(),
        today_day=today_day,
    )
    pending_access_requests = await _single_value(
        graph,
        """
        MATCH (ar:CampaignAccessRequest {status: "pending"})
        RETURN count(ar) as value
        """,
    )
    overdue_deadlines = await _single_value(
        graph,
        """
        MATCH (d:CampaignDeadline)
        WHERE d.completed_at IS NULL
          AND d.due_at IS NOT NULL
          AND d.due_at < datetime()
        RETURN count(d) as value
        """,
    )
    trend_rows = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailySystemRollup)
        WHERE r.event_date >= $window_start_day
        RETURN
          r.event_date as day,
          coalesce(toInteger(r.chat_events), 0) as chats,
          coalesce(toInteger(r.report_events), 0) as reports,
          round(coalesce(toFloat(r.total_cost_estimate_usd), 0.0), 4) as cost,
          coalesce(toInteger(r.failure_events), 0) as failures
        ORDER BY day ASC
        """,
        window_start_day=window_start_day,
    )
    campaign_usage_rows = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailyCampaignRollup)
        WHERE r.event_date >= $window_start_day
          AND r.campaign_id IS NOT NULL
          AND trim(r.campaign_id) <> ""
          AND r.campaign_id <> "global"
          AND r.campaign_id <> "unknown_campaign"
        WITH r.campaign_id as campaign_id,
             coalesce(sum(toInteger(r.chat_events)), 0) as chats,
             coalesce(sum(toInteger(r.report_events)), 0) as reports,
             round(coalesce(sum(toFloat(r.total_cost_estimate_usd)), 0.0), 4) as cost
        WITH campaign_id, chats, reports, cost
        ORDER BY chats DESC, reports DESC, cost DESC
        LIMIT 25
        WITH campaign_id, chats, reports, cost
        OPTIONAL MATCH (c:Campaign)
        WHERE c.id = campaign_id OR c.tenant_id = campaign_id
        RETURN
          campaign_id,
          coalesce(c.name, c.title, "") as campaign_name,
          chats,
          reports,
          cost
        """,
        window_start_day=window_start_day,
    )
    user_usage_rows = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailyUserRollup)
        WHERE r.event_date >= $window_start_day
          AND coalesce(r.user_id, "") <> "system:deadline-reminder"
          AND coalesce(r.user_id, "") <> "unknown_user"
        WITH coalesce(r.user_id, "") as user_id,
             coalesce(sum(toInteger(r.chat_events)), 0) as chats,
             coalesce(sum(toInteger(r.report_events)), 0) as reports,
             round(coalesce(sum(toFloat(r.total_cost_estimate_usd)), 0.0), 4) as cost
        WHERE trim(user_id) <> ""
        WITH user_id, chats, reports, cost
        ORDER BY chats DESC, reports DESC, cost DESC
        LIMIT 25
        WITH user_id, chats, reports, cost
        OPTIONAL MATCH (u:User {id: user_id})
        WITH user_id, chats, reports, cost, u,
             CASE
                WHEN u["email"] IS NULL OR trim(u["email"]) = "" THEN NULL
                ELSE split(u["email"], "@")[0]
             END as email_prefix
        RETURN
          user_id,
          coalesce(u["display_name"], "") as user_display_name,
          coalesce(u["username"], "") as username,
          coalesce(u["email"], "") as email,
          coalesce(email_prefix, "") as email_prefix,
          chats,
          reports,
          cost
        """,
        window_start_day=window_start_day,
    )

    now = datetime.now(timezone.utc)
    days_in_month = monthrange(now.year, now.month)[1]
    month_elapsed_days = max(1, now.day)
    current_month_estimated_cost = float(month_cost.get("value") or 0.0)
    forecast_month_end_cost = round(
        (current_month_estimated_cost / float(month_elapsed_days)) * float(days_in_month),
        2,
    )
    total_reports = int(report_success.get("total") or 0)
    successful_reports = int(report_success.get("success") or 0)
    report_success_rate = round((successful_reports / total_reports) * 100.0, 2) if total_reports else 0.0
    latency_count = int(activity_metrics.get("latency_count") or 0)
    avg_response_latency_ms = (
        round(float(activity_metrics.get("latency_sum_ms") or 0.0) / float(latency_count), 2)
        if latency_count
        else 0.0
    )

    return {
        "active_users_7d": int(active_users.get("value") or 0),
        "active_campaigns_7d": int(active_campaigns.get("value") or 0),
        "chats_today": int(activity_metrics.get("chats") or 0),
        "reports_today": int(activity_metrics.get("reports") or 0),
        "avg_response_latency_ms": avg_response_latency_ms,
        "report_success_rate": report_success_rate,
        "current_month_estimated_cost": round(current_month_estimated_cost, 2),
        "forecast_month_end_cost": forecast_month_end_cost,
        "pending_access_requests": int(pending_access_requests.get("value") or 0),
        "overdue_deadlines": int(overdue_deadlines.get("value") or 0),
        "failed_reports_24h": int(activity_metrics.get("failed_reports") or 0),
        "timeframe": window["timeframe"],
        "timeframe_label": window["label"],
        "timeseries_30d": [
            {
                "day": row.get("day"),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
                "cost": round(float(row.get("cost") or 0.0), 4),
                "failures": int(row.get("failures") or 0),
            }
            for row in _fill_day_series(
                rows=trend_rows,
                window_days=window["window_days"],
                value_fields=["chats", "reports", "cost", "failures"],
                caster=float,
            )
        ],
        "campaign_usage_7d": [
            {
                "campaign_id": row.get("campaign_id"),
                "campaign_name": _safe_campaign_name(row.get("campaign_name"), row.get("campaign_id")),
                "campaign_secondary": _safe_campaign_secondary(row.get("campaign_id")),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in campaign_usage_rows
        ],
        "user_usage_7d": [
            {
                "user_id": row.get("user_id"),
                "user_label": _safe_user_label(
                    display_name=row.get("user_display_name"),
                    username=row.get("username"),
                    email=row.get("email"),
                    user_id=row.get("user_id"),
                ),
                "user_secondary": _safe_user_secondary(
                    row.get("user_id"),
                    _safe_user_label(
                        display_name=row.get("user_display_name"),
                        username=row.get("username"),
                        email=row.get("email"),
                        user_id=row.get("user_id"),
                    ),
                ),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in user_usage_rows
        ],
    }


@router.get("/mission-control/riley-performance")
async def mission_control_riley_performance(
    timeframe: str = Query("30d", pattern="^(24h|7d|30d)$"),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    window = _resolve_timeframe(timeframe)
    await _maybe_schedule_rollup_refresh(graph)
    window_start_day = _window_start_day(int(window["window_days"]))
    window_start_iso = _window_start_iso(int(window["window_hours"]))

    metrics = await _single_value(
        graph,
        """
        MATCH (r:AnalyticsDailySystemRollup)
        WHERE r.event_date >= $window_start_day
        RETURN
          coalesce(sum(toFloat(r.latency_sum_ms)), 0.0) as latency_sum_ms,
          coalesce(sum(toInteger(r.latency_count)), 0) as latency_count,
          coalesce(sum(toInteger(r.provider_fallback_successes)), 0) as provider_fallback_successes,
          coalesce(sum(toInteger(r.provider_fallback_failures)), 0) as provider_fallback_failures,
          coalesce(sum(toInteger(r.reranker_failures)), 0) as reranker_failures
        """,
        window_start_day=window_start_day,
    )
    provider_breakdown = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailyProviderRollup)
        WHERE r.event_date >= $window_start_day
          AND trim(coalesce(r.provider, "")) <> ""
        WITH coalesce(r.provider, "unknown") as provider,
             coalesce(sum(toFloat(r.latency_sum_ms)), 0.0) as latency_sum_ms,
             coalesce(sum(toInteger(r.latency_count)), 0) as latency_count,
             coalesce(sum(toInteger(r.fallback_triggered)), 0) as fallback_triggered,
             coalesce(sum(toInteger(r.fallback_succeeded)), 0) as fallback_succeeded,
             coalesce(sum(toInteger(r.fallback_failed)), 0) as fallback_failed
        RETURN
          provider,
          round(CASE WHEN latency_count = 0 THEN 0.0 ELSE latency_sum_ms / toFloat(latency_count) END, 2) as avg_latency_ms,
          fallback_triggered,
          fallback_succeeded,
          fallback_failed
        ORDER BY fallback_triggered DESC, avg_latency_ms DESC
        LIMIT 20
        """,
        window_start_day=window_start_day,
    )
    trend_rows = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailySystemRollup)
        WHERE r.event_date >= $window_start_day
        RETURN
          r.event_date as day,
          coalesce(toInteger(r.report_successes), 0) as success,
          coalesce(toInteger(r.report_failures), 0) as failure
        ORDER BY day ASC
        """,
        window_start_day=window_start_day,
    )
    campaign_rollup = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailyCampaignRollup)
        WHERE r.event_date >= $window_start_day
          AND r.campaign_id IS NOT NULL
          AND trim(r.campaign_id) <> ""
          AND r.campaign_id <> "global"
          AND r.campaign_id <> "unknown_campaign"
        WITH r.campaign_id as campaign_id,
             coalesce(sum(toInteger(r.chat_events)), 0) as chats,
             coalesce(sum(toInteger(r.report_events)), 0) as reports,
             coalesce(sum(toFloat(r.latency_sum_ms)), 0.0) as latency_sum_ms,
             coalesce(sum(toInteger(r.latency_count)), 0) as latency_count
        WITH campaign_id, chats, reports, latency_sum_ms, latency_count
        ORDER BY chats DESC, reports DESC, latency_count DESC
        LIMIT 20
        WITH campaign_id, chats, reports, latency_sum_ms, latency_count
        OPTIONAL MATCH (c:Campaign)
        WHERE c.id = campaign_id OR c.tenant_id = campaign_id
        RETURN
          campaign_id,
          coalesce(c.name, c.title, "") as campaign_name,
          chats,
          reports,
          round(CASE WHEN latency_count = 0 THEN 0.0 ELSE latency_sum_ms / toFloat(latency_count) END, 2) as avg_latency_ms
        """,
        window_start_day=window_start_day,
    )
    campaign_ids = [str(row.get("campaign_id") or "").strip() for row in campaign_rollup if str(row.get("campaign_id") or "").strip()]
    campaign_p95_rows: List[Dict[str, Any]] = []
    if campaign_ids:
        campaign_p95_rows = await _rows(
            graph,
            """
            MATCH (e:AnalyticsEvent)
            WHERE e.occurred_at >= $window_start_iso
              AND e.latency_ms IS NOT NULL
              AND e.campaign_id IN $campaign_ids
            RETURN
              e.campaign_id as campaign_id,
              round(percentileCont(toFloat(e.latency_ms), 0.95), 2) as p95_latency_ms
            """,
            window_start_iso=window_start_iso,
            campaign_ids=campaign_ids,
        )
    p_rows = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.occurred_at >= $window_start_iso
          AND e.latency_ms IS NOT NULL
        RETURN
          percentileCont(toFloat(e.latency_ms), 0.50) as p50_ms,
          percentileCont(toFloat(e.latency_ms), 0.75) as p75_ms,
          percentileCont(toFloat(e.latency_ms), 0.90) as p90_ms,
          percentileCont(toFloat(e.latency_ms), 0.95) as p95_ms
        """,
        window_start_iso=window_start_iso,
    )

    fallback_successes = int(metrics.get("provider_fallback_successes") or 0)
    fallback_failures = int(metrics.get("provider_fallback_failures") or 0)
    fallback_total = fallback_successes + fallback_failures
    fallback_success_rate = round((fallback_successes / float(fallback_total)) * 100.0, 2) if fallback_total else 0.0
    latency_count = int(metrics.get("latency_count") or 0)
    avg_latency_ms = (
        round(float(metrics.get("latency_sum_ms") or 0.0) / float(latency_count), 2)
        if latency_count
        else 0.0
    )
    campaign_p95_by_id = {str(row.get("campaign_id") or ""): float(row.get("p95_latency_ms") or 0.0) for row in campaign_p95_rows}
    return {
        "timeframe": window["timeframe"],
        "timeframe_label": window["label"],
        "avg_latency_ms_7d": avg_latency_ms,
        "p95_latency_ms_7d": round(float(p_rows.get("p95_ms") or 0.0), 2),
        "provider_fallback_successes_7d": fallback_successes,
        "provider_fallback_failures_7d": fallback_failures,
        "provider_fallback_success_rate_7d": fallback_success_rate,
        "reranker_failures_7d": int(metrics.get("reranker_failures") or 0),
        "latency_percentiles_7d": {
            "p50_ms": round(float(p_rows.get("p50_ms") or 0.0), 2),
            "p75_ms": round(float(p_rows.get("p75_ms") or 0.0), 2),
            "p90_ms": round(float(p_rows.get("p90_ms") or 0.0), 2),
            "p95_ms": round(float(p_rows.get("p95_ms") or 0.0), 2),
        },
        "success_failure_trend_30d": [
            {
                "day": row.get("day"),
                "success": int(row.get("success") or 0),
                "failure": int(row.get("failure") or 0),
            }
            for row in _fill_day_series(rows=trend_rows, window_days=window["window_days"], value_fields=["success", "failure"], caster=int)
        ],
        "campaign_performance_7d": [
            {
                "campaign_id": row.get("campaign_id"),
                "campaign_name": _safe_campaign_name(row.get("campaign_name"), row.get("campaign_id")),
                "campaign_secondary": _safe_campaign_secondary(row.get("campaign_id")),
                "avg_latency_ms": round(float(row.get("avg_latency_ms") or 0.0), 2),
                "p95_latency_ms": round(float(campaign_p95_by_id.get(str(row.get("campaign_id") or ""), 0.0)), 2),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
            }
            for row in campaign_rollup
        ],
        "provider_breakdown_7d": [
            {
                "provider": row.get("provider") or "unknown",
                "avg_latency_ms": round(float(row.get("avg_latency_ms") or 0.0), 2),
                "fallback_triggered": int(row.get("fallback_triggered") or 0),
                "fallback_succeeded": int(row.get("fallback_succeeded") or 0),
                "fallback_failed": int(row.get("fallback_failed") or 0),
            }
            for row in provider_breakdown
        ],
    }


@router.get("/mission-control/cost-summary")
async def mission_control_cost_summary(
    timeframe: str = Query("30d", pattern="^(24h|7d|30d)$"),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    window = _resolve_timeframe(timeframe)
    await _maybe_schedule_rollup_refresh(graph)
    window_start_day = _window_start_day(int(window["window_days"]))
    window_start_iso = _window_start_iso(int(window["window_hours"]))
    today_day = date.today().isoformat()
    month_start_day = date.today().replace(day=1).isoformat()
    month_to_date_cost = await _single_value(
        graph,
        """
        MATCH (r:AnalyticsDailySystemRollup)
        WHERE r.event_date >= $month_start_day
          AND r.event_date <= $today_day
        RETURN coalesce(sum(toFloat(r.total_cost_estimate_usd)), 0.0) as value
        """,
        month_start_day=month_start_day,
        today_day=today_day,
    )
    totals = await _single_value(
        graph,
        """
        MATCH (r:AnalyticsDailySystemRollup)
        WHERE r.event_date >= $window_start_day
        RETURN
          coalesce(sum(toFloat(r.total_cost_estimate_usd)), 0.0) as window_cost,
          coalesce(sum(CASE WHEN r.event_date >= $last_7d_start_day
                            THEN toFloat(r.total_cost_estimate_usd) ELSE 0.0 END), 0.0) as last_7d_cost
        """,
        window_start_day=window_start_day,
        last_7d_start_day=(date.today() - timedelta(days=6)).isoformat(),
    )
    confidence_totals = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.occurred_at >= $window_start_iso
          AND e.cost_estimate_usd IS NOT NULL
        RETURN
          coalesce(sum(CASE WHEN coalesce(e.cost_confidence, "proxy_only") = "exact_usage"
                            THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as exact_usage_cost_30d,
          coalesce(sum(CASE WHEN coalesce(e.cost_confidence, "proxy_only") = "estimated_units"
                            THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as estimated_units_cost_30d,
          coalesce(sum(CASE WHEN coalesce(e.cost_confidence, "proxy_only") = "proxy_only"
                            THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as proxy_only_cost_30d
        """,
        window_start_iso=window_start_iso,
    )
    by_provider_rollup_rows = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailyProviderRollup)
        WHERE r.event_date >= $window_start_day
        WITH coalesce(r.provider, "unknown") as provider,
             coalesce(sum(toFloat(r.total_cost_estimate_usd)), 0.0) as cost
        RETURN provider, round(cost, 4) as cost
        ORDER BY cost DESC
        LIMIT 25
        """,
        window_start_day=window_start_day,
    )
    cost_by_campaign_rows = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailyCampaignRollup)
        WHERE r.event_date >= $window_start_day
          AND r.campaign_id IS NOT NULL
          AND trim(r.campaign_id) <> ""
          AND r.campaign_id <> "global"
          AND r.campaign_id <> "unknown_campaign"
        WITH r.campaign_id as campaign_id,
             coalesce(sum(toFloat(r.total_cost_estimate_usd)), 0.0) as cost
        WITH campaign_id, cost
        ORDER BY cost DESC
        LIMIT 25
        WITH campaign_id, cost
        OPTIONAL MATCH (c:Campaign)
        WHERE c.id = campaign_id OR c.tenant_id = campaign_id
        RETURN
          campaign_id,
          coalesce(c.name, c.title, "") as campaign_name,
          round(cost, 4) as cost
        """,
        window_start_day=window_start_day,
    )
    cost_by_user_rows = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailyUserRollup)
        WHERE r.event_date >= $window_start_day
          AND coalesce(r.user_id, "") <> "system:deadline-reminder"
          AND coalesce(r.user_id, "") <> "unknown_user"
        WITH coalesce(r.user_id, "") as user_id, coalesce(sum(toFloat(r.total_cost_estimate_usd)), 0.0) as cost
        WHERE trim(user_id) <> ""
        WITH user_id, cost
        ORDER BY cost DESC
        LIMIT 25
        WITH user_id, cost
        OPTIONAL MATCH (u:User {id: user_id})
        WITH user_id, u, cost,
             CASE
                 WHEN u["email"] IS NULL OR trim(u["email"]) = "" THEN NULL
                 ELSE split(u["email"], "@")[0]
             END as email_prefix
        RETURN
          user_id,
          coalesce(u["display_name"], "") as user_display_name,
          coalesce(u["username"], "") as username,
          coalesce(u["email"], "") as email,
          coalesce(email_prefix, "") as email_prefix,
          round(cost, 4) as cost
        """,
        window_start_day=window_start_day,
    )
    cost_trend_rows = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailySystemRollup)
        WHERE r.event_date >= $window_start_day
        RETURN
          r.event_date as day,
          round(coalesce(toFloat(r.total_cost_estimate_usd), 0.0), 4) as cost
        ORDER BY day ASC
        """,
        window_start_day=window_start_day,
    )
    activity_counts = await _single_value(
        graph,
        """
        MATCH (r:AnalyticsDailySystemRollup)
        WHERE r.event_date >= $window_start_day
        RETURN
          coalesce(sum(toInteger(r.chat_events)), 0) as chats,
          coalesce(sum(toInteger(r.report_events)), 0) as reports
        """,
        window_start_day=window_start_day,
    )
    window_cost = round(float(totals.get("window_cost") or 0.0), 2)
    current_month_cost = round(float(month_to_date_cost.get("value") or 0.0), 2)
    chats_30d = int(activity_counts.get("chats") or 0)
    reports_30d = int(activity_counts.get("reports") or 0)
    cost_per_chat = round(window_cost / float(chats_30d), 6) if chats_30d else 0.0
    cost_per_report = round(window_cost / float(reports_30d), 6) if reports_30d else 0.0
    now = datetime.now(timezone.utc)
    days_in_month = monthrange(now.year, now.month)[1]
    month_elapsed_days = max(1, now.day)
    daily_burn_rate = round(current_month_cost / float(month_elapsed_days), 4)
    projected_month_end_cost = round(daily_burn_rate * float(days_in_month), 2)
    projected_curve = []
    for day in range(1, days_in_month + 1):
        projected_value = daily_burn_rate * float(day)
        projected_curve.append({"day": day, "projected_cost": round(projected_value, 2)})
    return {
        "timeframe": window["timeframe"],
        "timeframe_label": window["label"],
        "month_estimated_cost": current_month_cost,
        "selected_window_cost": window_cost,
        "current_month_cost": current_month_cost,
        "average_daily_burn_rate": daily_burn_rate,
        "projected_month_end_cost": projected_month_end_cost,
        "current_month_days_elapsed": month_elapsed_days,
        "current_month_total_days": days_in_month,
        "last_7d_estimated_cost": round(float(totals.get("last_7d_cost") or 0.0), 2),
        "cost_confidence_breakdown_30d": {
            "exact_usage": round(float(confidence_totals.get("exact_usage_cost_30d") or 0.0), 4),
            "estimated_units": round(float(confidence_totals.get("estimated_units_cost_30d") or 0.0), 4),
            "proxy_only": round(float(confidence_totals.get("proxy_only_cost_30d") or 0.0), 4),
        },
        "provider_cost_30d": [
            {"provider": row.get("provider"), "cost": float(row.get("cost") or 0.0)}
            for row in by_provider_rollup_rows
        ],
        "cost_per_chat": cost_per_chat,
        "cost_per_report": cost_per_report,
        "cost_by_campaign_30d": [
            {
                "campaign_id": row.get("campaign_id"),
                "campaign_name": _safe_campaign_name(row.get("campaign_name"), row.get("campaign_id")),
                "campaign_secondary": _safe_campaign_secondary(row.get("campaign_id")),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in cost_by_campaign_rows
        ],
        "cost_by_user_30d": [
            {
                "user_id": row.get("user_id"),
                "user_label": _safe_user_label(
                    display_name=row.get("user_display_name"),
                    username=row.get("username"),
                    email=row.get("email"),
                    user_id=row.get("user_id"),
                ),
                "user_secondary": _safe_user_secondary(
                    row.get("user_id"),
                    _safe_user_label(
                        display_name=row.get("user_display_name"),
                        username=row.get("username"),
                        email=row.get("email"),
                        user_id=row.get("user_id"),
                    ),
                ),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in cost_by_user_rows
        ],
        "cost_trend_30d": [
            {"day": row.get("day"), "cost": round(float(row.get("cost") or 0.0), 4)}
            for row in _fill_day_series(rows=cost_trend_rows, window_days=window["window_days"], value_fields=["cost"], caster=float)
        ],
        "projected_monthly_curve": projected_curve,
    }


@router.get("/mission-control/adoption-summary")
async def mission_control_adoption_summary(
    timeframe: str = Query("30d", pattern="^(24h|7d|30d)$"),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    window = _resolve_timeframe(timeframe)
    await _maybe_schedule_rollup_refresh(graph)
    window_start_day = _window_start_day(int(window["window_days"]))
    metrics = await _single_value(
        graph,
        """
        CALL {
          MATCH (r:AnalyticsDailySystemRollup)
          WHERE r.event_date >= $window_start_day
          RETURN coalesce(sum(toInteger(r.event_count)), 0) as events_30d
        }
        CALL {
          MATCH (r:AnalyticsDailyCampaignRollup)
          WHERE r.event_date >= $window_start_day
            AND r.campaign_id IS NOT NULL
            AND trim(r.campaign_id) <> ""
            AND r.campaign_id <> "global"
            AND r.campaign_id <> "unknown_campaign"
          RETURN count(DISTINCT r.campaign_id) as unique_campaigns_30d
        }
        CALL {
          MATCH (r:AnalyticsDailyUserRollup)
          WHERE r.event_date >= $window_start_day
            AND r.user_id IS NOT NULL
            AND trim(r.user_id) <> ""
            AND r.user_id <> "system:deadline-reminder"
            AND r.user_id <> "unknown_user"
          RETURN
            count(DISTINCT r.user_id) as unique_users_30d,
            count(DISTINCT CASE WHEN coalesce(toInteger(r.chat_events), 0) > 0 THEN r.user_id ELSE NULL END) as chat_users_30d,
            count(DISTINCT CASE WHEN coalesce(toInteger(r.report_events), 0) > 0 THEN r.user_id ELSE NULL END) as report_users_30d
        }
        RETURN
          events_30d,
          unique_users_30d,
          unique_campaigns_30d,
          chat_users_30d,
          report_users_30d
        """,
        window_start_day=window_start_day,
    )
    user_rows = await _rows(
        graph,
        """
        MATCH (r:AnalyticsDailyUserRollup)
        WHERE r.event_date >= $window_start_day
          AND r.user_id IS NOT NULL
          AND trim(r.user_id) <> ""
          AND r.user_id <> "system:deadline-reminder"
          AND r.user_id <> "unknown_user"
        WITH r.user_id as user_id,
             coalesce(sum(toInteger(r.event_count)), 0) as events,
             coalesce(sum(toInteger(r.chat_events)), 0) as chats,
             coalesce(sum(toInteger(r.report_events)), 0) as reports
        WHERE trim(user_id) <> ""
        WITH user_id, events, chats, reports
        ORDER BY events DESC
        LIMIT 25
        WITH user_id, events, chats, reports
        OPTIONAL MATCH (u:User {id: user_id})
        WITH user_id, events, chats, reports, u,
             CASE
                 WHEN u["email"] IS NULL OR trim(u["email"]) = "" THEN NULL
                 ELSE split(u["email"], "@")[0]
             END as email_prefix
        RETURN
          user_id,
          coalesce(u["display_name"], "") as user_display_name,
          coalesce(u["username"], "") as username,
          coalesce(u["email"], "") as email,
          coalesce(email_prefix, "") as email_prefix,
          events,
          chats,
          reports
        """,
        window_start_day=window_start_day,
    )
    return {
        "timeframe": window["timeframe"],
        "timeframe_label": window["label"],
        "events_30d": int(metrics.get("events_30d") or 0),
        "unique_users_30d": int(metrics.get("unique_users_30d") or 0),
        "unique_campaigns_30d": int(metrics.get("unique_campaigns_30d") or 0),
        "chat_users_30d": int(metrics.get("chat_users_30d") or 0),
        "report_users_30d": int(metrics.get("report_users_30d") or 0),
        "user_activity_30d": [
            {
                "user_id": row.get("user_id"),
                "user_label": _safe_user_label(
                    display_name=row.get("user_display_name"),
                    username=row.get("username"),
                    email=row.get("email"),
                    user_id=row.get("user_id"),
                ),
                "user_secondary": _safe_user_secondary(
                    row.get("user_id"),
                    _safe_user_label(
                        display_name=row.get("user_display_name"),
                        username=row.get("username"),
                        email=row.get("email"),
                        user_id=row.get("user_id"),
                    ),
                ),
                "events": int(row.get("events") or 0),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
            }
            for row in user_rows
        ],
    }


@router.get("/mission-control/workflow-health-summary")
async def mission_control_workflow_health_summary(
    timeframe: str = Query("30d", pattern="^(24h|7d|30d)$"),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    window = _resolve_timeframe(timeframe)
    workflow = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({hours: $window_hours})
        RETURN
          sum(CASE WHEN e.source_event_type_raw = "access_request_created" THEN 1 ELSE 0 END) as access_requests_24h,
          sum(CASE WHEN e.source_event_type_raw IN ["access_request_approved", "access_request_denied"] THEN 1 ELSE 0 END) as access_decisions_24h,
          sum(CASE WHEN e.source_event_type_raw IN ["deadline_reminder_10m", "deadline_happening_now"] THEN 1 ELSE 0 END) as deadline_reminders_24h,
          sum(CASE WHEN e.source_event_type_raw = "preview_generation_failed" THEN 1 ELSE 0 END) as preview_failures_24h
        """,
        window_hours=window["window_hours"],
    )
    pending_access = await _single_value(
        graph,
        "MATCH (ar:CampaignAccessRequest {status: 'pending'}) RETURN count(ar) as value",
    )
    overdue_deadlines = await _single_value(
        graph,
        """
        MATCH (d:CampaignDeadline)
        WHERE d.completed_at IS NULL AND d.due_at IS NOT NULL AND d.due_at < datetime()
        RETURN count(d) as value
        """,
    )
    pending_access_rows = await _rows(
        graph,
        """
        MATCH (ar:CampaignAccessRequest {status: "pending"})
        OPTIONAL MATCH (c:Campaign)
        WHERE c.id = ar.campaign_id OR c.tenant_id = ar.campaign_id
        OPTIONAL MATCH (u:User {id: ar.user_id})
        WITH ar, c, u,
             CASE
                 WHEN u["email"] IS NULL OR trim(u["email"]) = "" THEN NULL
                 ELSE split(u["email"], "@")[0]
             END as email_prefix
        RETURN
          ar.id as request_id,
          ar.campaign_id as campaign_id,
          ar.status as status,
          coalesce(c.name, c.title, "") as campaign_name,
          ar.user_id as user_id,
          coalesce(u["display_name"], "") as user_display_name,
          coalesce(u["username"], "") as username,
          coalesce(u["email"], "") as email,
          coalesce(email_prefix, "") as email_prefix,
          ar.created_at as created_at
        ORDER BY datetime(ar.created_at) ASC
        LIMIT 50
        """,
    )
    overdue_deadline_rows = await _rows(
        graph,
        """
        MATCH (d:CampaignDeadline)
        WHERE d.completed_at IS NULL
          AND d.due_at IS NOT NULL
          AND d.due_at < datetime()
        OPTIONAL MATCH (c:Campaign)
        WHERE c.id = d.campaign_id OR c.tenant_id = d.campaign_id
        OPTIONAL MATCH (u:User {id: d.assigned_user_id})
        WITH d, c, u,
             CASE
                 WHEN u["email"] IS NULL OR trim(u["email"]) = "" THEN NULL
                 ELSE split(u["email"], "@")[0]
             END as assignee_email_prefix
        RETURN
          d.id as deadline_id,
          d.campaign_id as campaign_id,
          coalesce(c.name, c.title, "") as campaign_name,
          d.title as title,
          d.due_at as due_at,
          d.assigned_user_id as assigned_user_id,
          coalesce(u["display_name"], "") as assignee_display_name,
          coalesce(u["username"], "") as assignee_username,
          coalesce(u["email"], "") as assignee_email,
          coalesce(assignee_email_prefix, "") as assignee_email_prefix
        ORDER BY datetime(d.due_at) ASC
        LIMIT 50
        """,
    )
    stale_assignment_rows = await _rows(
        graph,
        """
        MATCH (d:CampaignDeadline)
        WHERE d.completed_at IS NULL
          AND d.visibility = "personal"
          AND d.due_at IS NOT NULL
          AND d.due_at < datetime() - duration({days: 7})
        OPTIONAL MATCH (c:Campaign)
        WHERE c.id = d.campaign_id OR c.tenant_id = d.campaign_id
        OPTIONAL MATCH (u:User {id: d.assigned_user_id})
        WITH d, c, u,
             CASE
                 WHEN u["email"] IS NULL OR trim(u["email"]) = "" THEN NULL
                 ELSE split(u["email"], "@")[0]
             END as assignee_email_prefix
        RETURN
          d.id as deadline_id,
          d.campaign_id as campaign_id,
          coalesce(c.name, c.title, "") as campaign_name,
          d.title as title,
          d.due_at as due_at,
          d.assigned_user_id as assigned_user_id,
          coalesce(u["display_name"], "") as assignee_display_name,
          coalesce(u["username"], "") as assignee_username,
          coalesce(u["email"], "") as assignee_email,
          coalesce(assignee_email_prefix, "") as assignee_email_prefix
        ORDER BY datetime(d.due_at) ASC
        LIMIT 50
        """,
    )
    return {
        "timeframe": window["timeframe"],
        "timeframe_label": window["label"],
        "access_requests_24h": int(workflow.get("access_requests_24h") or 0),
        "access_decisions_24h": int(workflow.get("access_decisions_24h") or 0),
        "deadline_reminders_24h": int(workflow.get("deadline_reminders_24h") or 0),
        "preview_failures_24h": int(workflow.get("preview_failures_24h") or 0),
        "pending_access_requests": int(pending_access.get("value") or 0),
        "overdue_deadlines": int(overdue_deadlines.get("value") or 0),
        "pending_access_request_list": [
            {
                "request_id": row.get("request_id"),
                "campaign_id": row.get("campaign_id"),
                "status": row.get("status") or "pending",
                "campaign_name": _safe_campaign_name(row.get("campaign_name"), row.get("campaign_id")),
                "campaign_secondary": _safe_campaign_secondary(row.get("campaign_id")),
                "user_id": row.get("user_id"),
                "user_label": _safe_user_label(
                    display_name=row.get("user_display_name"),
                    username=row.get("username"),
                    email=row.get("email"),
                    user_id=row.get("user_id"),
                ),
                "user_secondary": _safe_user_secondary(
                    row.get("user_id"),
                    _safe_user_label(
                        display_name=row.get("user_display_name"),
                        username=row.get("username"),
                        email=row.get("email"),
                        user_id=row.get("user_id"),
                    ),
                ),
                "created_at": row.get("created_at"),
            }
            for row in pending_access_rows
        ],
        "overdue_deadline_list": [
            {
                "deadline_id": row.get("deadline_id"),
                "campaign_id": row.get("campaign_id"),
                "campaign_name": _safe_campaign_name(row.get("campaign_name"), row.get("campaign_id")),
                "campaign_secondary": _safe_campaign_secondary(row.get("campaign_id")),
                "title": row.get("title") or "Untitled deadline",
                "due_at": row.get("due_at"),
                "assigned_user_id": row.get("assigned_user_id"),
                "assigned_user_label": _safe_user_label(
                    display_name=row.get("assignee_display_name"),
                    username=row.get("assignee_username"),
                    email=row.get("assignee_email"),
                    user_id=row.get("assigned_user_id"),
                ),
                "assigned_user_secondary": _safe_user_secondary(
                    row.get("assigned_user_id"),
                    _safe_user_label(
                        display_name=row.get("assignee_display_name"),
                        username=row.get("assignee_username"),
                        email=row.get("assignee_email"),
                        user_id=row.get("assigned_user_id"),
                    ),
                ),
            }
            for row in overdue_deadline_rows
        ],
        "stale_assignment_list": [
            {
                "deadline_id": row.get("deadline_id"),
                "campaign_id": row.get("campaign_id"),
                "campaign_name": _safe_campaign_name(row.get("campaign_name"), row.get("campaign_id")),
                "campaign_secondary": _safe_campaign_secondary(row.get("campaign_id")),
                "title": row.get("title") or "Untitled deadline",
                "due_at": row.get("due_at"),
                "assigned_user_id": row.get("assigned_user_id"),
                "assigned_user_label": _safe_user_label(
                    display_name=row.get("assignee_display_name"),
                    username=row.get("assignee_username"),
                    email=row.get("assignee_email"),
                    user_id=row.get("assigned_user_id"),
                ),
                "assigned_user_secondary": _safe_user_secondary(
                    row.get("assigned_user_id"),
                    _safe_user_label(
                        display_name=row.get("assignee_display_name"),
                        username=row.get("assignee_username"),
                        email=row.get("assignee_email"),
                        user_id=row.get("assigned_user_id"),
                    ),
                ),
            }
            for row in stale_assignment_rows
        ],
    }


@router.get("/mission-control/system-health-summary")
async def mission_control_system_health_summary(
    timeframe: str = Query("30d", pattern="^(24h|7d|30d)$"),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    window = _resolve_timeframe(timeframe)
    system = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({hours: $window_hours})
        RETURN
          sum(CASE WHEN e.source_event_type_raw = "auth_tenant_access_denied" THEN 1 ELSE 0 END) as auth_denied_24h,
          sum(CASE WHEN e.source_event_type_raw = "worker_failed" THEN 1 ELSE 0 END) as worker_failures_24h,
          sum(CASE WHEN e.source_event_type_raw = "report_job_status_changed" AND e.status = "failed" THEN 1 ELSE 0 END) as report_failures_24h,
          sum(CASE WHEN e.source_event_type_raw = "ingestion_failed" THEN 1 ELSE 0 END) as ingestion_failures_24h
        """,
        window_hours=window["window_hours"],
    )
    recent_failures = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({hours: $window_hours})
          AND (
            e.source_event_type_raw = "auth_tenant_access_denied"
            OR e.source_event_type_raw = "worker_failed"
            OR e.source_event_type_raw = "ingestion_failed"
            OR e.source_event_type_raw = "preview_generation_failed"
            OR (e.source_entity = "RileyReportJob" AND e.status = "failed")
          )
        OPTIONAL MATCH (c:Campaign)
        WHERE c.id = e.campaign_id OR c.tenant_id = e.campaign_id
        RETURN
          e.source_event_type_raw as type,
          e.occurred_at as occurred_at,
          e.source_entity as source_entity,
          e.provider as provider,
          e.model as model,
          e.campaign_id as campaign_id,
          coalesce(c.name, c.title, "") as campaign_name,
          e.object_id as object_id,
          e.status as status,
          e.metadata_json as metadata_json
        ORDER BY datetime(e.occurred_at) DESC
        LIMIT 50
        """,
        window_hours=window["window_hours"],
    )
    enriched_failures: List[Dict[str, Any]] = []
    for row in recent_failures:
        metadata = _metadata_json_to_dict(row.get("metadata_json"))
        object_id = str(row.get("object_id") or "").strip()
        worker_name = (
            str(metadata.get("worker") or "").strip()
            or str(metadata.get("worker_name") or "").strip()
            or str(row.get("source_entity") or "").strip()
        )
        detail_message = (
            str(metadata.get("error_message") or "").strip()
            or str(metadata.get("message") or "").strip()
            or str(metadata.get("error_type") or "").strip()
            or str(metadata.get("failure_reason") or "").strip()
        )
        detail_message = " ".join(detail_message.split())[:220]
        failure_class = _failure_classification(
            event_type=str(row.get("type") or ""),
            worker_name=worker_name,
            detail=detail_message,
            metadata=metadata,
        )
        enriched_failures.append(
            {
                "type": row.get("type") or "unknown",
                "failure_label": _failure_type_label(str(row.get("type") or "")),
                "failure_class": failure_class,
                "occurred_at": row.get("occurred_at"),
                "campaign_id": row.get("campaign_id"),
                "campaign_name": _failure_scope_campaign_name(row.get("campaign_name"), row.get("campaign_id")),
                "campaign_secondary": _failure_scope_campaign_secondary(row.get("campaign_id")),
                "object_id": object_id,
                "status": row.get("status"),
                "worker_name": worker_name,
                "provider": row.get("provider"),
                "model": row.get("model"),
                "detail": detail_message,
            }
        )
    return {
        "timeframe": window["timeframe"],
        "timeframe_label": window["label"],
        "auth_denied_24h": int(system.get("auth_denied_24h") or 0),
        "worker_failures_24h": int(system.get("worker_failures_24h") or 0),
        "report_failures_24h": int(system.get("report_failures_24h") or 0),
        "ingestion_failures_24h": int(system.get("ingestion_failures_24h") or 0),
        "recent_failures": enriched_failures,
    }


@router.post("/mission-control/rollups/rebuild")
async def mission_control_rebuild_rollups(
    days_back: int = Query(30, ge=1, le=365),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    result = await graph.rebuild_analytics_daily_rollups(days_back=days_back)
    return {"status": "ok", "days_back": days_back, "rollups": result}

