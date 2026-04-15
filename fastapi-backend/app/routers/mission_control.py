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
from app.services.cloud_billing import cloud_billing_service
from app.services.graph import GraphService
from app.services.llm_cost_guardrail import get_current_month_llm_cost_usd
from app.services.qdrant import vector_service

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
    normalized_display_name = str(display_name or "").strip()
    if normalized_display_name and normalized_display_name.lower() != "unknown user":
        return normalized_display_name
    normalized_username = str(username or "").strip()
    if normalized_username:
        return normalized_username
    normalized_email = str(email or "").strip()
    if normalized_email:
        email_prefix = normalized_email.split("@")[0].strip()
        return email_prefix or normalized_email
    return "Unknown user"


def _safe_user_secondary(user_id: Optional[str], primary_label: str) -> str:
    return ""


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


def _guardrail_status_from_percent(percent_budget_used: float) -> str:
    pct = max(0.0, float(percent_budget_used or 0.0))
    if pct >= 100.0:
        return "exceeded"
    if pct >= 85.0:
        return "red"
    if pct >= 60.0:
        return "yellow"
    return "green"


def _is_llm_event_row(*, provider: Optional[str], model: Optional[str]) -> bool:
    """Return True when an analytics event row should count as LLM spend."""
    normalized_provider = str(provider or "").strip().lower()
    normalized_model = str(model or "").strip().lower()
    return (
        normalized_provider in {"google_gemini", "gemini", "openai"}
        or "gemini" in normalized_model
        or "gpt" in normalized_model
    )


def _request_operation_from_event_type(event_type: Optional[str]) -> str:
    raw = str(event_type or "").strip().lower()
    prefix = "http_request_"
    suffix = "_completed"
    if not raw.startswith(prefix) or not raw.endswith(suffix):
        return "other"
    value = raw[len(prefix) : -len(suffix)]
    return value or "other"


def _window_cloud_cost_from_daily_series(*, daily_series: List[Dict[str, Any]], window_days: int) -> float:
    if not daily_series:
        return 0.0
    start_day = date.today() - timedelta(days=max(1, int(window_days)) - 1)
    total = 0.0
    for row in daily_series:
        day_text = str(row.get("date") or "").strip()
        if not day_text:
            continue
        try:
            if date.fromisoformat(day_text) >= start_day:
                total += float(row.get("cost") or 0.0)
        except Exception:
            continue
    return round(total, 4)


def _sum_daily_cost_for_range(*, daily_series: List[Dict[str, Any]], days: int) -> float:
    if not daily_series:
        return 0.0
    normalized_days = max(1, int(days))
    start_day = date.today() - timedelta(days=normalized_days - 1)
    total = 0.0
    for row in daily_series:
        day_text = str(row.get("day") or row.get("date") or "").strip()
        if not day_text:
            continue
        try:
            if date.fromisoformat(day_text) >= start_day:
                total += float(row.get("cost") or 0.0)
        except Exception:
            continue
    return float(total)


def _weighted_projection_model(
    *,
    daily_series: List[Dict[str, Any]],
    mtd_total: float,
    month_elapsed_days: int,
    days_in_month: int,
) -> Dict[str, float | str]:
    recent_24h_avg = _sum_daily_cost_for_range(daily_series=daily_series, days=1)
    recent_7d_avg = _sum_daily_cost_for_range(daily_series=daily_series, days=7) / 7.0
    mtd_avg = (float(mtd_total or 0.0) / float(max(1, int(month_elapsed_days))))

    weighted_daily = (recent_24h_avg * 0.6) + (recent_7d_avg * 0.3) + (mtd_avg * 0.1)
    projection_mode = "weighted"
    projected_daily = weighted_daily
    if recent_24h_avg < (recent_7d_avg * 0.5):
        projected_daily = recent_24h_avg
        projection_mode = "recent_override"

    projected_month = projected_daily * float(max(1, int(days_in_month)))
    return {
        "recent_24h_avg": round(recent_24h_avg, 6),
        "recent_7d_avg": round(recent_7d_avg, 6),
        "mtd_avg": round(mtd_avg, 6),
        "weighted_daily": round(weighted_daily, 6),
        "projected_daily": round(projected_daily, 6),
        "projected_month": round(projected_month, 2),
        "projection_mode": projection_mode,
    }


def _build_cloud_cost_optimization_recommendations(
    *,
    top_operations: List[Dict[str, Any]],
    endpoint_rows: List[Dict[str, Any]],
) -> List[str]:
    recommendations: List[str] = []
    if not top_operations:
        return recommendations

    largest = top_operations[0]
    largest_operation = str(largest.get("operation_type") or "other")
    largest_percent = float(largest.get("percent_of_cloud_window") or 0.0)
    if largest_operation == "polling_cycle" and largest_percent >= 15.0:
        recommendations.append(
            "Polling endpoints are the top Cloud Run driver; reduce poll frequency and switch high-traffic surfaces to push updates where possible."
        )
    if largest_operation in {"ingestion_job", "document_processing"} and largest_percent >= 15.0:
        recommendations.append(
            "Background processing dominates runtime; prioritize chunk batching, OCR gating, and early-exit checks to reduce worker execution time."
        )
    if largest_operation == "retry" and largest_percent >= 5.0:
        recommendations.append(
            "Retries are a material cost source; inspect failing worker paths and tighten retry policies/timeouts to prevent repeated expensive runs."
        )
    expensive_endpoints = [row for row in endpoint_rows if float(row.get("total_estimated_spend") or 0.0) > 0]
    if expensive_endpoints:
        recommendations.append(
            "Prioritize optimization on the highest-cost endpoints first, starting with latency reduction and caching of repeated GET requests."
        )
    return recommendations[:3]


def _map_llm_feature_category(
    *,
    source_event_type_raw: Optional[str],
    source_entity: Optional[str],
    model: Optional[str],
    metadata: Optional[Dict[str, Any]],
    settings: Any,
) -> Dict[str, str]:
    """Map raw LLM analytics events into Mission Control product feature buckets.

    Mapping priority (highest -> lowest confidence):
    1) Explicit metadata/source markers (mode/service/endpoint/source_event_type_raw)
    2) Inference heuristics (model-name match)
    3) Fallback bucket ("Other LLM")

    Mapping uses existing AnalyticsEvent fields:
    - source_event_type_raw
    - source_entity
    - model
    - metadata_json (parsed)

    For chat_generation_completed, explicit metadata mode is preferred over model-name inference.
    Legacy rows without mode metadata still fall back to model-based inference.
    Unmatched LLM events are intentionally grouped under "Other LLM" to avoid silent drops.
    """
    raw = str(source_event_type_raw or "").strip().lower()
    entity = str(source_entity or "").strip().lower()
    model_name = str(model or "").strip().lower()
    metadata_dict = metadata or {}
    mode_hint = str(
        metadata_dict.get("mode")
        or metadata_dict.get("request_mode")
        or metadata_dict.get("chat_mode")
        or ""
    ).strip().lower()
    service_hint = str(
        metadata_dict.get("service")
        or metadata_dict.get("feature")
        or metadata_dict.get("operation")
        or ""
    ).strip().lower()
    endpoint_hint = str(
        metadata_dict.get("endpoint")
        or metadata_dict.get("route")
        or metadata_dict.get("path")
        or ""
    ).strip().lower()
    deep_model = str(getattr(settings, "RILEY_DEEP_MODEL", "") or "").strip().lower()
    fast_model = str(getattr(settings, "RILEY_MODEL", "") or "").strip().lower()

    if raw == "chat_generation_completed":
        if mode_hint in {"deep", "deep_chat", "deep-mode", "report_deep"} or "deep" in endpoint_hint:
            return {"feature": "Deep Chat", "confidence": "explicit"}
        if mode_hint in {"fast", "fast_chat", "fast-mode", "chat"} or "fast" in endpoint_hint:
            return {"feature": "Fast Chat", "confidence": "explicit"}
        if (deep_model and model_name == deep_model) or ("pro" in model_name and "flash" not in model_name):
            return {"feature": "Deep Chat", "confidence": "inferred"}
        if (fast_model and model_name == fast_model) or ("flash" in model_name):
            return {"feature": "Fast Chat", "confidence": "inferred"}
        return {"feature": "Other LLM", "confidence": "fallback_other"}

    if raw in {
        "report_generation_attempt_completed",
        "report_provider_fallback_triggered",
        "report_provider_fallback_succeeded",
        "report_provider_fallback_failed",
        "report_job_status_changed",
        "report_job_created",
    } or "report" in service_hint or entity == "rileyreportjob":
        return {"feature": "Reports", "confidence": "explicit"}

    if (
        raw == "embedding_indexing_completed"
        or "embedding" in raw
        or ("ingestion" in entity and "embedding" in model_name)
        or "embedding" in service_hint
        or "ingestion" in service_hint
    ):
        return {"feature": "Ingestion / Embeddings", "confidence": "explicit"}

    if raw in {"rerank_completed", "rerank_failed"} or raw.startswith("rerank_") or "rerank" in service_hint:
        return {"feature": "Rerank / Retrieval Enhancement", "confidence": "explicit"}

    if (
        "doc_intel" in raw
        or "document_intelligence" in raw
        or "documentintel" in entity
        or "document_intelligence" in service_hint
        or "doc_intel" in service_hint
    ):
        return {"feature": "Document Intelligence", "confidence": "explicit"}

    if "vision" in raw or "visual" in raw or "vision" in entity or "vision" in service_hint:
        return {"feature": "Vision", "confidence": "explicit"}

    return {"feature": "Other LLM", "confidence": "fallback_other"}


def _rollup_llm_feature_cost(
    *,
    rows: List[Dict[str, Any]],
    settings: Any,
) -> Dict[str, Any]:
    ordered_features = [
        "Fast Chat",
        "Deep Chat",
        "Reports",
        "Ingestion / Embeddings",
        "Document Intelligence",
        "Rerank / Retrieval Enhancement",
        "Vision",
        "Other LLM",
    ]
    totals: Dict[str, float] = {feature: 0.0 for feature in ordered_features}
    confidence_totals: Dict[str, float] = {"explicit": 0.0, "inferred": 0.0, "fallback_other": 0.0}
    per_feature_confidence: Dict[str, Dict[str, float]] = {
        feature: {"explicit": 0.0, "inferred": 0.0, "fallback_other": 0.0} for feature in ordered_features
    }
    for row in rows:
        mapping = _map_llm_feature_category(
            source_event_type_raw=row.get("source_event_type_raw"),
            source_entity=row.get("source_entity"),
            model=row.get("model"),
            metadata=_metadata_json_to_dict(row.get("metadata_json")),
            settings=settings,
        )
        feature = str(mapping.get("feature") or "Other LLM")
        confidence = str(mapping.get("confidence") or "fallback_other")
        cost_value = float(row.get("cost_estimate_usd") or 0.0)
        totals[feature] = float(totals.get(feature) or 0.0) + cost_value
        confidence_totals[confidence] = float(confidence_totals.get(confidence) or 0.0) + cost_value
        if feature in per_feature_confidence and confidence in per_feature_confidence[feature]:
            per_feature_confidence[feature][confidence] = float(per_feature_confidence[feature][confidence] or 0.0) + cost_value
    total_cost = float(sum(totals.values()))
    rows_out: List[Dict[str, Any]] = [
        {
            "feature": feature,
            "spend": round(float(totals.get(feature) or 0.0), 4),
            "percent_of_llm": round(
                ((float(totals.get(feature) or 0.0) / total_cost) * 100.0) if total_cost > 0 else 0.0,
                2,
            ),
            "attribution_confidence": max(
                per_feature_confidence.get(feature, {"explicit": 0.0, "inferred": 0.0, "fallback_other": 0.0}).items(),
                key=lambda item: float(item[1] or 0.0),
            )[0],
        }
        for feature in ordered_features
    ]
    explicit_spend = float(confidence_totals.get("explicit") or 0.0)
    inferred_spend = float(confidence_totals.get("inferred") or 0.0)
    fallback_other_spend = float(confidence_totals.get("fallback_other") or 0.0)
    inferred_or_other_spend = inferred_spend + fallback_other_spend
    other_llm_spend = float(totals.get("Other LLM") or 0.0)
    return {
        "rows": rows_out,
        "mapping_confidence": {
            "explicit_spend": round(explicit_spend, 4),
            "inferred_spend": round(inferred_spend, 4),
            "fallback_other_spend": round(fallback_other_spend, 4),
            "total_llm_spend": round(total_cost, 4),
            "explicit_percent": round(((explicit_spend / total_cost) * 100.0) if total_cost > 0 else 0.0, 2),
            "inferred_or_other_percent": round(
                ((inferred_or_other_spend / total_cost) * 100.0) if total_cost > 0 else 0.0,
                2,
            ),
            "other_llm_spend": round(other_llm_spend, 4),
            "other_llm_percent": round(((other_llm_spend / total_cost) * 100.0) if total_cost > 0 else 0.0, 2),
        },
    }


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


async def _batch_resolve_user_identities(
    graph: GraphService,
    user_ids: List[Optional[str]],
) -> Dict[str, Dict[str, Optional[str]]]:
    unique_ids = sorted(
        {
            str(value or "").strip()
            for value in user_ids
            if str(value or "").strip()
            and str(value or "").strip().lower() not in {"unknown_user", "unknown", "none", "null"}
            and not str(value or "").strip().lower().startswith("system:")
        }
    )
    resolved = await graph.resolve_user_identities_batch(unique_ids)
    normalized: Dict[str, Dict[str, Optional[str]]] = {}
    for user_id in unique_ids:
        identity = resolved.get(user_id) or {}
        normalized[user_id] = {
            "display_name": str(identity.get("display_name") or "").strip() or "Unknown user",
            "email": str(identity.get("email") or "").strip() or None,
            "avatar_url": str(identity.get("avatar_url") or "").strip() or None,
        }
    return normalized


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
    month_event_cost_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($month_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
        RETURN
          coalesce(e.provider, "") as provider,
          coalesce(e.model, "") as model,
          toFloat(e.cost_estimate_usd) as cost_estimate_usd
        """,
        month_start_iso=datetime.now(timezone.utc).replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        ).isoformat(),
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
    llm_trend_cost_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($window_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
        WITH date(datetime(e.occurred_at)) as event_day, coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as cost
        RETURN toString(event_day) as day, round(cost, 4) as cost
        ORDER BY day ASC
        """,
        window_start_iso=window_start_iso,
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
    current_month_llm_spend = round(float(await get_current_month_llm_cost_usd()), 2)
    current_month_non_llm_event_spend = round(
        sum(
            float(row.get("cost_estimate_usd") or 0.0)
            for row in month_event_cost_rows
            if not _is_llm_event_row(provider=row.get("provider"), model=row.get("model"))
        ),
        2,
    )
    forecast_month_end_llm_spend = round(
        (current_month_llm_spend / float(month_elapsed_days)) * float(days_in_month),
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

    llm_cost_by_day = {
        str(row.get("day") or ""): round(float(row.get("cost") or 0.0), 4)
        for row in llm_trend_cost_rows
    }
    overview_user_identity = await _batch_resolve_user_identities(
        graph,
        [row.get("user_id") for row in user_usage_rows],
    )
    return {
        "active_users_7d": int(active_users.get("value") or 0),
        "active_campaigns_7d": int(active_campaigns.get("value") or 0),
        "chats_today": int(activity_metrics.get("chats") or 0),
        "reports_today": int(activity_metrics.get("reports") or 0),
        "avg_response_latency_ms": avg_response_latency_ms,
        "report_success_rate": report_success_rate,
        # Data source note: LLM spend fields are computed from AnalyticsEvent provider/model
        # filters used by llm_cost_guardrail.py (Gemini/OpenAI family only).
        "current_month_llm_spend": current_month_llm_spend,
        "forecast_month_end_llm_spend": forecast_month_end_llm_spend,
        # Data source note: non-LLM event spend tracks operational event-attributed cost
        # (e.g., cloud_run/gcs/vision) and is kept separate from cloud billing totals.
        "current_month_non_llm_event_spend": current_month_non_llm_event_spend,
        # Backward-compatible aliases retained for existing frontend consumers.
        "current_month_estimated_cost": current_month_llm_spend,
        "forecast_month_end_cost": forecast_month_end_llm_spend,
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
                "cost": round(float(llm_cost_by_day.get(str(row.get("day") or ""), 0.0)), 4),
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
                "display_name": (
                    overview_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name")
                    or _safe_user_label(
                        display_name=overview_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
                        username=row.get("username"),
                        email=row.get("email"),
                        user_id=row.get("user_id"),
                    )
                ),
                "email": (
                    overview_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("email")
                    or (str(row.get("email") or "").strip() or None)
                ),
                "avatar_url": (
                    overview_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("avatar_url")
                    or None
                ),
                "user_label": _safe_user_label(
                    display_name=overview_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
                    username=row.get("username"),
                    email=(
                        overview_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("email")
                        or row.get("email")
                    ),
                    user_id=row.get("user_id"),
                ),
                "user_secondary": _safe_user_secondary(
                    row.get("user_id"),
                    _safe_user_label(
                        display_name=overview_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
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
    settings = get_settings()
    window = _resolve_timeframe(timeframe)
    await _maybe_schedule_rollup_refresh(graph)
    window_start_day = _window_start_day(int(window["window_days"]))
    window_start_iso = _window_start_iso(int(window["window_hours"]))
    last_7d_start_iso = (datetime.now(timezone.utc) - timedelta(days=6)).isoformat()
    today_day = date.today().isoformat()
    month_start_day = date.today().replace(day=1).isoformat()
    month_start_iso = datetime.now(timezone.utc).replace(
        day=1,
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ).isoformat()
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
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
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
    # Data source note: canonical LLM/non-LLM event spend split from raw AnalyticsEvent
    # so LLM cards do not accidentally include cloud_run / storage / worker-runtime costs.
    llm_month_summary = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($month_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
        RETURN
          coalesce(sum(CASE
            WHEN (
              toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
              OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
              OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
            ) THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as llm_month_cost,
          coalesce(sum(CASE
            WHEN (
              toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
              OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
              OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
            ) THEN 0.0 ELSE toFloat(e.cost_estimate_usd) END), 0.0) as non_llm_month_cost
        """,
        month_start_iso=month_start_iso,
    )
    llm_window_summary = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($window_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
        RETURN
          coalesce(sum(CASE
            WHEN (
              toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
              OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
              OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
            ) THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as llm_window_cost,
          coalesce(sum(CASE
            WHEN (
              toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
              OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
              OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
            ) THEN 0.0 ELSE toFloat(e.cost_estimate_usd) END), 0.0) as non_llm_window_cost
        """,
        window_start_iso=window_start_iso,
    )
    llm_last_7d_summary = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($last_7d_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
        RETURN coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as llm_last_7d_cost
        """,
        last_7d_start_iso=last_7d_start_iso,
    )
    llm_provider_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($window_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
        WITH coalesce(e.provider, "unknown") as provider, coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as cost
        RETURN provider, round(cost, 4) as cost
        ORDER BY cost DESC
        LIMIT 25
        """,
        window_start_iso=window_start_iso,
    )
    non_llm_provider_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($window_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND NOT (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
        WITH coalesce(e.provider, "unknown") as provider, coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as cost
        RETURN provider, round(cost, 4) as cost
        ORDER BY cost DESC
        LIMIT 25
        """,
        window_start_iso=window_start_iso,
    )
    llm_cost_by_campaign_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($window_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
          AND e.campaign_id IS NOT NULL
          AND trim(e.campaign_id) <> ""
          AND e.campaign_id <> "global"
          AND e.campaign_id <> "unknown_campaign"
        WITH e.campaign_id as campaign_id, coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as cost
        ORDER BY cost DESC
        LIMIT 25
        OPTIONAL MATCH (c:Campaign)
        WHERE c.id = campaign_id OR c.tenant_id = campaign_id
        RETURN campaign_id, coalesce(c.name, c.title, "") as campaign_name, round(cost, 4) as cost
        """,
        window_start_iso=window_start_iso,
    )
    llm_cost_by_user_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($window_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
          AND coalesce(e.user_id, "") <> ""
          AND coalesce(e.user_id, "") <> "unknown_user"
          AND coalesce(e.user_id, "") <> "system:deadline-reminder"
        WITH coalesce(e.user_id, "") as user_id, coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as cost
        ORDER BY cost DESC
        LIMIT 25
        OPTIONAL MATCH (u:User {id: user_id})
        WITH user_id, u, cost,
             CASE
                 WHEN u["email"] IS NULL OR trim(u["email"]) = "" THEN NULL
                 ELSE split(u["email"], "@")[0]
             END as email_prefix
        RETURN
          user_id,
          coalesce(u["username"], "") as username,
          coalesce(u["email"], "") as email,
          coalesce(email_prefix, "") as email_prefix,
          round(cost, 4) as cost
        """,
        window_start_iso=window_start_iso,
    )
    llm_cost_trend_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($window_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
        WITH date(datetime(e.occurred_at)) as event_day, coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as cost
        RETURN toString(event_day) as day, round(cost, 4) as cost
        ORDER BY day ASC
        """,
        window_start_iso=window_start_iso,
    )
    llm_month_daily_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($month_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
        WITH date(datetime(e.occurred_at)) as event_day, coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as cost
        RETURN toString(event_day) as day, round(cost, 6) as cost
        ORDER BY day ASC
        """,
        month_start_iso=month_start_iso,
    )
    window_cost = round(float(llm_window_summary.get("llm_window_cost") or 0.0), 2)
    current_month_cost = round(float(llm_month_summary.get("llm_month_cost") or 0.0), 2)
    current_month_non_llm_event_spend = round(float(llm_month_summary.get("non_llm_month_cost") or 0.0), 2)
    non_llm_event_window_spend = round(float(llm_window_summary.get("non_llm_window_cost") or 0.0), 2)
    chats_30d = int(activity_counts.get("chats") or 0)
    reports_30d = int(activity_counts.get("reports") or 0)
    cost_per_chat = round(window_cost / float(chats_30d), 6) if chats_30d else 0.0
    cost_per_report = round(window_cost / float(reports_30d), 6) if reports_30d else 0.0
    now = datetime.now(timezone.utc)
    days_in_month = monthrange(now.year, now.month)[1]
    month_elapsed_days = max(1, now.day)
    llm_projection = _weighted_projection_model(
        daily_series=llm_month_daily_rows,
        mtd_total=current_month_cost,
        month_elapsed_days=month_elapsed_days,
        days_in_month=days_in_month,
    )
    daily_burn_rate = float(llm_projection.get("projected_daily") or 0.0)
    projected_month_end_cost = float(llm_projection.get("projected_month") or 0.0)
    monthly_llm_budget_limit = round(float(settings.MAX_MONTHLY_COST or 2000.0), 2)
    current_month_llm_spend = round(float(await get_current_month_llm_cost_usd()), 2)
    projected_month_end_llm_spend = round(float(projected_month_end_cost), 2)
    percent_budget_used = round(
        (current_month_llm_spend / monthly_llm_budget_limit) * 100.0,
        2,
    ) if monthly_llm_budget_limit > 0 else 0.0
    guardrail_status = _guardrail_status_from_percent(percent_budget_used)
    cloud_usage = await cloud_billing_service.get_cloud_infra_cost_metrics()
    cloud_month_total = float(cloud_usage.get("current_month_cloud_cost") or 0.0)
    cloud_daily_series = [
        {"day": str(row.get("date") or ""), "cost": float(row.get("cost") or 0.0)}
        for row in (cloud_usage.get("daily_cloud_cost_series") or [])
        if str(row.get("date") or "").strip()
    ]
    cloud_window_spend = round(
        _window_cloud_cost_from_daily_series(
            daily_series=list(cloud_usage.get("daily_cloud_cost_series") or []),
            window_days=int(window["window_days"]),
        ),
        2,
    )
    cloud_projection = _weighted_projection_model(
        daily_series=cloud_daily_series,
        mtd_total=cloud_month_total,
        month_elapsed_days=month_elapsed_days,
        days_in_month=days_in_month,
    )
    projected_month_end_cloud_spend = round(float(cloud_projection.get("projected_month") or 0.0), 2)
    qdrant_variable_projected_month_end_spend = 0.0
    try:
        qdrant_usage = await vector_service.get_qdrant_usage_metrics()
        qdrant_variable_projected_month_end_spend = round(float(qdrant_usage.get("estimated_monthly_cost") or 0.0), 2)
    except Exception as exc:
        logger.warning("mission_control_qdrant_total_spend_unavailable error=%s", exc)
    window_days = max(1, int(window["window_days"]))
    qdrant_variable_daily_rate = (
        (qdrant_variable_projected_month_end_spend / float(days_in_month))
        if days_in_month > 0 else 0.0
    )
    qdrant_variable_current_month_spend = round(
        qdrant_variable_daily_rate * float(window_days),
        2,
    )
    qdrant_fixed_projected_month_end_spend = round(float(settings.QDRANT_BASE_MONTHLY_COST_USD or 0.0), 2)
    qdrant_fixed_daily_rate = (
        (qdrant_fixed_projected_month_end_spend / float(days_in_month))
        if days_in_month > 0 else 0.0
    )
    qdrant_fixed_current_month_spend = round(
        qdrant_fixed_daily_rate * float(window_days),
        2,
    )
    fixed_saas_clerk_projected_month_end_spend = round(float(settings.CLERK_MONTHLY_COST_USD or 0.0), 2)
    fixed_saas_vercel_projected_month_end_spend = round(float(settings.VERCEL_MONTHLY_COST_USD or 0.0), 2)
    fixed_saas_other_projected_month_end_spend = round(float(settings.OTHER_FIXED_MONTHLY_COST_USD or 0.0), 2)
    other_fixed_saas_projected_month_end_spend = round(
        fixed_saas_clerk_projected_month_end_spend
        + fixed_saas_vercel_projected_month_end_spend
        + fixed_saas_other_projected_month_end_spend,
        2,
    )
    fixed_saas_clerk_daily_rate = (
        (fixed_saas_clerk_projected_month_end_spend / float(days_in_month))
        if days_in_month > 0 else 0.0
    )
    fixed_saas_vercel_daily_rate = (
        (fixed_saas_vercel_projected_month_end_spend / float(days_in_month))
        if days_in_month > 0 else 0.0
    )
    fixed_saas_other_daily_rate = (
        (fixed_saas_other_projected_month_end_spend / float(days_in_month))
        if days_in_month > 0 else 0.0
    )
    fixed_saas_clerk_current_month_spend = round(
        fixed_saas_clerk_daily_rate * float(window_days),
        2,
    )
    fixed_saas_vercel_current_month_spend = round(
        fixed_saas_vercel_daily_rate * float(window_days),
        2,
    )
    fixed_saas_other_current_month_spend = round(
        fixed_saas_other_daily_rate * float(window_days),
        2,
    )
    other_fixed_saas_current_month_spend = round(
        fixed_saas_clerk_current_month_spend
        + fixed_saas_vercel_current_month_spend
        + fixed_saas_other_current_month_spend,
        2,
    )
    estimated_accrual_daily_rate = (
        qdrant_variable_daily_rate
        + qdrant_fixed_daily_rate
        + fixed_saas_clerk_daily_rate
        + fixed_saas_vercel_daily_rate
        + fixed_saas_other_daily_rate
    )

    llm_day_cost_by_date = {
        str(row.get("day") or "").strip(): float(row.get("cost") or 0.0)
        for row in llm_month_daily_rows
        if str(row.get("day") or "").strip()
    }
    cloud_day_cost_by_date = {
        str(row.get("day") or "").strip(): float(row.get("cost") or 0.0)
        for row in cloud_daily_series
        if str(row.get("day") or "").strip()
    }
    total_projection_daily_series: List[Dict[str, Any]] = []
    month_start_date = date.today().replace(day=1)
    for day_index in range(month_elapsed_days):
        day_value = (month_start_date + timedelta(days=day_index)).isoformat()
        total_projection_daily_series.append(
            {
                "day": day_value,
                "cost": (
                    llm_day_cost_by_date.get(day_value, 0.0)
                    + cloud_day_cost_by_date.get(day_value, 0.0)
                    + estimated_accrual_daily_rate
                ),
            }
        )
    current_month_actual_or_metered_spend = round(
        window_cost + cloud_window_spend,
        2,
    )
    current_month_estimated_accrual_spend = round(
        qdrant_variable_current_month_spend
        + qdrant_fixed_current_month_spend
        + other_fixed_saas_current_month_spend,
        2,
    )
    current_month_blended_total_spend = round(
        current_month_actual_or_metered_spend + current_month_estimated_accrual_spend,
        2,
    )
    total_projection = _weighted_projection_model(
        daily_series=total_projection_daily_series,
        mtd_total=(
            float(current_month_cost)
            + float(cloud_month_total)
            + (estimated_accrual_daily_rate * float(month_elapsed_days))
        ),
        month_elapsed_days=month_elapsed_days,
        days_in_month=days_in_month,
    )
    projected_month_end_total_spend = round(
        float(total_projection.get("projected_month") or 0.0),
        2,
    )
    projection_mode = str(total_projection.get("projection_mode") or "weighted")
    current_month_cloud_spend = cloud_window_spend
    total_spend_breakdown = [
        {
            "category": "LLM",
            "current": window_cost,
            "current_semantics": "actual_or_metered_mtd",
            "projected": projected_month_end_llm_spend,
        },
        {
            "category": "Cloud Infrastructure",
            "current": current_month_cloud_spend,
            "current_semantics": "actual_or_metered_mtd",
            "projected": projected_month_end_cloud_spend,
        },
        {
            "category": "Qdrant Variable",
            "current": qdrant_variable_current_month_spend,
            "current_semantics": "estimated_accrual_mtd",
            "projected": qdrant_variable_projected_month_end_spend,
        },
        {
            "category": "Qdrant Fixed",
            "current": qdrant_fixed_current_month_spend,
            "current_semantics": "estimated_accrual_mtd",
            "projected": qdrant_fixed_projected_month_end_spend,
        },
        {
            "category": "Other Fixed SaaS",
            "current": other_fixed_saas_current_month_spend,
            "current_semantics": "estimated_accrual_mtd",
            "projected": other_fixed_saas_projected_month_end_spend,
        },
    ]
    llm_month_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($month_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
        RETURN
          e.source_event_type_raw as source_event_type_raw,
          e.source_entity as source_entity,
          e.model as model,
          e.metadata_json as metadata_json,
          toFloat(e.cost_estimate_usd) as cost_estimate_usd
        """,
        month_start_iso=month_start_iso,
    )
    llm_last_30d_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($last_30d_start_iso)
          AND e.cost_estimate_usd IS NOT NULL
          AND (
            toLower(coalesce(e.provider, "")) IN ["google_gemini", "gemini", "openai"]
            OR toLower(coalesce(e.model, "")) CONTAINS "gemini"
            OR toLower(coalesce(e.model, "")) CONTAINS "gpt"
          )
        RETURN
          e.source_event_type_raw as source_event_type_raw,
          e.source_entity as source_entity,
          e.model as model,
          e.metadata_json as metadata_json,
          toFloat(e.cost_estimate_usd) as cost_estimate_usd
        """,
        last_30d_start_iso=(datetime.now(timezone.utc) - timedelta(days=30)).isoformat(),
    )
    current_month_feature_rollup = _rollup_llm_feature_cost(rows=llm_month_rows, settings=settings)
    last_30d_feature_rollup = _rollup_llm_feature_cost(rows=llm_last_30d_rows, settings=settings)
    current_month_llm_cost_by_feature = list(current_month_feature_rollup.get("rows") or [])
    last_30d_llm_cost_by_feature = list(last_30d_feature_rollup.get("rows") or [])
    cost_user_identity = await _batch_resolve_user_identities(
        graph,
        [row.get("user_id") for row in llm_cost_by_user_rows],
    )
    projected_curve = []
    for day in range(1, days_in_month + 1):
        projected_value = float(llm_projection.get("projected_daily") or 0.0) * float(day)
        projected_curve.append({"day": day, "projected_cost": round(projected_value, 2)})
    return {
        "timeframe": window["timeframe"],
        "timeframe_label": window["label"],
        "month_estimated_cost": window_cost,
        "selected_window_cost": window_cost,
        "current_month_cost": window_cost,
        "average_daily_burn_rate": round(float(llm_projection.get("projected_daily") or 0.0), 4),
        "projected_month_end_cost": projected_month_end_cost,
        "current_month_days_elapsed": month_elapsed_days,
        "current_month_total_days": days_in_month,
        "last_7d_estimated_cost": round(float(llm_last_7d_summary.get("llm_last_7d_cost") or 0.0), 2),
        "projection_mode": projection_mode,
        "cost_confidence_breakdown_30d": {
            "exact_usage": round(float(confidence_totals.get("exact_usage_cost_30d") or 0.0), 4),
            "estimated_units": round(float(confidence_totals.get("estimated_units_cost_30d") or 0.0), 4),
            "proxy_only": round(float(confidence_totals.get("proxy_only_cost_30d") or 0.0), 4),
        },
        # Data source note: provider_cost_30d is LLM-only (Gemini/OpenAI-family events).
        "provider_cost_30d": [
            {"provider": row.get("provider"), "cost": float(row.get("cost") or 0.0)}
            for row in llm_provider_rows
        ],
        # Data source note: tracked_non_llm_event_spend values are event-attributed non-LLM
        # operational estimates and are excluded from total-spend rollups to avoid cloud overlap.
        "current_month_non_llm_event_spend": current_month_non_llm_event_spend,
        "window_non_llm_event_spend": non_llm_event_window_spend,
        "tracked_non_llm_event_provider_breakdown_30d": [
            {"provider": row.get("provider"), "cost": float(row.get("cost") or 0.0)}
            for row in non_llm_provider_rows
        ],
        "cost_per_chat": cost_per_chat,
        "cost_per_report": cost_per_report,
        "monthly_llm_budget_limit": monthly_llm_budget_limit,
        "current_month_llm_spend": current_month_llm_spend,
        "projected_month_end_llm_spend": projected_month_end_llm_spend,
        "percent_budget_used": percent_budget_used,
        "guardrail_status": guardrail_status,
        "current_month_cloud_spend": current_month_cloud_spend,
        "projected_month_end_cloud_spend": projected_month_end_cloud_spend,
        "cloud_cost_is_available": bool(cloud_usage.get("is_available")),
        "cloud_cost_unavailable_reason": str(cloud_usage.get("unavailable_reason") or ""),
        "cloud_cost_last_data_timestamp": cloud_usage.get("last_data_timestamp"),
        "cloud_cost_billing_data_lag_hours": (
            float(cloud_usage.get("billing_data_lag_hours") or 0.0)
            if cloud_usage.get("billing_data_lag_hours") is not None
            else None
        ),
        "current_month_qdrant_variable_spend": qdrant_variable_current_month_spend,
        "projected_month_end_qdrant_variable_spend": qdrant_variable_projected_month_end_spend,
        "current_month_qdrant_fixed_spend": qdrant_fixed_current_month_spend,
        "projected_month_end_qdrant_fixed_spend": qdrant_fixed_projected_month_end_spend,
        "current_month_other_fixed_saas_spend": other_fixed_saas_current_month_spend,
        "projected_month_end_other_fixed_saas_spend": other_fixed_saas_projected_month_end_spend,
        "fixed_saas_component_breakdown": [
            {
                "component": "Clerk",
                "current_mtd": fixed_saas_clerk_current_month_spend,
                "projected_month_end": fixed_saas_clerk_projected_month_end_spend,
            },
            {
                "component": "Vercel",
                "current_mtd": fixed_saas_vercel_current_month_spend,
                "projected_month_end": fixed_saas_vercel_projected_month_end_spend,
            },
            {
                "component": "Other Fixed SaaS",
                "current_mtd": fixed_saas_other_current_month_spend,
                "projected_month_end": fixed_saas_other_projected_month_end_spend,
            },
        ],
        # Backward-compatible aliases (deprecated)
        "current_month_qdrant_spend": qdrant_variable_current_month_spend,
        "projected_month_end_qdrant_spend": qdrant_variable_projected_month_end_spend,
        "current_month_fixed_saas_spend": (
            qdrant_fixed_current_month_spend + other_fixed_saas_current_month_spend
        ),
        "projected_month_end_fixed_saas_spend": (
            qdrant_fixed_projected_month_end_spend + other_fixed_saas_projected_month_end_spend
        ),
        # Total-spend semantics:
        # - actual_or_metered_mtd: measured/metered MTD categories (Cloud may lag billing export ingestion)
        # - estimated_accrual_mtd: prorated/accrual estimates for fixed + variable components
        # - blended_total_mtd: sum(actual_or_metered_mtd + estimated_accrual_mtd)
        "current_month_actual_or_metered_spend": current_month_actual_or_metered_spend,
        "current_month_estimated_accrual_spend": current_month_estimated_accrual_spend,
        "current_month_blended_total_spend": current_month_blended_total_spend,
        # Backward-compatible alias (deprecated): this is blended, not pure actual.
        "current_month_total_spend": current_month_blended_total_spend,
        "projected_month_end_total_spend": projected_month_end_total_spend,
        "total_spend_breakdown": total_spend_breakdown,
        "current_month_llm_cost_by_feature": current_month_llm_cost_by_feature,
        "last_30d_llm_cost_by_feature": last_30d_llm_cost_by_feature,
        "current_month_llm_feature_mapping_confidence": current_month_feature_rollup.get("mapping_confidence") or {},
        "last_30d_llm_feature_mapping_confidence": last_30d_feature_rollup.get("mapping_confidence") or {},
        "cost_by_campaign_30d": [
            {
                "campaign_id": row.get("campaign_id"),
                "campaign_name": _safe_campaign_name(row.get("campaign_name"), row.get("campaign_id")),
                "campaign_secondary": _safe_campaign_secondary(row.get("campaign_id")),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in llm_cost_by_campaign_rows
        ],
        "cost_by_user_30d": [
            {
                "user_id": row.get("user_id"),
                "display_name": (
                    cost_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name")
                    or _safe_user_label(
                        display_name=cost_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
                        username=row.get("username"),
                        email=row.get("email"),
                        user_id=row.get("user_id"),
                    )
                ),
                "email": (
                    cost_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("email")
                    or (str(row.get("email") or "").strip() or None)
                ),
                "avatar_url": (
                    cost_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("avatar_url")
                    or None
                ),
                "user_label": _safe_user_label(
                    display_name=cost_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
                    username=row.get("username"),
                    email=(
                        cost_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("email")
                        or row.get("email")
                    ),
                    user_id=row.get("user_id"),
                ),
                "user_secondary": _safe_user_secondary(
                    row.get("user_id"),
                    _safe_user_label(
                        display_name=cost_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
                        username=row.get("username"),
                        email=row.get("email"),
                        user_id=row.get("user_id"),
                    ),
                ),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in llm_cost_by_user_rows
        ],
        "cost_trend_30d": [
            {"day": row.get("day"), "cost": round(float(row.get("cost") or 0.0), 4)}
            for row in _fill_day_series(rows=llm_cost_trend_rows, window_days=window["window_days"], value_fields=["cost"], caster=float)
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
    window_start_iso = _window_start_iso(int(window["window_hours"]))
    excluded_system_actor_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.occurred_at >= $window_start_iso
          AND e.actor_user_id IS NOT NULL
          AND (
            trim(coalesce(e.actor_user_id, "")) = ""
            OR toLower(trim(coalesce(e.actor_user_id, ""))) STARTS WITH "system:"
            OR toLower(trim(coalesce(e.actor_user_id, ""))) IN ["system", "unknown_user", "unknown", "null", "none"]
          )
        RETURN
          coalesce(e.actor_user_id, "") as actor_user_id,
          coalesce(e.user_id, "") as user_id,
          coalesce(e.source_event_type_raw, "") as event_type
        LIMIT 50
        """,
        window_start_iso=window_start_iso,
    )
    for row in excluded_system_actor_rows:
        logger.info(
            "mission_control_event_excluded_system_actor actor_user_id=%s user_id=%s event_type=%s",
            row.get("actor_user_id"),
            row.get("user_id"),
            row.get("event_type"),
        )
    filter_audit = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.occurred_at >= $window_start_iso
        WITH
          count(e) as total_events,
          count(
            CASE
              WHEN (
                e.actor_user_id IS NOT NULL
                AND trim(coalesce(e.actor_user_id, "")) <> ""
                AND NOT toLower(trim(coalesce(e.actor_user_id, ""))) STARTS WITH "system:"
                AND NOT (toLower(trim(coalesce(e.actor_user_id, ""))) IN ["system", "unknown_user", "unknown", "null", "none"])
              )
              THEN 1
              WHEN (
                e.actor_user_id IS NULL
                AND trim(coalesce(e.user_id, "")) <> ""
                AND NOT toLower(trim(coalesce(e.user_id, ""))) STARTS WITH "system:"
                AND NOT (toLower(trim(coalesce(e.user_id, ""))) IN ["system", "unknown_user", "unknown", "null", "none"])
              )
              THEN 1
              ELSE NULL
            END
          ) as included_events
        RETURN total_events, included_events
        """,
        window_start_iso=window_start_iso,
    )
    excluded_non_user = int(filter_audit.get("total_events") or 0) - int(filter_audit.get("included_events") or 0)
    if excluded_non_user > 0:
        logger.info(
            "mission_control_event_excluded_non_user timeframe=%s excluded_count=%s total_events=%s",
            window.get("timeframe"),
            excluded_non_user,
            int(filter_audit.get("total_events") or 0),
        )
    metrics = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.occurred_at >= $window_start_iso
        WITH
          e,
          CASE
            WHEN (
              e.actor_user_id IS NOT NULL
              AND trim(coalesce(e.actor_user_id, "")) <> ""
              AND NOT toLower(trim(coalesce(e.actor_user_id, ""))) STARTS WITH "system:"
              AND NOT (toLower(trim(coalesce(e.actor_user_id, ""))) IN ["system", "unknown_user", "unknown", "null", "none"])
            )
            THEN trim(coalesce(e.actor_user_id, ""))
            WHEN (
              e.actor_user_id IS NULL
              AND trim(coalesce(e.user_id, "")) <> ""
              AND NOT toLower(trim(coalesce(e.user_id, ""))) STARTS WITH "system:"
              AND NOT (toLower(trim(coalesce(e.user_id, ""))) IN ["system", "unknown_user", "unknown", "null", "none"])
            )
            THEN trim(coalesce(e.user_id, ""))
            ELSE ""
          END as human_user_id
        WHERE human_user_id <> ""
        RETURN
          count(e) as events_30d,
          count(DISTINCT human_user_id) as unique_users_30d,
          count(
            DISTINCT CASE
              WHEN (
                e.campaign_id IS NOT NULL
                AND trim(e.campaign_id) <> ""
                AND e.campaign_id <> "global"
                AND e.campaign_id <> "unknown_campaign"
              )
              THEN e.campaign_id
              ELSE NULL
            END
          ) as unique_campaigns_30d,
          count(
            DISTINCT CASE
              WHEN e.source_event_type_raw = "chat_generation_completed"
              THEN human_user_id
              ELSE NULL
            END
          ) as chat_users_30d,
          count(
            DISTINCT CASE
              WHEN (e.source_entity = "RileyReportJob" AND e.object_id IS NOT NULL)
              THEN human_user_id
              ELSE NULL
            END
          ) as report_users_30d
        """,
        window_start_iso=window_start_iso,
    )
    user_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.occurred_at >= $window_start_iso
        WITH
          e,
          CASE
            WHEN (
              e.actor_user_id IS NOT NULL
              AND trim(coalesce(e.actor_user_id, "")) <> ""
              AND NOT toLower(trim(coalesce(e.actor_user_id, ""))) STARTS WITH "system:"
              AND NOT (toLower(trim(coalesce(e.actor_user_id, ""))) IN ["system", "unknown_user", "unknown", "null", "none"])
            )
            THEN trim(coalesce(e.actor_user_id, ""))
            WHEN (
              e.actor_user_id IS NULL
              AND trim(coalesce(e.user_id, "")) <> ""
              AND NOT toLower(trim(coalesce(e.user_id, ""))) STARTS WITH "system:"
              AND NOT (toLower(trim(coalesce(e.user_id, ""))) IN ["system", "unknown_user", "unknown", "null", "none"])
            )
            THEN trim(coalesce(e.user_id, ""))
            ELSE ""
          END as user_id
        WHERE trim(user_id) <> ""
        WITH user_id,
             count(e) as events,
             count(
               DISTINCT CASE
                 WHEN e.source_event_type_raw = "chat_generation_completed"
                 THEN coalesce(e.event_id, "")
                 ELSE NULL
               END
             ) as chats,
             sum(
               CASE
                 WHEN (e.source_entity = "RileyReportJob" AND e.object_id IS NOT NULL)
                 THEN 1
                 ELSE 0
               END
             ) as reports
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
          coalesce(u["username"], "") as username,
          coalesce(u["email"], "") as email,
          coalesce(email_prefix, "") as email_prefix,
          events,
          chats,
          reports
        """,
        window_start_iso=window_start_iso,
    )
    adoption_user_identity = await _batch_resolve_user_identities(
        graph,
        [row.get("user_id") for row in user_rows],
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
                "display_name": (
                    adoption_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name")
                    or _safe_user_label(
                        display_name=adoption_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
                        username=row.get("username"),
                        email=row.get("email"),
                        user_id=row.get("user_id"),
                    )
                ),
                "email": (
                    adoption_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("email")
                    or (str(row.get("email") or "").strip() or None)
                ),
                "avatar_url": (
                    adoption_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("avatar_url")
                    or None
                ),
                "user_label": _safe_user_label(
                    display_name=adoption_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
                    username=row.get("username"),
                    email=(
                        adoption_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("email")
                        or row.get("email")
                    ),
                    user_id=row.get("user_id"),
                ),
                "user_secondary": _safe_user_secondary(
                    row.get("user_id"),
                    _safe_user_label(
                        display_name=adoption_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
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
          coalesce(u["username"], "") as username,
          coalesce(u["email"], "") as email,
          coalesce(email_prefix, "") as email_prefix,
          toString(ar.created_at) as created_at
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
          toString(d.due_at) as due_at,
          d.assigned_user_id as assigned_user_id,
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
          toString(d.due_at) as due_at,
          d.assigned_user_id as assigned_user_id,
          coalesce(u["username"], "") as assignee_username,
          coalesce(u["email"], "") as assignee_email,
          coalesce(assignee_email_prefix, "") as assignee_email_prefix
        ORDER BY datetime(d.due_at) ASC
        LIMIT 50
        """,
    )
    workflow_user_identity = await _batch_resolve_user_identities(
        graph,
        [
            *[row.get("user_id") for row in pending_access_rows],
            *[row.get("assigned_user_id") for row in overdue_deadline_rows],
            *[row.get("assigned_user_id") for row in stale_assignment_rows],
        ],
    )
    return {
        "timeframe": window["timeframe"],
        "timeframe_label": window["label"],
        "access_requests_24h": int(workflow.get("access_requests_24h") or 0),
        "access_decisions_24h": int(workflow.get("access_decisions_24h") or 0),
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
                "display_name": (
                    workflow_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name")
                    or _safe_user_label(
                        display_name=workflow_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
                        username=row.get("username"),
                        email=row.get("email"),
                        user_id=row.get("user_id"),
                    )
                ),
                "email": (
                    workflow_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("email")
                    or (str(row.get("email") or "").strip() or None)
                ),
                "avatar_url": (
                    workflow_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("avatar_url")
                    or None
                ),
                "user_label": _safe_user_label(
                    display_name=workflow_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
                    username=row.get("username"),
                    email=(
                        workflow_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("email")
                        or row.get("email")
                    ),
                    user_id=row.get("user_id"),
                ),
                "user_secondary": _safe_user_secondary(
                    row.get("user_id"),
                    _safe_user_label(
                        display_name=workflow_user_identity.get(str(row.get("user_id") or "").strip(), {}).get("display_name"),
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
                "assigned_user_display_name": (
                    workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("display_name")
                    or _safe_user_label(
                        display_name=workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("display_name"),
                        username=row.get("assignee_username"),
                        email=row.get("assignee_email"),
                        user_id=row.get("assigned_user_id"),
                    )
                ),
                "assigned_user_email": (
                    workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("email")
                    or (str(row.get("assignee_email") or "").strip() or None)
                ),
                "assigned_user_avatar_url": (
                    workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("avatar_url")
                    or None
                ),
                "assigned_user_label": _safe_user_label(
                    display_name=workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("display_name"),
                    username=row.get("assignee_username"),
                    email=(
                        workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("email")
                        or row.get("assignee_email")
                    ),
                    user_id=row.get("assigned_user_id"),
                ),
                "assigned_user_secondary": _safe_user_secondary(
                    row.get("assigned_user_id"),
                    _safe_user_label(
                        display_name=workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("display_name"),
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
                "assigned_user_display_name": (
                    workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("display_name")
                    or _safe_user_label(
                        display_name=workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("display_name"),
                        username=row.get("assignee_username"),
                        email=row.get("assignee_email"),
                        user_id=row.get("assigned_user_id"),
                    )
                ),
                "assigned_user_email": (
                    workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("email")
                    or (str(row.get("assignee_email") or "").strip() or None)
                ),
                "assigned_user_avatar_url": (
                    workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("avatar_url")
                    or None
                ),
                "assigned_user_label": _safe_user_label(
                    display_name=workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("display_name"),
                    username=row.get("assignee_username"),
                    email=(
                        workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("email")
                        or row.get("assignee_email")
                    ),
                    user_id=row.get("assigned_user_id"),
                ),
                "assigned_user_secondary": _safe_user_secondary(
                    row.get("assigned_user_id"),
                    _safe_user_label(
                        display_name=workflow_user_identity.get(str(row.get("assigned_user_id") or "").strip(), {}).get("display_name"),
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
          AND coalesce(e.resolved, false) = false
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
          e.event_id as failure_id,
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
                "failure_id": row.get("failure_id"),
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


@router.post("/mission-control/failures/{failure_id}/resolve")
async def mission_control_resolve_failure(
    failure_id: str,
    current_user: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    resolved_by = str(current_user.get("id") or "").strip() or "unknown"
    row = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent {event_id: $failure_id})
        WHERE
          e.source_event_type_raw = "auth_tenant_access_denied"
          OR e.source_event_type_raw = "worker_failed"
          OR e.source_event_type_raw = "ingestion_failed"
          OR e.source_event_type_raw = "preview_generation_failed"
          OR (e.source_entity = "RileyReportJob" AND e.status = "failed")
        WITH e, coalesce(e.resolved, false) as already_resolved
        SET
          e.resolved = true,
          e.resolved_at = coalesce(e.resolved_at, datetime()),
          e.resolved_by = coalesce(e.resolved_by, $resolved_by),
          e.updated_at = datetime()
        RETURN
          e.event_id as failure_id,
          e.source_event_type_raw as type,
          already_resolved as already_resolved,
          toString(e.resolved_at) as resolved_at,
          e.resolved_by as resolved_by
        """,
        failure_id=failure_id,
        resolved_by=resolved_by,
    )
    if not row:
        raise HTTPException(status_code=404, detail="Failure not found.")
    logger.info(
        "mission_control_failure_resolved failure_id=%s resolved_by=%s already_resolved=%s",
        row.get("failure_id"),
        resolved_by,
        bool(row.get("already_resolved")),
    )
    return {
        "failure_id": row.get("failure_id"),
        "type": row.get("type"),
        "resolved": True,
        "already_resolved": bool(row.get("already_resolved")),
        "resolved_at": row.get("resolved_at"),
        "resolved_by": row.get("resolved_by"),
    }


@router.post("/mission-control/rollups/rebuild")
async def mission_control_rebuild_rollups(
    days_back: int = Query(30, ge=1, le=365),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    result = await graph.rebuild_analytics_daily_rollups(days_back=days_back)
    return {"status": "ok", "days_back": days_back, "rollups": result}


@router.get("/metrics/qdrant")
async def mission_control_qdrant_metrics(
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    """Return Qdrant usage and estimated monthly storage cost."""
    usage = await vector_service.get_qdrant_usage_metrics()
    campaign_rows = usage.get("campaigns") or []
    campaign_ids = [
        str(row.get("campaign_id") or "").strip()
        for row in campaign_rows
        if str(row.get("campaign_id") or "").strip()
    ]
    campaign_name_by_id = await graph.get_campaign_names_by_ids(campaign_ids) if campaign_ids else {}
    enriched_campaigns: List[Dict[str, Any]] = []
    for row in campaign_rows:
        campaign_id = str(row.get("campaign_id") or "").strip()
        resolved_name = str(campaign_name_by_id.get(campaign_id) or campaign_id or "unknown_campaign")
        enriched_campaigns.append(
            {
                "campaign_id": campaign_id,
                "campaign_name": resolved_name,
                "vectors": int(row.get("vectors") or 0),
                "estimated_size_mb": round(float(row.get("estimated_size_mb") or 0.0), 2),
            }
        )
    total_vectors = int(usage.get("total_vectors") or 0)
    total_estimated_size_mb = round(float(usage.get("total_estimated_size_mb") or 0.0), 2)
    estimated_monthly_cost = round(float(usage.get("estimated_monthly_cost") or 0.0), 2)
    projection_available = False
    projection_unavailable_reason: Optional[str] = None
    projected_next_month_storage_mb = 0.0
    projected_next_month_cost = 0.0
    growth_percent = 0.0
    growth_mb_per_day = 0.0
    projection_confidence = "unavailable"
    projection_snapshot_count = 0
    projection_min_snapshots_required = 7
    projection_window_days_used = 0
    projection_method = "recent-window-linear-growth"
    warning_state = "normal"
    warning_reasons: List[str] = []

    try:
        await graph.upsert_qdrant_usage_snapshot(
            total_vectors=total_vectors,
            total_estimated_size_mb=total_estimated_size_mb,
            estimated_monthly_cost=estimated_monthly_cost,
        )
    except Exception as exc:
        logger.warning("mission_control_qdrant_snapshot_upsert_failed error=%s", exc)

    trend_series = await graph.list_qdrant_usage_snapshots(days_back=60, limit=120)
    projection_snapshot_count = len(trend_series)
    if projection_snapshot_count >= projection_min_snapshots_required:
        # Forecast from recent history (last 7-14 snapshots), not first-vs-last ever.
        recent_window_size = min(14, projection_snapshot_count)
        recent_window = trend_series[-recent_window_size:]
        projection_window_days_used = recent_window_size
        first = recent_window[0]
        last = recent_window[-1]
        first_date = str(first.get("date") or "")
        last_date = str(last.get("date") or "")
        try:
            first_dt = datetime.fromisoformat(first_date)
            last_dt = datetime.fromisoformat(last_date)
            days_between = max(1, (last_dt - first_dt).days)
            size_start = float(first.get("total_estimated_size_mb") or 0.0)
            size_end = float(last.get("total_estimated_size_mb") or 0.0)
            growth_mb_per_day = (size_end - size_start) / float(days_between)

            next_month_year = datetime.now(timezone.utc).year + (1 if datetime.now(timezone.utc).month == 12 else 0)
            next_month_num = 1 if datetime.now(timezone.utc).month == 12 else datetime.now(timezone.utc).month + 1
            next_month_days = monthrange(next_month_year, next_month_num)[1]
            projected_next_month_storage_mb = round(
                max(0.0, size_end + (growth_mb_per_day * float(next_month_days))),
                2,
            )
            qdrant_cost_per_gb_month_usd = float(get_settings().QDRANT_COST_PER_GB_MONTH_USD or 0.25)
            projected_next_month_cost = round(
                (projected_next_month_storage_mb / 1024.0) * qdrant_cost_per_gb_month_usd,
                2,
            )
            growth_percent = round(
                ((size_end - size_start) / size_start) * 100.0 if size_start > 0 else 0.0,
                2,
            )
            projection_available = True
            projection_confidence = "high" if projection_snapshot_count >= 10 else "low"
        except Exception as exc:
            projection_unavailable_reason = f"Projection unavailable due to invalid snapshot timeline: {exc}"
            projection_confidence = "unavailable"
    else:
        projection_unavailable_reason = (
            f"Collecting history: {projection_snapshot_count}/{projection_min_snapshots_required} daily snapshots available."
        )
        projection_confidence = "unavailable"

    settings = get_settings()
    growth_warning_threshold = float(settings.QDRANT_GROWTH_WARNING_PERCENT or 20.0)
    projected_cost_target = float(settings.QDRANT_PROJECTED_MONTHLY_COST_TARGET_USD or 0.0)
    # Only trigger warning state when projection confidence is sufficient.
    if projection_available and projection_confidence == "high":
        if growth_percent > growth_warning_threshold:
            warning_reasons.append("growth_threshold_exceeded")
        if projected_cost_target > 0 and projected_next_month_cost > projected_cost_target:
            warning_reasons.append("projected_cost_target_exceeded")
    if warning_reasons:
        warning_state = "warning"

    return {
        "total_vectors": total_vectors,
        "total_estimated_size_mb": total_estimated_size_mb,
        "campaigns": enriched_campaigns,
        "estimated_monthly_cost": estimated_monthly_cost,
        "trend_series": trend_series,
        "projection_available": projection_available,
        "projection_unavailable_reason": projection_unavailable_reason,
        "projection_confidence": projection_confidence,
        "projection_snapshot_count": projection_snapshot_count,
        "projection_min_snapshots_required": projection_min_snapshots_required,
        "projection_window_days_used": projection_window_days_used,
        "projection_method": projection_method,
        "projected_next_month_storage_mb": projected_next_month_storage_mb,
        "projected_next_month_cost": projected_next_month_cost,
        "growth_percent": growth_percent,
        "growth_mb_per_day": round(float(growth_mb_per_day), 4),
        "warning_state": warning_state,
        "warning_reasons": warning_reasons,
    }


@router.get("/metrics/cloud-infrastructure-cost")
async def mission_control_cloud_infrastructure_cost(
    _: Dict = Depends(verify_mission_control_admin),
) -> Dict[str, Any]:
    """Return Cloud Infrastructure billing metrics from configured GCP billing export."""
    usage = await cloud_billing_service.get_cloud_infra_cost_metrics()
    return {
        "current_month_cloud_cost": round(float(usage.get("current_month_cloud_cost") or 0.0), 2),
        "projected_month_end_cloud_cost": round(float(usage.get("projected_month_end_cloud_cost") or 0.0), 2),
        "daily_cloud_cost_series": usage.get("daily_cloud_cost_series") or [],
        "cloud_cost_breakdown": usage.get("cloud_cost_breakdown") or [],
        "is_available": bool(usage.get("is_available")),
        "unavailable_reason": usage.get("unavailable_reason"),
        "last_data_timestamp": usage.get("last_data_timestamp"),
        "billing_data_lag_hours": usage.get("billing_data_lag_hours"),
        "billing_source": str(usage.get("billing_source") or ""),
    }


@router.get("/mission-control/cloud-run-cost-attribution")
async def mission_control_cloud_run_cost_attribution(
    timeframe: str = Query("30d", pattern="^(24h|7d|30d)$"),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    """Attribute Cloud Run spend to concrete Riley request behaviors."""
    window = _resolve_timeframe(timeframe)
    window_start_iso = _window_start_iso(int(window["window_hours"]))
    cloud_usage = await cloud_billing_service.get_cloud_infra_cost_metrics()
    daily_series = list(cloud_usage.get("daily_cloud_cost_series") or [])
    cloud_window_cost = _window_cloud_cost_from_daily_series(
        daily_series=daily_series,
        window_days=int(window["window_days"]),
    )

    request_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($window_start_iso)
          AND e.source_entity = "HttpRequest"
          AND e.source_event_type_raw STARTS WITH "http_request_"
          AND e.source_event_type_raw ENDS WITH "_completed"
        RETURN
          e.source_event_type_raw as event_type,
          coalesce(e.object_id, "unknown_endpoint") as endpoint,
          coalesce(e.user_id, "") as user_id,
          count(e) as requests,
          coalesce(sum(toFloat(e.latency_ms)), 0.0) as duration_ms
        ORDER BY duration_ms DESC
        """,
        window_start_iso=window_start_iso,
    )

    total_duration_ms = sum(float(row.get("duration_ms") or 0.0) for row in request_rows)
    operation_rollup: Dict[str, Dict[str, float]] = {}
    endpoint_rollup: Dict[str, Dict[str, float]] = {}
    user_rollup: Dict[str, Dict[str, float]] = {}

    for row in request_rows:
        operation_type = _request_operation_from_event_type(row.get("event_type"))
        endpoint = str(row.get("endpoint") or "unknown_endpoint")
        user_id = str(row.get("user_id") or "").strip() or "system_or_unknown"
        requests_count = int(row.get("requests") or 0)
        duration_ms = float(row.get("duration_ms") or 0.0)
        allocated_cost = (
            (duration_ms / total_duration_ms) * cloud_window_cost
            if total_duration_ms > 0 and cloud_window_cost > 0
            else 0.0
        )

        op_row = operation_rollup.setdefault(operation_type, {"requests": 0.0, "duration_ms": 0.0, "cost": 0.0})
        op_row["requests"] += float(requests_count)
        op_row["duration_ms"] += duration_ms
        op_row["cost"] += allocated_cost

        endpoint_row = endpoint_rollup.setdefault(endpoint, {"requests": 0.0, "duration_ms": 0.0, "cost": 0.0})
        endpoint_row["requests"] += float(requests_count)
        endpoint_row["duration_ms"] += duration_ms
        endpoint_row["cost"] += allocated_cost

        user_row = user_rollup.setdefault(user_id, {"requests": 0.0, "duration_ms": 0.0, "cost": 0.0})
        user_row["requests"] += float(requests_count)
        user_row["duration_ms"] += duration_ms
        user_row["cost"] += allocated_cost

    operation_rows = sorted(
        [
            {
                "operation_type": key,
                "action_count": int(value["requests"]),
                "total_estimated_spend": round(float(value["cost"]), 4),
                "estimated_avg_spend_per_action": round(
                    (float(value["cost"]) / float(value["requests"])) if value["requests"] > 0 else 0.0,
                    6,
                ),
                "avg_latency_ms": round(
                    (float(value["duration_ms"]) / float(value["requests"])) if value["requests"] > 0 else 0.0,
                    2,
                ),
                "percent_of_cloud_window": round(
                    ((float(value["cost"]) / cloud_window_cost) * 100.0) if cloud_window_cost > 0 else 0.0,
                    2,
                ),
            }
            for key, value in operation_rollup.items()
        ],
        key=lambda item: float(item.get("total_estimated_spend") or 0.0),
        reverse=True,
    )
    endpoint_rows = sorted(
        [
            {
                "endpoint": key,
                "action_count": int(value["requests"]),
                "total_estimated_spend": round(float(value["cost"]), 4),
                "estimated_avg_spend_per_action": round(
                    (float(value["cost"]) / float(value["requests"])) if value["requests"] > 0 else 0.0,
                    6,
                ),
                "avg_latency_ms": round(
                    (float(value["duration_ms"]) / float(value["requests"])) if value["requests"] > 0 else 0.0,
                    2,
                ),
            }
            for key, value in endpoint_rollup.items()
        ],
        key=lambda item: float(item.get("total_estimated_spend") or 0.0),
        reverse=True,
    )
    user_rows = sorted(
        [
            {
                "user_id": key,
                "action_count": int(value["requests"]),
                "total_estimated_spend": round(float(value["cost"]), 4),
                "estimated_avg_spend_per_action": round(
                    (float(value["cost"]) / float(value["requests"])) if value["requests"] > 0 else 0.0,
                    6,
                ),
                "avg_latency_ms": round(
                    (float(value["duration_ms"]) / float(value["requests"])) if value["requests"] > 0 else 0.0,
                    2,
                ),
            }
            for key, value in user_rollup.items()
        ],
        key=lambda item: float(item.get("total_estimated_spend") or 0.0),
        reverse=True,
    )

    default_action_costs = {
        "chat": 0.0,
        "report": 0.0,
        "ingestion_job": 0.0,
        "document_upload": 0.0,
        "polling_cycle": 0.0,
    }
    op_cost_map = {str(row.get("operation_type")): float(row.get("total_estimated_spend") or 0.0) for row in operation_rows}
    cost_per_action_estimates = {
        "chat": round(op_cost_map.get("chat_request", 0.0), 4),
        "report": round(op_cost_map.get("report_generation", 0.0), 4),
        "ingestion_job": round(op_cost_map.get("ingestion_job", 0.0), 4),
        "document_upload": round(op_cost_map.get("document_upload", 0.0), 4),
        "polling_cycle": round(op_cost_map.get("polling_cycle", 0.0), 4),
    }
    for key in default_action_costs:
        cost_per_action_estimates.setdefault(key, default_action_costs[key])

    top_cost_drivers = operation_rows[:3]
    most_expensive_endpoints = endpoint_rows[:10]
    ocr_summary = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime($window_start_iso)
          AND e.source_event_type_raw = "ocr_completed"
        RETURN
          count(e) as action_count,
          coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as total_estimated_spend,
          coalesce(avg(toFloat(e.latency_ms)), 0.0) as avg_latency_ms
        """,
        window_start_iso=window_start_iso,
    )
    ocr_action_count = int(ocr_summary.get("action_count") or 0)
    ocr_total_spend = round(float(ocr_summary.get("total_estimated_spend") or 0.0), 4)
    ocr_avg_spend_per_action = round(
        (ocr_total_spend / float(ocr_action_count)) if ocr_action_count > 0 else 0.0,
        6,
    )
    ocr_avg_latency_ms = round(float(ocr_summary.get("avg_latency_ms") or 0.0), 2)
    recommendations = _build_cloud_cost_optimization_recommendations(
        top_operations=top_cost_drivers,
        endpoint_rows=most_expensive_endpoints,
    )

    return {
        "timeframe": window["timeframe"],
        "timeframe_label": window["label"],
        "cloud_cost_is_available": bool(cloud_usage.get("is_available")),
        "cloud_cost_unavailable_reason": str(cloud_usage.get("unavailable_reason") or ""),
        "cloud_window_cost_usd": round(cloud_window_cost, 4),
        "request_volume": {
            "total_requests": int(sum(int(row.get("requests") or 0) for row in request_rows)),
            "total_duration_ms": round(float(total_duration_ms), 2),
            "window_start_iso": window_start_iso,
        },
        "cost_per_action_estimates": cost_per_action_estimates,
        "cost_by_operation_type": operation_rows,
        "cost_by_endpoint": endpoint_rows,
        "cost_by_user": user_rows[:25],
        "ocr_observability": {
            "action_count": ocr_action_count,
            "total_estimated_spend": ocr_total_spend,
            "estimated_avg_spend_per_action": ocr_avg_spend_per_action,
            "avg_latency_ms": ocr_avg_latency_ms,
        },
        "top_cost_drivers": top_cost_drivers,
        "most_expensive_endpoints": most_expensive_endpoints,
        "recommended_optimizations": recommendations,
    }

