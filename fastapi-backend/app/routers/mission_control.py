from calendar import monthrange
from datetime import datetime, timezone
from typing import Any, Dict, List

from fastapi import APIRouter, Depends, Query

from app.dependencies.auth import verify_mission_control_admin
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService

router = APIRouter()


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
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    active_users = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
        WITH coalesce(e.user_id, e.actor_user_id, "") as uid
        WHERE trim(uid) <> ""
        RETURN count(DISTINCT uid) as value
        """,
    )
    active_campaigns = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND e.campaign_id IS NOT NULL
          AND trim(e.campaign_id) <> ""
          AND e.campaign_id <> "global"
        RETURN count(DISTINCT e.campaign_id) as value
        """,
    )
    chats_today = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE date(datetime(e.occurred_at)) = date()
          AND (
            e.feature_area = "chat"
            OR e.source_entity IN ["Message", "TeamMessage", "ThreadMessage"]
          )
        RETURN count(e) as value
        """,
    )
    reports_today = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE date(datetime(e.occurred_at)) = date()
          AND e.source_entity = "RileyReportJob"
          AND e.object_id IS NOT NULL
        RETURN count(DISTINCT e.object_id) as value
        """,
    )
    avg_latency = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND e.feature_area = "chat"
          AND e.latency_ms IS NOT NULL
        RETURN avg(toFloat(e.latency_ms)) as value
        """,
    )
    report_success = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND e.source_entity = "RileyReportJob"
          AND e.status IN ["complete", "failed"]
          AND e.object_id IS NOT NULL
        WITH e.object_id as report_job_id, e.status as status, datetime(e.occurred_at) as ts
        ORDER BY report_job_id, ts DESC
        WITH report_job_id, collect(status)[0] as latest_status
        RETURN
          count(report_job_id) as total,
          sum(CASE WHEN latest_status = "complete" THEN 1 ELSE 0 END) as success
        """,
    )
    month_cost = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.cost_estimate_usd IS NOT NULL
          AND date(datetime(e.occurred_at)).year = date().year
          AND date(datetime(e.occurred_at)).month = date().month
        RETURN coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0) as value
        """,
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
    failed_reports_24h = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({hours: 24})
          AND e.source_entity = "RileyReportJob"
          AND e.status = "failed"
          AND e.object_id IS NOT NULL
        RETURN count(DISTINCT e.object_id) as value
        """,
    )
    trend_rows = await _rows(
        graph,
        """
        UNWIND range(0, 29) as day_offset
        WITH date() - duration({days: day_offset}) as d
        OPTIONAL MATCH (e:AnalyticsEvent)
        WHERE date(datetime(e.occurred_at)) = d
        WITH d, collect(e) as events
        RETURN
          toString(d) as day,
          size([ev IN events WHERE ev.feature_area = "chat" OR ev.source_entity IN ["Message", "TeamMessage", "ThreadMessage"]]) as chats,
          size([ev IN events WHERE ev.source_entity = "RileyReportJob" AND ev.object_id IS NOT NULL]) as reports,
          reduce(cost = 0.0, ev IN events | cost + coalesce(toFloat(ev.cost_estimate_usd), 0.0)) as cost,
          size([ev IN events WHERE (ev.source_event_type_raw = "worker_failed")
                               OR (ev.source_event_type_raw = "ingestion_failed")
                               OR (ev.source_event_type_raw = "preview_generation_failed")
                               OR (ev.source_entity = "RileyReportJob" AND ev.status = "failed")]) as failures
        ORDER BY day ASC
        """,
    )
    campaign_usage_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND e.campaign_id IS NOT NULL
          AND trim(e.campaign_id) <> ""
          AND e.campaign_id <> "global"
        OPTIONAL MATCH (c:Campaign {tenant_id: e.campaign_id})
        WITH e.campaign_id as campaign_id, coalesce(c.name, c.title, e.campaign_id) as campaign_name, collect(e) as events
        RETURN
          campaign_id,
          campaign_name,
          size([ev IN events WHERE ev.feature_area = "chat" OR ev.source_entity IN ["Message", "TeamMessage", "ThreadMessage"]]) as chats,
          size([ev IN events WHERE ev.source_entity = "RileyReportJob" AND ev.object_id IS NOT NULL]) as reports,
          round(reduce(cost = 0.0, ev IN events | cost + coalesce(toFloat(ev.cost_estimate_usd), 0.0)), 4) as cost
        ORDER BY (chats + reports) DESC, cost DESC
        LIMIT 25
        """,
    )
    user_usage_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
        WITH coalesce(e.user_id, e.actor_user_id, "") as user_id, collect(e) as events
        WHERE trim(user_id) <> ""
        RETURN
          user_id,
          size([ev IN events WHERE ev.feature_area = "chat" OR ev.source_entity IN ["Message", "TeamMessage", "ThreadMessage"]]) as chats,
          size([ev IN events WHERE ev.source_entity = "RileyReportJob" AND ev.object_id IS NOT NULL]) as reports,
          round(reduce(cost = 0.0, ev IN events | cost + coalesce(toFloat(ev.cost_estimate_usd), 0.0)), 4) as cost
        ORDER BY (chats + reports) DESC, cost DESC
        LIMIT 25
        """,
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

    return {
        "active_users_7d": int(active_users.get("value") or 0),
        "active_campaigns_7d": int(active_campaigns.get("value") or 0),
        "chats_today": int(chats_today.get("value") or 0),
        "reports_today": int(reports_today.get("value") or 0),
        "avg_response_latency_ms": round(float(avg_latency.get("value") or 0.0), 2),
        "report_success_rate": report_success_rate,
        "current_month_estimated_cost": round(current_month_estimated_cost, 2),
        "forecast_month_end_cost": forecast_month_end_cost,
        "pending_access_requests": int(pending_access_requests.get("value") or 0),
        "overdue_deadlines": int(overdue_deadlines.get("value") or 0),
        "failed_reports_24h": int(failed_reports_24h.get("value") or 0),
        "timeseries_30d": [
            {
                "day": row.get("day"),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
                "cost": round(float(row.get("cost") or 0.0), 4),
                "failures": int(row.get("failures") or 0),
            }
            for row in trend_rows
        ],
        "campaign_usage_7d": [
            {
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name") or row.get("campaign_id"),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in campaign_usage_rows
        ],
        "user_usage_7d": [
            {
                "user_id": row.get("user_id"),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in user_usage_rows
        ],
    }


@router.get("/mission-control/riley-performance")
async def mission_control_riley_performance(
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    metrics = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
        RETURN
          avg(toFloat(e.latency_ms)) as avg_latency_ms,
          percentileCont(toFloat(e.latency_ms), 0.95) as p95_latency_ms,
          sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_succeeded" THEN 1 ELSE 0 END) as provider_fallback_successes,
          sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_failed" THEN 1 ELSE 0 END) as provider_fallback_failures,
          sum(CASE WHEN e.source_event_type_raw = "reranker_failed" THEN 1 ELSE 0 END) as reranker_failures
        """,
    )
    by_campaign = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND e.campaign_id IS NOT NULL
          AND trim(e.campaign_id) <> ""
          AND e.campaign_id <> "global"
        OPTIONAL MATCH (c:Campaign {tenant_id: e.campaign_id})
        WITH e.campaign_id as campaign_id, coalesce(c.name, c.title, e.campaign_id) as campaign_name, e
        RETURN
          campaign_id,
          campaign_name,
          round(avg(toFloat(e.latency_ms)), 2) as avg_latency_ms,
          round(percentileCont(toFloat(e.latency_ms), 0.95), 2) as p95_latency_ms,
          sum(CASE WHEN e.feature_area = "chat" OR e.source_entity IN ["Message", "TeamMessage", "ThreadMessage"] THEN 1 ELSE 0 END) as chats,
          sum(CASE WHEN e.source_entity = "RileyReportJob" AND e.object_id IS NOT NULL THEN 1 ELSE 0 END) as reports
        ORDER BY p95_latency_ms DESC, reports DESC
        LIMIT 20
        """,
    )
    provider_breakdown = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND e.provider IS NOT NULL
          AND trim(e.provider) <> ""
        WITH e.provider as provider, e
        RETURN
          provider,
          round(avg(toFloat(e.latency_ms)), 2) as avg_latency_ms,
          sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_triggered" THEN 1 ELSE 0 END) as fallback_triggered,
          sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_succeeded" THEN 1 ELSE 0 END) as fallback_succeeded,
          sum(CASE WHEN e.source_event_type_raw = "report_provider_fallback_failed" THEN 1 ELSE 0 END) as fallback_failed
        ORDER BY fallback_triggered DESC, avg_latency_ms DESC
        LIMIT 20
        """,
    )
    trend_rows = await _rows(
        graph,
        """
        UNWIND range(0, 29) as day_offset
        WITH date() - duration({days: day_offset}) as d
        OPTIONAL MATCH (e:AnalyticsEvent)
        WHERE date(datetime(e.occurred_at)) = d
        WITH d, collect(e) as events
        RETURN
          toString(d) as day,
          size([ev IN events WHERE ev.source_entity = "RileyReportJob" AND ev.status = "complete" AND ev.object_id IS NOT NULL]) as report_successes,
          size([ev IN events WHERE ev.source_entity = "RileyReportJob" AND ev.status = "failed" AND ev.object_id IS NOT NULL]) as report_failures
        ORDER BY day ASC
        """,
    )
    p50 = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND e.latency_ms IS NOT NULL
        RETURN percentileCont(toFloat(e.latency_ms), 0.50) as value
        """,
    )
    p75 = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND e.latency_ms IS NOT NULL
        RETURN percentileCont(toFloat(e.latency_ms), 0.75) as value
        """,
    )
    p90 = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND e.latency_ms IS NOT NULL
        RETURN percentileCont(toFloat(e.latency_ms), 0.90) as value
        """,
    )
    fallback_successes = int(metrics.get("provider_fallback_successes") or 0)
    fallback_failures = int(metrics.get("provider_fallback_failures") or 0)
    fallback_total = fallback_successes + fallback_failures
    fallback_success_rate = round((fallback_successes / float(fallback_total)) * 100.0, 2) if fallback_total else 0.0
    return {
        "avg_latency_ms_7d": round(float(metrics.get("avg_latency_ms") or 0.0), 2),
        "p95_latency_ms_7d": round(float(metrics.get("p95_latency_ms") or 0.0), 2),
        "provider_fallback_successes_7d": fallback_successes,
        "provider_fallback_failures_7d": fallback_failures,
        "provider_fallback_success_rate_7d": fallback_success_rate,
        "reranker_failures_7d": int(metrics.get("reranker_failures") or 0),
        "latency_percentiles_7d": {
            "p50_ms": round(float(p50.get("value") or 0.0), 2),
            "p75_ms": round(float(p75.get("value") or 0.0), 2),
            "p90_ms": round(float(p90.get("value") or 0.0), 2),
            "p95_ms": round(float(metrics.get("p95_latency_ms") or 0.0), 2),
        },
        "success_failure_trend_30d": [
            {
                "day": row.get("day"),
                "success": int(row.get("report_successes") or 0),
                "failure": int(row.get("report_failures") or 0),
            }
            for row in trend_rows
        ],
        "campaign_performance_7d": [
            {
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name") or row.get("campaign_id"),
                "avg_latency_ms": round(float(row.get("avg_latency_ms") or 0.0), 2),
                "p95_latency_ms": round(float(row.get("p95_latency_ms") or 0.0), 2),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
            }
            for row in by_campaign
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
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    totals = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.cost_estimate_usd IS NOT NULL
        RETURN
          coalesce(sum(CASE WHEN date(datetime(e.occurred_at)).year = date().year
                             AND date(datetime(e.occurred_at)).month = date().month
                            THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as month_cost,
          coalesce(sum(CASE WHEN datetime(e.occurred_at) >= datetime() - duration({days: 7})
                            THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as last_7d_cost,
          coalesce(sum(CASE WHEN datetime(e.occurred_at) >= datetime() - duration({days: 30})
                             AND coalesce(e.cost_confidence, "proxy_only") = "exact_usage"
                            THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as exact_usage_cost_30d,
          coalesce(sum(CASE WHEN datetime(e.occurred_at) >= datetime() - duration({days: 30})
                             AND coalesce(e.cost_confidence, "proxy_only") = "estimated_units"
                            THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as estimated_units_cost_30d,
          coalesce(sum(CASE WHEN datetime(e.occurred_at) >= datetime() - duration({days: 30})
                             AND coalesce(e.cost_confidence, "proxy_only") = "proxy_only"
                            THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as proxy_only_cost_30d
        """,
    )
    by_provider_rows: list[Dict[str, Any]] = []
    async with graph.driver.session() as session:
        result = await session.run(
            """
            MATCH (e:AnalyticsEvent)
            WHERE e.cost_estimate_usd IS NOT NULL
              AND datetime(e.occurred_at) >= datetime() - duration({days: 30})
            RETURN coalesce(e.provider, "unknown") as provider,
                   round(sum(toFloat(e.cost_estimate_usd)), 4) as cost
            ORDER BY cost DESC
            """
        )
        async for row in result:
            by_provider_rows.append({"provider": row.get("provider"), "cost": float(row.get("cost") or 0.0)})
    cost_by_campaign_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.cost_estimate_usd IS NOT NULL
          AND datetime(e.occurred_at) >= datetime() - duration({days: 30})
          AND e.campaign_id IS NOT NULL
          AND trim(e.campaign_id) <> ""
          AND e.campaign_id <> "global"
        OPTIONAL MATCH (c:Campaign {tenant_id: e.campaign_id})
        RETURN
          e.campaign_id as campaign_id,
          coalesce(c.name, c.title, e.campaign_id) as campaign_name,
          round(sum(toFloat(e.cost_estimate_usd)), 4) as cost
        ORDER BY cost DESC
        LIMIT 25
        """,
    )
    cost_by_user_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE e.cost_estimate_usd IS NOT NULL
          AND datetime(e.occurred_at) >= datetime() - duration({days: 30})
        WITH coalesce(e.user_id, e.actor_user_id, "") as user_id, e
        WHERE trim(user_id) <> ""
        RETURN
          user_id,
          round(sum(toFloat(e.cost_estimate_usd)), 4) as cost
        ORDER BY cost DESC
        LIMIT 25
        """,
    )
    cost_trend_rows = await _rows(
        graph,
        """
        UNWIND range(0, 29) as day_offset
        WITH date() - duration({days: day_offset}) as d
        OPTIONAL MATCH (e:AnalyticsEvent)
        WHERE date(datetime(e.occurred_at)) = d
          AND e.cost_estimate_usd IS NOT NULL
        RETURN
          toString(d) as day,
          round(coalesce(sum(toFloat(e.cost_estimate_usd)), 0.0), 4) as cost
        ORDER BY day ASC
        """,
    )
    chat_count_30d = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 30})
          AND (e.feature_area = "chat" OR e.source_entity IN ["Message", "TeamMessage", "ThreadMessage"])
        RETURN count(e) as value
        """,
    )
    report_count_30d = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 30})
          AND e.source_entity = "RileyReportJob"
          AND e.object_id IS NOT NULL
        RETURN count(DISTINCT e.object_id) as value
        """,
    )
    month_cost = round(float(totals.get("month_cost") or 0.0), 2)
    chats_30d = int(chat_count_30d.get("value") or 0)
    reports_30d = int(report_count_30d.get("value") or 0)
    cost_per_chat = round(month_cost / float(chats_30d), 6) if chats_30d else 0.0
    cost_per_report = round(month_cost / float(reports_30d), 6) if reports_30d else 0.0
    now = datetime.now(timezone.utc)
    days_in_month = monthrange(now.year, now.month)[1]
    month_elapsed_days = max(1, now.day)
    projected_curve = []
    for day in range(1, days_in_month + 1):
        projected_value = (month_cost / float(month_elapsed_days)) * float(day)
        projected_curve.append({"day": day, "projected_cost": round(projected_value, 2)})
    return {
        "month_estimated_cost": month_cost,
        "last_7d_estimated_cost": round(float(totals.get("last_7d_cost") or 0.0), 2),
        "cost_confidence_breakdown_30d": {
            "exact_usage": round(float(totals.get("exact_usage_cost_30d") or 0.0), 4),
            "estimated_units": round(float(totals.get("estimated_units_cost_30d") or 0.0), 4),
            "proxy_only": round(float(totals.get("proxy_only_cost_30d") or 0.0), 4),
        },
        "provider_cost_30d": by_provider_rows,
        "cost_per_chat": cost_per_chat,
        "cost_per_report": cost_per_report,
        "cost_by_campaign_30d": [
            {
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name") or row.get("campaign_id"),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in cost_by_campaign_rows
        ],
        "cost_by_user_30d": [
            {
                "user_id": row.get("user_id"),
                "cost": round(float(row.get("cost") or 0.0), 4),
            }
            for row in cost_by_user_rows
        ],
        "cost_trend_30d": [
            {"day": row.get("day"), "cost": round(float(row.get("cost") or 0.0), 4)}
            for row in cost_trend_rows
        ],
        "projected_monthly_curve": projected_curve,
    }


@router.get("/mission-control/adoption-summary")
async def mission_control_adoption_summary(
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    metrics = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 30})
        WITH e, date(datetime(e.occurred_at)) as d
        RETURN
          count(e) as events_30d,
          count(DISTINCT coalesce(e.user_id, e.actor_user_id, "")) as unique_users_30d,
          count(DISTINCT coalesce(e.campaign_id, "")) as unique_campaigns_30d,
          count(DISTINCT CASE WHEN e.feature_area = "chat" THEN coalesce(e.user_id, e.actor_user_id, "") END) as chat_users_30d,
          count(DISTINCT CASE WHEN e.source_entity = "RileyReportJob" THEN coalesce(e.user_id, e.actor_user_id, "") END) as report_users_30d
        """,
    )
    user_rows = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 30})
        WITH coalesce(e.user_id, e.actor_user_id, "") as user_id, collect(e) as events
        WHERE trim(user_id) <> ""
        RETURN
          user_id,
          size(events) as events,
          size([ev IN events WHERE ev.feature_area = "chat" OR ev.source_entity IN ["Message", "TeamMessage", "ThreadMessage"]]) as chats,
          size([ev IN events WHERE ev.source_entity = "RileyReportJob" AND ev.object_id IS NOT NULL]) as reports
        ORDER BY events DESC
        LIMIT 25
        """,
    )
    return {
        "events_30d": int(metrics.get("events_30d") or 0),
        "unique_users_30d": int(metrics.get("unique_users_30d") or 0),
        "unique_campaigns_30d": int(metrics.get("unique_campaigns_30d") or 0),
        "chat_users_30d": int(metrics.get("chat_users_30d") or 0),
        "report_users_30d": int(metrics.get("report_users_30d") or 0),
        "user_activity_30d": [
            {
                "user_id": row.get("user_id"),
                "events": int(row.get("events") or 0),
                "chats": int(row.get("chats") or 0),
                "reports": int(row.get("reports") or 0),
            }
            for row in user_rows
        ],
    }


@router.get("/mission-control/workflow-health-summary")
async def mission_control_workflow_health_summary(
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    workflow = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({hours: 24})
        RETURN
          sum(CASE WHEN e.source_event_type_raw = "access_request_created" THEN 1 ELSE 0 END) as access_requests_24h,
          sum(CASE WHEN e.source_event_type_raw IN ["access_request_approved", "access_request_denied"] THEN 1 ELSE 0 END) as access_decisions_24h,
          sum(CASE WHEN e.source_event_type_raw IN ["deadline_reminder_10m", "deadline_happening_now"] THEN 1 ELSE 0 END) as deadline_reminders_24h,
          sum(CASE WHEN e.source_event_type_raw = "preview_generation_failed" THEN 1 ELSE 0 END) as preview_failures_24h
        """,
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
        OPTIONAL MATCH (c:Campaign {tenant_id: ar.campaign_id})
        RETURN
          ar.id as request_id,
          ar.campaign_id as campaign_id,
          coalesce(c.name, c.title, ar.campaign_id) as campaign_name,
          ar.user_id as user_id,
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
        OPTIONAL MATCH (c:Campaign {tenant_id: d.campaign_id})
        RETURN
          d.id as deadline_id,
          d.campaign_id as campaign_id,
          coalesce(c.name, c.title, d.campaign_id) as campaign_name,
          d.title as title,
          d.due_at as due_at,
          d.assigned_user_id as assigned_user_id
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
        OPTIONAL MATCH (c:Campaign {tenant_id: d.campaign_id})
        RETURN
          d.id as deadline_id,
          d.campaign_id as campaign_id,
          coalesce(c.name, c.title, d.campaign_id) as campaign_name,
          d.title as title,
          d.due_at as due_at,
          d.assigned_user_id as assigned_user_id
        ORDER BY datetime(d.due_at) ASC
        LIMIT 50
        """,
    )
    return {
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
                "campaign_name": row.get("campaign_name") or row.get("campaign_id"),
                "user_id": row.get("user_id"),
                "created_at": row.get("created_at"),
            }
            for row in pending_access_rows
        ],
        "overdue_deadline_list": [
            {
                "deadline_id": row.get("deadline_id"),
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name") or row.get("campaign_id"),
                "title": row.get("title") or "Untitled deadline",
                "due_at": row.get("due_at"),
                "assigned_user_id": row.get("assigned_user_id"),
            }
            for row in overdue_deadline_rows
        ],
        "stale_assignment_list": [
            {
                "deadline_id": row.get("deadline_id"),
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name") or row.get("campaign_id"),
                "title": row.get("title") or "Untitled deadline",
                "due_at": row.get("due_at"),
                "assigned_user_id": row.get("assigned_user_id"),
            }
            for row in stale_assignment_rows
        ],
    }


@router.get("/mission-control/system-health-summary")
async def mission_control_system_health_summary(
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    system = await _single_value(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({hours: 24})
        RETURN
          sum(CASE WHEN e.source_event_type_raw = "auth_tenant_access_denied" THEN 1 ELSE 0 END) as auth_denied_24h,
          sum(CASE WHEN e.source_event_type_raw = "worker_failed" THEN 1 ELSE 0 END) as worker_failures_24h,
          sum(CASE WHEN e.source_event_type_raw = "report_job_status_changed" AND e.status = "failed" THEN 1 ELSE 0 END) as report_failures_24h,
          sum(CASE WHEN e.source_event_type_raw = "ingestion_failed" THEN 1 ELSE 0 END) as ingestion_failures_24h
        """,
    )
    recent_failures = await _rows(
        graph,
        """
        MATCH (e:AnalyticsEvent)
        WHERE datetime(e.occurred_at) >= datetime() - duration({days: 7})
          AND (
            e.source_event_type_raw = "auth_tenant_access_denied"
            OR e.source_event_type_raw = "worker_failed"
            OR e.source_event_type_raw = "ingestion_failed"
            OR e.source_event_type_raw = "preview_generation_failed"
            OR (e.source_entity = "RileyReportJob" AND e.status = "failed")
          )
        OPTIONAL MATCH (c:Campaign {tenant_id: e.campaign_id})
        RETURN
          e.source_event_type_raw as type,
          e.occurred_at as occurred_at,
          e.campaign_id as campaign_id,
          coalesce(c.name, c.title, e.campaign_id) as campaign_name,
          e.object_id as object_id,
          e.status as status
        ORDER BY datetime(e.occurred_at) DESC
        LIMIT 50
        """,
    )
    return {
        "auth_denied_24h": int(system.get("auth_denied_24h") or 0),
        "worker_failures_24h": int(system.get("worker_failures_24h") or 0),
        "report_failures_24h": int(system.get("report_failures_24h") or 0),
        "ingestion_failures_24h": int(system.get("ingestion_failures_24h") or 0),
        "recent_failures": [
            {
                "type": row.get("type") or "unknown",
                "occurred_at": row.get("occurred_at"),
                "campaign_id": row.get("campaign_id"),
                "campaign_name": row.get("campaign_name") or row.get("campaign_id") or "",
                "object_id": row.get("object_id"),
                "status": row.get("status"),
            }
            for row in recent_failures
        ],
    }


@router.post("/mission-control/rollups/rebuild")
async def mission_control_rebuild_rollups(
    days_back: int = Query(30, ge=1, le=365),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    result = await graph.rebuild_analytics_daily_rollups(days_back=days_back)
    return {"status": "ok", "days_back": days_back, "rollups": result}

