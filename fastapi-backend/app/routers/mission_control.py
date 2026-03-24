from calendar import monthrange
from datetime import datetime, timezone
from typing import Any, Dict

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
    return {
        "avg_latency_ms_7d": round(float(metrics.get("avg_latency_ms") or 0.0), 2),
        "p95_latency_ms_7d": round(float(metrics.get("p95_latency_ms") or 0.0), 2),
        "provider_fallback_successes_7d": int(metrics.get("provider_fallback_successes") or 0),
        "provider_fallback_failures_7d": int(metrics.get("provider_fallback_failures") or 0),
        "reranker_failures_7d": int(metrics.get("reranker_failures") or 0),
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
                            THEN toFloat(e.cost_estimate_usd) ELSE 0.0 END), 0.0) as last_7d_cost
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
    return {
        "month_estimated_cost": round(float(totals.get("month_cost") or 0.0), 2),
        "last_7d_estimated_cost": round(float(totals.get("last_7d_cost") or 0.0), 2),
        "provider_cost_30d": by_provider_rows,
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
    return {
        "events_30d": int(metrics.get("events_30d") or 0),
        "unique_users_30d": int(metrics.get("unique_users_30d") or 0),
        "unique_campaigns_30d": int(metrics.get("unique_campaigns_30d") or 0),
        "chat_users_30d": int(metrics.get("chat_users_30d") or 0),
        "report_users_30d": int(metrics.get("report_users_30d") or 0),
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
    return {
        "access_requests_24h": int(workflow.get("access_requests_24h") or 0),
        "access_decisions_24h": int(workflow.get("access_decisions_24h") or 0),
        "deadline_reminders_24h": int(workflow.get("deadline_reminders_24h") or 0),
        "preview_failures_24h": int(workflow.get("preview_failures_24h") or 0),
        "pending_access_requests": int(pending_access.get("value") or 0),
        "overdue_deadlines": int(overdue_deadlines.get("value") or 0),
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
    return {
        "auth_denied_24h": int(system.get("auth_denied_24h") or 0),
        "worker_failures_24h": int(system.get("worker_failures_24h") or 0),
        "report_failures_24h": int(system.get("report_failures_24h") or 0),
        "ingestion_failures_24h": int(system.get("ingestion_failures_24h") or 0),
    }


@router.post("/mission-control/rollups/rebuild")
async def mission_control_rebuild_rollups(
    days_back: int = Query(30, ge=1, le=365),
    _: Dict = Depends(verify_mission_control_admin),
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    result = await graph.rebuild_analytics_daily_rollups(days_back=days_back)
    return {"status": "ok", "days_back": days_back, "rollups": result}

