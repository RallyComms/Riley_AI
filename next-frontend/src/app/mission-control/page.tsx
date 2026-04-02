"use client";

import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import Link from "next/link";
import { useAuth } from "@clerk/nextjs";
import { Activity, AlertTriangle, BarChart3, ChevronRight, DollarSign, ShieldCheck, Users } from "lucide-react";
import { apiFetch, ApiRequestError } from "@app/lib/api";

type MissionControlTab =
  | "overview"
  | "performance"
  | "cost"
  | "system"
  | "adoption"
  | "workflow";

type MissionControlTimeframe = "24h" | "7d" | "30d";

type OverviewData = {
  timeframe?: MissionControlTimeframe;
  timeframe_label?: string;
  active_users_7d: number;
  active_campaigns_7d: number;
  chats_today: number;
  reports_today: number;
  avg_response_latency_ms: number;
  report_success_rate: number;
  current_month_llm_spend?: number;
  forecast_month_end_llm_spend?: number;
  current_month_non_llm_event_spend?: number;
  current_month_estimated_cost: number;
  forecast_month_end_cost: number;
  pending_access_requests: number;
  overdue_deadlines: number;
  failed_reports_24h: number;
  timeseries_30d: Array<{ day: string; chats: number; reports: number; cost: number; failures: number }>;
  campaign_usage_7d: Array<{
    campaign_id: string;
    campaign_name: string;
    campaign_secondary?: string;
    chats: number;
    reports: number;
    cost: number;
  }>;
  user_usage_7d: Array<{
    user_id: string;
    user_label?: string;
    user_secondary?: string;
    chats: number;
    reports: number;
    cost: number;
  }>;
};

type PerformanceData = {
  timeframe?: MissionControlTimeframe;
  timeframe_label?: string;
  avg_latency_ms_7d: number;
  p95_latency_ms_7d: number;
  provider_fallback_successes_7d: number;
  provider_fallback_failures_7d: number;
  provider_fallback_success_rate_7d: number;
  reranker_failures_7d: number;
  latency_percentiles_7d: { p50_ms: number; p75_ms: number; p90_ms: number; p95_ms: number };
  success_failure_trend_30d: Array<{ day: string; success: number; failure: number }>;
  campaign_performance_7d: Array<{
    campaign_id: string;
    campaign_name: string;
    campaign_secondary?: string;
    avg_latency_ms: number;
    p95_latency_ms: number;
    chats: number;
    reports: number;
  }>;
  provider_breakdown_7d: Array<{
    provider: string;
    avg_latency_ms: number;
    fallback_triggered: number;
    fallback_succeeded: number;
    fallback_failed: number;
  }>;
};

type CostData = {
  timeframe?: MissionControlTimeframe;
  timeframe_label?: string;
  month_estimated_cost: number;
  selected_window_cost?: number;
  current_month_cost?: number;
  average_daily_burn_rate?: number;
  projected_month_end_cost?: number;
  current_month_days_elapsed?: number;
  current_month_total_days?: number;
  last_7d_estimated_cost: number;
  provider_cost_30d: Array<{ provider: string; cost: number }>;
  cost_per_chat: number;
  cost_per_report: number;
  cost_by_campaign_30d: Array<{ campaign_id: string; campaign_name: string; campaign_secondary?: string; cost: number }>;
  cost_by_user_30d: Array<{ user_id: string; user_label?: string; user_secondary?: string; cost: number }>;
  cost_trend_30d: Array<{ day: string; cost: number }>;
  projected_monthly_curve: Array<{ day: number; projected_cost: number }>;
  monthly_llm_budget_limit: number;
  current_month_llm_spend: number;
  projected_month_end_llm_spend: number;
  percent_budget_used: number;
  guardrail_status: "green" | "yellow" | "red" | "exceeded";
  current_month_cloud_spend: number;
  projected_month_end_cloud_spend: number;
  cloud_cost_is_available: boolean;
  cloud_cost_unavailable_reason?: string;
  cloud_cost_last_data_timestamp?: string;
  cloud_cost_billing_data_lag_hours?: number;
  current_month_qdrant_spend: number;
  projected_month_end_qdrant_spend: number;
  current_month_fixed_saas_spend: number;
  projected_month_end_fixed_saas_spend: number;
  current_month_actual_or_metered_spend: number;
  current_month_estimated_accrual_spend: number;
  current_month_blended_total_spend: number;
  current_month_total_spend: number; // deprecated alias; represents blended MTD
  projected_month_end_total_spend: number;
  total_spend_breakdown: Array<{
    category: string;
    current: number;
    projected: number;
    current_semantics?: "actual_or_metered_mtd" | "estimated_accrual_mtd";
  }>;
  fixed_saas_component_breakdown: Array<{
    component: string;
    current_mtd: number;
    projected_month_end: number;
  }>;
  current_month_llm_cost_by_feature: Array<{
    feature: string;
    spend: number;
    percent_of_llm: number;
    attribution_confidence?: "explicit" | "inferred" | "fallback_other";
  }>;
  last_30d_llm_cost_by_feature: Array<{
    feature: string;
    spend: number;
    percent_of_llm: number;
    attribution_confidence?: "explicit" | "inferred" | "fallback_other";
  }>;
  current_month_llm_feature_mapping_confidence: {
    explicit_spend: number;
    inferred_spend: number;
    fallback_other_spend: number;
    total_llm_spend: number;
    explicit_percent: number;
    inferred_or_other_percent: number;
    other_llm_spend: number;
    other_llm_percent: number;
  };
  last_30d_llm_feature_mapping_confidence: {
    explicit_spend: number;
    inferred_spend: number;
    fallback_other_spend: number;
    total_llm_spend: number;
    explicit_percent: number;
    inferred_or_other_percent: number;
    other_llm_spend: number;
    other_llm_percent: number;
  };
  current_month_non_llm_event_spend: number;
  window_non_llm_event_spend: number;
  tracked_non_llm_event_provider_breakdown_30d: Array<{ provider: string; cost: number }>;
};

type QdrantMetricsData = {
  total_vectors: number;
  total_estimated_size_mb: number;
  campaigns: Array<{ campaign_id: string; campaign_name?: string; vectors: number; estimated_size_mb: number }>;
  estimated_monthly_cost: number;
  trend_series: Array<{ date: string; total_vectors: number; total_estimated_size_mb: number; estimated_monthly_cost: number }>;
  projection_available: boolean;
  projection_unavailable_reason?: string;
  projection_confidence: "unavailable" | "low" | "high";
  projection_snapshot_count: number;
  projection_min_snapshots_required: number;
  projection_window_days_used: number;
  projection_method: string;
  projected_next_month_storage_mb: number;
  projected_next_month_cost: number;
  growth_percent: number;
  growth_mb_per_day: number;
  warning_state: "normal" | "warning";
  warning_reasons: string[];
};

type CloudInfrastructureCostData = {
  current_month_cloud_cost: number;
  projected_month_end_cloud_cost: number;
  daily_cloud_cost_series: Array<{ date: string; cost: number }>;
  cloud_cost_breakdown: Array<{ service: string; cost: number }>;
  is_available: boolean;
  unavailable_reason?: string;
  last_data_timestamp?: string;
  billing_data_lag_hours?: number;
  billing_source?: string;
};

type AdoptionData = {
  timeframe?: MissionControlTimeframe;
  timeframe_label?: string;
  events_30d: number;
  unique_users_30d: number;
  unique_campaigns_30d: number;
  chat_users_30d: number;
  report_users_30d: number;
  user_activity_30d: Array<{
    user_id: string;
    user_label?: string;
    user_secondary?: string;
    events: number;
    chats: number;
    reports: number;
  }>;
};

type WorkflowData = {
  timeframe?: MissionControlTimeframe;
  timeframe_label?: string;
  access_requests_24h: number;
  access_decisions_24h: number;
  preview_failures_24h: number;
  pending_access_requests: number;
  overdue_deadlines: number;
  pending_access_request_list: Array<{
    request_id: string;
    campaign_id: string;
    campaign_name: string;
    campaign_secondary?: string;
    user_id: string;
    user_label?: string;
    user_secondary?: string;
    created_at: string;
    status?: string;
  }>;
  overdue_deadline_list: Array<{
    deadline_id: string;
    campaign_id: string;
    campaign_name: string;
    campaign_secondary?: string;
    title: string;
    due_at: string;
    assigned_user_id: string;
    assigned_user_label?: string;
    assigned_user_secondary?: string;
  }>;
  stale_assignment_list: Array<{
    deadline_id: string;
    campaign_id: string;
    campaign_name: string;
    campaign_secondary?: string;
    title: string;
    due_at: string;
    assigned_user_id: string;
    assigned_user_label?: string;
    assigned_user_secondary?: string;
  }>;
};

type SystemData = {
  timeframe?: MissionControlTimeframe;
  timeframe_label?: string;
  auth_denied_24h: number;
  worker_failures_24h: number;
  report_failures_24h: number;
  ingestion_failures_24h: number;
  recent_failures: Array<{
    failure_id?: string;
    type: string;
    failure_label?: string;
    failure_class?: string;
    worker_name?: string;
    occurred_at: string;
    campaign_id: string;
    campaign_name: string;
    campaign_secondary?: string;
    object_id: string;
    status: string;
    detail?: string;
  }>;
};

type UnknownRecord = Record<string, unknown>;

const tabs: Array<{ id: MissionControlTab; label: string }> = [
  { id: "overview", label: "Overview" },
  { id: "performance", label: "Riley Performance" },
  { id: "cost", label: "Cost & Forecasting" },
  { id: "system", label: "System Health" },
  { id: "adoption", label: "Adoption & Engagement" },
  { id: "workflow", label: "Workflow Health" },
];

const timeframeOptions: Array<{ value: MissionControlTimeframe; label: string }> = [
  { value: "24h", label: "Today / Last 24h" },
  { value: "7d", label: "Last 7d" },
  { value: "30d", label: "Last 30d" },
];

const defaultOverview: OverviewData = {
  active_users_7d: 0,
  active_campaigns_7d: 0,
  chats_today: 0,
  reports_today: 0,
  avg_response_latency_ms: 0,
  report_success_rate: 0,
  current_month_estimated_cost: 0,
  forecast_month_end_cost: 0,
  pending_access_requests: 0,
  overdue_deadlines: 0,
  failed_reports_24h: 0,
  timeseries_30d: [],
  campaign_usage_7d: [],
  user_usage_7d: [],
};

const defaultPerformance: PerformanceData = {
  avg_latency_ms_7d: 0,
  p95_latency_ms_7d: 0,
  provider_fallback_successes_7d: 0,
  provider_fallback_failures_7d: 0,
  provider_fallback_success_rate_7d: 0,
  reranker_failures_7d: 0,
  latency_percentiles_7d: { p50_ms: 0, p75_ms: 0, p90_ms: 0, p95_ms: 0 },
  success_failure_trend_30d: [],
  campaign_performance_7d: [],
  provider_breakdown_7d: [],
};

const defaultCost: CostData = {
  month_estimated_cost: 0,
  selected_window_cost: 0,
  current_month_cost: 0,
  average_daily_burn_rate: 0,
  projected_month_end_cost: 0,
  current_month_days_elapsed: 0,
  current_month_total_days: 0,
  last_7d_estimated_cost: 0,
  provider_cost_30d: [],
  cost_per_chat: 0,
  cost_per_report: 0,
  cost_by_campaign_30d: [],
  cost_by_user_30d: [],
  cost_trend_30d: [],
  projected_monthly_curve: [],
  monthly_llm_budget_limit: 0,
  current_month_llm_spend: 0,
  projected_month_end_llm_spend: 0,
  percent_budget_used: 0,
  guardrail_status: "green",
  current_month_cloud_spend: 0,
  projected_month_end_cloud_spend: 0,
  cloud_cost_is_available: false,
  cloud_cost_unavailable_reason: "",
  cloud_cost_last_data_timestamp: "",
  cloud_cost_billing_data_lag_hours: 0,
  current_month_qdrant_spend: 0,
  projected_month_end_qdrant_spend: 0,
  current_month_fixed_saas_spend: 0,
  projected_month_end_fixed_saas_spend: 0,
  current_month_actual_or_metered_spend: 0,
  current_month_estimated_accrual_spend: 0,
  current_month_blended_total_spend: 0,
  current_month_total_spend: 0,
  projected_month_end_total_spend: 0,
  total_spend_breakdown: [],
  fixed_saas_component_breakdown: [],
  current_month_llm_cost_by_feature: [],
  last_30d_llm_cost_by_feature: [],
  current_month_llm_feature_mapping_confidence: {
    explicit_spend: 0,
    inferred_spend: 0,
    fallback_other_spend: 0,
    total_llm_spend: 0,
    explicit_percent: 0,
    inferred_or_other_percent: 0,
    other_llm_spend: 0,
    other_llm_percent: 0,
  },
  last_30d_llm_feature_mapping_confidence: {
    explicit_spend: 0,
    inferred_spend: 0,
    fallback_other_spend: 0,
    total_llm_spend: 0,
    explicit_percent: 0,
    inferred_or_other_percent: 0,
    other_llm_spend: 0,
    other_llm_percent: 0,
  },
  current_month_non_llm_event_spend: 0,
  window_non_llm_event_spend: 0,
  tracked_non_llm_event_provider_breakdown_30d: [],
};

const defaultAdoption: AdoptionData = {
  events_30d: 0,
  unique_users_30d: 0,
  unique_campaigns_30d: 0,
  chat_users_30d: 0,
  report_users_30d: 0,
  user_activity_30d: [],
};

const defaultWorkflow: WorkflowData = {
  access_requests_24h: 0,
  access_decisions_24h: 0,
  preview_failures_24h: 0,
  pending_access_requests: 0,
  overdue_deadlines: 0,
  pending_access_request_list: [],
  overdue_deadline_list: [],
  stale_assignment_list: [],
};

const defaultSystem: SystemData = {
  auth_denied_24h: 0,
  worker_failures_24h: 0,
  report_failures_24h: 0,
  ingestion_failures_24h: 0,
  recent_failures: [],
};

const defaultQdrantMetrics: QdrantMetricsData = {
  total_vectors: 0,
  total_estimated_size_mb: 0,
  campaigns: [],
  estimated_monthly_cost: 0,
  trend_series: [],
  projection_available: false,
  projection_unavailable_reason: "",
  projection_confidence: "unavailable",
  projection_snapshot_count: 0,
  projection_min_snapshots_required: 7,
  projection_window_days_used: 0,
  projection_method: "",
  projected_next_month_storage_mb: 0,
  projected_next_month_cost: 0,
  growth_percent: 0,
  growth_mb_per_day: 0,
  warning_state: "normal",
  warning_reasons: [],
};

const defaultCloudInfrastructureCost: CloudInfrastructureCostData = {
  current_month_cloud_cost: 0,
  projected_month_end_cloud_cost: 0,
  daily_cloud_cost_series: [],
  cloud_cost_breakdown: [],
  is_available: false,
  unavailable_reason: "",
  last_data_timestamp: "",
  billing_data_lag_hours: 0,
  billing_source: "",
};

function asNumber(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function asString(value: unknown, fallback = ""): string {
  const s = String(value ?? "").trim();
  return s || fallback;
}

function asRecord(value: unknown): UnknownRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as UnknownRecord;
}

function asArray(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function normalizeMetricValue(value: unknown): string | number {
  if (value === null || value === undefined) return 0;
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  if (typeof value === "string") return value;
  if (typeof value === "boolean") return value ? "true" : "false";
  return 0;
}

function normalizeOverview(raw: unknown): OverviewData {
  const src = asRecord(raw);
  const trendRows = asArray(src.timeseries_30d);
  const campaignRows = asArray(src.campaign_usage_7d);
  const userRows = asArray(src.user_usage_7d);
  return {
    timeframe: asString(src.timeframe, "30d") as MissionControlTimeframe,
    timeframe_label: asString(src.timeframe_label, ""),
    active_users_7d: asNumber(src.active_users_7d),
    active_campaigns_7d: asNumber(src.active_campaigns_7d),
    chats_today: asNumber(src.chats_today),
    reports_today: asNumber(src.reports_today),
    avg_response_latency_ms: asNumber(src.avg_response_latency_ms),
    report_success_rate: asNumber(src.report_success_rate),
    current_month_llm_spend: asNumber(src.current_month_llm_spend),
    forecast_month_end_llm_spend: asNumber(src.forecast_month_end_llm_spend),
    current_month_non_llm_event_spend: asNumber(src.current_month_non_llm_event_spend),
    current_month_estimated_cost: asNumber(src.current_month_estimated_cost),
    forecast_month_end_cost: asNumber(src.forecast_month_end_cost),
    pending_access_requests: asNumber(src.pending_access_requests),
    overdue_deadlines: asNumber(src.overdue_deadlines),
    failed_reports_24h: asNumber(src.failed_reports_24h),
    timeseries_30d: trendRows.map((row) => {
      const item = asRecord(row);
      return {
        day: asString(item.day, ""),
        chats: asNumber(item.chats),
        reports: asNumber(item.reports),
        cost: asNumber(item.cost),
        failures: asNumber(item.failures),
      };
    }),
    campaign_usage_7d: campaignRows.map((row, idx) => {
      const item = asRecord(row);
      return {
        campaign_id: asString(item.campaign_id, `campaign_${idx + 1}`),
        campaign_name: asString(item.campaign_name, "Unresolved Campaign"),
        campaign_secondary: asString(item.campaign_secondary, ""),
        chats: asNumber(item.chats),
        reports: asNumber(item.reports),
        cost: asNumber(item.cost),
      };
    }),
    user_usage_7d: userRows.map((row, idx) => {
      const item = asRecord(row);
      return {
        user_id: asString(item.user_id, `user_${idx + 1}`),
        user_label: asString(item.user_label, asString(item.user_id, `user_${idx + 1}`)),
        user_secondary: asString(item.user_secondary, ""),
        chats: asNumber(item.chats),
        reports: asNumber(item.reports),
        cost: asNumber(item.cost),
      };
    }),
  };
}

function normalizePerformance(raw: unknown): PerformanceData {
  const src = asRecord(raw);
  const percentiles = asRecord(src.latency_percentiles_7d);
  const trend = asArray(src.success_failure_trend_30d);
  const byCampaign = asArray(src.campaign_performance_7d);
  const byProvider = asArray(src.provider_breakdown_7d);
  return {
    timeframe: asString(src.timeframe, "30d") as MissionControlTimeframe,
    timeframe_label: asString(src.timeframe_label, ""),
    avg_latency_ms_7d: asNumber(src.avg_latency_ms_7d),
    p95_latency_ms_7d: asNumber(src.p95_latency_ms_7d),
    provider_fallback_successes_7d: asNumber(src.provider_fallback_successes_7d),
    provider_fallback_failures_7d: asNumber(src.provider_fallback_failures_7d),
    provider_fallback_success_rate_7d: asNumber(src.provider_fallback_success_rate_7d),
    reranker_failures_7d: asNumber(src.reranker_failures_7d),
    latency_percentiles_7d: {
      p50_ms: asNumber(percentiles.p50_ms),
      p75_ms: asNumber(percentiles.p75_ms),
      p90_ms: asNumber(percentiles.p90_ms),
      p95_ms: asNumber(percentiles.p95_ms),
    },
    success_failure_trend_30d: trend.map((row) => {
      const item = asRecord(row);
      return {
        day: asString(item.day, ""),
        success: asNumber(item.success),
        failure: asNumber(item.failure),
      };
    }),
    campaign_performance_7d: byCampaign.map((row, idx) => {
      const item = asRecord(row);
      return {
        campaign_id: asString(item.campaign_id, `campaign_${idx + 1}`),
        campaign_name: asString(item.campaign_name, "Unresolved Campaign"),
        campaign_secondary: asString(item.campaign_secondary, ""),
        avg_latency_ms: asNumber(item.avg_latency_ms),
        p95_latency_ms: asNumber(item.p95_latency_ms),
        chats: asNumber(item.chats),
        reports: asNumber(item.reports),
      };
    }),
    provider_breakdown_7d: byProvider.map((row, idx) => {
      const item = asRecord(row);
      return {
        provider: asString(item.provider, `provider_${idx + 1}`),
        avg_latency_ms: asNumber(item.avg_latency_ms),
        fallback_triggered: asNumber(item.fallback_triggered),
        fallback_succeeded: asNumber(item.fallback_succeeded),
        fallback_failed: asNumber(item.fallback_failed),
      };
    }),
  };
}

function normalizeCost(raw: unknown): CostData {
  const src = asRecord(raw);
  const rows = asArray(src.provider_cost_30d);
  const campaignRows = asArray(src.cost_by_campaign_30d);
  const userRows = asArray(src.cost_by_user_30d);
  const trendRows = asArray(src.cost_trend_30d);
  const projectionRows = asArray(src.projected_monthly_curve);
  return {
    timeframe: asString(src.timeframe, "30d") as MissionControlTimeframe,
    timeframe_label: asString(src.timeframe_label, ""),
    month_estimated_cost: asNumber(src.month_estimated_cost),
    selected_window_cost: asNumber(src.selected_window_cost),
    current_month_cost: asNumber(src.current_month_cost),
    average_daily_burn_rate: asNumber(src.average_daily_burn_rate),
    projected_month_end_cost: asNumber(src.projected_month_end_cost),
    current_month_days_elapsed: asNumber(src.current_month_days_elapsed),
    current_month_total_days: asNumber(src.current_month_total_days),
    last_7d_estimated_cost: asNumber(src.last_7d_estimated_cost),
    provider_cost_30d: rows.map((row, idx) => {
      const item = (row && typeof row === "object" ? row : {}) as Record<string, unknown>;
      return {
        provider: asString(item.provider, `provider_${idx + 1}`),
        cost: asNumber(item.cost),
      };
    }),
    cost_per_chat: asNumber(src.cost_per_chat),
    cost_per_report: asNumber(src.cost_per_report),
    cost_by_campaign_30d: campaignRows.map((row, idx) => {
      const item = asRecord(row);
      return {
        campaign_id: asString(item.campaign_id, `campaign_${idx + 1}`),
        campaign_name: asString(item.campaign_name, "Unresolved Campaign"),
        campaign_secondary: asString(item.campaign_secondary, ""),
        cost: asNumber(item.cost),
      };
    }),
    cost_by_user_30d: userRows.map((row, idx) => {
      const item = asRecord(row);
      return {
        user_id: asString(item.user_id, `user_${idx + 1}`),
        user_label: asString(item.user_label, asString(item.user_id, `user_${idx + 1}`)),
        user_secondary: asString(item.user_secondary, ""),
        cost: asNumber(item.cost),
      };
    }),
    cost_trend_30d: trendRows.map((row) => {
      const item = asRecord(row);
      return {
        day: asString(item.day, ""),
        cost: asNumber(item.cost),
      };
    }),
    projected_monthly_curve: projectionRows.map((row) => {
      const item = asRecord(row);
      return {
        day: asNumber(item.day),
        projected_cost: asNumber(item.projected_cost),
      };
    }),
    monthly_llm_budget_limit: asNumber(src.monthly_llm_budget_limit),
    current_month_llm_spend: asNumber(src.current_month_llm_spend),
    projected_month_end_llm_spend: asNumber(src.projected_month_end_llm_spend),
    percent_budget_used: asNumber(src.percent_budget_used),
    guardrail_status: asString(src.guardrail_status, "green") as CostData["guardrail_status"],
    current_month_cloud_spend: asNumber(src.current_month_cloud_spend),
    projected_month_end_cloud_spend: asNumber(src.projected_month_end_cloud_spend),
    cloud_cost_is_available: Boolean(src.cloud_cost_is_available),
    cloud_cost_unavailable_reason: asString(src.cloud_cost_unavailable_reason, ""),
    cloud_cost_last_data_timestamp: asString(src.cloud_cost_last_data_timestamp, ""),
    cloud_cost_billing_data_lag_hours: asNumber(src.cloud_cost_billing_data_lag_hours),
    current_month_qdrant_spend: asNumber(src.current_month_qdrant_spend),
    projected_month_end_qdrant_spend: asNumber(src.projected_month_end_qdrant_spend),
    current_month_fixed_saas_spend: asNumber(src.current_month_fixed_saas_spend),
    projected_month_end_fixed_saas_spend: asNumber(src.projected_month_end_fixed_saas_spend),
    current_month_actual_or_metered_spend: asNumber(src.current_month_actual_or_metered_spend),
    current_month_estimated_accrual_spend: asNumber(src.current_month_estimated_accrual_spend),
    current_month_blended_total_spend: asNumber(src.current_month_blended_total_spend),
    current_month_total_spend: asNumber(src.current_month_total_spend),
    projected_month_end_total_spend: asNumber(src.projected_month_end_total_spend),
    total_spend_breakdown: asArray(src.total_spend_breakdown).map((row, idx) => {
      const item = asRecord(row);
      return {
        category: asString(item.category, `category_${idx + 1}`),
        current: asNumber(item.current),
        projected: asNumber(item.projected),
        current_semantics: asString(item.current_semantics, "estimated_accrual_mtd") as
          | "actual_or_metered_mtd"
          | "estimated_accrual_mtd",
      };
    }),
    fixed_saas_component_breakdown: asArray(src.fixed_saas_component_breakdown).map((row, idx) => {
      const item = asRecord(row);
      return {
        component: asString(item.component, `component_${idx + 1}`),
        current_mtd: asNumber(item.current_mtd),
        projected_month_end: asNumber(item.projected_month_end),
      };
    }),
    current_month_llm_cost_by_feature: asArray(src.current_month_llm_cost_by_feature).map((row, idx) => {
      const item = asRecord(row);
      return {
        feature: asString(item.feature, `feature_${idx + 1}`),
        spend: asNumber(item.spend),
        percent_of_llm: asNumber(item.percent_of_llm),
        attribution_confidence: asString(item.attribution_confidence, "fallback_other") as
          | "explicit"
          | "inferred"
          | "fallback_other",
      };
    }),
    last_30d_llm_cost_by_feature: asArray(src.last_30d_llm_cost_by_feature).map((row, idx) => {
      const item = asRecord(row);
      return {
        feature: asString(item.feature, `feature_${idx + 1}`),
        spend: asNumber(item.spend),
        percent_of_llm: asNumber(item.percent_of_llm),
        attribution_confidence: asString(item.attribution_confidence, "fallback_other") as
          | "explicit"
          | "inferred"
          | "fallback_other",
      };
    }),
    current_month_llm_feature_mapping_confidence: (() => {
      const item = asRecord(src.current_month_llm_feature_mapping_confidence);
      return {
        explicit_spend: asNumber(item.explicit_spend),
        inferred_spend: asNumber(item.inferred_spend),
        fallback_other_spend: asNumber(item.fallback_other_spend),
        total_llm_spend: asNumber(item.total_llm_spend),
        explicit_percent: asNumber(item.explicit_percent),
        inferred_or_other_percent: asNumber(item.inferred_or_other_percent),
        other_llm_spend: asNumber(item.other_llm_spend),
        other_llm_percent: asNumber(item.other_llm_percent),
      };
    })(),
    last_30d_llm_feature_mapping_confidence: (() => {
      const item = asRecord(src.last_30d_llm_feature_mapping_confidence);
      return {
        explicit_spend: asNumber(item.explicit_spend),
        inferred_spend: asNumber(item.inferred_spend),
        fallback_other_spend: asNumber(item.fallback_other_spend),
        total_llm_spend: asNumber(item.total_llm_spend),
        explicit_percent: asNumber(item.explicit_percent),
        inferred_or_other_percent: asNumber(item.inferred_or_other_percent),
        other_llm_spend: asNumber(item.other_llm_spend),
        other_llm_percent: asNumber(item.other_llm_percent),
      };
    })(),
    current_month_non_llm_event_spend: asNumber(src.current_month_non_llm_event_spend),
    window_non_llm_event_spend: asNumber(src.window_non_llm_event_spend),
    tracked_non_llm_event_provider_breakdown_30d: asArray(src.tracked_non_llm_event_provider_breakdown_30d).map((row, idx) => {
      const item = asRecord(row);
      return {
        provider: asString(item.provider, `provider_${idx + 1}`),
        cost: asNumber(item.cost),
      };
    }),
  };
}

function normalizeAdoption(raw: unknown): AdoptionData {
  const src = asRecord(raw);
  const userRows = asArray(src.user_activity_30d);
  return {
    timeframe: asString(src.timeframe, "30d") as MissionControlTimeframe,
    timeframe_label: asString(src.timeframe_label, ""),
    events_30d: asNumber(src.events_30d),
    unique_users_30d: asNumber(src.unique_users_30d),
    unique_campaigns_30d: asNumber(src.unique_campaigns_30d),
    chat_users_30d: asNumber(src.chat_users_30d),
    report_users_30d: asNumber(src.report_users_30d),
    user_activity_30d: userRows.map((row, idx) => {
      const item = asRecord(row);
      return {
        user_id: asString(item.user_id, `user_${idx + 1}`),
        user_label: asString(item.user_label, asString(item.user_id, `user_${idx + 1}`)),
        user_secondary: asString(item.user_secondary, ""),
        events: asNumber(item.events),
        chats: asNumber(item.chats),
        reports: asNumber(item.reports),
      };
    }),
  };
}

function normalizeWorkflow(raw: unknown): WorkflowData {
  const src = asRecord(raw);
  const pendingRows = asArray(src.pending_access_request_list);
  const overdueRows = asArray(src.overdue_deadline_list);
  const staleRows = asArray(src.stale_assignment_list);
  return {
    timeframe: asString(src.timeframe, "30d") as MissionControlTimeframe,
    timeframe_label: asString(src.timeframe_label, ""),
    access_requests_24h: asNumber(src.access_requests_24h),
    access_decisions_24h: asNumber(src.access_decisions_24h),
    preview_failures_24h: asNumber(src.preview_failures_24h),
    pending_access_requests: asNumber(src.pending_access_requests),
    overdue_deadlines: asNumber(src.overdue_deadlines),
    pending_access_request_list: pendingRows.map((row, idx) => {
      const item = asRecord(row);
      return {
        request_id: asString(item.request_id, `request_${idx + 1}`),
        campaign_id: asString(item.campaign_id, ""),
        campaign_name: asString(item.campaign_name, "Unresolved Campaign"),
        campaign_secondary: asString(item.campaign_secondary, ""),
        user_id: asString(item.user_id, "unknown"),
        user_label: asString(item.user_label, asString(item.user_id, "unknown")),
        user_secondary: asString(item.user_secondary, ""),
        created_at: asString(item.created_at, ""),
        status: asString(item.status, "pending"),
      };
    }),
    overdue_deadline_list: overdueRows.map((row, idx) => {
      const item = asRecord(row);
      return {
        deadline_id: asString(item.deadline_id, `deadline_${idx + 1}`),
        campaign_id: asString(item.campaign_id, ""),
        campaign_name: asString(item.campaign_name, "Unresolved Campaign"),
        campaign_secondary: asString(item.campaign_secondary, ""),
        title: asString(item.title, "Untitled deadline"),
        due_at: asString(item.due_at, ""),
        assigned_user_id: asString(item.assigned_user_id, ""),
        assigned_user_label: asString(item.assigned_user_label, asString(item.assigned_user_id, "")),
        assigned_user_secondary: asString(item.assigned_user_secondary, ""),
      };
    }),
    stale_assignment_list: staleRows.map((row, idx) => {
      const item = asRecord(row);
      return {
        deadline_id: asString(item.deadline_id, `stale_${idx + 1}`),
        campaign_id: asString(item.campaign_id, ""),
        campaign_name: asString(item.campaign_name, "Unresolved Campaign"),
        campaign_secondary: asString(item.campaign_secondary, ""),
        title: asString(item.title, "Untitled assignment"),
        due_at: asString(item.due_at, ""),
        assigned_user_id: asString(item.assigned_user_id, ""),
        assigned_user_label: asString(item.assigned_user_label, asString(item.assigned_user_id, "")),
        assigned_user_secondary: asString(item.assigned_user_secondary, ""),
      };
    }),
  };
}

function normalizeSystem(raw: unknown): SystemData {
  const src = asRecord(raw);
  const failures = asArray(src.recent_failures);
  return {
    timeframe: asString(src.timeframe, "30d") as MissionControlTimeframe,
    timeframe_label: asString(src.timeframe_label, ""),
    auth_denied_24h: asNumber(src.auth_denied_24h),
    worker_failures_24h: asNumber(src.worker_failures_24h),
    report_failures_24h: asNumber(src.report_failures_24h),
    ingestion_failures_24h: asNumber(src.ingestion_failures_24h),
    recent_failures: failures.map((row, idx) => {
      const item = asRecord(row);
      return {
        failure_id: asString(item.failure_id, ""),
        type: asString(item.type, "unknown"),
        failure_label: asString(item.failure_label, asString(item.type, "Unknown Failure")),
        failure_class: asString(item.failure_class, ""),
        worker_name: asString(item.worker_name, ""),
        occurred_at: asString(item.occurred_at, ""),
        campaign_id: asString(item.campaign_id, ""),
        campaign_name: asString(item.campaign_name, "Unresolved Campaign"),
        campaign_secondary: asString(item.campaign_secondary, ""),
        object_id: asString(item.object_id, ""),
        status: asString(item.status, ""),
        detail: asString(item.detail, ""),
      };
    }),
  };
}

function normalizeQdrantMetrics(raw: unknown): QdrantMetricsData {
  const src = asRecord(raw);
  const campaigns = asArray(src.campaigns);
  const trendRows = asArray(src.trend_series);
  return {
    total_vectors: asNumber(src.total_vectors),
    total_estimated_size_mb: asNumber(src.total_estimated_size_mb),
    estimated_monthly_cost: asNumber(src.estimated_monthly_cost),
    campaigns: campaigns.map((row, idx) => {
      const item = asRecord(row);
      return {
        campaign_id: asString(item.campaign_id, `campaign_${idx + 1}`),
        campaign_name: asString(item.campaign_name, ""),
        vectors: asNumber(item.vectors),
        estimated_size_mb: asNumber(item.estimated_size_mb),
      };
    }),
    trend_series: trendRows.map((row) => {
      const item = asRecord(row);
      return {
        date: asString(item.date, ""),
        total_vectors: asNumber(item.total_vectors),
        total_estimated_size_mb: asNumber(item.total_estimated_size_mb),
        estimated_monthly_cost: asNumber(item.estimated_monthly_cost),
      };
    }),
    projection_available: Boolean(src.projection_available),
    projection_unavailable_reason: asString(src.projection_unavailable_reason, ""),
    projection_confidence: asString(src.projection_confidence, "unavailable") as QdrantMetricsData["projection_confidence"],
    projection_snapshot_count: asNumber(src.projection_snapshot_count),
    projection_min_snapshots_required: Math.max(0, asNumber(src.projection_min_snapshots_required) || 7),
    projection_window_days_used: asNumber(src.projection_window_days_used),
    projection_method: asString(src.projection_method, ""),
    projected_next_month_storage_mb: asNumber(src.projected_next_month_storage_mb),
    projected_next_month_cost: asNumber(src.projected_next_month_cost),
    growth_percent: asNumber(src.growth_percent),
    growth_mb_per_day: asNumber(src.growth_mb_per_day),
    warning_state: asString(src.warning_state, "normal") as QdrantMetricsData["warning_state"],
    warning_reasons: asArray(src.warning_reasons).map((item) => asString(item, "")),
  };
}

function normalizeCloudInfrastructureCost(raw: unknown): CloudInfrastructureCostData {
  const src = asRecord(raw);
  const dailyRows = asArray(src.daily_cloud_cost_series);
  const breakdownRows = asArray(src.cloud_cost_breakdown);
  return {
    current_month_cloud_cost: asNumber(src.current_month_cloud_cost),
    projected_month_end_cloud_cost: asNumber(src.projected_month_end_cloud_cost),
    daily_cloud_cost_series: dailyRows.map((row) => {
      const item = asRecord(row);
      return {
        date: asString(item.date, ""),
        cost: asNumber(item.cost),
      };
    }),
    cloud_cost_breakdown: breakdownRows.map((row, idx) => {
      const item = asRecord(row);
      return {
        service: asString(item.service, `service_${idx + 1}`),
        cost: asNumber(item.cost),
      };
    }),
    is_available: Boolean(src.is_available),
    unavailable_reason: asString(src.unavailable_reason, ""),
    last_data_timestamp: asString(src.last_data_timestamp, ""),
    billing_data_lag_hours: asNumber(src.billing_data_lag_hours),
    billing_source: asString(src.billing_source, ""),
  };
}

function safeDateLabel(value: string): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString(undefined, { month: "short", day: "numeric" });
}

function safeDateTime(value: string): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString();
}

function renderCampaignLabel(name: string, _campaignId: string, _secondary?: string): string {
  return asString(name, "Unresolved Campaign");
}

function renderUserLabel(userLabel: string, userId: string, _secondary?: string): string {
  return asString(userLabel, asString(userId, "unknown_user"));
}

function resolveGuardrailStyle(status: CostData["guardrail_status"]): {
  label: string;
  badgeClass: string;
  panelClass: string;
} {
  if (status === "exceeded") {
    return {
      label: "Limit reached",
      badgeClass: "bg-rose-100 text-rose-700 border border-rose-200",
      panelClass: "border-rose-200 bg-rose-50",
    };
  }
  if (status === "red") {
    return {
      label: "Near limit",
      badgeClass: "bg-orange-100 text-orange-700 border border-orange-200",
      panelClass: "border-orange-200 bg-orange-50/70",
    };
  }
  if (status === "yellow") {
    return {
      label: "Watch usage",
      badgeClass: "bg-amber-100 text-amber-700 border border-amber-200",
      panelClass: "border-amber-200 bg-amber-50/70",
    };
  }
  return {
    label: "Healthy",
    badgeClass: "bg-emerald-100 text-emerald-700 border border-emerald-200",
    panelClass: "border-emerald-200 bg-emerald-50/70",
  };
}

type TabLoadingState = Record<MissionControlTab, boolean>;

const defaultTabLoadingState: TabLoadingState = {
  overview: false,
  performance: false,
  cost: false,
  system: false,
  adoption: false,
  workflow: false,
};

function getMissionControlCacheKey(tab: MissionControlTab, timeframe: MissionControlTimeframe): string {
  return `${tab}:${timeframe}`;
}

function getMissionControlEndpoint(tab: MissionControlTab): string {
  switch (tab) {
    case "overview":
      return "/api/v1/mission-control/overview";
    case "performance":
      return "/api/v1/mission-control/riley-performance";
    case "cost":
      return "/api/v1/mission-control/cost-summary";
    case "system":
      return "/api/v1/mission-control/system-health-summary";
    case "adoption":
      return "/api/v1/mission-control/adoption-summary";
    case "workflow":
      return "/api/v1/mission-control/workflow-health-summary";
    default:
      return "/api/v1/mission-control/overview";
  }
}

export default function MissionControlPage() {
  const { getToken, isLoaded } = useAuth();
  const [activeTab, setActiveTab] = useState<MissionControlTab>("overview");
  const [timeframe, setTimeframe] = useState<MissionControlTimeframe>("30d");
  const [overviewDrilldown, setOverviewDrilldown] = useState<"campaigns" | "users" | "join_requests">("campaigns");
  const [initialPageLoading, setInitialPageLoading] = useState(true);
  const [tabLoading, setTabLoading] = useState<TabLoadingState>(defaultTabLoadingState);
  const [accessDenied, setAccessDenied] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [overview, setOverview] = useState<OverviewData>(defaultOverview);
  const [performance, setPerformance] = useState<PerformanceData>(defaultPerformance);
  const [cost, setCost] = useState<CostData>(defaultCost);
  const [adoption, setAdoption] = useState<AdoptionData>(defaultAdoption);
  const [workflow, setWorkflow] = useState<WorkflowData>(defaultWorkflow);
  const [system, setSystem] = useState<SystemData>(defaultSystem);
  const [qdrantMetrics, setQdrantMetrics] = useState<QdrantMetricsData>(defaultQdrantMetrics);
  const [qdrantMetricsLoading, setQdrantMetricsLoading] = useState(false);
  const [cloudInfraCost, setCloudInfraCost] = useState<CloudInfrastructureCostData>(defaultCloudInfrastructureCost);
  const [cloudInfraCostLoading, setCloudInfraCostLoading] = useState(false);
  const [resolvingFailureIds, setResolvingFailureIds] = useState<Record<string, boolean>>({});
  const cacheRef = useRef<Map<string, unknown>>(new Map());
  const abortControllersRef = useRef<Map<string, AbortController>>(new Map());
  const activeTabRef = useRef<MissionControlTab>(activeTab);
  const timeframeRef = useRef<MissionControlTimeframe>(timeframe);

  const timeframeLabel = useMemo(
    () => timeframeOptions.find((option) => option.value === timeframe)?.label || "Last 30d",
    [timeframe]
  );

  useEffect(() => {
    activeTabRef.current = activeTab;
  }, [activeTab]);

  useEffect(() => {
    timeframeRef.current = timeframe;
  }, [timeframe]);

  const setTabLoadingValue = useCallback((tab: MissionControlTab, loading: boolean) => {
    setTabLoading((prev) => (prev[tab] === loading ? prev : { ...prev, [tab]: loading }));
  }, []);

  const applyTabPayload = useCallback((tab: MissionControlTab, payload: unknown) => {
    switch (tab) {
      case "overview":
        setOverview(payload as OverviewData);
        break;
      case "performance":
        setPerformance(payload as PerformanceData);
        break;
      case "cost":
        setCost(payload as CostData);
        break;
      case "adoption":
        setAdoption(payload as AdoptionData);
        break;
      case "workflow":
        setWorkflow(payload as WorkflowData);
        break;
      case "system":
        setSystem(payload as SystemData);
        break;
      default:
        break;
    }
  }, []);

  const normalizeTabPayload = useCallback((tab: MissionControlTab, payload: unknown): unknown => {
    switch (tab) {
      case "overview":
        return normalizeOverview(payload as OverviewData);
      case "performance":
        return normalizePerformance(payload as PerformanceData);
      case "cost":
        return normalizeCost(payload as CostData);
      case "adoption":
        return normalizeAdoption(payload as AdoptionData);
      case "workflow":
        return normalizeWorkflow(payload as WorkflowData);
      case "system":
        return normalizeSystem(payload as SystemData);
      default:
        return payload;
    }
  }, []);

  const abortAllInFlightRequests = useCallback(() => {
    abortControllersRef.current.forEach((controller) => controller.abort());
    abortControllersRef.current.clear();
  }, []);

  const fetchMissionControlTab = useCallback(async (
    tab: MissionControlTab,
    options?: { force?: boolean; background?: boolean }
  ) => {
    if (!isLoaded) return;
    const requestedTimeframe = timeframeRef.current;
    const force = Boolean(options?.force);
    const background = Boolean(options?.background);
    const cacheKey = getMissionControlCacheKey(tab, requestedTimeframe);
    if (!force) {
      const cached = cacheRef.current.get(cacheKey);
      if (cached) {
        applyTabPayload(tab, cached);
        if (tab === "overview") {
          setInitialPageLoading(false);
        }
        return;
      }
    }

    const requestKey = cacheKey;
    if (!force && abortControllersRef.current.has(requestKey)) {
      return;
    }
    const existingController = abortControllersRef.current.get(requestKey);
    if (existingController) {
      existingController.abort();
      abortControllersRef.current.delete(requestKey);
    }

    const controller = new AbortController();
    abortControllersRef.current.set(requestKey, controller);
    try {
      if (!background) {
        setTabLoadingValue(tab, true);
        setError(null);
      }
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required.");
      }

      const query = `?timeframe=${encodeURIComponent(requestedTimeframe)}`;
      const payload = await apiFetch(
        `${getMissionControlEndpoint(tab)}${query}`,
        { token, method: "GET", signal: controller.signal }
      );

      if (controller.signal.aborted) {
        return;
      }
      if (abortControllersRef.current.get(requestKey) !== controller) {
        return;
      }

      const normalizedPayload = normalizeTabPayload(tab, payload);
      cacheRef.current.set(cacheKey, normalizedPayload);

      if (timeframeRef.current === requestedTimeframe) {
        applyTabPayload(tab, normalizedPayload);
        setAccessDenied(false);
      }
    } catch (err) {
      if (controller.signal.aborted) {
        return;
      }
      if (err instanceof ApiRequestError && err.status === 403) {
        setAccessDenied(true);
        return;
      }
      if (!background && activeTabRef.current === tab && timeframeRef.current === requestedTimeframe) {
        setError(err instanceof Error ? err.message : "Failed to load Mission Control.");
      }
    } finally {
      if (abortControllersRef.current.get(requestKey) === controller) {
        abortControllersRef.current.delete(requestKey);
      }
      if (!background) {
        setTabLoadingValue(tab, false);
      }
      if (tab === "overview") {
        setInitialPageLoading(false);
      }
    }
  }, [applyTabPayload, getToken, isLoaded, normalizeTabPayload, setTabLoadingValue]);

  useEffect(() => {
    if (!isLoaded) return;
    abortAllInFlightRequests();
    void fetchMissionControlTab(activeTab);
    return () => {
      abortAllInFlightRequests();
    };
  }, [abortAllInFlightRequests, activeTab, fetchMissionControlTab, isLoaded, timeframe]);

  useEffect(() => {
    if (!isLoaded) return;
    const overviewKey = getMissionControlCacheKey("overview", timeframe);
    const costKey = getMissionControlCacheKey("cost", timeframe);
    if (!cacheRef.current.has(overviewKey) || cacheRef.current.has(costKey)) {
      return;
    }
    void fetchMissionControlTab("cost", { background: true });
  }, [fetchMissionControlTab, isLoaded, timeframe]);

  const fetchQdrantMetrics = useCallback(async () => {
    if (!isLoaded) return;
    setQdrantMetricsLoading(true);
    try {
      const token = await getToken();
      if (!token) throw new Error("Authentication required.");
      const payload = await apiFetch("/api/v1/metrics/qdrant", { token, method: "GET" });
      setQdrantMetrics(normalizeQdrantMetrics(payload));
    } catch (err) {
      if (err instanceof ApiRequestError && err.status === 403) {
        setAccessDenied(true);
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load Qdrant metrics.");
    } finally {
      setQdrantMetricsLoading(false);
    }
  }, [getToken, isLoaded]);

  const fetchCloudInfrastructureCost = useCallback(async () => {
    if (!isLoaded) return;
    setCloudInfraCostLoading(true);
    try {
      const token = await getToken();
      if (!token) throw new Error("Authentication required.");
      const payload = await apiFetch("/api/v1/metrics/cloud-infrastructure-cost", { token, method: "GET" });
      setCloudInfraCost(normalizeCloudInfrastructureCost(payload));
    } catch (err) {
      if (err instanceof ApiRequestError && err.status === 403) {
        setAccessDenied(true);
        return;
      }
      setCloudInfraCost({
        ...defaultCloudInfrastructureCost,
        is_available: false,
        unavailable_reason: err instanceof Error ? err.message : "Cloud billing data unavailable.",
      });
    } finally {
      setCloudInfraCostLoading(false);
    }
  }, [getToken, isLoaded]);

  useEffect(() => {
    if (!isLoaded || activeTab !== "cost") return;
    void fetchQdrantMetrics();
    void fetchCloudInfrastructureCost();
  }, [activeTab, fetchCloudInfrastructureCost, fetchQdrantMetrics, isLoaded]);

  const topKpis = useMemo(() => {
    return [
      {
        label: `Active Users (${timeframeLabel})`,
        value: String(overview.active_users_7d),
        icon: Users,
        onClick: () => {
          setActiveTab("overview");
          setOverviewDrilldown("users");
        },
      },
      {
        label: `Active Campaigns (${timeframeLabel})`,
        value: String(overview.active_campaigns_7d),
        icon: Activity,
        onClick: () => {
          setActiveTab("overview");
          setOverviewDrilldown("campaigns");
        },
      },
      { label: "Report Success Rate", value: `${overview.report_success_rate}%`, icon: ShieldCheck },
      { label: "LLM Spend (MTD)", value: `$${asNumber(overview.current_month_llm_spend).toFixed(2)}`, icon: DollarSign },
    ];
  }, [overview, timeframeLabel]);

  const activeTabLoading = tabLoading[activeTab];
  const activeTabLabel = tabs.find((tab) => tab.id === activeTab)?.label || "Active tab";
  const guardrailStyle = resolveGuardrailStyle(cost.guardrail_status);
  const showGlobalGuardrailWarning = cost.guardrail_status === "red" || cost.guardrail_status === "exceeded";
  const cloudLagHours = asNumber(cost.cloud_cost_billing_data_lag_hours);
  const cloudBillingLagged = cost.cloud_cost_is_available && cloudLagHours >= 24;
  const cloudFreshnessBadgeLabel = !cost.cloud_cost_is_available
    ? "Cloud billing unavailable"
    : cloudBillingLagged
      ? "Cloud billing lagged"
      : "Cloud billing fresh";
  const cloudFreshnessBadgeClass = !cost.cloud_cost_is_available
    ? "border-amber-300 bg-amber-50 text-amber-800"
    : cloudBillingLagged
      ? "border-yellow-300 bg-yellow-50 text-yellow-800"
      : "border-emerald-300 bg-emerald-50 text-emerald-800";
  const currentMonthFeatureConfidence = cost.current_month_llm_feature_mapping_confidence;
  const showFeatureAttributionNote =
    asNumber(currentMonthFeatureConfidence.inferred_or_other_percent) >= 15 ||
    asNumber(currentMonthFeatureConfidence.other_llm_percent) >= 10;

  const handleResolveFailure = useCallback(async (failureId: string) => {
    const normalized = asString(failureId, "");
    if (!normalized || resolvingFailureIds[normalized]) return;
    setResolvingFailureIds((prev) => ({ ...prev, [normalized]: true }));
    const previousFailures = system.recent_failures;
    setSystem((prev) => ({
      ...prev,
      recent_failures: prev.recent_failures.filter((row) => asString(row.failure_id, "") !== normalized),
    }));
    try {
      const token = await getToken();
      if (!token) throw new Error("Authentication required.");
      await apiFetch(`/api/v1/mission-control/failures/${encodeURIComponent(normalized)}/resolve`, {
        token,
        method: "POST",
      });
    } catch (err) {
      setSystem((prev) => ({ ...prev, recent_failures: previousFailures }));
      setError(err instanceof Error ? err.message : "Failed to resolve failure.");
    } finally {
      setResolvingFailureIds((prev) => {
        const next = { ...prev };
        delete next[normalized];
        return next;
      });
    }
  }, [getToken, resolvingFailureIds, system.recent_failures]);

  return (
    <div className="min-h-screen bg-slate-100 text-slate-900">
      <div className="mx-auto max-w-7xl px-6 py-8">
        <header className="mb-8 flex items-center justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Admin Surface</p>
            <h1 className="mt-2 text-4xl font-semibold tracking-tight">Mission Control</h1>
            <p className="mt-2 text-sm text-slate-600">Executive analytics and platform health for Riley.</p>
            {showGlobalGuardrailWarning ? (
              <div className="mt-3">
                <span className={`inline-flex items-center rounded-full px-2.5 py-1 text-xs font-semibold ${guardrailStyle.badgeClass}`}>
                  LLM Budget {guardrailStyle.label} ({asNumber(cost.percent_budget_used).toFixed(2)}% used)
                </span>
              </div>
            ) : null}
          </div>
          <div className="flex items-center gap-3">
            <div className="rounded-lg border border-slate-300 bg-white p-1">
              {timeframeOptions.map((option) => (
                <button
                  key={option.value}
                  type="button"
                  onClick={() => setTimeframe(option.value)}
                  className={`rounded px-2 py-1 text-xs font-medium ${
                    timeframe === option.value ? "bg-slate-900 text-white" : "text-slate-700 hover:bg-slate-100"
                  }`}
                >
                  {option.label}
                </button>
              ))}
            </div>
            <button
              type="button"
              onClick={() => void fetchMissionControlTab(activeTab, { force: true })}
              className="rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
            >
              Refresh
            </button>
            <Link
              href="/"
              className="inline-flex items-center gap-1 rounded-lg border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
            >
              Back to Riley
              <ChevronRight className="h-4 w-4" />
            </Link>
          </div>
        </header>

        {initialPageLoading ? (
          <div className="rounded-2xl border border-slate-200 bg-white p-10 text-center text-slate-600">Loading Mission Control...</div>
        ) : accessDenied ? (
          <div className="rounded-2xl border border-rose-200 bg-rose-50 p-10 text-center">
            <AlertTriangle className="mx-auto mb-3 h-6 w-6 text-rose-600" />
            <h2 className="text-lg font-semibold text-rose-900">Access denied</h2>
            <p className="mt-1 text-sm text-rose-700">You are not allowed to access Mission Control.</p>
          </div>
        ) : error ? (
          <div className="rounded-2xl border border-amber-200 bg-amber-50 p-10 text-center text-amber-800">{error}</div>
        ) : (
          <>
            <section className="mb-6 grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
              {topKpis.map((kpi) => {
                const Icon = kpi.icon;
                return (
                  <div key={kpi.label} className="rounded-xl border border-slate-200 bg-white p-5">
                    <div className="mb-3 flex items-center justify-between">
                      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{kpi.label}</p>
                      <Icon className="h-4 w-4 text-slate-400" />
                    </div>
                    <button type="button" onClick={kpi.onClick} className="text-left text-2xl font-semibold">
                      {kpi.value}
                    </button>
                  </div>
                );
              })}
            </section>

            <section className="rounded-2xl border border-slate-200 bg-white p-3">
              <div className="mb-4 flex flex-wrap gap-2">
                {tabs.map((tab) => (
                  <button
                    key={tab.id}
                    type="button"
                    onClick={() => setActiveTab(tab.id)}
                    className={`rounded-lg px-3 py-2 text-sm font-medium ${
                      activeTab === tab.id
                        ? "bg-slate-900 text-white"
                        : "bg-slate-100 text-slate-700 hover:bg-slate-200"
                    }`}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>

              <div className="rounded-xl border border-slate-100 bg-slate-50 p-5">
                <div className="mb-3 flex items-center justify-between">
                  <p className="text-xs font-medium uppercase tracking-wide text-slate-500">Timeframe: {timeframeLabel}</p>
                  {activeTabLoading ? <p className="text-xs text-slate-500">Updating {activeTabLabel}...</p> : null}
                </div>
                {activeTab === "overview" && (
                  <div className="space-y-5">
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                      <Metric label={`Chats (${timeframeLabel})`} value={overview.chats_today} />
                      <Metric label={`Reports (${timeframeLabel})`} value={overview.reports_today} />
                      <Metric label="Avg Response Latency (ms)" value={overview.avg_response_latency_ms} />
                      <Metric label="Forecast Month-End LLM Spend" value={`$${asNumber(overview.forecast_month_end_llm_spend).toFixed(2)}`} />
                      <Metric label="Tracked Non-LLM Event Spend (MTD)" value={`$${asNumber(overview.current_month_non_llm_event_spend).toFixed(2)}`} />
                      <Metric
                        label="Pending Campaign Join Requests"
                        value={overview.pending_access_requests}
                        onClick={() => {
                          setOverviewDrilldown("join_requests");
                          void fetchMissionControlTab("workflow");
                        }}
                      />
                      <Metric label="Overdue Deadlines" value={overview.overdue_deadlines} />
                      <Metric label={`Failed Reports (${timeframeLabel})`} value={overview.failed_reports_24h} />
                    </div>

                    <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                      <LineChartCard
                        title={`Chats and Reports (${timeframeLabel})`}
                        series={[
                          { name: "Chats", color: "#0f766e", points: overview.timeseries_30d.map((p) => ({ x: safeDateLabel(p.day), y: p.chats })) },
                          { name: "Reports", color: "#4338ca", points: overview.timeseries_30d.map((p) => ({ x: safeDateLabel(p.day), y: p.reports })) },
                        ]}
                      />
                      <LineChartCard
                        title={`LLM Spend and Failures (${timeframeLabel})`}
                        series={[
                          { name: "LLM Spend ($)", color: "#b45309", points: overview.timeseries_30d.map((p) => ({ x: safeDateLabel(p.day), y: p.cost })) },
                          { name: "Failures", color: "#b91c1c", points: overview.timeseries_30d.map((p) => ({ x: safeDateLabel(p.day), y: p.failures })) },
                        ]}
                      />
                    </div>

                    <div className="rounded-lg border border-slate-200 bg-white p-4">
                      <div className="mb-3 flex items-center gap-2">
                        <button
                          type="button"
                          onClick={() => setOverviewDrilldown("campaigns")}
                          className={`rounded px-3 py-1 text-sm ${overviewDrilldown === "campaigns" ? "bg-slate-900 text-white" : "bg-slate-100 text-slate-700"}`}
                        >
                          Campaign Drilldown
                        </button>
                        <button
                          type="button"
                          onClick={() => setOverviewDrilldown("users")}
                          className={`rounded px-3 py-1 text-sm ${overviewDrilldown === "users" ? "bg-slate-900 text-white" : "bg-slate-100 text-slate-700"}`}
                        >
                          User Drilldown
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            setOverviewDrilldown("join_requests");
                            void fetchMissionControlTab("workflow");
                          }}
                          className={`rounded px-3 py-1 text-sm ${overviewDrilldown === "join_requests" ? "bg-slate-900 text-white" : "bg-slate-100 text-slate-700"}`}
                        >
                          Join Requests
                        </button>
                      </div>
                      {overviewDrilldown === "campaigns" ? (
                        <SimpleTable
                          emptyLabel="No campaign drilldown data."
                          columns={["Campaign", "Chats", "Reports", "Cost"]}
                          rows={overview.campaign_usage_7d.map((row) => [
                            renderCampaignLabel(row.campaign_name, row.campaign_id, row.campaign_secondary),
                            String(row.chats),
                            String(row.reports),
                            `$${asNumber(row.cost).toFixed(2)}`,
                          ])}
                        />
                      ) : overviewDrilldown === "users" ? (
                        <SimpleTable
                          emptyLabel="No user drilldown data."
                          columns={["User", "Chats", "Reports", "Cost"]}
                          rows={overview.user_usage_7d.map((row) => [
                            renderUserLabel(asString(row.user_label, row.user_id), row.user_id, row.user_secondary),
                            String(row.chats),
                            String(row.reports),
                            `$${asNumber(row.cost).toFixed(2)}`,
                          ])}
                        />
                      ) : tabLoading.workflow ? (
                        <p className="text-sm text-slate-500">Loading pending join requests...</p>
                      ) : (
                        <SimpleTable
                          emptyLabel="No pending campaign join requests."
                          columns={["Requester", "Campaign", "Requested At", "Status"]}
                          rows={workflow.pending_access_request_list.map((row) => [
                            renderUserLabel(asString(row.user_label, row.user_id), row.user_id, row.user_secondary),
                            renderCampaignLabel(row.campaign_name, row.campaign_id, row.campaign_secondary),
                            safeDateTime(row.created_at),
                            asString(row.status, "pending"),
                          ])}
                        />
                      )}
                    </div>
                  </div>
                )}

                {activeTab === "performance" && (
                  <div className="space-y-5">
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                      <Metric label={`Avg Latency (${timeframeLabel}, ms)`} value={performance.avg_latency_ms_7d} />
                      <Metric label={`P95 Latency (${timeframeLabel}, ms)`} value={performance.p95_latency_ms_7d} />
                      <Metric label={`Fallback Successes (${timeframeLabel})`} value={performance.provider_fallback_successes_7d} />
                      <Metric label={`Fallback Failures (${timeframeLabel})`} value={performance.provider_fallback_failures_7d} />
                      <Metric label={`Fallback Success Rate (${timeframeLabel})`} value={`${performance.provider_fallback_success_rate_7d}%`} />
                      <Metric label={`Reranker Failures (${timeframeLabel})`} value={performance.reranker_failures_7d} />
                    </div>

                    <LineChartCard
                      title={`Report Success vs Failure Trend (${timeframeLabel})`}
                      series={[
                        {
                          name: "Success",
                          color: "#047857",
                          points: performance.success_failure_trend_30d.map((p) => ({ x: safeDateLabel(p.day), y: p.success })),
                        },
                        {
                          name: "Failure",
                          color: "#b91c1c",
                          points: performance.success_failure_trend_30d.map((p) => ({ x: safeDateLabel(p.day), y: p.failure })),
                        },
                      ]}
                    />

                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
                      <Metric label="Latency P50 (ms)" value={performance.latency_percentiles_7d.p50_ms} />
                      <Metric label="Latency P75 (ms)" value={performance.latency_percentiles_7d.p75_ms} />
                      <Metric label="Latency P90 (ms)" value={performance.latency_percentiles_7d.p90_ms} />
                      <Metric label="Latency P95 (ms)" value={performance.latency_percentiles_7d.p95_ms} />
                    </div>

                    <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                      <div className="rounded-lg border border-slate-200 bg-white p-4">
                        <p className="mb-2 text-sm font-semibold">Performance by Campaign ({timeframeLabel})</p>
                        <SimpleTable
                          emptyLabel="No per-campaign performance data."
                          columns={["Campaign", "Avg Latency", "P95 Latency", "Chats", "Reports"]}
                          rows={performance.campaign_performance_7d.map((row) => [
                            renderCampaignLabel(row.campaign_name, row.campaign_id, row.campaign_secondary),
                            `${row.avg_latency_ms} ms`,
                            `${row.p95_latency_ms} ms`,
                            String(row.chats),
                            String(row.reports),
                          ])}
                        />
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-white p-4">
                        <p className="mb-2 text-sm font-semibold">Provider Breakdown (7d)</p>
                        <SimpleTable
                          emptyLabel="No provider performance data."
                          columns={["Provider", "Avg Latency", "Fallback %", "Triggered"]}
                          rows={performance.provider_breakdown_7d.map((row) => {
                            const triggered = asNumber(row.fallback_triggered);
                            const successRate = triggered
                              ? ((asNumber(row.fallback_succeeded) / triggered) * 100).toFixed(1)
                              : "0.0";
                            return [
                              row.provider,
                              `${row.avg_latency_ms} ms`,
                              `${successRate}%`,
                              String(row.fallback_triggered),
                            ];
                          })}
                        />
                      </div>
                    </div>
                  </div>
                )}

                {activeTab === "cost" && (
                  <div className="space-y-4">
                    {/* Data source: Aggregated display-only total from separate category feeds:
                        LLM (AnalyticsEvent LLM-only) + Cloud Billing + Qdrant estimate + Fixed SaaS config. */}
                    <div className="rounded-lg border border-slate-200 bg-white p-4">
                      <div className="mb-3 flex items-center justify-between gap-2">
                        <p className="text-sm font-semibold">Total Monthly Spend</p>
                        <span className="text-xs text-slate-500">
                          Blended view: metered/actual + estimated accrual categories
                        </span>
                      </div>
                      <div className="mb-3 flex items-center gap-2">
                        <span className={`rounded-full border px-2.5 py-1 text-xs font-semibold ${cloudFreshnessBadgeClass}`}>
                          {cloudFreshnessBadgeLabel}
                        </span>
                        {(!cost.cloud_cost_is_available || cloudBillingLagged) && (
                          <span className="text-xs text-slate-600">
                            Total spend may be delayed/incomplete until Cloud billing data catches up.
                          </span>
                        )}
                      </div>
                      {!cost.cloud_cost_is_available && cost.cloud_cost_unavailable_reason ? (
                        <div className="mb-3 rounded border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                          Cloud freshness source unavailable: {cost.cloud_cost_unavailable_reason}
                        </div>
                      ) : null}
                      <div className="mb-3 rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                        Cloud MTD is billing-export metered and may lag. Qdrant and fixed SaaS MTD are estimated accruals.
                      </div>
                      <div className="mb-4 grid grid-cols-1 gap-3 md:grid-cols-3">
                        <Metric
                          label="Blended MTD Spend (Actual/Metered + Estimated)"
                          value={`$${asNumber(cost.current_month_blended_total_spend || cost.current_month_total_spend).toFixed(2)}`}
                        />
                        <Metric
                          label="Actual or Metered MTD"
                          value={`$${asNumber(cost.current_month_actual_or_metered_spend).toFixed(2)}`}
                        />
                        <Metric
                          label="Estimated Accrual MTD"
                          value={`$${asNumber(cost.current_month_estimated_accrual_spend).toFixed(2)}`}
                        />
                      </div>
                      <div className="mb-4 grid grid-cols-1 gap-3 md:grid-cols-1">
                        <Metric label="Projected Month-End Spend" value={`$${asNumber(cost.projected_month_end_total_spend).toFixed(2)}`} />
                      </div>
                      <SimpleTable
                        emptyLabel="No total spend category breakdown available."
                        columns={["Category", "Current MTD (Semantics)", "Projected (Month-End)"]}
                        rows={cost.total_spend_breakdown.map((row) => [
                          row.category,
                          `${`$${asNumber(row.current).toFixed(2)}`} (${row.current_semantics === "actual_or_metered_mtd" ? "actual/metered" : "estimated accrual"})`,
                          `$${asNumber(row.projected).toFixed(2)}`,
                        ])}
                      />
                      <div className="mt-3 rounded border border-slate-200 bg-slate-50 p-3">
                        <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                          Other Fixed SaaS Composition
                        </p>
                        <p className="mb-2 text-xs text-slate-500">
                          Includes Clerk, Vercel, and Other only. Qdrant Fixed is tracked separately in the total category breakdown.
                        </p>
                        <SimpleTable
                          emptyLabel="No fixed SaaS composition configured."
                          columns={["Component", "Current MTD (estimated accrual)", "Projected (Month-End)"]}
                          rows={cost.fixed_saas_component_breakdown.map((row) => [
                            row.component,
                            `$${asNumber(row.current_mtd).toFixed(2)}`,
                            `$${asNumber(row.projected_month_end).toFixed(2)}`,
                          ])}
                        />
                      </div>
                    </div>
                    <div className={`rounded-lg border p-4 ${guardrailStyle.panelClass}`}>
                      {/* Data source: LLM-only spend and budget status from guardrail-compatible
                          AnalyticsEvent provider/model filters (Gemini/OpenAI family). */}
                      <div className="mb-3 flex items-center justify-between gap-3">
                        <p className="text-sm font-semibold text-slate-900">LLM Guardrail Status</p>
                        <span className={`rounded-full px-2.5 py-1 text-xs font-semibold ${guardrailStyle.badgeClass}`}>
                          {guardrailStyle.label}
                        </span>
                      </div>
                      <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-5">
                        <Metric label="Monthly LLM Budget Limit" value={`$${asNumber(cost.monthly_llm_budget_limit).toFixed(2)}`} />
                        <Metric label="Current Month LLM Spend" value={`$${asNumber(cost.current_month_llm_spend).toFixed(2)}`} />
                        <Metric label="Projected Month-End LLM Spend" value={`$${asNumber(cost.projected_month_end_llm_spend).toFixed(2)}`} />
                        <Metric label="Budget Used" value={`${asNumber(cost.percent_budget_used).toFixed(2)}%`} />
                        <Metric label="Guardrail Status" value={guardrailStyle.label} />
                      </div>
                    </div>
                    <div className="rounded-lg border border-slate-200 bg-white p-4">
                      {/* Data source: LLM-only AnalyticsEvent mapped by source_event_type_raw/source_entity/metadata_json.
                          Mapping confidence is explicit when event metadata/mode markers are present, otherwise inferred/fallback. */}
                      <div className="mb-3 flex items-center justify-between gap-2">
                        <p className="text-sm font-semibold">LLM Cost by Feature</p>
                        <span className="text-xs text-slate-500">Current month + last 30d</span>
                      </div>
                      <p className={`mb-3 text-xs ${showFeatureAttributionNote ? "text-amber-700" : "text-slate-500"}`}>
                        Attribution confidence (current month): {asNumber(currentMonthFeatureConfidence.explicit_percent).toFixed(1)}%
                        explicit, {asNumber(currentMonthFeatureConfidence.inferred_or_other_percent).toFixed(1)}% inferred/other.
                        {showFeatureAttributionNote
                          ? " A meaningful share is heuristic or Other LLM; treat feature splits as directional."
                          : ""}
                      </p>
                      <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                        <div>
                          <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                            Current Month LLM Spend
                          </p>
                          <SimpleTable
                            emptyLabel="No current-month LLM feature cost data."
                            columns={["Feature", "Spend", "% of LLM"]}
                            rows={cost.current_month_llm_cost_by_feature.map((row) => [
                              row.feature,
                              `$${asNumber(row.spend).toFixed(2)}`,
                              `${asNumber(row.percent_of_llm).toFixed(2)}%`,
                            ])}
                          />
                        </div>
                        <div>
                          <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                            Last 30 Days LLM Spend
                          </p>
                          <SimpleTable
                            emptyLabel="No last-30d LLM feature cost data."
                            columns={["Feature", "Spend", "% of LLM"]}
                            rows={cost.last_30d_llm_cost_by_feature.map((row) => [
                              row.feature,
                              `$${asNumber(row.spend).toFixed(2)}`,
                              `${asNumber(row.percent_of_llm).toFixed(2)}%`,
                            ])}
                          />
                        </div>
                      </div>
                    </div>
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                      <Metric label="Current Month LLM Spend (MTD)" value={`$${asNumber(cost.current_month_llm_spend).toFixed(2)}`} />
                      <Metric label="Avg Daily LLM Burn" value={`$${asNumber(cost.average_daily_burn_rate).toFixed(2)} / day`} />
                      <Metric label="Projected Month-End LLM Spend" value={`$${asNumber(cost.projected_month_end_llm_spend).toFixed(2)}`} />
                      <Metric label="Last 7d LLM Spend" value={`$${asNumber(cost.last_7d_estimated_cost).toFixed(2)}`} />
                      <Metric label="LLM Spend per Chat" value={`$${asNumber(cost.cost_per_chat).toFixed(4)}`} />
                      <Metric label="LLM Spend per Report" value={`$${asNumber(cost.cost_per_report).toFixed(4)}`} />
                    </div>
                    <div className="rounded-lg border border-slate-200 bg-white p-4">
                      {/* Data source: Non-LLM event-attributed operational estimates from AnalyticsEvent.
                          Kept separate and excluded from total-spend to avoid overlap with Cloud Billing. */}
                      <p className="mb-2 text-sm font-semibold">Tracked Non-LLM Operational Event Spend</p>
                      <div className="mb-3 rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                        Observability-only signal. Excluded from Total Monthly Spend to prevent overlap with billed Cloud Infrastructure costs.
                      </div>
                      <div className="mb-3 grid grid-cols-1 gap-3 md:grid-cols-2">
                        <Metric label="Current Month Non-LLM Event Spend" value={`$${asNumber(cost.current_month_non_llm_event_spend).toFixed(2)}`} />
                        <Metric label={`Selected Window Non-LLM Event Spend (${timeframeLabel})`} value={`$${asNumber(cost.window_non_llm_event_spend).toFixed(2)}`} />
                      </div>
                      <SimpleTable
                        emptyLabel="No tracked non-LLM operational event spend."
                        columns={["Provider", "Spend"]}
                        rows={cost.tracked_non_llm_event_provider_breakdown_30d.map((row) => [
                          row.provider,
                          `$${asNumber(row.cost).toFixed(2)}`,
                        ])}
                      />
                    </div>
                    <div className="rounded-lg border border-slate-200 bg-white p-4">
                      {/* Data source: Cloud Billing export (BigQuery), independent of AnalyticsEvent costs. */}
                      <div className="mb-3 flex items-center justify-between gap-2">
                        <p className="text-sm font-semibold">Cloud Infrastructure</p>
                        <span className="text-xs text-slate-500">
                          {asString(cloudInfraCost.billing_source, "") || "billing source unavailable"}
                        </span>
                      </div>
                      <div className="mb-3 text-xs text-slate-500">
                        <p>
                          Last updated:{" "}
                          {cloudInfraCost.last_data_timestamp
                            ? safeDateTime(cloudInfraCost.last_data_timestamp)
                            : "Unavailable"}
                        </p>
                        <p>
                          Billing data may lag by{" "}
                          {cloudInfraCost.last_data_timestamp
                            ? `${asNumber(cloudInfraCost.billing_data_lag_hours).toFixed(1)} hours`
                            : "an unknown interval"}.
                        </p>
                      </div>
                      {cloudInfraCostLoading ? (
                        <p className="text-sm text-slate-500">Loading Cloud Infrastructure cost...</p>
                      ) : !cloudInfraCost.is_available ? (
                        <div className="rounded border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800">
                          Cloud billing data is currently unavailable.
                          {cloudInfraCost.unavailable_reason ? ` ${cloudInfraCost.unavailable_reason}` : ""}
                        </div>
                      ) : (
                        <div className="space-y-4">
                          <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                            <Metric label="Current Month Cloud Spend" value={`$${asNumber(cloudInfraCost.current_month_cloud_cost).toFixed(2)}`} />
                            <Metric label="Projected Month-End Cloud Spend" value={`$${asNumber(cloudInfraCost.projected_month_end_cloud_cost).toFixed(2)}`} />
                          </div>
                          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                            <LineChartCard
                              title="Daily Cloud Cost Trend"
                              series={[
                                {
                                  name: "Cloud Cost",
                                  color: "#0369a1",
                                  points: cloudInfraCost.daily_cloud_cost_series.map((p) => ({
                                    x: safeDateLabel(p.date),
                                    y: p.cost,
                                  })),
                                },
                              ]}
                            />
                            <div className="rounded-lg border border-slate-200 bg-white p-4">
                              <p className="mb-2 text-sm font-semibold">Cloud Service Breakdown (MTD)</p>
                              <SimpleTable
                                emptyLabel="No cloud service breakdown available."
                                columns={["Service", "Cost"]}
                                rows={cloudInfraCost.cloud_cost_breakdown.map((row) => [
                                  row.service,
                                  `$${asNumber(row.cost).toFixed(2)}`,
                                ])}
                              />
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                    <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                      <LineChartCard
                        title={`LLM Spend Trend (${timeframeLabel})`}
                        series={[
                          {
                            name: "LLM Spend",
                            color: "#b45309",
                            points: cost.cost_trend_30d.map((p) => ({ x: safeDateLabel(p.day), y: p.cost })),
                          },
                        ]}
                      />
                      <LineChartCard
                        title="Projected Month-End LLM Spend Curve"
                        series={[
                          {
                            name: "Projected LLM Spend",
                            color: "#1d4ed8",
                            points: cost.projected_monthly_curve.map((p) => ({ x: String(p.day), y: p.projected_cost })),
                          },
                        ]}
                      />
                    </div>
                    <div className="rounded-lg border border-slate-200 bg-white p-4">
                      <div className="mb-2 flex items-center gap-2">
                        <BarChart3 className="h-4 w-4 text-slate-500" />
                        <p className="text-sm font-semibold">Provider LLM Spend Breakdown ({timeframeLabel})</p>
                      </div>
                      {cost.provider_cost_30d?.length ? (
                        <div className="space-y-2">
                          {cost.provider_cost_30d.map((row, idx) => (
                            <div key={`${row.provider}-${idx}`} className="flex items-center justify-between text-sm">
                              <span className="capitalize text-slate-600">{row.provider}</span>
                              <span className="font-medium text-slate-900">${asNumber(row.cost).toFixed(2)}</span>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <p className="text-sm text-slate-500">No provider cost data returned.</p>
                      )}
                    </div>
                    <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
                      <div className="rounded-lg border border-slate-200 bg-white p-4">
                        <p className="mb-2 text-sm font-semibold">LLM Spend by Campaign ({timeframeLabel})</p>
                        <SimpleTable
                          emptyLabel="No campaign cost data."
                          columns={["Campaign", "Cost"]}
                          rows={cost.cost_by_campaign_30d.map((row) => [
                            renderCampaignLabel(row.campaign_name, row.campaign_id, row.campaign_secondary),
                            `$${asNumber(row.cost).toFixed(2)}`,
                          ])}
                        />
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-white p-4">
                        <p className="mb-2 text-sm font-semibold">LLM Spend by User ({timeframeLabel})</p>
                        <SimpleTable
                          emptyLabel="No user cost data."
                          columns={["User", "Cost"]}
                          rows={cost.cost_by_user_30d.map((row) => [
                            renderUserLabel(asString(row.user_label, row.user_id), row.user_id, row.user_secondary),
                            `$${asNumber(row.cost).toFixed(2)}`,
                          ])}
                        />
                      </div>
                    </div>
                    {/* Data source: live Qdrant collections + stored usage snapshots for trend/projection. */}
                    <div
                      className={`rounded-lg border bg-white p-4 ${
                        qdrantMetrics.warning_state === "warning"
                          ? "border-amber-300 bg-amber-50/40"
                          : "border-slate-200"
                      }`}
                    >
                      <div className="mb-3 flex items-center justify-between gap-2">
                        <p className="text-sm font-semibold">Qdrant Usage</p>
                        <span className="text-xs text-slate-500">
                          Projection confidence:{" "}
                          {qdrantMetrics.projection_confidence === "high"
                            ? "High"
                            : qdrantMetrics.projection_confidence === "low"
                              ? "Low"
                              : "Collecting history"}
                        </span>
                        <button
                          type="button"
                          onClick={() => void fetchQdrantMetrics()}
                          className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-50"
                        >
                          Refresh Qdrant
                        </button>
                      </div>
                      {qdrantMetrics.warning_state === "warning" && (
                        <div className="mb-3 rounded border border-amber-300 bg-amber-50 px-3 py-2 text-xs text-amber-800">
                          Qdrant growth warning: high-confidence projected storage/cost trend is above configured thresholds.
                        </div>
                      )}
                      {qdrantMetrics.projection_available && qdrantMetrics.projection_confidence === "low" && (
                        <div className="mb-3 rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                          Forecast is low-confidence ({qdrantMetrics.projection_snapshot_count} snapshots).
                          Collecting more daily history before enabling projection-based warnings.
                        </div>
                      )}
                      {!qdrantMetrics.projection_available && (
                        <div className="mb-3 rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                          Collecting history for forecasting: {qdrantMetrics.projection_snapshot_count}/
                          {qdrantMetrics.projection_min_snapshots_required} daily snapshots.
                        </div>
                      )}
                      <div className="mb-3 grid grid-cols-1 gap-3 md:grid-cols-3">
                        <Metric label="Total Vectors" value={qdrantMetrics.total_vectors} />
                        <Metric
                          label="Estimated Storage"
                          value={`${asNumber(qdrantMetrics.total_estimated_size_mb).toFixed(2)} MB`}
                        />
                        <Metric
                          label="Estimated Monthly Cost"
                          value={`$${asNumber(qdrantMetrics.estimated_monthly_cost).toFixed(2)}`}
                        />
                      </div>
                      <div className="mb-3 grid grid-cols-1 gap-3 md:grid-cols-3">
                        <Metric
                          label="Current Month Qdrant Spend (estimated accrual)"
                          value={`$${asNumber(cost.current_month_qdrant_spend).toFixed(2)}`}
                        />
                      </div>
                      <div className="mb-3 rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                        Qdrant current-month spend is an accrual estimate from usage/storage projection and is not invoice-backed.
                      </div>
                      <div className="mb-3 grid grid-cols-1 gap-3 md:grid-cols-3">
                        <Metric
                          label="Projected Next-Month Storage"
                          value={
                            qdrantMetrics.projection_available
                              ? `${asNumber(qdrantMetrics.projected_next_month_storage_mb).toFixed(2)} MB`
                              : "Unavailable"
                          }
                        />
                        <Metric
                          label="Projected Next-Month Cost"
                          value={
                            qdrantMetrics.projection_available
                              ? `$${asNumber(qdrantMetrics.projected_next_month_cost).toFixed(2)}`
                              : "Unavailable"
                          }
                        />
                        <Metric
                          label="Observed Growth (window)"
                          value={
                            qdrantMetrics.projection_available
                              ? `${asNumber(qdrantMetrics.growth_percent).toFixed(2)}%`
                              : "Unavailable"
                          }
                        />
                      </div>
                      <div className="mb-3 text-xs text-slate-500">
                        Forecast method: {qdrantMetrics.projection_method || "recent-window growth"}; window:{" "}
                        {qdrantMetrics.projection_window_days_used > 0
                          ? `${qdrantMetrics.projection_window_days_used} days`
                          : "insufficient history"}
                        .
                      </div>
                      {!qdrantMetrics.projection_available && qdrantMetrics.projection_unavailable_reason ? (
                        <div className="mb-3 rounded border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                          {qdrantMetrics.projection_unavailable_reason}
                        </div>
                      ) : null}
                      {qdrantMetrics.trend_series.length > 0 && (
                        <div className="mb-3">
                          <LineChartCard
                            title="Qdrant Trend"
                            series={[
                              {
                                name: "Storage (MB)",
                                color: "#0f766e",
                                points: qdrantMetrics.trend_series.map((p) => ({
                                  x: safeDateLabel(p.date),
                                  y: p.total_estimated_size_mb,
                                })),
                              },
                              {
                                name: "Estimated Monthly Cost ($)",
                                color: "#1d4ed8",
                                points: qdrantMetrics.trend_series.map((p) => ({
                                  x: safeDateLabel(p.date),
                                  y: p.estimated_monthly_cost,
                                })),
                              },
                            ]}
                          />
                        </div>
                      )}
                      {qdrantMetricsLoading ? (
                        <p className="text-sm text-slate-500">Loading Qdrant usage...</p>
                      ) : (
                        <SimpleTable
                          emptyLabel="No campaign vector usage available."
                          columns={["Campaign", "Vectors", "Estimated Size (MB)"]}
                          rows={qdrantMetrics.campaigns.map((row) => [
                            asString(row.campaign_name, row.campaign_id),
                            String(row.vectors),
                            asNumber(row.estimated_size_mb).toFixed(2),
                          ])}
                        />
                      )}
                    </div>
                  </div>
                )}

                {activeTab === "system" && (
                  <div className="space-y-4">
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
                      <Metric label="Auth Denied (24h)" value={system.auth_denied_24h} />
                      <Metric label="Worker Failures (24h)" value={system.worker_failures_24h} />
                      <Metric label="Report Failures (24h)" value={system.report_failures_24h} />
                      <Metric label="Ingestion Failures (24h)" value={system.ingestion_failures_24h} />
                    </div>
                    <div className="rounded-lg border border-slate-200 bg-white p-4">
                      <p className="mb-2 text-sm font-semibold">Recent Failures ({timeframeLabel})</p>
                      <SimpleTable
                        emptyLabel="No recent failures."
                        columns={["Failure", "Worker", "Campaign", "Object/Job", "Status", "Timestamp", "Detail", "Action"]}
                        rows={system.recent_failures.map((row) => [
                          row.failure_class
                            ? `${asString(row.failure_label, row.type)} (${row.failure_class})`
                            : asString(row.failure_label, row.type),
                          row.worker_name || "-",
                          renderCampaignLabel(row.campaign_name, row.campaign_id, row.campaign_secondary),
                          row.object_id || "-",
                          row.status || "-",
                          safeDateTime(row.occurred_at),
                          row.detail || "-",
                          row.failure_id ? (
                            <button
                              type="button"
                              onClick={() => void handleResolveFailure(row.failure_id || "")}
                              disabled={Boolean(resolvingFailureIds[row.failure_id || ""])}
                              className="rounded border border-emerald-600/40 bg-emerald-500/10 px-2 py-1 text-xs text-emerald-300 hover:bg-emerald-500/20 disabled:opacity-50"
                            >
                              {resolvingFailureIds[row.failure_id || ""] ? "Resolving..." : "Resolve"}
                            </button>
                          ) : (
                            "-"
                          ),
                        ])}
                      />
                    </div>
                  </div>
                )}

                {activeTab === "adoption" && (
                  <div className="space-y-3">
                    <p className="text-sm text-slate-600">Initial adoption snapshot is live. Expanded trend views are next.</p>
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                      <Metric label="Events (30d)" value={adoption.events_30d} />
                      <Metric label="Unique Users (30d)" value={adoption.unique_users_30d} />
                      <Metric label="Unique Campaigns (30d)" value={adoption.unique_campaigns_30d} />
                      <Metric label="Chat Users (30d)" value={adoption.chat_users_30d} />
                      <Metric label="Report Users (30d)" value={adoption.report_users_30d} />
                    </div>
                    <div className="rounded-lg border border-slate-200 bg-white p-4">
                      <p className="mb-2 text-sm font-semibold">User Activity Drilldown ({timeframeLabel})</p>
                      <SimpleTable
                        emptyLabel="No user activity data."
                        columns={["User", "Events", "Chats", "Reports"]}
                        rows={adoption.user_activity_30d.map((row) => [
                          renderUserLabel(asString(row.user_label, row.user_id), row.user_id, row.user_secondary),
                          String(row.events),
                          String(row.chats),
                          String(row.reports),
                        ])}
                      />
                    </div>
                  </div>
                )}

                {activeTab === "workflow" && (
                  <div className="space-y-3">
                    <p className="text-sm text-slate-600">Workflow health shell is active with core operational indicators.</p>
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                      <Metric label={`Access Requests (${timeframeLabel})`} value={workflow.access_requests_24h} />
                      <Metric label={`Access Decisions (${timeframeLabel})`} value={workflow.access_decisions_24h} />
                      <Metric label={`Preview Failures (${timeframeLabel})`} value={workflow.preview_failures_24h} />
                      <Metric label="Pending Access Requests" value={workflow.pending_access_requests} />
                      <Metric label="Overdue Deadlines" value={workflow.overdue_deadlines} />
                    </div>
                    <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
                      <div className="rounded-lg border border-slate-200 bg-white p-4">
                        <p className="mb-2 text-sm font-semibold">Pending Access Requests</p>
                        <SimpleTable
                          emptyLabel="No pending access requests."
                          columns={["Campaign", "User", "Created"]}
                          rows={workflow.pending_access_request_list.map((row) => [
                            renderCampaignLabel(row.campaign_name, row.campaign_id, row.campaign_secondary),
                            renderUserLabel(asString(row.user_label, row.user_id), row.user_id, row.user_secondary),
                            safeDateTime(row.created_at),
                          ])}
                        />
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-white p-4">
                        <p className="mb-2 text-sm font-semibold">Overdue Deadlines</p>
                        <SimpleTable
                          emptyLabel="No overdue deadlines."
                          columns={["Campaign", "Title", "Due"]}
                          rows={workflow.overdue_deadline_list.map((row) => [
                            renderCampaignLabel(row.campaign_name, row.campaign_id, row.campaign_secondary),
                            row.title,
                            safeDateTime(row.due_at),
                          ])}
                        />
                      </div>
                      <div className="rounded-lg border border-slate-200 bg-white p-4">
                        <p className="mb-2 text-sm font-semibold">Stale Assignments</p>
                        <SimpleTable
                          emptyLabel="No stale assignments."
                          columns={["Campaign", "Title", "Assignee"]}
                          rows={workflow.stale_assignment_list.map((row) => [
                            renderCampaignLabel(row.campaign_name, row.campaign_id, row.campaign_secondary),
                            row.title,
                            row.assigned_user_id
                              ? renderUserLabel(
                                  asString(row.assigned_user_label, row.assigned_user_id),
                                  row.assigned_user_id,
                                  row.assigned_user_secondary
                                )
                              : "-",
                          ])}
                        />
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </section>
          </>
        )}
      </div>
    </div>
  );
}

function Metric({ label, value, onClick }: { label: string; value: unknown; onClick?: () => void }) {
  const safeValue = normalizeMetricValue(value);
  const content = (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</p>
      <p className="mt-1 text-xl font-semibold text-slate-900">{safeValue}</p>
    </div>
  );
  if (!onClick) {
    return content;
  }
  return (
    <button type="button" onClick={onClick} className="w-full text-left">
      {content}
    </button>
  );
}

function LineChartCard({
  title,
  series,
}: {
  title: string;
  series: Array<{ name: string; color: string; points: Array<{ x: string; y: number }> }>;
}) {
  const width = 540;
  const height = 220;
  const padding = 28;
  const allPoints = series.flatMap((s) => s.points);
  const maxY = Math.max(1, ...allPoints.map((p) => asNumber(p.y)));
  const count = Math.max(1, Math.max(...series.map((s) => s.points.length)));

  const buildPath = (points: Array<{ x: string; y: number }>) =>
    points
      .map((point, idx) => {
        const x = padding + ((width - padding * 2) * idx) / Math.max(1, count - 1);
        const y = height - padding - ((height - padding * 2) * asNumber(point.y)) / maxY;
        return `${idx === 0 ? "M" : "L"} ${x} ${y}`;
      })
      .join(" ");

  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <p className="mb-2 text-sm font-semibold">{title}</p>
      {allPoints.length ? (
        <>
          <svg viewBox={`0 0 ${width} ${height}`} className="h-52 w-full overflow-visible">
            <line x1={padding} y1={height - padding} x2={width - padding} y2={height - padding} stroke="#cbd5e1" strokeWidth="1" />
            <line x1={padding} y1={padding} x2={padding} y2={height - padding} stroke="#cbd5e1" strokeWidth="1" />
            {series.map((line) => (
              <path key={line.name} d={buildPath(line.points)} fill="none" stroke={line.color} strokeWidth="2.5" />
            ))}
          </svg>
          <div className="mt-2 flex flex-wrap gap-3">
            {series.map((line) => (
              <span key={line.name} className="inline-flex items-center gap-1 text-xs text-slate-600">
                <span className="h-2 w-2 rounded-full" style={{ backgroundColor: line.color }} />
                {line.name}
              </span>
            ))}
          </div>
        </>
      ) : (
        <p className="text-sm text-slate-500">No time-series data available.</p>
      )}
    </div>
  );
}

function SimpleTable({
  columns,
  rows,
  emptyLabel,
}: {
  columns: string[];
  rows: Array<Array<string | ReactNode>>;
  emptyLabel: string;
}) {
  if (!rows.length) {
    return <p className="text-sm text-slate-500">{emptyLabel}</p>;
  }
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full border-collapse text-sm">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column} className="border-b border-slate-200 px-2 py-1 text-left text-xs uppercase tracking-wide text-slate-500">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={`row-${idx}`} className="border-b border-slate-100 last:border-b-0">
              {row.map((cell, cellIdx) => (
                <td key={`cell-${idx}-${cellIdx}`} className="px-2 py-1.5 text-slate-700">
                  {cell}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

