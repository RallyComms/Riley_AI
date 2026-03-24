"use client";

import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import { useAuth } from "@clerk/nextjs";
import { Activity, AlertTriangle, BarChart3, ChevronRight, DollarSign, Gauge, ShieldCheck, Users } from "lucide-react";
import { apiFetch, ApiRequestError } from "@app/lib/api";

type MissionControlTab =
  | "overview"
  | "performance"
  | "cost"
  | "system"
  | "adoption"
  | "workflow";

type OverviewData = {
  active_users_7d: number;
  active_campaigns_7d: number;
  chats_today: number;
  reports_today: number;
  avg_response_latency_ms: number;
  report_success_rate: number;
  current_month_estimated_cost: number;
  forecast_month_end_cost: number;
  pending_access_requests: number;
  overdue_deadlines: number;
  failed_reports_24h: number;
};

type PerformanceData = {
  avg_latency_ms_7d: number;
  p95_latency_ms_7d: number;
  provider_fallback_successes_7d: number;
  provider_fallback_failures_7d: number;
  reranker_failures_7d: number;
};

type CostData = {
  month_estimated_cost: number;
  last_7d_estimated_cost: number;
  provider_cost_30d: Array<{ provider: string; cost: number }>;
};

type AdoptionData = {
  events_30d: number;
  unique_users_30d: number;
  unique_campaigns_30d: number;
  chat_users_30d: number;
  report_users_30d: number;
};

type WorkflowData = {
  access_requests_24h: number;
  access_decisions_24h: number;
  deadline_reminders_24h: number;
  preview_failures_24h: number;
  pending_access_requests: number;
  overdue_deadlines: number;
};

type SystemData = {
  auth_denied_24h: number;
  worker_failures_24h: number;
  report_failures_24h: number;
  ingestion_failures_24h: number;
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
};

const defaultPerformance: PerformanceData = {
  avg_latency_ms_7d: 0,
  p95_latency_ms_7d: 0,
  provider_fallback_successes_7d: 0,
  provider_fallback_failures_7d: 0,
  reranker_failures_7d: 0,
};

const defaultCost: CostData = {
  month_estimated_cost: 0,
  last_7d_estimated_cost: 0,
  provider_cost_30d: [],
};

const defaultAdoption: AdoptionData = {
  events_30d: 0,
  unique_users_30d: 0,
  unique_campaigns_30d: 0,
  chat_users_30d: 0,
  report_users_30d: 0,
};

const defaultWorkflow: WorkflowData = {
  access_requests_24h: 0,
  access_decisions_24h: 0,
  deadline_reminders_24h: 0,
  preview_failures_24h: 0,
  pending_access_requests: 0,
  overdue_deadlines: 0,
};

const defaultSystem: SystemData = {
  auth_denied_24h: 0,
  worker_failures_24h: 0,
  report_failures_24h: 0,
  ingestion_failures_24h: 0,
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
  return {
    active_users_7d: asNumber(src.active_users_7d),
    active_campaigns_7d: asNumber(src.active_campaigns_7d),
    chats_today: asNumber(src.chats_today),
    reports_today: asNumber(src.reports_today),
    avg_response_latency_ms: asNumber(src.avg_response_latency_ms),
    report_success_rate: asNumber(src.report_success_rate),
    current_month_estimated_cost: asNumber(src.current_month_estimated_cost),
    forecast_month_end_cost: asNumber(src.forecast_month_end_cost),
    pending_access_requests: asNumber(src.pending_access_requests),
    overdue_deadlines: asNumber(src.overdue_deadlines),
    failed_reports_24h: asNumber(src.failed_reports_24h),
  };
}

function normalizePerformance(raw: unknown): PerformanceData {
  const src = asRecord(raw);
  return {
    avg_latency_ms_7d: asNumber(src.avg_latency_ms_7d),
    p95_latency_ms_7d: asNumber(src.p95_latency_ms_7d),
    provider_fallback_successes_7d: asNumber(src.provider_fallback_successes_7d),
    provider_fallback_failures_7d: asNumber(src.provider_fallback_failures_7d),
    reranker_failures_7d: asNumber(src.reranker_failures_7d),
  };
}

function normalizeCost(raw: unknown): CostData {
  const src = asRecord(raw);
  const rows = asArray(src.provider_cost_30d);
  return {
    month_estimated_cost: asNumber(src.month_estimated_cost),
    last_7d_estimated_cost: asNumber(src.last_7d_estimated_cost),
    provider_cost_30d: rows.map((row, idx) => {
      const item = (row && typeof row === "object" ? row : {}) as Record<string, unknown>;
      return {
        provider: asString(item.provider, `provider_${idx + 1}`),
        cost: asNumber(item.cost),
      };
    }),
  };
}

function normalizeAdoption(raw: unknown): AdoptionData {
  const src = asRecord(raw);
  return {
    events_30d: asNumber(src.events_30d),
    unique_users_30d: asNumber(src.unique_users_30d),
    unique_campaigns_30d: asNumber(src.unique_campaigns_30d),
    chat_users_30d: asNumber(src.chat_users_30d),
    report_users_30d: asNumber(src.report_users_30d),
  };
}

function normalizeWorkflow(raw: unknown): WorkflowData {
  const src = asRecord(raw);
  return {
    access_requests_24h: asNumber(src.access_requests_24h),
    access_decisions_24h: asNumber(src.access_decisions_24h),
    deadline_reminders_24h: asNumber(src.deadline_reminders_24h),
    preview_failures_24h: asNumber(src.preview_failures_24h),
    pending_access_requests: asNumber(src.pending_access_requests),
    overdue_deadlines: asNumber(src.overdue_deadlines),
  };
}

function normalizeSystem(raw: unknown): SystemData {
  const src = asRecord(raw);
  return {
    auth_denied_24h: asNumber(src.auth_denied_24h),
    worker_failures_24h: asNumber(src.worker_failures_24h),
    report_failures_24h: asNumber(src.report_failures_24h),
    ingestion_failures_24h: asNumber(src.ingestion_failures_24h),
  };
}

export default function MissionControlPage() {
  const { getToken, isLoaded } = useAuth();
  const [activeTab, setActiveTab] = useState<MissionControlTab>("overview");
  const [isLoading, setIsLoading] = useState(true);
  const [accessDenied, setAccessDenied] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [overview, setOverview] = useState<OverviewData>(defaultOverview);
  const [performance, setPerformance] = useState<PerformanceData>(defaultPerformance);
  const [cost, setCost] = useState<CostData>(defaultCost);
  const [adoption, setAdoption] = useState<AdoptionData>(defaultAdoption);
  const [workflow, setWorkflow] = useState<WorkflowData>(defaultWorkflow);
  const [system, setSystem] = useState<SystemData>(defaultSystem);

  const fetchMissionControl = async () => {
    if (!isLoaded) return;
    try {
      setIsLoading(true);
      setError(null);
      setAccessDenied(false);
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required.");
      }

      const settled = await Promise.allSettled([
        apiFetch<OverviewData>("/api/v1/mission-control/overview", { token, method: "GET" }),
        apiFetch<PerformanceData>("/api/v1/mission-control/riley-performance", { token, method: "GET" }),
        apiFetch<CostData>("/api/v1/mission-control/cost-summary", { token, method: "GET" }),
        apiFetch<AdoptionData>("/api/v1/mission-control/adoption-summary", { token, method: "GET" }),
        apiFetch<WorkflowData>("/api/v1/mission-control/workflow-health-summary", { token, method: "GET" }),
        apiFetch<SystemData>("/api/v1/mission-control/system-health-summary", { token, method: "GET" }),
      ]);

      const denied = settled.some(
        (result) => result.status === "rejected" && result.reason instanceof ApiRequestError && result.reason.status === 403
      );
      if (denied) {
        setAccessDenied(true);
        return;
      }

      const getValue = <T,>(index: number, fallback: T): T => {
        const result = settled[index];
        return result.status === "fulfilled" ? (result.value as T) : fallback;
      };

      setOverview(normalizeOverview(getValue(0, defaultOverview)));
      setPerformance(normalizePerformance(getValue(1, defaultPerformance)));
      setCost(normalizeCost(getValue(2, defaultCost)));
      setAdoption(normalizeAdoption(getValue(3, defaultAdoption)));
      setWorkflow(normalizeWorkflow(getValue(4, defaultWorkflow)));
      setSystem(normalizeSystem(getValue(5, defaultSystem)));
    } catch (err) {
      if (err instanceof ApiRequestError && err.status === 403) {
        setAccessDenied(true);
        return;
      }
      setError(err instanceof Error ? err.message : "Failed to load Mission Control.");
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    void fetchMissionControl();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoaded]);

  const topKpis = useMemo(() => {
    return [
      { label: "Active Users (7d)", value: String(overview.active_users_7d), icon: Users },
      { label: "Active Campaigns (7d)", value: String(overview.active_campaigns_7d), icon: Activity },
      { label: "Report Success Rate", value: `${overview.report_success_rate}%`, icon: ShieldCheck },
      { label: "Month Est. Cost", value: `$${asNumber(overview.current_month_estimated_cost).toFixed(2)}`, icon: DollarSign },
    ];
  }, [overview]);

  return (
    <div className="min-h-screen bg-slate-100 text-slate-900">
      <div className="mx-auto max-w-7xl px-6 py-8">
        <header className="mb-8 flex items-center justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-slate-500">Admin Surface</p>
            <h1 className="mt-2 text-4xl font-semibold tracking-tight">Mission Control</h1>
            <p className="mt-2 text-sm text-slate-600">Executive analytics and platform health for Riley.</p>
          </div>
          <div className="flex items-center gap-3">
            <button
              type="button"
              onClick={() => void fetchMissionControl()}
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

        {isLoading ? (
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
                    <p className="text-2xl font-semibold">{kpi.value}</p>
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
                {activeTab === "overview" && (
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                    <Metric label="Chats Today" value={overview.chats_today} />
                    <Metric label="Reports Today" value={overview.reports_today} />
                    <Metric label="Avg Response Latency (ms)" value={overview.avg_response_latency_ms} />
                    <Metric label="Forecast Month-End Cost" value={`$${asNumber(overview.forecast_month_end_cost).toFixed(2)}`} />
                    <Metric label="Pending Access Requests" value={overview.pending_access_requests} />
                    <Metric label="Overdue Deadlines" value={overview.overdue_deadlines} />
                    <Metric label="Failed Reports (24h)" value={overview.failed_reports_24h} />
                  </div>
                )}

                {activeTab === "performance" && (
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                    <Metric label="Avg Latency (7d, ms)" value={performance.avg_latency_ms_7d} />
                    <Metric label="P95 Latency (7d, ms)" value={performance.p95_latency_ms_7d} />
                    <Metric label="Fallback Successes (7d)" value={performance.provider_fallback_successes_7d} />
                    <Metric label="Fallback Failures (7d)" value={performance.provider_fallback_failures_7d} />
                    <Metric label="Reranker Failures (7d)" value={performance.reranker_failures_7d} />
                  </div>
                )}

                {activeTab === "cost" && (
                  <div className="space-y-4">
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                      <Metric label="Month Estimated Cost" value={`$${asNumber(cost.month_estimated_cost).toFixed(2)}`} />
                      <Metric label="Last 7d Estimated Cost" value={`$${asNumber(cost.last_7d_estimated_cost).toFixed(2)}`} />
                    </div>
                    <div className="rounded-lg border border-slate-200 bg-white p-4">
                      <div className="mb-2 flex items-center gap-2">
                        <BarChart3 className="h-4 w-4 text-slate-500" />
                        <p className="text-sm font-semibold">Provider Cost Breakdown (30d)</p>
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
                  </div>
                )}

                {activeTab === "system" && (
                  <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-4">
                    <Metric label="Auth Denied (24h)" value={system.auth_denied_24h} />
                    <Metric label="Worker Failures (24h)" value={system.worker_failures_24h} />
                    <Metric label="Report Failures (24h)" value={system.report_failures_24h} />
                    <Metric label="Ingestion Failures (24h)" value={system.ingestion_failures_24h} />
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
                  </div>
                )}

                {activeTab === "workflow" && (
                  <div className="space-y-3">
                    <p className="text-sm text-slate-600">Workflow health shell is active with core operational indicators.</p>
                    <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
                      <Metric label="Access Requests (24h)" value={workflow.access_requests_24h} />
                      <Metric label="Access Decisions (24h)" value={workflow.access_decisions_24h} />
                      <Metric label="Deadline Reminders (24h)" value={workflow.deadline_reminders_24h} />
                      <Metric label="Preview Failures (24h)" value={workflow.preview_failures_24h} />
                      <Metric label="Pending Access Requests" value={workflow.pending_access_requests} />
                      <Metric label="Overdue Deadlines" value={workflow.overdue_deadlines} />
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

function Metric({ label, value }: { label: string; value: unknown }) {
  const safeValue = normalizeMetricValue(value);
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <p className="text-xs font-medium uppercase tracking-wide text-slate-500">{label}</p>
      <p className="mt-1 text-xl font-semibold text-slate-900">{safeValue}</p>
    </div>
  );
}

