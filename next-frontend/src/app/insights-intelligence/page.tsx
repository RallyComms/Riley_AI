"use client";

import { useEffect, useMemo, useState } from "react";
import { useAuth, UserButton } from "@clerk/nextjs";
import { usePathname, useRouter } from "next/navigation";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  BookOpen,
  Compass,
  Globe,
  LayoutDashboard,
  Plus,
  Radar,
  Radio,
  Shield,
  Sparkles,
  TrendingUp,
} from "lucide-react";
import { CreateCampaignModal } from "@app/components/dashboard/CreateCampaignModal";
import { CampaignDirectory } from "@app/components/dashboard/CampaignDirectory";
import { GlobalNotificationBell } from "@app/components/layout/GlobalNotificationBell";
import { apiFetch } from "@app/lib/api";

type InsightsTab = "overview" | "signals" | "campaigns" | "themes";
type HorizonOption = "24h" | "7d" | "30d";

interface CampaignBucket {
  name: string;
  role: string;
  lastActive: string;
  mentions: number;
  pendingRequests: number;
  userRole: "Lead" | "Member";
  themeColor: string;
  campaignId: string;
}

interface MomentumSnapshot {
  campaignId: string;
  campaignName: string;
  momentum: number;
  volatility: number;
  signalDelta: number;
  strategicNeed: "Watch" | "Stabilize" | "Accelerate";
  riskLabel: "Low" | "Medium" | "High";
}

interface SignalCategory {
  name: string;
  coverage: number;
  activeWatches: number;
  movement: string;
}

interface LiveWatch {
  id: string;
  campaignName: string;
  source: string;
  status: "Escalating" | "New" | "Tracking";
  minutesAgo: number;
  summary: string;
}

interface ThemePulse {
  name: string;
  confidence: "High" | "Medium";
  coverage: string;
  implication: string;
}

const tabs: Array<{ id: InsightsTab; label: string }> = [
  { id: "overview", label: "Momentum Overview" },
  { id: "signals", label: "Signals Watch" },
  { id: "campaigns", label: "Campaign Compare" },
  { id: "themes", label: "Themes & Focus" },
];

const momentumSnapshots: MomentumSnapshot[] = [
  {
    campaignId: "cmp-1",
    campaignName: "Grid Reliability Coalition",
    momentum: 78,
    volatility: 64,
    signalDelta: 14,
    strategicNeed: "Stabilize",
    riskLabel: "High",
  },
  {
    campaignId: "cmp-2",
    campaignName: "Affordable Mobility Alliance",
    momentum: 69,
    volatility: 40,
    signalDelta: 8,
    strategicNeed: "Accelerate",
    riskLabel: "Medium",
  },
  {
    campaignId: "cmp-3",
    campaignName: "Water Equity Initiative",
    momentum: 56,
    volatility: 72,
    signalDelta: -6,
    strategicNeed: "Stabilize",
    riskLabel: "High",
  },
  {
    campaignId: "cmp-4",
    campaignName: "Manufacturing Growth Compact",
    momentum: 63,
    volatility: 31,
    signalDelta: 5,
    strategicNeed: "Watch",
    riskLabel: "Low",
  },
];

const signalCategories: SignalCategory[] = [
  { name: "Narrative Movement", coverage: 84, activeWatches: 18, movement: "+6 this week" },
  { name: "Policy / Docket Signals", coverage: 71, activeWatches: 11, movement: "+2 this week" },
  { name: "Audience Anxiety Indicators", coverage: 67, activeWatches: 14, movement: "+4 this week" },
  { name: "Competitive Counter-Messaging", coverage: 59, activeWatches: 9, movement: "+1 this week" },
];

const liveWatches: LiveWatch[] = [
  {
    id: "watch-1",
    campaignName: "Grid Reliability Coalition",
    source: "Regional TV + Press Monitor",
    status: "Escalating",
    minutesAgo: 8,
    summary: "Evening cycle increases skepticism around delivery timelines in two priority markets.",
  },
  {
    id: "watch-2",
    campaignName: "Affordable Mobility Alliance",
    source: "Community Listening Stream",
    status: "New",
    minutesAgo: 16,
    summary: "Transit affordability language now outperforms climate-first language in volunteer channels.",
  },
  {
    id: "watch-3",
    campaignName: "Water Equity Initiative",
    source: "Legislative Calendar Watch",
    status: "Tracking",
    minutesAgo: 28,
    summary: "Committee hearing moved up by six days, compressing prep windows for coalition partners.",
  },
  {
    id: "watch-4",
    campaignName: "Manufacturing Growth Compact",
    source: "Stakeholder Sentiment Agent",
    status: "Tracking",
    minutesAgo: 35,
    summary: "Business leaders remain favorable, but confidence softens around workforce execution language.",
  },
];

const themePulses: ThemePulse[] = [
  {
    name: "Cost-of-living pressure framing",
    confidence: "High",
    coverage: "3 campaigns / 5 streams",
    implication: "Elevate concrete household outcomes in next 72-hour message cycle.",
  },
  {
    name: "Institutional trust volatility",
    confidence: "Medium",
    coverage: "2 campaigns / 4 streams",
    implication: "Deploy local validators before major media windows.",
  },
  {
    name: "Timeline skepticism",
    confidence: "High",
    coverage: "2 campaigns / 3 streams",
    implication: "Publish milestone proof points and operational checkpoints.",
  },
];

const strategicFocusAreas = [
  "Launch a shared rapid-response brief for campaigns with high volatility and rising signal volume.",
  "Prioritize spokesperson prep for campaigns showing momentum growth with trust-sensitive narratives.",
  "Align policy and communications teams on accelerated hearing timelines now appearing across watch streams.",
  "Run cross-campaign message variant test focused on concrete near-term outcomes.",
];

function sidebarItemClass(active: boolean) {
  if (active) {
    return "flex w-full items-center gap-2.5 rounded-xl border border-[#d5c291] bg-[#efe4c8] px-3 py-2 text-sm font-semibold text-[#1f2a44] shadow-[inset_0_1px_0_rgba(255,255,255,0.4)]";
  }
  return "flex w-full items-center gap-2.5 rounded-xl px-3 py-2 text-sm font-medium text-[#49566f] transition-colors hover:bg-[#e7ddca] hover:text-[#1f2a44]";
}

function riskPillClass(risk: MomentumSnapshot["riskLabel"]) {
  if (risk === "High") return "border-[#f1b2ab] bg-[#fdf2f1] text-[#9e3a32]";
  if (risk === "Medium") return "border-[#f2d5a7] bg-[#fff7eb] text-[#8a5a16]";
  return "border-[#c6dfcf] bg-[#edf7f1] text-[#2f7047]";
}

function watchStatusClass(status: LiveWatch["status"]) {
  if (status === "Escalating") return "border-[#f1b2ab] bg-[#fdf2f1] text-[#9e3a32]";
  if (status === "New") return "border-[#c9d6f0] bg-[#eff4ff] text-[#2f4f88]";
  return "border-[#d9d4c7] bg-[#f7f3ea] text-[#5f6470]";
}

export default function InsightsIntelligencePage() {
  const router = useRouter();
  const pathname = usePathname();
  const { getToken } = useAuth();

  const [canAccessMissionControl, setCanAccessMissionControl] = useState(false);
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [isDirectoryOpen, setIsDirectoryOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<InsightsTab>("overview");
  const [horizon, setHorizon] = useState<HorizonOption>("7d");

  const isHomeRoute = pathname === "/";
  const isRileyRoute = pathname.startsWith("/riley");
  const isInsightsRoute = pathname.startsWith("/insights-intelligence");
  const isMissionControlRoute = pathname.startsWith("/mission-control");

  useEffect(() => {
    let active = true;
    const checkMissionControlAccess = async () => {
      try {
        const token = await getToken();
        if (!token || !active) {
          if (active) setCanAccessMissionControl(false);
          return;
        }
        await apiFetch("/api/v1/mission-control/access", { token, method: "GET" });
        if (active) setCanAccessMissionControl(true);
      } catch {
        if (active) setCanAccessMissionControl(false);
      }
    };
    void checkMissionControlAccess();
    return () => {
      active = false;
    };
  }, [getToken]);

  const overviewMetrics = useMemo(() => {
    const totalCampaigns = momentumSnapshots.length;
    const highRiskCampaigns = momentumSnapshots.filter((entry) => entry.riskLabel === "High").length;
    const avgMomentum = Math.round(
      momentumSnapshots.reduce((acc, entry) => acc + entry.momentum, 0) / totalCampaigns,
    );
    const avgVolatility = Math.round(
      momentumSnapshots.reduce((acc, entry) => acc + entry.volatility, 0) / totalCampaigns,
    );
    return {
      totalCampaigns,
      highRiskCampaigns,
      avgMomentum,
      avgVolatility,
    };
  }, []);

  const handleRequestAccess = async (campaignId: string, message?: string) => {
    const token = await getToken();
    if (!token) throw new Error("No authentication token available");
    await apiFetch(`/api/v1/campaigns/${encodeURIComponent(campaignId)}/access-request`, {
      token,
      method: "POST",
      body: {
        justification: message?.trim() || undefined,
      },
    });
  };

  return (
    <div className="min-h-screen bg-[#f6f1e7] text-[#1f2a44]">
      <div className="flex min-h-screen">
        <aside className="w-72 shrink-0 border-r border-[#e1d7c4] bg-[#f1ebde]">
          <div className="border-b border-[#e1d7c4] px-6 py-6">
            <div className="flex items-center gap-2">
              <span className="inline-flex h-8 w-8 items-center justify-center rounded-lg bg-[#1f2a44] text-xs font-semibold text-white">
                R
              </span>
              <div>
                <p className="text-sm font-semibold text-[#1f2a44]">Riley</p>
                <p className="text-xs text-[#7b869c]">Global Operations</p>
              </div>
            </div>
          </div>
          <nav className="space-y-1 px-3 py-4">
            <button type="button" onClick={() => router.push("/")} className={sidebarItemClass(isHomeRoute)}>
              <LayoutDashboard className="h-4 w-4" />
              Campaigns
            </button>
            <button type="button" onClick={() => setIsCreateModalOpen(true)} className={sidebarItemClass(false)}>
              <Plus className="h-4 w-4" />
              New Mission
            </button>
            <button type="button" onClick={() => setIsDirectoryOpen(true)} className={sidebarItemClass(false)}>
              <BookOpen className="h-4 w-4" />
              Firm Knowledgebase
            </button>
            <button type="button" onClick={() => router.push("/riley")} className={sidebarItemClass(isRileyRoute)}>
              <Globe className="h-4 w-4" />
              Riley Global
            </button>
            <button type="button" onClick={() => router.push("/insights-intelligence")} className={sidebarItemClass(isInsightsRoute)}>
              <Compass className="h-4 w-4" />
              Insights & Intelligence
            </button>
            {canAccessMissionControl ? (
              <button
                type="button"
                onClick={() => router.push("/mission-control")}
                className={sidebarItemClass(isMissionControlRoute)}
              >
                <BarChart3 className="h-4 w-4" />
                Mission Control
                <Shield className="ml-auto h-3.5 w-3.5 text-[#7b869c]" />
              </button>
            ) : null}
          </nav>
        </aside>

        <div className="flex min-w-0 flex-1 flex-col">
          <header className="border-b border-[#e6dece] bg-[#f8f4eb]">
            <div className="flex items-center justify-end gap-3 px-8 py-4">
              <GlobalNotificationBell />
              <div className="flex h-8 w-8 items-center justify-center overflow-hidden rounded-full border border-[#d7cfbf] bg-white">
                <UserButton appearance={{ elements: { userButtonAvatarBox: "h-8 w-8" } }} />
              </div>
            </div>
          </header>

          <main className="flex-1 overflow-y-auto px-8 py-7">
            <section className="rounded-2xl border border-[#e2d8c4] bg-[#fbf8f1] p-6">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                  <div className="mb-2 inline-flex items-center gap-2 rounded-full border border-[#d4ad47]/40 bg-[#f4ecd7] px-2.5 py-1 text-[11px] font-semibold uppercase tracking-wide text-[#6d560f]">
                    <Sparkles className="h-3.5 w-3.5" />
                    Riley V3 Preview
                  </div>
                  <h1 className="text-2xl font-semibold tracking-tight text-[#1f2a44]">
                    Insights & Intelligence
                  </h1>
                  <p className="mt-1 max-w-3xl text-sm text-[#6f788a]">
                    Global cross-campaign intelligence surface for strategist and admin teams. Compare momentum,
                    track issue movement, and prioritize intervention where signal risk is rising.
                  </p>
                  <p className="mt-2 text-xs text-[#9a8a5b]">
                    Preview data is illustrative and designed for product framing.
                  </p>
                </div>
                <div className="inline-flex items-center gap-2 rounded-xl border border-[#ddd3bf] bg-white p-1">
                  {(["24h", "7d", "30d"] as HorizonOption[]).map((option) => (
                    <button
                      key={option}
                      type="button"
                      onClick={() => setHorizon(option)}
                      className={
                        horizon === option
                          ? "rounded-lg bg-[#efe4c8] px-3 py-1.5 text-xs font-semibold text-[#1f2a44]"
                          : "rounded-lg px-3 py-1.5 text-xs font-medium text-[#6f788a] hover:bg-[#f7f1e3]"
                      }
                    >
                      {option}
                    </button>
                  ))}
                </div>
              </div>

              <div className="mt-5 inline-flex flex-wrap items-center gap-2 rounded-xl border border-[#ddd3bf] bg-white p-1">
                {tabs.map((tab) => (
                  <button
                    key={tab.id}
                    type="button"
                    onClick={() => setActiveTab(tab.id)}
                    className={
                      activeTab === tab.id
                        ? "rounded-lg bg-[#efe4c8] px-3 py-1.5 text-xs font-semibold text-[#1f2a44]"
                        : "rounded-lg px-3 py-1.5 text-xs font-medium text-[#6f788a] hover:bg-[#f7f1e3]"
                    }
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
            </section>

            {activeTab === "overview" ? (
              <section className="mt-6 space-y-5">
                <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
                  <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4">
                    <p className="text-xs font-semibold uppercase tracking-wide text-[#6f788a]">Campaigns Monitored</p>
                    <p className="mt-1 text-2xl font-semibold text-[#1f2a44]">{overviewMetrics.totalCampaigns}</p>
                    <p className="mt-1 text-xs text-[#8a90a0]">Cross-account campaigns in this preview</p>
                  </div>
                  <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4">
                    <p className="text-xs font-semibold uppercase tracking-wide text-[#6f788a]">Average Momentum</p>
                    <p className="mt-1 text-2xl font-semibold text-[#1f2a44]">{overviewMetrics.avgMomentum}</p>
                    <p className="mt-1 text-xs text-[#8a90a0]">Composite narrative momentum score ({horizon})</p>
                  </div>
                  <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4">
                    <p className="text-xs font-semibold uppercase tracking-wide text-[#6f788a]">Average Volatility</p>
                    <p className="mt-1 text-2xl font-semibold text-[#1f2a44]">{overviewMetrics.avgVolatility}</p>
                    <p className="mt-1 text-xs text-[#8a90a0]">Signal instability across active watch streams</p>
                  </div>
                  <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4">
                    <p className="text-xs font-semibold uppercase tracking-wide text-[#6f788a]">Needs Intervention</p>
                    <p className="mt-1 text-2xl font-semibold text-[#1f2a44]">{overviewMetrics.highRiskCampaigns}</p>
                    <p className="mt-1 text-xs text-[#8a90a0]">Campaigns currently tagged as high risk</p>
                  </div>
                </div>

                <div className="grid grid-cols-1 gap-5 xl:grid-cols-3">
                  <div className="xl:col-span-2 rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-5">
                    <div className="mb-4 flex items-center justify-between">
                      <h2 className="text-sm font-semibold uppercase tracking-wide text-[#6f788a]">
                        Campaign Momentum Across Accounts
                      </h2>
                      <span className="inline-flex items-center gap-1 text-xs text-[#8a90a0]">
                        <TrendingUp className="h-3.5 w-3.5" />
                        Updated for {horizon} horizon
                      </span>
                    </div>
                    <div className="space-y-3">
                      {momentumSnapshots.map((snapshot) => (
                        <div key={snapshot.campaignId}>
                          <div className="mb-1 flex items-center justify-between gap-3">
                            <p className="truncate text-sm font-medium text-[#1f2a44]">{snapshot.campaignName}</p>
                            <p className="text-xs text-[#6f788a]">Momentum {snapshot.momentum}</p>
                          </div>
                          <div className="h-2 rounded-full bg-[#ebe3d5]">
                            <div
                              className="h-2 rounded-full bg-gradient-to-r from-[#cfa44b] to-[#8aa2d8]"
                              style={{ width: `${snapshot.momentum}%` }}
                            />
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>

                  <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-5">
                    <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-[#6f788a]">
                      Strategic Focus Queue
                    </h2>
                    <ul className="space-y-2.5">
                      {strategicFocusAreas.slice(0, 3).map((area) => (
                        <li key={area} className="flex items-start gap-2 text-sm text-[#3a455c]">
                          <span className="mt-1 inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full bg-[#efe4c8] text-[10px] font-semibold text-[#6d560f]">
                            !
                          </span>
                          <span>{area}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              </section>
            ) : null}

            {activeTab === "signals" ? (
              <section className="mt-6 space-y-5">
                <div className="grid grid-cols-1 gap-4 lg:grid-cols-2 xl:grid-cols-4">
                  {signalCategories.map((category) => (
                    <div key={category.name} className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4">
                      <p className="text-xs font-semibold uppercase tracking-wide text-[#6f788a]">{category.name}</p>
                      <p className="mt-1 text-2xl font-semibold text-[#1f2a44]">{category.coverage}%</p>
                      <p className="mt-1 text-xs text-[#8a90a0]">{category.activeWatches} active watches</p>
                      <p className="mt-2 inline-flex rounded-full border border-[#d8d0bf] bg-[#f8f3e9] px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-[#6f788a]">
                        {category.movement}
                      </p>
                    </div>
                  ))}
                </div>

                <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-5">
                  <div className="mb-4 flex items-center gap-2">
                    <Radio className="h-4 w-4 text-[#6f788a]" />
                    <h2 className="text-sm font-semibold uppercase tracking-wide text-[#6f788a]">
                      Live Signals / Active Watches
                    </h2>
                  </div>
                  <div className="space-y-3">
                    {liveWatches.map((watch) => (
                      <div key={watch.id} className="rounded-lg border border-[#ece6d9] bg-white p-3">
                        <div className="flex flex-wrap items-center justify-between gap-2">
                          <p className="text-sm font-semibold text-[#1f2a44]">{watch.campaignName}</p>
                          <div className="flex items-center gap-2">
                            <span
                              className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${watchStatusClass(watch.status)}`}
                            >
                              {watch.status}
                            </span>
                            <span className="text-[11px] text-[#8a90a0]">{watch.minutesAgo}m ago</span>
                          </div>
                        </div>
                        <p className="mt-1 text-xs text-[#8a90a0]">{watch.source}</p>
                        <p className="mt-2 text-sm text-[#49566f]">{watch.summary}</p>
                      </div>
                    ))}
                  </div>
                </div>
              </section>
            ) : null}

            {activeTab === "campaigns" ? (
              <section className="mt-6 space-y-5">
                <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-5">
                  <div className="mb-4 flex items-center gap-2">
                    <BarChart3 className="h-4 w-4 text-[#6f788a]" />
                    <h2 className="text-sm font-semibold uppercase tracking-wide text-[#6f788a]">
                      Campaign Comparison Snapshot
                    </h2>
                  </div>
                  <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
                    {momentumSnapshots.map((snapshot) => (
                      <div key={snapshot.campaignId} className="rounded-lg border border-[#ece6d9] bg-white p-4">
                        <div className="mb-2 flex items-center justify-between gap-2">
                          <p className="text-sm font-semibold text-[#1f2a44]">{snapshot.campaignName}</p>
                          <span
                            className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${riskPillClass(snapshot.riskLabel)}`}
                          >
                            {snapshot.riskLabel} risk
                          </span>
                        </div>
                        <div className="grid grid-cols-3 gap-2 text-center">
                          <div className="rounded-md bg-[#f8f3e9] p-2">
                            <p className="text-[10px] uppercase tracking-wide text-[#6f788a]">Momentum</p>
                            <p className="text-base font-semibold text-[#1f2a44]">{snapshot.momentum}</p>
                          </div>
                          <div className="rounded-md bg-[#f8f3e9] p-2">
                            <p className="text-[10px] uppercase tracking-wide text-[#6f788a]">Volatility</p>
                            <p className="text-base font-semibold text-[#1f2a44]">{snapshot.volatility}</p>
                          </div>
                          <div className="rounded-md bg-[#f8f3e9] p-2">
                            <p className="text-[10px] uppercase tracking-wide text-[#6f788a]">Signal Delta</p>
                            <p className="text-base font-semibold text-[#1f2a44]">
                              {snapshot.signalDelta > 0 ? "+" : ""}
                              {snapshot.signalDelta}
                            </p>
                          </div>
                        </div>
                        <p className="mt-3 text-xs text-[#6f788a]">
                          Recommended posture: <span className="font-semibold text-[#3b4a66]">{snapshot.strategicNeed}</span>
                        </p>
                      </div>
                    ))}
                  </div>
                </div>
              </section>
            ) : null}

            {activeTab === "themes" ? (
              <section className="mt-6 grid grid-cols-1 gap-5 xl:grid-cols-3">
                <div className="xl:col-span-2 rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-5">
                  <div className="mb-4 flex items-center gap-2">
                    <Radar className="h-4 w-4 text-[#6f788a]" />
                    <h2 className="text-sm font-semibold uppercase tracking-wide text-[#6f788a]">
                      Emerging Theme Streams
                    </h2>
                  </div>
                  <div className="space-y-3">
                    {themePulses.map((theme) => (
                      <div key={theme.name} className="rounded-lg border border-[#ece6d9] bg-white p-3">
                        <div className="mb-1 flex flex-wrap items-center justify-between gap-2">
                          <p className="text-sm font-semibold text-[#1f2a44]">{theme.name}</p>
                          <span
                            className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${
                              theme.confidence === "High"
                                ? "border-[#c6dfcf] bg-[#edf7f1] text-[#2f7047]"
                                : "border-[#f2d5a7] bg-[#fff7eb] text-[#8a5a16]"
                            }`}
                          >
                            {theme.confidence} confidence
                          </span>
                        </div>
                        <p className="text-xs text-[#8a90a0]">{theme.coverage}</p>
                        <p className="mt-2 text-sm text-[#49566f]">{theme.implication}</p>
                      </div>
                    ))}
                  </div>
                </div>

                <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-5">
                  <div className="mb-4 flex items-center gap-2">
                    <Activity className="h-4 w-4 text-[#6f788a]" />
                    <h2 className="text-sm font-semibold uppercase tracking-wide text-[#6f788a]">
                      Recommended Strategic Focus
                    </h2>
                  </div>
                  <ul className="space-y-3">
                    {strategicFocusAreas.map((item) => (
                      <li key={item} className="flex items-start gap-2 text-sm text-[#3a455c]">
                        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0 text-[#b48a2c]" />
                        <span>{item}</span>
                      </li>
                    ))}
                  </ul>
                </div>
              </section>
            ) : null}
          </main>
        </div>
      </div>

      <CreateCampaignModal
        isOpen={isCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onCampaignCreated={(campaign: CampaignBucket) => {
          router.push(`/campaign/${campaign.campaignId}`);
          setIsCreateModalOpen(false);
        }}
      />
      <CampaignDirectory
        isOpen={isDirectoryOpen}
        onClose={() => setIsDirectoryOpen(false)}
        onEnterCampaign={(campaignId) => router.push(`/campaign/${campaignId}`)}
        onRequestAccess={handleRequestAccess}
      />
    </div>
  );
}
