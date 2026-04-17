"use client";

import { useEffect, useMemo, useState } from "react";
import { useAuth, useUser, UserButton } from "@clerk/nextjs";
import { usePathname, useRouter } from "next/navigation";
import {
  BarChart3,
  BookOpen,
  ChevronLeft,
  ChevronRight,
  Compass,
  Globe,
  LayoutDashboard,
  Plus,
  Shield,
} from "lucide-react";
import { CreateCampaignModal } from "@app/components/dashboard/CreateCampaignModal";
import { GlobalNotificationBell } from "@app/components/layout/GlobalNotificationBell";
import { apiFetch } from "@app/lib/api";

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

const SIDEBAR_COLLAPSED_KEY = "riley_global_sidebar_collapsed";

function navItemClass(active: boolean, collapsed: boolean): string {
  const base = "group flex w-full items-center rounded-xl border transition-all duration-150";
  const spacing = collapsed
    ? "mx-auto h-11 w-11 justify-center px-0 py-0"
    : "gap-3 px-3.5 py-3 text-[15px] font-semibold";
  if (active) {
    return `${base} ${spacing} border-[#d8c289] bg-[#efe4c8] text-[#1f2a44]`;
  }
  return `${base} ${spacing} border-transparent text-[#4c5a74] hover:border-[#e3d8c2] hover:bg-[#ece4d3] hover:text-[#1f2a44]`;
}

export function GlobalShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const { getToken, isLoaded } = useAuth();
  const { user } = useUser();

  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [isSidebarCollapsed, setIsSidebarCollapsed] = useState(false);
  const [canAccessMissionControl, setCanAccessMissionControl] = useState(false);

  useEffect(() => {
    try {
      const stored = window.localStorage.getItem(SIDEBAR_COLLAPSED_KEY);
      if (stored === "1") setIsSidebarCollapsed(true);
      if (stored === "0") setIsSidebarCollapsed(false);
    } catch {
      // no-op
    }
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem(SIDEBAR_COLLAPSED_KEY, isSidebarCollapsed ? "1" : "0");
    } catch {
      // no-op
    }
  }, [isSidebarCollapsed]);

  useEffect(() => {
    let cancelled = false;

    async function checkMissionControlAccess() {
      if (!isLoaded || !user?.id) return;
      try {
        const token = await getToken();
        if (!token) return;
        const result = await apiFetch<{ allowed?: boolean }>("/api/v1/mission-control/access", {
          token,
          method: "GET",
        });
        if (!cancelled) setCanAccessMissionControl(Boolean(result?.allowed));
      } catch {
        if (!cancelled) setCanAccessMissionControl(false);
      }
    }

    void checkMissionControlAccess();
    return () => {
      cancelled = true;
    };
  }, [getToken, isLoaded, user?.id]);

  const routeState = useMemo(
    () => ({
      campaigns: pathname === "/",
      knowledgebase: pathname.startsWith("/knowledgebase") || pathname.startsWith("/firm-directory"),
      riley: pathname.startsWith("/riley"),
      insights: pathname.startsWith("/insights-intelligence"),
      missionControl: pathname.startsWith("/mission-control"),
    }),
    [pathname],
  );

  const sidebarWidthClass = isSidebarCollapsed ? "w-24" : "w-72";
  const navIconClass = isSidebarCollapsed ? "h-5 w-5 shrink-0" : "h-[18px] w-[18px] shrink-0";

  return (
    <div className="min-h-screen bg-[#f6f1e7] text-[#1f2a44]">
      <div className="flex min-h-screen">
        <aside
          className={`shrink-0 border-r border-[#e1d7c4] bg-[#f1ebde] transition-[width] duration-200 ${sidebarWidthClass}`}
        >
          <div className="border-b border-[#e1d7c4] px-4 py-5">
            <div
              className={`flex ${
                isSidebarCollapsed ? "flex-col items-center gap-3" : "items-start justify-between gap-3"
              }`}
            >
              {isSidebarCollapsed ? (
                <div className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-[#dccaa4] bg-[#ecdfc6] text-base font-semibold text-[#2b3650]">
                  R
                </div>
              ) : (
                <div>
                  <p className="text-[13px] font-bold uppercase tracking-[0.2em] text-[#cfad5b]">
                    We Are Rally
                  </p>
                  <p className="mt-1 text-[28px] font-semibold leading-none tracking-tight text-[#25314a]">Riley</p>
                </div>
              )}
              <button
                type="button"
                onClick={() => setIsSidebarCollapsed((prev) => !prev)}
                className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-[#d9cfbb] bg-white text-[#5c6881] transition-colors hover:bg-[#f6f1e6] hover:text-[#1f2a44]"
                aria-label={isSidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
                title={isSidebarCollapsed ? "Expand sidebar" : "Collapse sidebar"}
              >
                {isSidebarCollapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
              </button>
            </div>
          </div>

          <nav className="space-y-1 px-3 py-4">
            <button
              type="button"
              onClick={() => router.push("/")}
              className={navItemClass(routeState.campaigns, isSidebarCollapsed)}
              title="Campaigns"
            >
              <LayoutDashboard className={navIconClass} />
              {!isSidebarCollapsed ? <span>Campaigns</span> : null}
            </button>
            <button
              type="button"
              onClick={() => setIsCreateModalOpen(true)}
              className={navItemClass(isCreateModalOpen, isSidebarCollapsed)}
              title="New Mission"
            >
              <Plus className={navIconClass} />
              {!isSidebarCollapsed ? <span>New Mission</span> : null}
            </button>
            <button
              type="button"
              onClick={() => router.push("/knowledgebase")}
              className={navItemClass(routeState.knowledgebase, isSidebarCollapsed)}
              title="Firm Knowledgebase"
            >
              <BookOpen className={navIconClass} />
              {!isSidebarCollapsed ? <span>Firm Knowledgebase</span> : null}
            </button>
            <button
              type="button"
              onClick={() => router.push("/riley")}
              className={navItemClass(routeState.riley, isSidebarCollapsed)}
              title="Riley Global"
            >
              <Globe className={navIconClass} />
              {!isSidebarCollapsed ? <span>Riley Global</span> : null}
            </button>
            <button
              type="button"
              onClick={() => router.push("/insights-intelligence")}
              className={navItemClass(routeState.insights, isSidebarCollapsed)}
              title="Insights & Intelligence"
            >
              <Compass className={navIconClass} />
              {!isSidebarCollapsed ? <span>Insights & Intelligence</span> : null}
            </button>
            {canAccessMissionControl ? (
              <button
                type="button"
                onClick={() => router.push("/mission-control")}
                className={navItemClass(routeState.missionControl, isSidebarCollapsed)}
                title="Mission Control"
              >
                <BarChart3 className={navIconClass} />
                {!isSidebarCollapsed ? (
                  <>
                    <span>Mission Control</span>
                    <Shield className="ml-auto h-3.5 w-3.5 text-[#7b869c]" />
                  </>
                ) : null}
              </button>
            ) : null}
          </nav>
        </aside>

        <div className="flex min-w-0 flex-1 flex-col">
          <header className="border-b border-[#e6dece] bg-[#f8f4eb]">
            <div className="flex items-center justify-end gap-3 px-8 py-5">
              <GlobalNotificationBell />
              <div className="flex h-9 w-9 items-center justify-center overflow-hidden rounded-full border border-[#d7cfbf] bg-white shadow-[0_1px_1px_rgba(31,42,68,0.06)]">
                <UserButton appearance={{ elements: { userButtonAvatarBox: "h-9 w-9" } }} />
              </div>
            </div>
          </header>

          <main className="flex-1 overflow-y-auto px-6 pb-8 pt-6 lg:px-8">{children}</main>
        </div>
      </div>

      <CreateCampaignModal
        isOpen={isCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onCampaignCreated={(campaign: CampaignBucket) => {
          setIsCreateModalOpen(false);
          router.push(`/campaign/${campaign.campaignId}`);
        }}
      />
    </div>
  );
}
