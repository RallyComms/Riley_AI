"use client";

import { useEffect, useMemo, useState } from "react";
import { useAuth, useUser, UserButton } from "@clerk/nextjs";
import { useRouter } from "next/navigation";
import { Archive, BookOpen, ChevronDown, Globe, Plus } from "lucide-react";
import { CreateCampaignModal } from "@app/components/dashboard/CreateCampaignModal";
import { CampaignDirectory } from "@app/components/dashboard/CampaignDirectory";
import { apiFetch } from "@app/lib/api";

interface BackendCampaign {
  id: string;
  name: string;
  description: string | null;
  role?: "Lead" | "Member";
  access: "member" | "requestable";
  status?: "active" | "archived";
  updated_at?: string | null;
  created_at?: string | null;
  owner_name?: string | null;
}

interface CampaignsResponse {
  campaigns: BackendCampaign[];
}

interface CampaignMember {
  user_id: string;
  display_name?: string;
  username?: string;
  email?: string;
}

interface CampaignMembersResponse {
  members: CampaignMember[];
}

interface TeamMeta {
  count: number;
  names: string[];
}

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

interface UserCampaign {
  name: string;
  role: string;
  lastActive: string;
  mentions: number;
  pendingRequests?: number;
  userRole?: "Lead" | "Member";
  themeColor: string;
  campaignId: string;
  access: "member" | "requestable";
  ownerName?: string;
  description?: string | null;
  createdAt?: string | null;
}

const THEME_COLORS = ["#0f766e", "#4f46e5", "#e11d48", "#059669", "#7c3aed", "#facc15"];

function mapBackendToUserCampaign(campaign: BackendCampaign, index: number): UserCampaign {
  const roleDisplay =
    campaign.role === "Lead"
      ? "Lead Strategist"
      : campaign.role === "Member"
      ? "Team Member"
      : "Member";
  return {
    name: campaign.name,
    role: roleDisplay,
    lastActive: "Recently active",
    mentions: 0,
    pendingRequests: 0,
    userRole: campaign.role,
    themeColor: THEME_COLORS[index % THEME_COLORS.length],
    campaignId: campaign.id,
    access: campaign.access,
    ownerName: campaign.owner_name ?? undefined,
    description: campaign.description,
    createdAt: campaign.updated_at ?? campaign.created_at ?? null,
  };
}

function mapUserCampaignToBucket(campaign: UserCampaign): CampaignBucket {
  return {
    name: campaign.name,
    role: campaign.role,
    lastActive: campaign.lastActive,
    mentions: campaign.mentions,
    pendingRequests: campaign.pendingRequests ?? 0,
    userRole: campaign.userRole ?? "Member",
    themeColor: campaign.themeColor,
    campaignId: campaign.campaignId,
  };
}

function relativeTime(iso?: string | null): string {
  if (!iso) return "Today";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "Today";

  const now = new Date();
  const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const startOfTargetDay = new Date(date.getFullYear(), date.getMonth(), date.getDate());
  const diffDays = Math.floor(
    (startOfToday.getTime() - startOfTargetDay.getTime()) / (1000 * 60 * 60 * 24),
  );

  if (diffDays <= 0) return "Today";
  if (diffDays === 1) return "Yesterday";
  return `${diffDays} days ago`;
}

function initials(name: string): string {
  const words = name.trim().split(/\s+/).filter(Boolean);
  if (words.length === 0) return "";
  if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
  return `${words[0][0]}${words[1][0]}`.toUpperCase();
}

function resolveUserLabel(member: CampaignMember): string {
  const displayName = String(member.display_name || "").trim();
  if (displayName) return displayName;

  const username = String(member.username || "").trim();
  if (username) return username;

  const email = String(member.email || "").trim();
  if (email) {
    const emailPrefix = email.split("@")[0]?.trim();
    if (emailPrefix) return emailPrefix;
  }

  return String(member.user_id || "").trim() || "Unknown user";
}

function getGreeting(): string {
  const hour = new Date().getHours();
  if (hour < 12) return "Good morning";
  if (hour < 17) return "Good afternoon";
  return "Good evening";
}

export default function HomePage() {
  const router = useRouter();
  const { user } = useUser();
  const { getToken, isLoaded } = useAuth();

  const [directoryCampaigns, setDirectoryCampaigns] = useState<UserCampaign[]>([]);
  const [teamMetaByCampaignId, setTeamMetaByCampaignId] = useState<Record<string, TeamMeta>>({});
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [statusFilter, setStatusFilter] = useState<"all" | "active" | "planning">("all");

  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [isDirectoryOpen, setIsDirectoryOpen] = useState(false);
  const [terminatingCampaignId, setTerminatingCampaignId] = useState<string | null>(null);
  const [archiveTarget, setArchiveTarget] = useState<{ id: string; name: string } | null>(null);
  const [isArchiving, setIsArchiving] = useState(false);
  const [campaignsVersion, setCampaignsVersion] = useState(0);

  const [canAccessMissionControl, setCanAccessMissionControl] = useState(false);

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
        if (!cancelled) {
          setCanAccessMissionControl(Boolean(result?.allowed));
        }
      } catch {
        if (!cancelled) {
          setCanAccessMissionControl(false);
        }
      }
    }
    void checkMissionControlAccess();
    return () => {
      cancelled = true;
    };
  }, [getToken, isLoaded, user?.id]);

  const refreshCampaigns = async () => {
    if (!isLoaded) return;
    if (!user?.id) {
      setDirectoryCampaigns([]);
      setTeamMetaByCampaignId({});
      setIsLoading(false);
      setError(null);
      return;
    }

    try {
      setIsLoading(true);
      setError(null);
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");

      const data: CampaignsResponse = await apiFetch("/api/v1/campaigns", {
        token,
        method: "GET",
      });
      const mapped = data.campaigns.map((campaign, index) => mapBackendToUserCampaign(campaign, index));
      setDirectoryCampaigns(mapped);

      const memberCampaigns = mapped.filter((campaign) => campaign.access === "member");
      const entries = await Promise.all(
        memberCampaigns.map(async (campaign) => {
          try {
            const membersData = await apiFetch<CampaignMembersResponse>(
              `/api/v1/campaigns/${encodeURIComponent(campaign.campaignId)}/members?mentions_only=true`,
              { token, method: "GET" },
            );
            const names = (membersData.members || [])
              .map((member) => resolveUserLabel(member))
              .filter(Boolean)
              .slice(0, 4);
            return [campaign.campaignId, { count: membersData.members?.length || 0, names }] as const;
          } catch {
            return [campaign.campaignId, { count: 0, names: [] }] as const;
          }
        }),
      );
      setTeamMetaByCampaignId(Object.fromEntries(entries));
    } catch (err) {
      setDirectoryCampaigns([]);
      setTeamMetaByCampaignId({});
      setError(err instanceof Error ? err.message : "Unable to load campaigns. Please refresh.");
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    void refreshCampaigns();
  }, [isLoaded, user?.id, getToken]);

  const visibleCampaigns = useMemo(() => {
    const memberCampaigns = directoryCampaigns.filter((campaign) => campaign.access === "member");
    if (statusFilter === "all") return memberCampaigns;
    if (statusFilter === "active") return memberCampaigns;
    return [];
  }, [directoryCampaigns, statusFilter]);

  const handleCampaignCreated = (newCampaign: CampaignBucket) => {
    setDirectoryCampaigns((prev) => [
      {
        name: newCampaign.name,
        role: newCampaign.role,
        lastActive: newCampaign.lastActive,
        mentions: newCampaign.mentions,
        pendingRequests: newCampaign.pendingRequests,
        userRole: newCampaign.userRole,
        themeColor: newCampaign.themeColor,
        campaignId: newCampaign.campaignId,
        access: "member",
      },
      ...prev,
    ]);
    setIsCreateModalOpen(false);
    void refreshCampaigns();
  };

  const handleEnterCampaign = (campaignId: string) => {
    router.push(`/campaign/${campaignId}`);
  };

  const handleRequestAccess = async (campaignId: string, message?: string) => {
    const token = await getToken();
    if (!token) throw new Error("No authentication token available");
    await apiFetch(`/api/v1/campaigns/${encodeURIComponent(campaignId)}/access-requests`, {
      token,
      method: "POST",
      body: { message: message || null },
    });
  };

  const handleArchive = (campaignId: string) => {
    const campaign = directoryCampaigns.find((item) => item.campaignId === campaignId);
    setArchiveTarget({ id: campaignId, name: campaign?.name || "this campaign" });
  };

  const handleConfirmArchive = async () => {
    if (!archiveTarget) return;
    setIsArchiving(true);
    try {
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");
      await apiFetch(`/api/v1/campaigns/${encodeURIComponent(archiveTarget.id)}/archive`, {
        token,
        method: "PATCH",
      });
      setArchiveTarget(null);
      setCampaignsVersion((prev) => prev + 1);
      void refreshCampaigns();
    } catch (err) {
      alert(`Failed to archive campaign: ${err instanceof Error ? err.message : "Unknown error"}`);
    } finally {
      setIsArchiving(false);
    }
  };

  const handleRestore = () => {
    console.log("Restore not implemented - campaigns are managed by backend");
  };

  const handleTerminate = async (campaignId: string) => {
    if (!confirm("Are you sure you want to permanently terminate this campaign? This cannot be undone.")) {
      return;
    }
    setTerminatingCampaignId(campaignId);
    try {
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");
      await apiFetch(`/api/v1/campaign/${campaignId}`, {
        token,
        method: "DELETE",
      });
      void refreshCampaigns();
    } catch (err) {
      alert(`Failed to terminate campaign: ${err instanceof Error ? err.message : "Unknown error"}`);
    } finally {
      setTerminatingCampaignId(null);
    }
  };

  return (
    <div className="min-h-screen bg-[#f8f5ef] text-[#1f2a44]">
      <header className="border-b border-[#e6dece] bg-[#fbf8f2]">
        <div className="mx-auto flex w-full max-w-6xl items-center justify-between px-8 py-4">
          <div className="flex items-center gap-8">
            <h1 className="text-xl font-semibold tracking-tight text-[#d4ad47]">Riley</h1>
            <nav className="flex items-center gap-1 text-sm">
              <button
                type="button"
                onClick={() => setIsCreateModalOpen(true)}
                className="inline-flex items-center gap-1.5 rounded-md px-3 py-2 text-[#253659] hover:bg-[#ece5d8]"
              >
                <Plus className="h-3.5 w-3.5" />
                New Mission
              </button>
              <button
                type="button"
                onClick={() => setIsDirectoryOpen(true)}
                className="inline-flex items-center gap-1.5 rounded-md px-3 py-2 text-[#253659] hover:bg-[#ece5d8]"
              >
                <BookOpen className="h-3.5 w-3.5" />
                Firm Knowledgebase
                <ChevronDown className="h-3 w-3 opacity-60" />
              </button>
              <button
                type="button"
                onClick={() => router.push("/riley")}
                className="inline-flex items-center gap-1.5 rounded-md px-3 py-2 text-[#253659] hover:bg-[#ece5d8]"
              >
                <Globe className="h-3.5 w-3.5" />
                Riley Global
              </button>
            </nav>
          </div>
          <div className="flex items-center gap-4">
            {canAccessMissionControl ? (
              <button
                type="button"
                onClick={() => router.push("/mission-control")}
                className="rounded-md border border-[#1f2a44] px-3 py-1.5 text-sm font-medium text-[#1f2a44] hover:bg-[#f4efe5]"
              >
                Mission Control
              </button>
            ) : null}
            <div className="flex h-8 w-8 items-center justify-center overflow-hidden rounded-full border border-[#d7cfbf] bg-white">
              <UserButton appearance={{ elements: { userButtonAvatarBox: "h-8 w-8" } }} />
            </div>
          </div>
        </div>
      </header>

      <main className="mx-auto w-full max-w-6xl px-8 py-10">
        <div className="mb-10">
          <h2 className="font-serif text-4xl font-semibold tracking-tight text-[#1f2a44]">
            {getGreeting()}, {user?.firstName || user?.username || "there"}
          </h2>
          <p className="mt-2 text-sm text-[#5b6578]">
            You have {visibleCampaigns.length} active campaigns across your portfolio.
          </p>
        </div>

        <div className="mb-6 flex items-center justify-between">
          <h3 className="font-serif text-xl font-medium text-[#1f2a44]">Your Campaigns</h3>
          <div className="flex items-center gap-2 text-xs">
            <button
              type="button"
              onClick={() => setStatusFilter("all")}
              className={`rounded-md px-3 py-1.5 ${
                statusFilter === "all" ? "bg-[#efe6d7] text-[#1f2a44]" : "text-[#6f788a] hover:bg-[#efe6d7]"
              }`}
            >
              All
            </button>
            <button
              type="button"
              onClick={() => setStatusFilter("active")}
              className={`rounded-md px-3 py-1.5 ${
                statusFilter === "active"
                  ? "bg-[#efe6d7] text-[#1f2a44]"
                  : "text-[#6f788a] hover:bg-[#efe6d7]"
              }`}
            >
              Active
            </button>
            <button
              type="button"
              onClick={() => setStatusFilter("planning")}
              className={`rounded-md px-3 py-1.5 ${
                statusFilter === "planning"
                  ? "bg-[#efe6d7] text-[#1f2a44]"
                  : "text-[#6f788a] hover:bg-[#efe6d7]"
              }`}
            >
              Planning
            </button>
          </div>
        </div>

        {isLoading ? (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
            {[1, 2, 3].map((idx) => (
              <div key={idx} className="h-52 animate-pulse rounded-lg bg-[#eee7db]" />
            ))}
          </div>
        ) : error ? (
          <div className="rounded-lg bg-[#fff3f3] p-5 text-sm text-[#9e3434]">{error}</div>
        ) : visibleCampaigns.length === 0 ? (
          <div className="rounded-lg bg-[#f0eadf] p-8 text-center text-sm text-[#5b6578]">
            No campaigns available for this filter.
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
            {visibleCampaigns.map((campaign) => {
              const teamMeta = teamMetaByCampaignId[campaign.campaignId] || { count: 0, names: [] };
              const statusLabel = "Active";
              const clientName =
                (campaign.description && campaign.description.trim()) ||
                campaign.ownerName ||
                "No client or organization listed";

              return (
                <button
                  key={campaign.campaignId}
                  type="button"
                  onClick={() => handleEnterCampaign(campaign.campaignId)}
                  className="group relative block rounded-lg bg-[#fbf8f2] p-5 text-left shadow-[0_1px_2px_rgba(31,42,68,0.08),0_10px_24px_rgba(31,42,68,0.06)] transition-all hover:-translate-y-0.5 hover:shadow-[0_2px_6px_rgba(31,42,68,0.12),0_14px_28px_rgba(31,42,68,0.08)]"
                >
                  <span
                    role="button"
                    aria-label={`Archive ${campaign.name}`}
                    title="Archive campaign"
                    onClick={(event) => {
                      event.stopPropagation();
                      handleArchive(campaign.campaignId);
                    }}
                    className="absolute right-3 top-3 inline-flex h-7 w-7 items-center justify-center rounded-md text-[#6f788a] opacity-0 transition-opacity hover:bg-[#ece5d8] hover:text-[#4d5871] group-hover:opacity-100"
                  >
                    <Archive className="h-3.5 w-3.5" />
                  </span>
                  <div className="mb-3 flex items-start justify-between pr-8">
                    <span className="rounded-full bg-[#e6d8ad] px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wider text-[#3a3f2a]">
                      {statusLabel}
                    </span>
                    <span className="mr-2 text-[11px] text-[#8a90a0]">{relativeTime(campaign.createdAt)}</span>
                  </div>
                  <h4 className="mb-1 font-serif text-base font-semibold leading-snug text-[#1f2a44] group-hover:text-[#22386a]">
                    {campaign.name}
                  </h4>
                  <p className="mb-4 line-clamp-2 text-xs text-[#5b6578]">{clientName}</p>
                  <div className="flex items-center gap-1.5">
                    {teamMeta.names.slice(0, 4).map((name, idx) => (
                      <div
                        key={`${campaign.campaignId}-${idx}`}
                        className="-ml-1 flex h-6 w-6 items-center justify-center rounded-full border-2 border-[#fbf8f2] bg-[#e7edf8] text-[9px] font-medium text-[#425577] first:ml-0"
                        title={name}
                      >
                        {initials(name)}
                      </div>
                    ))}
                    {teamMeta.count > 4 ? (
                      <span className="ml-1 text-[10px] text-[#8a90a0]">+{teamMeta.count - 4}</span>
                    ) : null}
                    {teamMeta.count === 0 ? (
                      <span className="text-[10px] text-[#8a90a0]">No team data</span>
                    ) : null}
                  </div>
                </button>
              );
            })}
          </div>
        )}
      </main>

      <CreateCampaignModal
        isOpen={isCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onCampaignCreated={handleCampaignCreated}
      />

      <CampaignDirectory
        isOpen={isDirectoryOpen}
        onClose={() => setIsDirectoryOpen(false)}
        userCampaigns={directoryCampaigns}
        archivedCampaigns={[]}
        onEnterCampaign={handleEnterCampaign}
        onRequestAccess={handleRequestAccess}
        onArchive={handleArchive}
        onRestore={handleRestore}
        onTerminate={handleTerminate}
        terminatingCampaignId={terminatingCampaignId}
        campaignsVersion={campaignsVersion}
      />

      {archiveTarget ? (
        <div className="fixed inset-0 z-[70] flex items-center justify-center p-4">
          <button
            type="button"
            aria-label="Close archive confirmation"
            className="absolute inset-0 bg-black/40"
            onClick={() => (isArchiving ? undefined : setArchiveTarget(null))}
          />
          <div className="relative z-10 w-full max-w-md rounded-xl bg-white p-6 shadow-2xl">
            <h2 className="text-lg font-semibold text-[#1f2a44]">Archive Campaign</h2>
            <p className="mt-2 text-sm text-[#5b6578]">Are you sure you want to archive this campaign?</p>
            <p className="mt-1 text-xs text-[#8a90a0]">{archiveTarget.name}</p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setArchiveTarget(null)}
                disabled={isArchiving}
                className="rounded-md border border-[#d9d4c8] px-3 py-1.5 text-sm text-[#4d5871] hover:bg-[#f4efe5] disabled:opacity-60"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleConfirmArchive}
                disabled={isArchiving}
                className="rounded-md bg-[#d4ad47] px-3 py-1.5 text-sm font-semibold text-[#1f2a44] hover:bg-[#bf993b] disabled:opacity-60"
              >
                {isArchiving ? "Archiving..." : "Archive"}
              </button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
