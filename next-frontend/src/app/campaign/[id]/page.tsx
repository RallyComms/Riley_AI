"use client";

import { useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import { useAuth, useUser } from "@clerk/nextjs";
import { Users, FileText, Calendar, Plus, X } from "lucide-react";
import { cn } from "@app/lib/utils";
import { useCampaignName } from "@app/lib/useCampaignName";
import { apiFetch } from "@app/lib/api";

interface CampaignEventItem {
  id: string;
  type: string;
  message: string;
  request_id?: string | null;
  campaign_id?: string | null;
  user_id?: string | null;
  actor_user_id?: string | null;
  created_at?: string | null;
  requester_display_name?: string | null;
  notification_status?: "unread" | "read" | "completed" | null;
}

interface DirectoryUserResult {
  id: string;
  email: string;
  display_name: string;
}

interface CampaignDeadlineItem {
  id: string;
  campaign_id: string;
  created_by: string;
  title: string;
  description?: string | null;
  due_at: string;
  visibility: "team" | "personal";
  assigned_user_id?: string | null;
  created_at?: string | null;
  completed_at?: string | null;
}

interface TeamMemberStatusItem {
  user_id: string;
  display_name: string;
  avatar_url?: string | null;
  role?: string | null;
  status?: "active" | "away" | "in_meeting" | null;
  status_updated_at?: string | null;
}

export default function CampaignOverviewPage() {
  const params = useParams();
  const campaignId = params.id as string;
  const { user } = useUser();
  const { getToken, isLoaded } = useAuth();
  const { name: campaignName } = useCampaignName(campaignId);
  const [events, setEvents] = useState<CampaignEventItem[]>([]);
  const [deadlines, setDeadlines] = useState<CampaignDeadlineItem[]>([]);
  const [teamMembers, setTeamMembers] = useState<TeamMemberStatusItem[]>([]);
  const [isDeadlineFormOpen, setIsDeadlineFormOpen] = useState(false);
  const [deadlineTitle, setDeadlineTitle] = useState("");
  const [deadlineDescription, setDeadlineDescription] = useState("");
  const [deadlineDate, setDeadlineDate] = useState("");
  const [deadlineTime, setDeadlineTime] = useState("");
  const [deadlineVisibility, setDeadlineVisibility] = useState<"team" | "personal">("team");
  const [isCreatingDeadline, setIsCreatingDeadline] = useState(false);
  const [completingDeadlineId, setCompletingDeadlineId] = useState<string | null>(null);
  const [deadlineError, setDeadlineError] = useState<string | null>(null);
  const [requestActionLoadingId, setRequestActionLoadingId] = useState<string | null>(null);
  const [activityDismissLoadingId, setActivityDismissLoadingId] = useState<string | null>(null);
  const [isAddUserModalOpen, setIsAddUserModalOpen] = useState(false);
  const [memberSearchQuery, setMemberSearchQuery] = useState("");
  const [memberSearchResults, setMemberSearchResults] = useState<DirectoryUserResult[]>([]);
  const [isSearchingMembers, setIsSearchingMembers] = useState(false);
  const [addingMemberUserId, setAddingMemberUserId] = useState<string | null>(null);
  const [addUserError, setAddUserError] = useState<string | null>(null);
  const headerSubtitle = campaignName?.trim()
    ? `${campaignName} — Campaign workspace overview`
    : "Campaign workspace overview";
  const recentActivity = useMemo(
    () =>
      events
        .filter((event) => {
          const status = String(event.notification_status || "").trim().toLowerCase();
          return status !== "read" && status !== "completed";
        })
        .slice(0, 8),
    [events]
  );
  const getAccessRequesterLabel = (event: CampaignEventItem) =>
    event.requester_display_name?.trim() || "Unknown user";

  const sortedTeamMembers = useMemo(() => {
    const rank = (s?: string | null) => {
      if (s === "active") return 0;
      if (s === "away") return 1;
      if (s === "in_meeting") return 2;
      return 3;
    };
    return [...teamMembers].sort((a, b) => {
      const r = rank(a.status) - rank(b.status);
      if (r !== 0) return r;
      return (a.display_name || "").localeCompare(b.display_name || "");
    });
  }, [teamMembers]);
  const isLead = useMemo(
    () => teamMembers.some((m) => m.user_id === user?.id && m.role === "Lead"),
    [teamMembers, user?.id]
  );

  const sortedDeadlines = useMemo(
    () =>
      [...deadlines].sort(
        (a, b) =>
          new Date(a.due_at).getTime() - new Date(b.due_at).getTime()
      ),
    [deadlines]
  );

  const formatDueLabel = (iso: string) => {
    const due = new Date(iso);
    const dateLabel = due.toLocaleDateString(undefined, {
      month: "short",
      day: "numeric",
    });
    const timeLabel = due.toLocaleTimeString(undefined, {
      hour: "numeric",
      minute: "2-digit",
    });
    return `${dateLabel} • ${timeLabel}`;
  };

  useEffect(() => {
    let mounted = true;
    let intervalId: number | null = null;
    const fetchEvents = async () => {
      if (!isLoaded || !campaignId) return;
      try {
        const token = await getToken();
        if (!token) return;
        const data = await apiFetch<{ events: CampaignEventItem[] }>(
          `/api/v1/campaigns/${campaignId}/events?limit=20`,
          {
            token,
            method: "GET",
          }
        );
        if (mounted) {
          setEvents(data.events || []);
        }
      } catch (err) {
        console.error("Failed to load campaign activity events:", err);
      }
    };

    const stopPolling = () => {
      if (intervalId !== null) {
        window.clearInterval(intervalId);
        intervalId = null;
      }
    };

    const startPollingIfVisible = () => {
      if (document.visibilityState !== "visible") {
        stopPolling();
        return;
      }
      if (intervalId !== null) return;
      intervalId = window.setInterval(() => {
        void fetchEvents();
      }, 30000);
    };

    if (document.visibilityState === "visible") {
      void fetchEvents();
    }
    startPollingIfVisible();

    const handleVisibility = () => {
      if (document.visibilityState === "visible") {
        void fetchEvents();
        startPollingIfVisible();
      } else {
        stopPolling();
      }
    };
    document.addEventListener("visibilitychange", handleVisibility);
    return () => {
      mounted = false;
      stopPolling();
      document.removeEventListener("visibilitychange", handleVisibility);
    };
  }, [campaignId, getToken, isLoaded]);

  useEffect(() => {
    const fetchTeamMembers = async () => {
      if (!isLoaded || !campaignId) return;
      try {
        const token = await getToken();
        if (!token) return;
        const data = await apiFetch<{ members: TeamMemberStatusItem[] }>(
          `/api/v1/campaigns/${campaignId}/members?mentions_only=true`,
          {
            token,
            method: "GET",
          }
        );
        setTeamMembers(data.members || []);
      } catch (err) {
        console.error("Failed to load campaign team status:", err);
        setTeamMembers([]);
      }
    };
    fetchTeamMembers();
  }, [campaignId, getToken, isLoaded]);

  useEffect(() => {
    const fetchDeadlines = async () => {
      if (!isLoaded || !campaignId) return;
      try {
        const token = await getToken();
        if (!token) return;
        const data = await apiFetch<{ deadlines: CampaignDeadlineItem[] }>(
          `/api/v1/campaigns/${campaignId}/deadlines?limit=100`,
          {
            token,
            method: "GET",
          }
        );
        setDeadlines(data.deadlines || []);
      } catch (err) {
        console.error("Failed to load campaign deadlines:", err);
        setDeadlines([]);
      }
    };
    fetchDeadlines();
  }, [campaignId, getToken, isLoaded]);

  const handleCreateDeadline = async () => {
    if (!deadlineTitle.trim() || !deadlineDate || !deadlineTime) {
      setDeadlineError("Title, date, and time are required.");
      return;
    }
    try {
      setIsCreatingDeadline(true);
      setDeadlineError(null);
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }
      const dueAt = new Date(`${deadlineDate}T${deadlineTime}:00`);
      await apiFetch(`/api/v1/campaigns/${campaignId}/deadlines`, {
        token,
        method: "POST",
        body: {
          title: deadlineTitle.trim(),
          description: deadlineDescription.trim() || null,
          due_at: dueAt.toISOString(),
          visibility: deadlineVisibility,
        },
      });
      setDeadlineTitle("");
      setDeadlineDescription("");
      setDeadlineDate("");
      setDeadlineTime("");
      setDeadlineVisibility("team");
      setIsDeadlineFormOpen(false);

      const [eventsData, deadlinesData] = await Promise.all([
        apiFetch<{ events: CampaignEventItem[] }>(
          `/api/v1/campaigns/${campaignId}/events?limit=20`,
          { token, method: "GET" }
        ),
        apiFetch<{ deadlines: CampaignDeadlineItem[] }>(
          `/api/v1/campaigns/${campaignId}/deadlines?limit=100`,
          { token, method: "GET" }
        ),
      ]);
      setEvents(eventsData.events || []);
      setDeadlines(deadlinesData.deadlines || []);
    } catch (err) {
      setDeadlineError(
        err instanceof Error ? err.message : "Failed to create deadline."
      );
    } finally {
      setIsCreatingDeadline(false);
    }
  };

  const getDeadlineTimingLabel = (iso: string) => {
    const now = new Date();
    const due = new Date(iso);
    const nowDate = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
    const dueDate = new Date(due.getFullYear(), due.getMonth(), due.getDate()).getTime();
    if (due.getTime() < now.getTime()) return "Overdue";
    if (dueDate === nowDate) return "Due today";
    return "Upcoming";
  };

  const handleCompleteDeadline = async (deadlineId: string) => {
    try {
      setCompletingDeadlineId(deadlineId);
      const token = await getToken();
      if (!token) return;
      await apiFetch(`/api/v1/campaigns/${campaignId}/deadlines/${encodeURIComponent(deadlineId)}/complete`, {
        token,
        method: "POST",
      });
      setDeadlines((prev) => prev.filter((deadline) => deadline.id !== deadlineId));
    } catch (err) {
      console.error("Failed to complete deadline:", err);
    } finally {
      setCompletingDeadlineId(null);
    }
  };

  const handleAccessRequestDecision = async (
    event: CampaignEventItem,
    action: "approve" | "deny"
  ) => {
    if (!event.request_id) return;
    try {
      setRequestActionLoadingId(event.id);
      const token = await getToken();
      if (!token) return;
      await apiFetch(
        `/api/v1/campaigns/${campaignId}/access-requests/${event.request_id}/decision`,
        {
          token,
          method: "POST",
          body: { action },
        }
      );
      setEvents((prev) => prev.filter((e) => e.id !== event.id));
    } catch (err) {
      console.error("Failed to resolve access request from activity:", err);
    } finally {
      setRequestActionLoadingId(null);
    }
  };

  const handleDismissActivityEvent = async (eventId: string) => {
    try {
      setActivityDismissLoadingId(eventId);
      const token = await getToken();
      if (!token) return;
      await apiFetch(
        `/api/v1/campaigns/${campaignId}/events/${encodeURIComponent(eventId)}/dismiss`,
        {
          token,
          method: "POST",
        }
      );
      setEvents((prev) => prev.filter((event) => event.id !== eventId));
    } catch (err) {
      console.error("Failed to dismiss activity event:", err);
    } finally {
      setActivityDismissLoadingId(null);
    }
  };

  const handleSearchMembers = async () => {
    if (!isLead) return;
    const query = memberSearchQuery.trim();
    if (query.length < 2) {
      setMemberSearchResults([]);
      return;
    }
    try {
      setIsSearchingMembers(true);
      setAddUserError(null);
      const token = await getToken();
      if (!token) return;
      const data = await apiFetch<{ users: DirectoryUserResult[] }>(
        `/api/v1/campaigns/${campaignId}/users/search?query=${encodeURIComponent(query)}&limit=10`,
        {
          token,
          method: "GET",
        }
      );
      const memberIds = new Set(teamMembers.map((m) => m.user_id));
      setMemberSearchResults((data.users || []).filter((candidate) => !memberIds.has(candidate.id)));
    } catch (err) {
      setMemberSearchResults([]);
      setAddUserError(err instanceof Error ? err.message : "Failed to search users.");
    } finally {
      setIsSearchingMembers(false);
    }
  };

  const handleAddMemberDirect = async (candidate: DirectoryUserResult) => {
    if (!isLead) return;
    const confirmed = window.confirm(
      "Are you sure you want to add this user to the campaign?"
    );
    if (!confirmed) return;
    try {
      setAddingMemberUserId(candidate.id);
      setAddUserError(null);
      const token = await getToken();
      if (!token) return;
      await apiFetch(`/api/v1/campaigns/${campaignId}/members`, {
        token,
        method: "POST",
        body: { email: candidate.email, user_id: candidate.id, role: "Member" },
      });
      setMemberSearchResults((prev) => prev.filter((u) => u.id !== candidate.id));
      setMemberSearchQuery("");
      const [eventsData, membersData] = await Promise.all([
        apiFetch<{ events: CampaignEventItem[] }>(
          `/api/v1/campaigns/${campaignId}/events?limit=20`,
          { token, method: "GET" }
        ),
        apiFetch<{ members: TeamMemberStatusItem[] }>(
          `/api/v1/campaigns/${campaignId}/members?mentions_only=true`,
          { token, method: "GET" }
        ),
      ]);
      setEvents(eventsData.events || []);
      setTeamMembers(membersData.members || []);
    } catch (err) {
      setAddUserError(err instanceof Error ? err.message : "Failed to add member.");
    } finally {
      setAddingMemberUserId(null);
    }
  };

  useEffect(() => {
    if (!isAddUserModalOpen || !isLead) return;
    if (memberSearchQuery.trim().length < 2) {
      setMemberSearchResults([]);
      return;
    }
    const handle = setTimeout(() => {
      void handleSearchMembers();
    }, 250);
    return () => clearTimeout(handle);
  }, [campaignId, isAddUserModalOpen, isLead, memberSearchQuery]);

  return (
    <div className="min-h-full overflow-y-auto bg-[#f7f5ef] px-4 py-6 text-[#1f2a44] sm:px-6 lg:px-8 lg:py-8">
      <div className="mx-auto w-full max-w-5xl">
        <div className="mb-8 flex items-end justify-between">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-[#1f2a44]">
              Campaign Overview
            </h1>
            <p className="mt-1 max-w-3xl break-words text-sm text-[#6f788a]">{headerSubtitle}</p>
          </div>
        </div>

        <div className="grid grid-cols-1 gap-8 lg:grid-cols-3 lg:gap-0 lg:divide-x lg:divide-[#e5ddce]">
          {/* Campaign Team */}
          <section className="lg:pr-6">
            <div className="mb-4 flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <Users className="h-4 w-4 text-[#6f788a]" />
                <h2 className="text-xs font-semibold uppercase tracking-wider text-[#6f788a]">Campaign Team</h2>
              </div>
              {isLead ? (
                <button
                  type="button"
                  onClick={() => {
                    setIsAddUserModalOpen(true);
                    setAddUserError(null);
                  }}
                  className="inline-flex items-center justify-center rounded-md border border-[#d8d0bf] p-1.5 text-[#6f788a] transition hover:bg-[#f1ece2]"
                  title="Add user to campaign"
                  aria-label="Add user to campaign"
                >
                  <Plus className="h-3.5 w-3.5" />
                </button>
              ) : null}
            </div>
            <div className="space-y-3">
              {sortedTeamMembers.length === 0 ? (
                <p className="text-sm text-[#8a90a0]">No campaign members found.</p>
              ) : (
                sortedTeamMembers.map((member) => (
                  <div key={member.user_id} className="flex items-center gap-3">
                    <div className="relative">
                      <div className="flex h-8 w-8 items-center justify-center rounded-full bg-[#ebe4d6] text-xs font-medium text-[#1f2a44]">
                        {member.display_name
                          .split(" ")
                          .map((n) => n[0])
                          .join("")
                          .slice(0, 2)
                          .toUpperCase()}
                      </div>
                      <div
                        className={cn(
                          "absolute -bottom-0.5 -right-0.5 h-2.5 w-2.5 rounded-full border border-[#f7f5ef]",
                          member.status === "active"
                            ? "bg-emerald-500"
                            : member.status === "away"
                            ? "bg-amber-500"
                            : "bg-sky-500"
                        )}
                      />
                    </div>
                    <div className="min-w-0">
                      <p className="truncate text-sm font-medium text-[#1f2a44]">{member.display_name}</p>
                      <p className="truncate text-[11px] text-[#8a90a0]">
                        {member.role || "Member"} •{" "}
                        {member.status === "in_meeting"
                          ? "In a meeting"
                          : member.status === "away"
                          ? "Away"
                          : "Active"}
                      </p>
                    </div>
                  </div>
                ))
              )}
            </div>
          </section>

          {/* Recent Activity */}
          <section className="lg:px-6">
            <div className="mb-4 flex items-center gap-2">
              <FileText className="h-4 w-4 text-[#6f788a]" />
              <h2 className="text-xs font-semibold uppercase tracking-wider text-[#6f788a]">Recent Activity</h2>
            </div>
            <div className="space-y-3">
              {recentActivity.length === 0 ? (
                <p className="text-sm text-[#8a90a0]">No recent campaign activity yet.</p>
              ) : (
                recentActivity.map((activity) => (
                  <div
                    key={activity.id}
                    className="flex items-start gap-3 border-b border-[#e5ddce] pb-3 last:border-0"
                  >
                    <div className="mt-1 shrink-0">
                      <div className="h-1.5 w-1.5 rounded-full bg-[#b7ad98]" />
                    </div>
                    <div className="min-w-0 flex-1">
                      <p className="text-sm leading-snug text-[#1f2a44]">
                        {activity.type === "access_request_created"
                          ? `${getAccessRequesterLabel(activity)} requested access to this campaign`
                          : activity.message}
                      </p>
                      <p className="mt-0.5 text-[11px] text-[#8a90a0]">
                        {activity.created_at
                          ? new Date(activity.created_at).toLocaleString()
                          : "Just now"}
                      </p>
                      {isLead &&
                      activity.type === "access_request_created" &&
                      activity.request_id ? (
                        <div className="mt-2 flex items-center gap-1.5">
                          <button
                            type="button"
                            onClick={() => handleAccessRequestDecision(activity, "approve")}
                            disabled={requestActionLoadingId === activity.id}
                            className="rounded-md border border-emerald-600/30 bg-emerald-500/10 px-2.5 py-1 text-[10px] font-medium text-emerald-700 transition hover:bg-emerald-500/20 disabled:opacity-50"
                          >
                            Accept
                          </button>
                          <button
                            type="button"
                            onClick={() => handleAccessRequestDecision(activity, "deny")}
                            disabled={requestActionLoadingId === activity.id}
                            className="rounded-md border border-red-600/30 bg-red-500/10 px-2.5 py-1 text-[10px] font-medium text-red-700 transition hover:bg-red-500/20 disabled:opacity-50"
                          >
                            Deny
                          </button>
                        </div>
                      ) : null}
                      <div className="mt-2 flex justify-end">
                        <button
                          type="button"
                          onClick={() => handleDismissActivityEvent(activity.id)}
                          disabled={activityDismissLoadingId === activity.id}
                          className="rounded-md border border-[#d8d0bf] px-2.5 py-1 text-[10px] font-medium text-[#6f788a] transition hover:bg-[#f1ece2] hover:text-[#1f2a44] disabled:opacity-50"
                        >
                          Done
                        </button>
                      </div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </section>

          {/* Upcoming Deadlines */}
          <section className="lg:pl-6">
            <div className="mb-4 flex items-center justify-between gap-2">
              <div className="flex items-center gap-2">
                <Calendar className="h-4 w-4 text-[#6f788a]" />
                <h2 className="text-xs font-semibold uppercase tracking-wider text-[#6f788a]">Upcoming Deadlines</h2>
              </div>
              <button
                type="button"
                onClick={() => setIsDeadlineFormOpen((prev) => !prev)}
                className="rounded-md border border-[#d8d0bf] px-2.5 py-1 text-xs font-medium text-[#6f788a] transition hover:bg-[#f1ece2]"
              >
                {isDeadlineFormOpen ? "Close" : "Add"}
              </button>
            </div>
            {isDeadlineFormOpen && (
              <div className="mb-4 space-y-2 rounded-lg border border-[#d8d0bf] bg-white/80 p-3">
                <input
                  value={deadlineTitle}
                  onChange={(e) => setDeadlineTitle(e.target.value)}
                  placeholder="Deadline title"
                  className="w-full rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44]"
                />
                <textarea
                  value={deadlineDescription}
                  onChange={(e) => setDeadlineDescription(e.target.value)}
                  placeholder="Description (optional)"
                  rows={2}
                  className="w-full rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44]"
                />
                <div className="grid grid-cols-2 gap-2">
                  <input
                    type="date"
                    value={deadlineDate}
                    onChange={(e) => setDeadlineDate(e.target.value)}
                    className="rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44]"
                  />
                  <input
                    type="time"
                    value={deadlineTime}
                    onChange={(e) => setDeadlineTime(e.target.value)}
                    className="rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44]"
                  />
                </div>
                <div className="inline-flex rounded-md border border-[#d8d0bf] bg-white p-1">
                  <button
                    type="button"
                    onClick={() => setDeadlineVisibility("team")}
                    className={cn(
                      "rounded px-2 py-1 text-xs",
                      deadlineVisibility === "team"
                        ? "bg-amber-500 text-zinc-900"
                        : "text-[#6f788a]"
                    )}
                  >
                    Team
                  </button>
                  <button
                    type="button"
                    onClick={() => setDeadlineVisibility("personal")}
                    className={cn(
                      "rounded px-2 py-1 text-xs",
                      deadlineVisibility === "personal"
                        ? "bg-amber-500 text-zinc-900"
                        : "text-[#6f788a]"
                    )}
                  >
                    Just for me
                  </button>
                </div>
                {deadlineError ? <p className="text-xs text-red-600">{deadlineError}</p> : null}
                <button
                  type="button"
                  onClick={handleCreateDeadline}
                  disabled={isCreatingDeadline}
                  className="w-full rounded-md bg-[#d4ad47] px-3 py-1.5 text-sm font-medium text-[#1f2a44] transition hover:bg-[#c89e32] disabled:opacity-60"
                >
                  {isCreatingDeadline ? "Creating..." : "Create Deadline"}
                </button>
              </div>
            )}
            <div className="space-y-3">
              {sortedDeadlines.length === 0 ? (
                <p className="text-sm text-[#8a90a0]">No active deadlines yet.</p>
              ) : (
                sortedDeadlines.slice(0, 8).map((deadline) => {
                  const timingLabel = getDeadlineTimingLabel(deadline.due_at);
                  const visibilityLabel =
                    deadline.visibility === "team" ? "Team" : "Just for me";
                  return (
                    <div
                      key={deadline.id}
                      className="flex items-start justify-between gap-3 border-b border-[#e5ddce] pb-3.5 last:border-0"
                    >
                      <div className="min-w-0 flex-1 space-y-1.5">
                        <p className="line-clamp-2 break-words text-sm font-semibold leading-5 text-[#1f2a44]">
                          {deadline.title}
                        </p>
                        <p className="text-xs text-[#6f788a]">
                          {formatDueLabel(deadline.due_at)}
                        </p>
                        <p className="text-[11px] text-[#8a90a0]">{visibilityLabel}</p>
                        {deadline.description ? (
                          <p className="line-clamp-2 break-words text-xs leading-4 text-[#8a90a0]">
                            {deadline.description}
                          </p>
                        ) : null}
                      </div>
                      <div className="flex shrink-0 flex-col items-end gap-2 self-start pl-1">
                        <span
                          className={cn(
                            "inline-flex items-center rounded-full border px-2.5 py-1 text-xs font-medium",
                            timingLabel === "Overdue"
                              ? "border-red-500/20 bg-red-500/10 text-red-700"
                              : timingLabel === "Due today"
                              ? "border-amber-500/20 bg-amber-500/10 text-amber-700"
                              : "border-emerald-500/20 bg-emerald-500/10 text-emerald-700"
                          )}
                        >
                          {timingLabel}
                        </span>
                        <button
                          type="button"
                          onClick={() => handleCompleteDeadline(deadline.id)}
                          disabled={completingDeadlineId === deadline.id}
                          className="rounded-md border border-[#d8d0bf] px-2.5 py-1 text-[10px] font-medium text-[#6f788a] transition hover:bg-[#f1ece2] disabled:opacity-50"
                        >
                          Done
                        </button>
                      </div>
                    </div>
                  );
                })
              )}
            </div>
          </section>
        </div>
      </div>

      {isAddUserModalOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-[#1f2a44]/30 p-4 backdrop-blur-sm">
          <div className="w-full max-w-lg rounded-xl border border-[#d8d0bf] bg-[#fcfbf8] p-4 shadow-xl shadow-[#1f2a44]/10">
            <div className="mb-3 flex items-center justify-between">
              <h3 className="text-sm font-semibold text-[#1f2a44]">Add User to Campaign</h3>
              <button
                type="button"
                onClick={() => setIsAddUserModalOpen(false)}
                className="rounded p-1 text-[#6f788a] hover:bg-[#f1ece2] hover:text-[#1f2a44]"
                aria-label="Close add-user modal"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <input
              value={memberSearchQuery}
              onChange={(e) => setMemberSearchQuery(e.target.value)}
              placeholder="Search by username or display name"
              className="w-full rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44]"
            />
            <p className="mt-2 text-xs text-[#8a90a0]">
              Search all Riley users. Select a user to add them to this campaign.
            </p>
            {addUserError ? (
              <p className="mt-2 text-xs text-red-600">{addUserError}</p>
            ) : null}
            <div className="mt-3 max-h-72 space-y-2 overflow-y-auto pr-1">
              {isSearchingMembers ? (
                <p className="text-xs text-[#6f788a]">Searching users...</p>
              ) : memberSearchQuery.trim().length < 2 ? (
                <p className="text-xs text-[#8a90a0]">Type at least 2 characters.</p>
              ) : memberSearchResults.length === 0 ? (
                <p className="text-xs text-[#8a90a0]">No matching users found.</p>
              ) : (
                memberSearchResults.map((candidate) => (
                  <div
                    key={candidate.id}
                    className="flex items-center justify-between rounded-md border border-[#e5ddce] bg-white px-3 py-2"
                  >
                    <div className="min-w-0">
                      <p className="truncate text-sm text-[#1f2a44]">{candidate.display_name}</p>
                      <p className="truncate text-xs text-[#8a90a0]">{candidate.email}</p>
                    </div>
                    <button
                      type="button"
                      onClick={() => handleAddMemberDirect(candidate)}
                      disabled={addingMemberUserId === candidate.id}
                      className="rounded-md border border-emerald-600/30 bg-emerald-500/10 px-2.5 py-1 text-[11px] font-medium text-emerald-700 hover:bg-emerald-500/20 disabled:opacity-50"
                    >
                      {addingMemberUserId === candidate.id ? "Adding..." : "Add"}
                    </button>
                  </div>
                ))
              )}
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}
