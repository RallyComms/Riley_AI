"use client";

import { useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Users, FileText, Calendar } from "lucide-react";
import { cn } from "@app/lib/utils";
import { useCampaignName } from "@app/lib/useCampaignName";
import { apiFetch } from "@app/lib/api";

interface CampaignEventItem {
  id: string;
  type: string;
  message: string;
  user_id?: string | null;
  actor_user_id?: string | null;
  created_at?: string | null;
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
}

const teamStatus = [
  { name: "Sarah Chen", role: "Lead Strategist", status: "online" },
  { name: "John Martinez", role: "Media Expert", status: "online" },
  { name: "Alex Rivera", role: "Research Lead", status: "away" },
  { name: "Emma Wilson", role: "Policy Analyst", status: "offline" },
];

export default function CampaignOverviewPage() {
  const params = useParams();
  const campaignId = params.id as string;
  const { getToken, isLoaded } = useAuth();
  const { name: campaignName } = useCampaignName(campaignId);
  const [events, setEvents] = useState<CampaignEventItem[]>([]);
  const [deadlines, setDeadlines] = useState<CampaignDeadlineItem[]>([]);
  const [isDeadlineFormOpen, setIsDeadlineFormOpen] = useState(false);
  const [deadlineTitle, setDeadlineTitle] = useState("");
  const [deadlineDescription, setDeadlineDescription] = useState("");
  const [deadlineDate, setDeadlineDate] = useState("");
  const [deadlineTime, setDeadlineTime] = useState("");
  const [deadlineVisibility, setDeadlineVisibility] = useState<"team" | "personal">("team");
  const [isCreatingDeadline, setIsCreatingDeadline] = useState(false);
  const [deadlineError, setDeadlineError] = useState<string | null>(null);
  const headerTitle = campaignName?.trim()
    ? `${campaignName} Dashboard`
    : "Campaign Dashboard";
  const recentActivity = useMemo(() => events.slice(0, 8), [events]);

  const sortedDeadlines = useMemo(
    () =>
      [...deadlines].sort(
        (a, b) =>
          new Date(a.due_at).getTime() - new Date(b.due_at).getTime()
      ),
    [deadlines]
  );

  const formatDueLabel = (iso: string) =>
    new Date(iso).toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });

  const getDaysLeft = (iso: string) => {
    const now = Date.now();
    const due = new Date(iso).getTime();
    const diff = due - now;
    return Math.ceil(diff / (1000 * 60 * 60 * 24));
  };

  useEffect(() => {
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
        setEvents(data.events || []);
      } catch (err) {
        console.error("Failed to load campaign activity events:", err);
        setEvents([]);
      }
    };
    fetchEvents();
  }, [campaignId, getToken, isLoaded]);

  useEffect(() => {
    const fetchDeadlines = async () => {
      if (!isLoaded || !campaignId) return;
      try {
        const token = await getToken();
        if (!token) return;
        const data = await apiFetch<{ deadlines: CampaignDeadlineItem[] }>(
          `/api/v1/campaigns/${campaignId}/deadlines?include_past=false&limit=100`,
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
          `/api/v1/campaigns/${campaignId}/deadlines?include_past=false&limit=100`,
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

  return (
    <div className="min-h-full p-6 bg-transparent">
      {/* Header */}
      <div className="mb-8 bg-transparent">
        <h1 className="text-3xl font-bold tracking-tight text-zinc-100 mb-2">
          {headerTitle}
        </h1>
        <p className="text-zinc-500">Campaign Status: <span className="text-emerald-400">Active</span></p>
      </div>

      {/* Bento Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
        {/* Recent Activity Widget */}
        <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 backdrop-blur-md p-6">
          <div className="flex items-center gap-2 mb-4">
            <FileText className="h-5 w-5 text-zinc-400" />
            <h2 className="text-lg font-semibold text-zinc-100">Recent Activity</h2>
          </div>
          <div className="space-y-3">
            {recentActivity.length === 0 ? (
              <p className="text-sm text-zinc-500">No recent campaign activity yet.</p>
            ) : (
              recentActivity.map((activity) => (
                <div
                  key={activity.id}
                  className="flex items-start gap-3 p-3 rounded-lg bg-zinc-800/30 hover:bg-zinc-800/50 transition-colors"
                >
                  <div className="flex-shrink-0 mt-0.5">
                    <div className="h-2 w-2 rounded-full bg-emerald-500" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-zinc-100">{activity.message}</p>
                    <p className="text-xs text-zinc-500 mt-0.5">
                      {activity.created_at
                        ? new Date(activity.created_at).toLocaleString()
                        : "Just now"}
                    </p>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Team Status Widget */}
        <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 backdrop-blur-md p-6">
          <div className="flex items-center gap-2 mb-4">
            <Users className="h-5 w-5 text-zinc-400" />
            <h2 className="text-lg font-semibold text-zinc-100">Team Status</h2>
          </div>
          <div className="space-y-3">
            {teamStatus.map((member, index) => (
              <div
                key={index}
                className="flex items-center gap-3 p-3 rounded-lg bg-zinc-800/30 hover:bg-zinc-800/50 transition-colors"
              >
                <div className="relative">
                  <div className="h-10 w-10 rounded-full bg-zinc-800 flex items-center justify-center text-xs font-medium text-zinc-100">
                    {member.name.split(" ").map((n) => n[0]).join("")}
                  </div>
                  <div
                    className={cn(
                      "absolute bottom-0 right-0 h-3 w-3 rounded-full border-2 border-zinc-900",
                      member.status === "online"
                        ? "bg-emerald-500"
                        : member.status === "away"
                        ? "bg-amber-500"
                        : "bg-zinc-600"
                    )}
                  />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-zinc-100 truncate">{member.name}</p>
                  <p className="text-xs text-zinc-500 truncate">{member.role}</p>
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Upcoming Deadlines Widget */}
        <div className="rounded-xl border border-zinc-800 bg-zinc-900/50 backdrop-blur-md p-6">
          <div className="flex items-center justify-between gap-2 mb-4">
            <div className="flex items-center gap-2">
              <Calendar className="h-5 w-5 text-zinc-400" />
              <h2 className="text-lg font-semibold text-zinc-100">Upcoming Deadlines</h2>
            </div>
            <button
              type="button"
              onClick={() => setIsDeadlineFormOpen((prev) => !prev)}
              className="rounded-md border border-zinc-700 px-2 py-1 text-xs text-zinc-300 hover:bg-zinc-800/60"
            >
              {isDeadlineFormOpen ? "Close" : "Add"}
            </button>
          </div>
          {isDeadlineFormOpen && (
            <div className="mb-4 rounded-lg border border-zinc-700 bg-zinc-900/70 p-3 space-y-2">
              <input
                value={deadlineTitle}
                onChange={(e) => setDeadlineTitle(e.target.value)}
                placeholder="Deadline title"
                className="w-full rounded-md border border-zinc-700 bg-zinc-950 px-2 py-1.5 text-sm text-zinc-100"
              />
              <textarea
                value={deadlineDescription}
                onChange={(e) => setDeadlineDescription(e.target.value)}
                placeholder="Description (optional)"
                rows={2}
                className="w-full rounded-md border border-zinc-700 bg-zinc-950 px-2 py-1.5 text-sm text-zinc-100"
              />
              <div className="grid grid-cols-2 gap-2">
                <input
                  type="date"
                  value={deadlineDate}
                  onChange={(e) => setDeadlineDate(e.target.value)}
                  className="rounded-md border border-zinc-700 bg-zinc-950 px-2 py-1.5 text-sm text-zinc-100"
                />
                <input
                  type="time"
                  value={deadlineTime}
                  onChange={(e) => setDeadlineTime(e.target.value)}
                  className="rounded-md border border-zinc-700 bg-zinc-950 px-2 py-1.5 text-sm text-zinc-100"
                />
              </div>
              <div className="inline-flex rounded-md border border-zinc-700 bg-zinc-950 p-1">
                <button
                  type="button"
                  onClick={() => setDeadlineVisibility("team")}
                  className={cn(
                    "rounded px-2 py-1 text-xs",
                    deadlineVisibility === "team"
                      ? "bg-amber-500 text-zinc-900"
                      : "text-zinc-300"
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
                      : "text-zinc-300"
                  )}
                >
                  Just for me
                </button>
              </div>
              {deadlineError ? <p className="text-xs text-red-400">{deadlineError}</p> : null}
              <button
                type="button"
                onClick={handleCreateDeadline}
                disabled={isCreatingDeadline}
                className="w-full rounded-md bg-amber-500 px-3 py-1.5 text-sm font-medium text-zinc-900 hover:bg-amber-400 disabled:opacity-60"
              >
                {isCreatingDeadline ? "Creating..." : "Create Deadline"}
              </button>
            </div>
          )}
          <div className="space-y-3">
            {sortedDeadlines.length === 0 ? (
              <p className="text-sm text-zinc-500">No upcoming deadlines yet.</p>
            ) : (
              sortedDeadlines.slice(0, 8).map((deadline) => (
                <div
                  key={deadline.id}
                  className="flex items-center justify-between p-3 rounded-lg bg-zinc-800/30 hover:bg-zinc-800/50 transition-colors"
                >
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-zinc-100 truncate">{deadline.title}</p>
                    <p className="text-xs text-zinc-500 mt-0.5">
                      {formatDueLabel(deadline.due_at)} •{" "}
                      {deadline.visibility === "team" ? "Team" : "Personal"}
                    </p>
                    {deadline.description ? (
                      <p className="mt-1 text-[11px] text-zinc-500 line-clamp-2">
                        {deadline.description}
                      </p>
                    ) : null}
                  </div>
                  <div className="flex-shrink-0 ml-3">
                    <span className="inline-flex items-center rounded-full bg-amber-500/10 border border-amber-500/20 px-2.5 py-1 text-xs font-medium text-amber-400">
                      {getDaysLeft(deadline.due_at)}d
                    </span>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
