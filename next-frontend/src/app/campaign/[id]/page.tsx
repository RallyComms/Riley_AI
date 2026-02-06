"use client";

import { useParams } from "next/navigation";
import { Users, FileText, Calendar } from "lucide-react";
import { cn } from "@app/lib/utils";

// Mock data
const recentActivity = [
  { id: "1", type: "upload", file: "Q3_Polling_Data.pdf", user: "Sarah Chen", time: "2 hours ago" },
  { id: "2", type: "upload", file: "Candidate_Bio_Draft.docx", user: "John Martinez", time: "5 hours ago" },
  { id: "3", type: "upload", file: "Strategic_Plan_Q1.xlsx", user: "Alex Rivera", time: "1 day ago" },
];

const teamStatus = [
  { name: "Sarah Chen", role: "Lead Strategist", status: "online" },
  { name: "John Martinez", role: "Media Expert", status: "online" },
  { name: "Alex Rivera", role: "Research Lead", status: "away" },
  { name: "Emma Wilson", role: "Policy Analyst", status: "offline" },
];

const upcomingDeadlines = [
  { id: "1", title: "Q3 Report Submission", date: "Jan 25, 2024", daysLeft: 3 },
  { id: "2", title: "Client Review Meeting", date: "Jan 28, 2024", daysLeft: 6 },
  { id: "3", title: "Campaign Launch Prep", date: "Feb 1, 2024", daysLeft: 10 },
];

// Helper function to format campaign name from ID
function formatCampaignName(campaignId: string): string {
  return campaignId
    .split("-")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
}

export default function CampaignOverviewPage() {
  const params = useParams();
  const campaignId = params.id as string;
  const campaignName = formatCampaignName(campaignId);

  return (
    <div className="min-h-full p-6 bg-transparent">
      {/* Header */}
      <div className="mb-8 bg-transparent">
        <h1 className="text-3xl font-bold tracking-tight text-zinc-100 mb-2">
          {campaignName} Dashboard
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
            {recentActivity.map((activity) => (
              <div
                key={activity.id}
                className="flex items-start gap-3 p-3 rounded-lg bg-zinc-800/30 hover:bg-zinc-800/50 transition-colors"
              >
                <div className="flex-shrink-0 mt-0.5">
                  <div className="h-2 w-2 rounded-full bg-emerald-500" />
                </div>
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-zinc-100 truncate">{activity.file}</p>
                  <p className="text-xs text-zinc-500 mt-0.5">
                    {activity.user} • {activity.time}
                  </p>
                </div>
              </div>
            ))}
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
          <div className="flex items-center gap-2 mb-4">
            <Calendar className="h-5 w-5 text-zinc-400" />
            <h2 className="text-lg font-semibold text-zinc-100">Upcoming Deadlines</h2>
          </div>
          <div className="space-y-3">
            {upcomingDeadlines.map((deadline) => (
              <div
                key={deadline.id}
                className="flex items-center justify-between p-3 rounded-lg bg-zinc-800/30 hover:bg-zinc-800/50 transition-colors"
              >
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-zinc-100 truncate">{deadline.title}</p>
                  <p className="text-xs text-zinc-500 mt-0.5">{deadline.date}</p>
                </div>
                <div className="flex-shrink-0 ml-3">
                  <span className="inline-flex items-center rounded-full bg-amber-500/10 border border-amber-500/20 px-2.5 py-1 text-xs font-medium text-amber-400">
                    {deadline.daysLeft}d
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
