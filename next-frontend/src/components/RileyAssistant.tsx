"use client";

import { useState } from "react";
import { Bot, Sparkles, X, AlertCircle, Calendar, FileText, ChevronUp } from "lucide-react";
import { cn } from "@app/lib/utils";

type Notification = {
  id: string;
  type: "slack" | "calendar" | "upload";
  icon: React.ReactNode;
  message: string;
  hasSummary?: boolean;
  summary?: string;
};

const mockNotifications: Notification[] = [
  {
    id: "1",
    type: "slack",
    icon: <AlertCircle className="h-4 w-4" />,
    message: "Urgent: VP of Comms posted in #general-announcements.",
    hasSummary: true,
    summary: "The VP of Communications shared an update about the upcoming press conference regarding the housing policy initiative.",
  },
  {
    id: "2",
    type: "calendar",
    icon: <Calendar className="h-4 w-4" />,
    message: "Reminder: Client Strategy Review starts in 15 mins.",
    hasSummary: false,
  },
  {
    id: "3",
    type: "upload",
    icon: <FileText className="h-4 w-4" />,
    message: "New Upload: 'Q3_Budget_Draft.xlsx' added to Housing Justice bucket.",
    hasSummary: false,
  },
];

export function RileyAssistant() {
  const [notifications] = useState<Notification[]>(mockNotifications);
  const [showSummary, setShowSummary] = useState<string | null>(null);
  const [isExpanded, setIsExpanded] = useState<boolean>(true);

  const handleReadSummary = (notification: Notification) => {
    if (notification.hasSummary && notification.summary) {
      setShowSummary(notification.summary);
    }
  };

  const closeSummary = () => {
    setShowSummary(null);
  };

  const alertCount = notifications.length;

  return (
    <div className="relative">
      {isExpanded ? (
        /* Expanded View: Full Card - Floating Overlay */
        <div className="absolute top-0 right-0 z-50 w-[400px] rounded-2xl border border-zinc-800 bg-zinc-900/95 backdrop-blur-md shadow-2xl animate-[fadeIn_0.2s_ease-out,zoomIn_0.2s_ease-out]">
          {/* Header with Bot Icon */}
          <div className="flex items-center gap-3 border-b border-zinc-800 bg-zinc-900/50 px-5 py-4">
            <div className="relative">
              <div className="absolute inset-0 animate-pulse rounded-full bg-blue-400/20 blur-md" />
              <div className="relative flex h-10 w-10 items-center justify-center rounded-full bg-gradient-to-br from-blue-500 to-indigo-600 shadow-sm">
                <Bot className="h-5 w-5 text-white" aria-hidden="true" />
              </div>
            </div>
            <div className="flex-1">
              <h3 className="text-sm font-semibold text-zinc-100">Riley Assistant</h3>
              <p className="text-xs text-zinc-400">Proactive Intelligence Feed</p>
            </div>
            <div className="flex items-center gap-2">
              <Sparkles className="h-4 w-4 text-blue-400" aria-hidden="true" />
              <button
                type="button"
                onClick={() => setIsExpanded(false)}
                className="rounded-lg p-1.5 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors"
                aria-label="Minimize"
              >
                <ChevronUp className="h-4 w-4" aria-hidden="true" />
              </button>
            </div>
          </div>

          {/* Notifications Feed - Read Only */}
          <div className="max-h-[500px] overflow-y-auto p-4 [&::-webkit-scrollbar]:hidden [-ms-overflow-style:'none'] [scrollbar-width:'none']">
            {notifications.length === 0 ? (
              /* Zero State */
              <div className="flex flex-col items-center justify-center py-12 text-center">
                <div className="text-4xl mb-3">🎉</div>
                <p className="text-sm text-zinc-400">
                  You're all caught up! No new alerts.
                </p>
              </div>
            ) : (
              <div className="space-y-3">
                {notifications.map((notification) => (
                  <div
                    key={notification.id}
                    className={cn(
                      "rounded-lg border border-zinc-800 bg-zinc-800/30 p-3 transition-all hover:border-zinc-700 hover:bg-zinc-800/50",
                      notification.type === "slack" && "border-amber-500/30 bg-amber-500/5"
                    )}
                  >
                    <div className="flex items-start gap-3">
                      <div
                        className={cn(
                          "mt-0.5 flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full",
                          notification.type === "slack" && "bg-amber-500/20 text-amber-400",
                          notification.type === "calendar" && "bg-blue-500/20 text-blue-400",
                          notification.type === "upload" && "bg-emerald-500/20 text-emerald-400"
                        )}
                      >
                        {notification.icon}
                      </div>
                      <div className="flex-1 min-w-0">
                        <p className="text-sm text-zinc-300">{notification.message}</p>
                        {notification.hasSummary && (
                          <button
                            type="button"
                            onClick={() => handleReadSummary(notification)}
                            className="mt-2 text-xs font-medium text-blue-400 hover:text-blue-300 hover:underline"
                          >
                            Read Summary →
                          </button>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      ) : (
        /* Minimized View: Pill/Badge */
        <button
          type="button"
          onClick={() => setIsExpanded(true)}
          className="inline-flex items-center gap-2 rounded-full border border-zinc-800 bg-zinc-900/60 backdrop-blur-sm px-4 py-2.5 shadow-sm transition-all hover:border-zinc-700 hover:shadow-md"
        >
          <div className="relative">
            <div className="absolute inset-0 animate-pulse rounded-full bg-blue-400/20 blur-sm" />
            <div className="relative flex h-6 w-6 items-center justify-center rounded-full bg-gradient-to-br from-blue-500 to-indigo-600">
              <Bot className="h-3.5 w-3.5 text-white" aria-hidden="true" />
            </div>
          </div>
          <span className="text-sm font-medium text-zinc-300">
            Riley is active ({alertCount} alert{alertCount !== 1 ? "s" : ""})
          </span>
          <Sparkles className="h-3.5 w-3.5 text-blue-400" aria-hidden="true" />
        </button>
      )}

      {/* Summary Modal/Toast */}
      {showSummary && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={closeSummary}>
          <div
            className="relative w-full max-w-md rounded-2xl border border-zinc-800 bg-zinc-900/95 backdrop-blur-xl shadow-xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between border-b border-zinc-800 bg-zinc-900/50 px-5 py-4">
              <div className="flex items-center gap-2">
                <Bot className="h-5 w-5 text-blue-400" aria-hidden="true" />
                <h3 className="text-sm font-semibold text-zinc-100">Riley Summary</h3>
              </div>
              <button
                type="button"
                onClick={closeSummary}
                className="rounded-lg p-1 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100"
              >
                <X className="h-4 w-4" aria-hidden="true" />
              </button>
            </div>
            <div className="px-5 py-4">
              <p className="text-sm leading-relaxed text-zinc-300">{showSummary}</p>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
