"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { usePathname } from "next/navigation";
import { Minus, AlertCircle, Calendar, FileText, MessageCircle, UserPlus } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence, useMotionValue } from "framer-motion";
import { apiFetch } from "@app/lib/api";

type FeedEvent = {
  id: string;
  type: string;
  message: string;
  campaign_id: string;
  campaign_name?: string | null;
  created_at?: string | null;
  user_id?: string | null;
  actor_user_id?: string | null;
  request_id?: string | null;
  requester_display_name?: string | null;
  member_role?: string | null;
};

interface SavedPosition {
  x: number;
  y: number;
}

const STORAGE_KEY = "riley_orb_pos";
const DEFAULT_POSITION = { x: 0, y: 0 }; // Relative to bottom-right corner

export function GlobalRileyOrb() {
  const pathname = usePathname();
  const isMissionControlRoute = pathname?.startsWith("/mission-control");
  const { user } = useUser();
  const { getToken, isLoaded } = useAuth();
  const [isCollapsed, setIsCollapsed] = useState(true);
  const [notifications, setNotifications] = useState<FeedEvent[]>([]);
  const [dismissLoadingEventId, setDismissLoadingEventId] = useState<string | null>(null);
  const [requestActionLoadingEventId, setRequestActionLoadingEventId] = useState<string | null>(null);
  const getAccessRequesterLabel = (event: FeedEvent) =>
    event.requester_display_name?.trim() || event.user_id?.trim() || "Unknown user";
  const popoverRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  
  // Position state - stored as offset from bottom-right corner
  const [position, setPosition] = useState<SavedPosition>(DEFAULT_POSITION);

  
  // Motion values for dragging
  const x = useMotionValue(0);
  const y = useMotionValue(0);

  // Load saved position from localStorage on mount
  useEffect(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      if (saved) {
        const parsed = JSON.parse(saved) as SavedPosition;
        setPosition(parsed);
        x.set(parsed.x);
        y.set(parsed.y);
      }
    } catch (err) {
      console.error("Failed to load saved orb position:", err);
    }
  }, [x, y]);

  // Update constraints on window resize
  useEffect(() => {
    const handleResize = () => {
      // Constraints will be recalculated on next render
    };
    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const fetchFeedRef = useRef<() => Promise<void>>();

  useEffect(() => {
    let mounted = true;
    const fetchFeed = async () => {
      if (isMissionControlRoute) return;
      if (!isLoaded || !user?.id) return;
      try {
        const token = await getToken();
        if (!token) return;
        const data = await apiFetch<{ events: FeedEvent[] }>(
          "/api/v1/campaigns/events/feed?limit=40",
          {
            token,
            method: "GET",
          }
        );
        if (mounted) {
          setNotifications(data.events || []);
        }
      } catch (err) {
        console.error("Failed to load Riley intelligence feed:", err);
      }
    };
    fetchFeedRef.current = fetchFeed;

    fetchFeed();
    const interval = setInterval(fetchFeed, 15000);

    const handleVisibility = () => {
      if (document.visibilityState === "visible") {
        fetchFeed();
      }
    };
    document.addEventListener("visibilitychange", handleVisibility);

    return () => {
      mounted = false;
      clearInterval(interval);
      document.removeEventListener("visibilitychange", handleVisibility);
    };
  }, [getToken, isLoaded, isMissionControlRoute, user?.id]);

  const getEventVisual = (eventType: string) => {
    if (eventType === "access_request_created") {
      return {
        icon: <AlertCircle className="h-4 w-4" />,
        iconClass: "bg-red-500/20 text-red-400",
        critical: true,
      };
    }
    if (eventType === "mention") {
      return {
        icon: <MessageCircle className="h-4 w-4" />,
        iconClass: "bg-amber-500/20 text-amber-400",
        critical: true,
      };
    }
    if (eventType === "deadline_created" || eventType === "deadline_upcoming") {
      return {
        icon: <Calendar className="h-4 w-4" />,
        iconClass: "bg-blue-500/20 text-blue-400",
        critical: false,
      };
    }
    if (
      eventType === "campaign_member_added"
      || eventType === "campaign_member_added_notification"
      || eventType === "access_request_approved"
    ) {
      return {
        icon: <UserPlus className="h-4 w-4" />,
        iconClass: "bg-emerald-500/20 text-emerald-400",
        critical: false,
      };
    }
    return {
      icon: <FileText className="h-4 w-4" />,
      iconClass: "bg-zinc-600/20 text-zinc-300",
      critical: false,
    };
  };

  const refreshFeed = useCallback(() => {
    fetchFeedRef.current?.();
  }, []);

  const handleDismiss = async (eventId: string) => {
    try {
      setDismissLoadingEventId(eventId);
      const token = await getToken();
      if (!token) return;
      await apiFetch(`/api/v1/campaigns/events/feed/${encodeURIComponent(eventId)}/dismiss`, {
        token,
        method: "POST",
      });
      setNotifications((prev) => prev.filter((event) => event.id !== eventId));
      refreshFeed();
    } catch (err) {
      console.error("Failed to dismiss feed event:", err);
    } finally {
      setDismissLoadingEventId(null);
    }
  };

  const handleAccessDecision = async (
    event: FeedEvent,
    action: "approve" | "deny"
  ) => {
    if (!event.request_id) return;
    try {
      setRequestActionLoadingEventId(event.id);
      const token = await getToken();
      if (!token) return;
      await apiFetch(
        `/api/v1/campaigns/${encodeURIComponent(event.campaign_id)}/access-requests/${encodeURIComponent(event.request_id)}/decision`,
        {
          token,
          method: "POST",
          body: { action },
        }
      );
      setNotifications((prev) => prev.filter((item) => item.id !== event.id));
      refreshFeed();
    } catch (err) {
      console.error("Failed to resolve access request from feed:", err);
    } finally {
      setRequestActionLoadingEventId(null);
    }
  };

  // Calculate viewport constraints
  const getConstraints = () => {
    if (typeof window === "undefined" || !containerRef.current) {
      return { left: -Infinity, right: Infinity, top: -Infinity, bottom: Infinity };
    }
    
    const orbSize = 56; // h-14 w-14 = 56px
    const padding = 24; // 6 * 4 = 24px (bottom-6 right-6)
    const viewportWidth = window.innerWidth;
    const viewportHeight = window.innerHeight;
    
    // Constraints relative to bottom-right corner
    // Left constraint: can't go further left than viewport edge
    const left = -(viewportWidth - orbSize - padding);
    // Right constraint: can't go further right than default position
    const right = 0;
    // Top constraint: can't go above viewport (accounting for orb size)
    const top = -(viewportHeight - orbSize - padding);
    // Bottom constraint: can't go below default position
    const bottom = 0;
    
    return { left, right, top, bottom };
  };

  // Save position to localStorage on drag end
  const handleDragEnd = () => {
    const newPosition = { x: x.get(), y: y.get() };
    setPosition(newPosition);
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(newPosition));
    } catch (err) {
      console.error("Failed to save orb position:", err);
    }
  };

  // Close popover when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (popoverRef.current && !popoverRef.current.contains(event.target as Node)) {
        setIsCollapsed(true);
      }
    }

    if (!isCollapsed) {
      document.addEventListener("mousedown", handleClickOutside);
    }

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [isCollapsed]);

  const alertCount = notifications.length;
  const hasNewAlerts = notifications.some((n) => getEventVisual(n.type).critical);
  const constraints = getConstraints();

  if (isMissionControlRoute) {
    return null;
  }

  return (
    <motion.div
      ref={containerRef}
      className="fixed bottom-6 right-6 z-50"
      style={{
        x,
        y,
      }}
      drag
      dragMomentum={false}
      dragConstraints={constraints}
      dragElastic={0}
      onDragEnd={handleDragEnd}
      whileDrag={{ cursor: "grabbing" }}
    >
      <div ref={popoverRef}>
      {/* Expanded Panel - Notification Feed */}
      <AnimatePresence>
        {!isCollapsed && (
          <motion.div
            initial={{ opacity: 0, scale: 0.95, y: 10 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.95, y: 10 }}
            transition={{ duration: 0.2 }}
            className="absolute bottom-20 right-0 w-96 rounded-2xl border border-zinc-800 bg-zinc-900/95 backdrop-blur-xl shadow-2xl"
          >
            {/* Header */}
            <div className="flex items-center justify-between border-b border-zinc-800 bg-zinc-900/50 px-5 py-4">
              <div className="flex items-center gap-3">
                <div className="relative">
                  <div className="absolute inset-0 animate-pulse rounded-full bg-blue-400/20 blur-md" />
                  <div className="relative flex h-10 w-10 items-center justify-center rounded-full bg-gradient-to-br from-blue-500 to-indigo-600">
                    <span className="text-sm font-semibold text-white">R</span>
                  </div>
                </div>
                <div>
                  <h3 className="text-sm font-semibold text-zinc-100">Intelligence Feed</h3>
                  <p className="text-xs text-zinc-400">Scanning for high-priority updates...</p>
                </div>
              </div>
              <button
                type="button"
                onClick={() => setIsCollapsed(true)}
                className="rounded-lg p-1.5 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors"
                aria-label="Minimize"
              >
                <Minus className="h-4 w-4" />
              </button>
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
                  {notifications.map((event) => {
                    const visual = getEventVisual(event.type);
                    return (
                    <div
                      key={event.id}
                      className={cn(
                        "rounded-lg border p-3 transition-all hover:border-zinc-700",
                        visual.critical
                          ? "border-red-500/50 bg-red-500/10 hover:bg-red-500/15"
                          : "border-zinc-800 bg-zinc-800/30 hover:bg-zinc-800/50"
                      )}
                    >
                      <div className="flex items-start gap-3">
                        <div
                          className={cn(
                            "mt-0.5 flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full",
                            visual.iconClass
                          )}
                        >
                          {visual.icon}
                        </div>
                        <div className="flex-1 min-w-0">
                          <p className="text-sm text-zinc-300">
                            {event.campaign_name
                              ? `${event.campaign_name}: ${
                                  event.type === "access_request_created"
                                    ? `${getAccessRequesterLabel(event)} requested access to this campaign`
                                    : event.message
                                }`
                              : event.type === "access_request_created"
                              ? `${getAccessRequesterLabel(event)} requested access to this campaign`
                              : event.message}
                          </p>
                          <p className="mt-1 text-[11px] text-zinc-500">
                            {event.created_at ? new Date(event.created_at).toLocaleString() : ""}
                          </p>
                          {event.type === "access_request_created" &&
                          event.request_id &&
                          event.member_role === "Lead" ? (
                            <div className="mt-2 flex gap-1.5">
                              <button
                                type="button"
                                onClick={() => handleAccessDecision(event, "approve")}
                                disabled={requestActionLoadingEventId === event.id}
                                className="rounded border border-emerald-600/40 bg-emerald-500/10 px-2 py-1 text-[10px] text-emerald-300 hover:bg-emerald-500/20 disabled:opacity-50"
                              >
                                Accept
                              </button>
                              <button
                                type="button"
                                onClick={() => handleAccessDecision(event, "deny")}
                                disabled={requestActionLoadingEventId === event.id}
                                className="rounded border border-red-600/40 bg-red-500/10 px-2 py-1 text-[10px] text-red-300 hover:bg-red-500/20 disabled:opacity-50"
                              >
                                Deny
                              </button>
                            </div>
                          ) : null}
                        </div>
                        <button
                          type="button"
                          onClick={() => handleDismiss(event.id)}
                          disabled={dismissLoadingEventId === event.id}
                          className="ml-2 rounded border border-zinc-700 px-2 py-1 text-[10px] text-zinc-400 hover:bg-zinc-700/50 hover:text-zinc-200 disabled:opacity-50"
                        >
                          Done
                        </button>
                      </div>
                    </div>
                  )})}
                </div>
              )}
            </div>

            {/* Status Footer */}
            <div className="border-t border-zinc-800 bg-zinc-900/50 px-5 py-3">
              <div className="flex items-center gap-2 text-xs text-zinc-500">
                <div className="h-2 w-2 rounded-full bg-emerald-400" />
                <span>Riley is active</span>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* Collapsed Orb Button */}
      <motion.button
        type="button"
        onClick={() => setIsCollapsed(!isCollapsed)}
        className="relative flex h-14 w-14 items-center justify-center rounded-full bg-gradient-to-br from-blue-500 to-indigo-600 shadow-lg transition-all hover:shadow-xl hover:scale-105 cursor-grab active:cursor-grabbing"
        whileHover={{ scale: 1.05 }}
        whileTap={{ scale: 0.95 }}
        aria-label="Intelligence Feed"
      >
        {/* Pulsing Glow Effect - Only if there are new alerts */}
        {hasNewAlerts && (
          <motion.div
            className="absolute inset-0 rounded-full bg-blue-400/30"
            animate={{
              scale: [1, 1.2, 1],
              opacity: [0.5, 0.3, 0.5],
            }}
            transition={{
              duration: 2,
              repeat: Infinity,
              ease: "easeInOut",
            }}
          />
        )}
        <span className="relative text-base font-semibold text-white">R</span>
        {/* Alert Badge */}
        {alertCount > 0 && (
          <span className="absolute -top-1 -right-1 flex h-5 w-5 items-center justify-center rounded-full bg-red-500 text-xs font-bold text-white">
            {alertCount > 9 ? "9+" : alertCount}
          </span>
        )}
      </motion.button>
      </div>
    </motion.div>
  );
}

