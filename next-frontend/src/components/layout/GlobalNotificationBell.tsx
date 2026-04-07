"use client";

import { useEffect, useRef, useState } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { usePathname, useRouter } from "next/navigation";
import { Bell, CheckCircle2 } from "lucide-react";
import { apiFetch } from "@app/lib/api";

interface NotificationItem {
  id: string;
  user_id: string;
  type: string;
  entity_id?: string | null;
  campaign_id?: string | null;
  message: string;
  status: "unread" | "read" | "completed";
  created_at?: string | null;
}

interface NotificationsResponse {
  notifications: NotificationItem[];
}

interface NotificationsUnreadCountResponse {
  unread_count: number;
}

function notificationTimeLabel(iso?: string | null): string {
  if (!iso) return "Just now";
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return "Just now";
  return date.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function notificationOpenPath(notification: NotificationItem): string {
  const campaignId = notification.campaign_id;
  if (!campaignId) return "/campaign";
  if (notification.type === "campaign_message_mentioned_user") {
    return `/campaign/${encodeURIComponent(campaignId)}/conversations`;
  }
  if (notification.type === "document_mentioned_user") {
    return `/campaign/${encodeURIComponent(campaignId)}/assets`;
  }
  return `/campaign/${encodeURIComponent(campaignId)}`;
}

export function GlobalNotificationBell() {
  const { getToken, isLoaded } = useAuth();
  const { user } = useUser();
  const router = useRouter();
  const pathname = usePathname();
  const containerRef = useRef<HTMLDivElement | null>(null);

  const [isOpen, setIsOpen] = useState(false);
  const [notifications, setNotifications] = useState<NotificationItem[]>([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dismissLoadingId, setDismissLoadingId] = useState<string | null>(null);
  const [decisionLoadingId, setDecisionLoadingId] = useState<string | null>(null);

  const refreshNotifications = async () => {
    if (!isLoaded || !user?.id) {
      setNotifications([]);
      setError(null);
      return;
    }
    try {
      setIsLoading(true);
      setError(null);
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");
      const data = await apiFetch<NotificationsResponse>("/api/v1/notifications?limit=10", {
        token,
        method: "GET",
      });
      setNotifications(data.notifications || []);
    } catch (err) {
      setNotifications([]);
      setError(err instanceof Error ? err.message : "Unable to load notifications.");
    } finally {
      setIsLoading(false);
    }
  };

  const refreshUnreadCount = async () => {
    if (!isLoaded || !user?.id) {
      setUnreadCount(0);
      return;
    }
    try {
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");
      const data = await apiFetch<NotificationsUnreadCountResponse>(
        "/api/v1/notifications/unread-count",
        {
          token,
          method: "GET",
        },
      );
      setUnreadCount(Math.max(0, Number(data.unread_count || 0)));
    } catch {
      // Keep previous unread badge state on transient failures.
    }
  };

  useEffect(() => {
    void refreshUnreadCount();
  }, [isLoaded, user?.id, getToken, pathname]);

  useEffect(() => {
    if (!isOpen) return;
    const onPointerDown = (event: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", onPointerDown);
    return () => {
      document.removeEventListener("mousedown", onPointerDown);
    };
  }, [isOpen]);

  const toggleOpen = () => {
    const nextOpen = !isOpen;
    setIsOpen(nextOpen);
    if (nextOpen) void refreshNotifications();
  };

  const handleNotificationMarkRead = async (notificationId: string) => {
    try {
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");
      await apiFetch(`/api/v1/notifications/${encodeURIComponent(notificationId)}/mark-read`, {
        token,
        method: "POST",
      });
      setNotifications((prev) =>
        prev.map((item) =>
          item.id === notificationId
            ? { ...item, status: "read" }
            : item,
        ),
      );
      void refreshUnreadCount();
    } catch (err) {
      alert(`Failed to mark as read: ${err instanceof Error ? err.message : "Unknown error"}`);
    }
  };

  const handleNotificationDismiss = async (notificationId: string) => {
    try {
      setDismissLoadingId(notificationId);
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");
      await apiFetch(`/api/v1/notifications/${encodeURIComponent(notificationId)}/complete`, {
        token,
        method: "POST",
      });
      setNotifications((prev) => prev.filter((item) => item.id !== notificationId));
      void refreshUnreadCount();
    } catch (err) {
      alert(`Failed to dismiss notification: ${err instanceof Error ? err.message : "Unknown error"}`);
    } finally {
      setDismissLoadingId(null);
    }
  };

  const handleAccessDecision = async (
    notification: NotificationItem,
    action: "approve" | "deny",
  ) => {
    const requestId = String(notification.entity_id || "").trim();
    const campaignId = String(notification.campaign_id || "").trim();
    if (!requestId || !campaignId) return;
    try {
      setDecisionLoadingId(notification.id);
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");
      await apiFetch(
        `/api/v1/campaigns/${encodeURIComponent(campaignId)}/access-requests/${encodeURIComponent(requestId)}/decision`,
        {
          token,
          method: "POST",
          body: { action },
        },
      );
      await apiFetch(`/api/v1/notifications/${encodeURIComponent(notification.id)}/complete`, {
        token,
        method: "POST",
      });
      setNotifications((prev) => prev.filter((item) => item.id !== notification.id));
      void refreshUnreadCount();
      void refreshNotifications();
    } catch (err) {
      alert(`Failed to ${action} request: ${err instanceof Error ? err.message : "Unknown error"}`);
    } finally {
      setDecisionLoadingId(null);
    }
  };

  const handleNotificationOpen = async (notification: NotificationItem) => {
    try {
      const token = await getToken();
      if (token && notification.status === "unread") {
        await apiFetch(`/api/v1/notifications/${encodeURIComponent(notification.id)}/mark-read`, {
          token,
          method: "POST",
        });
      }
    } catch {
      // Navigation is primary action; do not block on mark-read failure.
    }
    setNotifications((prev) =>
      prev.map((item) =>
        item.id === notification.id
          ? { ...item, status: item.status === "unread" ? "read" : item.status }
          : item,
      ),
    );
    if (notification.status === "unread") {
      setUnreadCount((prev) => Math.max(0, prev - 1));
    }
    void refreshUnreadCount();
    router.push(notificationOpenPath(notification));
    setIsOpen(false);
  };

  if (!isLoaded || !user?.id) {
    return null;
  }

  const bellUi = (
    <div className="relative" ref={containerRef}>
      <button
        type="button"
        onClick={toggleOpen}
        aria-label="Open notifications"
        className="relative inline-flex h-9 w-9 items-center justify-center rounded-md border border-[#ddd5c5] bg-[#fbf8f2] text-[#4d5871] shadow-sm hover:bg-[#f1ece2]"
      >
        <Bell className="h-4 w-4" />
        {unreadCount > 0 ? (
          <span className="absolute right-2 top-2 h-2 w-2 rounded-full bg-[#d4ad47]" />
        ) : null}
      </button>

      {isOpen ? (
        <div className="absolute right-0 z-30 mt-2 w-[24rem] overflow-hidden rounded-xl border border-[#e6dece] bg-[#fbf8f2] shadow-[0_14px_32px_rgba(31,42,68,0.14)]">
          <div className="flex items-center justify-between border-b border-[#ece4d4] px-4 py-3">
            <h3 className="font-serif text-base font-medium text-[#1f2a44]">Notifications</h3>
            <button
              type="button"
              onClick={() => void refreshNotifications()}
              className="rounded-md border border-[#ddd5c5] px-2 py-1 text-xs font-medium text-[#4d5871] hover:bg-[#f1ece2]"
            >
              Refresh
            </button>
          </div>
          {isLoading ? (
            <div className="px-4 py-5 text-sm text-[#5b6578]">Loading notifications...</div>
          ) : error ? (
            <div className="px-4 py-5 text-sm text-[#9e3434]">{error}</div>
          ) : notifications.length === 0 ? (
            <div className="px-4 py-6 text-sm text-[#5b6578]">No new notifications</div>
          ) : (
            <ul className="max-h-[28rem] divide-y divide-[#ece4d4] overflow-y-auto">
              {notifications.map((notification) => {
                const statusValue = String(notification.status || "unread");
                const isRead = statusValue === "read" || statusValue === "completed";
                const canResolveAccess =
                  notification.type === "access_request_created" &&
                  Boolean(notification.entity_id) &&
                  Boolean(notification.campaign_id);
                const isMention =
                  notification.type === "document_mentioned_user" ||
                  notification.type === "campaign_message_mentioned_user";
                return (
                  <li key={notification.id} className="px-4 py-3">
                    <div className="mb-1 flex items-center gap-2">
                      <span
                        className={`inline-block h-2 w-2 rounded-full ${
                          isRead ? "bg-[#bfd3c9]" : "bg-[#d4ad47]"
                        }`}
                      />
                      <span className="text-xs text-[#7b8394]">
                        {notificationTimeLabel(notification.created_at)}
                      </span>
                    </div>
                    <p className="text-sm font-medium text-[#1f2a44]">
                      {notification.message || "Campaign update"}
                    </p>
                    <div className="mt-2 flex flex-wrap items-center gap-2">
                      {canResolveAccess ? (
                        <>
                          <button
                            type="button"
                            onClick={() => void handleAccessDecision(notification, "approve")}
                            disabled={decisionLoadingId === notification.id}
                            className="rounded-md border border-[#8db9a6] px-2.5 py-1.5 text-xs font-medium text-[#2e5e4b] hover:bg-[#e4efe9] disabled:opacity-60"
                          >
                            Approve
                          </button>
                          <button
                            type="button"
                            onClick={() => void handleAccessDecision(notification, "deny")}
                            disabled={decisionLoadingId === notification.id}
                            className="rounded-md border border-[#d8b4b4] px-2.5 py-1.5 text-xs font-medium text-[#7f3b3b] hover:bg-[#f7e9e9] disabled:opacity-60"
                          >
                            Deny
                          </button>
                        </>
                      ) : null}
                      {isMention ? (
                        <button
                          type="button"
                          onClick={() => void handleNotificationOpen(notification)}
                          className="rounded-md border border-[#cfd9ee] px-2.5 py-1.5 text-xs font-medium text-[#385a8f] hover:bg-[#edf2fb]"
                        >
                          Open
                        </button>
                      ) : null}
                      {!isRead ? (
                        <button
                          type="button"
                          onClick={() => void handleNotificationMarkRead(notification.id)}
                          className="inline-flex items-center gap-1 rounded-md border border-[#ddd5c5] px-2.5 py-1.5 text-xs font-medium text-[#4d5871] hover:bg-[#f1ece2]"
                        >
                          <CheckCircle2 className="h-3.5 w-3.5" />
                          Mark read
                        </button>
                      ) : null}
                      <button
                        type="button"
                        onClick={() => void handleNotificationDismiss(notification.id)}
                        disabled={dismissLoadingId === notification.id}
                        className="rounded-md border border-[#ddd5c5] px-2.5 py-1.5 text-xs font-medium text-[#4d5871] hover:bg-[#f1ece2] disabled:opacity-60"
                      >
                        Dismiss
                      </button>
                    </div>
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      ) : null}
    </div>
  );

  return bellUi;
}

