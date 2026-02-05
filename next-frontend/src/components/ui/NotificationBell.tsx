"use client";

import { useState, useRef, useEffect } from "react";
import { Bell, X, Check, Settings } from "lucide-react";
import { cn } from "@app/lib/utils";

type Notification = {
  id: string;
  type: "tag" | "approval" | "system";
  title: string;
  message: string;
  timestamp: string;
  action?: {
    label: string;
    onClick: () => void;
  };
};

const mockNotifications: Notification[] = [
  {
    id: "1",
    type: "tag",
    title: "Tagged in Document",
    message: "Sarah tagged you in 'Q3 Pitch Deck'",
    timestamp: "5m ago",
    action: {
      label: "Review",
      onClick: () => console.log("Review Q3 Pitch Deck"),
    },
  },
  {
    id: "2",
    type: "approval",
    title: "Status Update",
    message: "John moved 'Press Release' to Approved.",
    timestamp: "12m ago",
    action: {
      label: "View",
      onClick: () => console.log("View Press Release"),
    },
  },
  {
    id: "3",
    type: "system",
    title: "System Alert",
    message: "New file uploaded to Housing Justice campaign.",
    timestamp: "1h ago",
  },
];

export function NotificationBell() {
  const [isOpen, setIsOpen] = useState(false);
  const [notifications, setNotifications] = useState<Notification[]>(mockNotifications);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }

    if (isOpen) {
      document.addEventListener("mousedown", handleClickOutside);
    }

    return () => {
      document.removeEventListener("mousedown", handleClickOutside);
    };
  }, [isOpen]);

  const unreadCount = notifications.length;
  const hasUnread = unreadCount > 0;

  const handleDismiss = (id: string) => {
    setNotifications((prev) => prev.filter((n) => n.id !== id));
  };

  const handleAction = (notification: Notification) => {
    if (notification.action) {
      notification.action.onClick();
      handleDismiss(notification.id);
    }
  };

  return (
    <div className="relative" ref={dropdownRef}>
      {/* Bell Button */}
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className="relative rounded-lg p-2 text-slate-600 transition-colors hover:bg-slate-100 hover:text-slate-900"
        aria-label="Notifications"
      >
        <Bell className="h-5 w-5" />
        {hasUnread && (
          <span className="absolute right-1 top-1 flex h-2 w-2">
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-amber-400 opacity-75" />
            <span className="relative inline-flex h-2 w-2 rounded-full bg-amber-500" />
          </span>
        )}
      </button>

      {/* Dropdown Panel */}
      {isOpen && (
        <div className="absolute right-0 top-12 z-50 w-96 rounded-xl border border-slate-200 bg-white/95 backdrop-blur-md shadow-2xl animate-in fade-in zoom-in-95 duration-200">
          {/* Header */}
          <div className="flex items-center justify-between border-b border-slate-200 px-5 py-4">
            <div className="flex items-center gap-2">
              <Bell className="h-4 w-4 text-slate-600" />
              <h3 className="text-sm font-semibold text-slate-900">
                Notifications {hasUnread && `(${unreadCount})`}
              </h3>
            </div>
            <button
              type="button"
              onClick={() => setIsOpen(false)}
              className="rounded-lg p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
              aria-label="Close"
            >
              <X className="h-4 w-4" />
            </button>
          </div>

          {/* Notifications List */}
          <div className="max-h-[400px] overflow-y-auto [&::-webkit-scrollbar]:hidden [-ms-overflow-style:'none'] [scrollbar-width:'none']">
            {notifications.length === 0 ? (
              <div className="px-5 py-8 text-center text-sm text-slate-500">
                No notifications
              </div>
            ) : (
              <div className="divide-y divide-slate-100">
                {notifications.map((notification) => (
                  <div
                    key={notification.id}
                    className={cn(
                      "px-5 py-4 transition-colors hover:bg-slate-50",
                      notification.type === "tag" && "bg-amber-50/30",
                      notification.type === "approval" && "bg-emerald-50/30"
                    )}
                  >
                    <div className="flex items-start gap-3">
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between gap-2">
                          <p className="text-xs font-medium text-slate-500">
                            {notification.title}
                          </p>
                          <span className="text-xs text-slate-400">
                            {notification.timestamp}
                          </span>
                        </div>
                        <p className="mt-1 text-sm text-slate-700">
                          {notification.message}
                        </p>
                        {notification.action && (
                          <button
                            type="button"
                            onClick={() => handleAction(notification)}
                            className="mt-2 text-xs font-medium text-amber-600 hover:text-amber-700 hover:underline"
                          >
                            {notification.action.label} →
                          </button>
                        )}
                      </div>
                      <button
                        type="button"
                        onClick={() => handleDismiss(notification.id)}
                        className="flex-shrink-0 rounded-lg p-1 text-slate-400 hover:bg-slate-100 hover:text-slate-600"
                        aria-label="Dismiss"
                      >
                        <X className="h-3.5 w-3.5" />
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Footer */}
          {notifications.length > 0 && (
            <div className="border-t border-slate-200 px-5 py-3">
              <button
                type="button"
                className="flex w-full items-center justify-center gap-2 rounded-lg px-3 py-2 text-xs font-medium text-slate-600 transition-colors hover:bg-slate-100"
              >
                <Settings className="h-3.5 w-3.5" />
                Notification Settings
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

