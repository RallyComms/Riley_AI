"use client";

import { useState, useRef, useEffect } from "react";
import { Bot, Sparkles, X, AlertCircle, Calendar, FileText, Minus, MessageCircle } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence, useMotionValue } from "framer-motion";

type Notification = {
  id: string;
  type: "slack" | "calendar" | "system" | "mention";
  icon: React.ReactNode;
  message: string;
  isCritical?: boolean;
};

// Felix Scenario - High-Value Alerts
const mockNotifications: Notification[] = [
  {
    id: "1",
    type: "slack",
    icon: <AlertCircle className="h-4 w-4" />,
    message: "Felix (CEO) posted in #general: 'Q4 Strategy shift - all hands on deck.'",
    isCritical: true,
  },
  {
    id: "2",
    type: "calendar",
    icon: <Calendar className="h-4 w-4" />,
    message: "Client Review: Clean Water Campaign starts in 15 mins.",
    isCritical: false,
  },
  {
    id: "3",
    type: "system",
    icon: <FileText className="h-4 w-4" />,
    message: "3 New Documents uploaded to Housing Justice.",
    isCritical: false,
  },
];

interface SavedPosition {
  x: number;
  y: number;
}

const STORAGE_KEY = "riley_orb_pos";
const DEFAULT_POSITION = { x: 0, y: 0 }; // Relative to bottom-right corner

export function GlobalRileyOrb() {
  const [isCollapsed, setIsCollapsed] = useState(true);
  const [notifications, setNotifications] = useState<Notification[]>(mockNotifications);
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

  // Listen for riley-notification custom events
  useEffect(() => {
    function handleRileyNotification(event: CustomEvent) {
      const { type, message, author } = event.detail;
      
      if (type === "mention") {
        const newNotification: Notification = {
          id: Date.now().toString(),
          type: "mention",
          icon: <MessageCircle className="h-4 w-4" />,
          message: `${author} mentioned you: "${message}"`,
          isCritical: true, // Mentions are critical alerts
        };
        
        setNotifications((prev) => [newNotification, ...prev]);
      }
    }

    window.addEventListener("riley-notification", handleRileyNotification as EventListener);

    return () => {
      window.removeEventListener("riley-notification", handleRileyNotification as EventListener);
    };
  }, []);

  const alertCount = notifications.length;
  const hasNewAlerts = notifications.some((n) => n.isCritical);
  const constraints = getConstraints();

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
                    <Bot className="h-5 w-5 text-white" />
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
                  {notifications.map((notification) => (
                    <div
                      key={notification.id}
                      className={cn(
                        "rounded-lg border p-3 transition-all hover:border-zinc-700",
                        notification.isCritical
                          ? "border-red-500/50 bg-red-500/10 hover:bg-red-500/15"
                          : "border-zinc-800 bg-zinc-800/30 hover:bg-zinc-800/50"
                      )}
                    >
                      <div className="flex items-start gap-3">
                        <div
                          className={cn(
                            "mt-0.5 flex h-6 w-6 flex-shrink-0 items-center justify-center rounded-full",
                            notification.type === "slack" && notification.isCritical
                              ? "bg-red-500/20 text-red-400"
                              : notification.type === "slack"
                              ? "bg-amber-500/20 text-amber-400"
                              : notification.type === "calendar"
                              ? "bg-blue-500/20 text-blue-400"
                              : notification.type === "mention"
                              ? "bg-amber-500/20 text-amber-400"
                              : "bg-emerald-500/20 text-emerald-400"
                          )}
                        >
                          {notification.icon}
                        </div>
                        <div className="flex-1 min-w-0">
                          <p className="text-sm text-zinc-300">{notification.message}</p>
                        </div>
                      </div>
                    </div>
                  ))}
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
        <Bot className="relative h-6 w-6 text-white" />
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

