"use client";

import type { CSSProperties } from "react";
import { useState, useRef, useEffect } from "react";
import Link from "next/link";
import { Bell, Shield, Trash2, Archive, RotateCcw } from "lucide-react";
import { motion } from "framer-motion";
import { cn } from "@app/lib/utils";
import { useTheme } from "next-themes";

export interface CampaignBucketCardProps {
  name: string;
  role: string;
  lastActive: string;
  mentions: number;
  pendingRequests?: number;
  userRole?: "Lead" | "Member";
  /**
   * Arbitrary CSS color value used to tint the card.
   * Example: "#22c55e", "rgb(59,130,246)", "hsl(222 84% 56%)"
   */
  themeColor: string;
  campaignId?: string;
  onClick?: () => void;
  onDelete?: (campaignId: string) => void;
  isArchived?: boolean;
  onArchive?: (campaignId: string) => void;
  onRestore?: (campaignId: string) => void;
  onTerminate?: (campaignId: string) => void;
  isTerminating?: boolean;
}

// Sparkline Graph Component
function SparklineGraph({ value, maxValue = 10 }: { value: number; maxValue?: number }) {
  const [isMounted, setIsMounted] = useState(false);

  // Prevent hydration mismatch - only render dynamic content after mount
  useEffect(() => {
    setIsMounted(true);
  }, []);

  // Return empty div with same dimensions during SSR
  if (!isMounted) {
    return (
      <div
        className="overflow-visible"
        style={{ width: "40px", height: "12px" }}
      />
    );
  }

  // Generate random data points for the sparkline
  const points = Array.from({ length: 12 }, (_, i) => {
    const base = (value / maxValue) * 100;
    const variation = Math.random() * 20 - 10;
    return Math.max(10, Math.min(90, base + variation));
  });

  const pathData = points
    .map((y, i) => {
      const x = (i / (points.length - 1)) * 100;
      return `${i === 0 ? "M" : "L"} ${x} ${100 - y}`;
    })
    .join(" ");

  return (
    <svg
      width="40"
      height="12"
      viewBox="0 0 100 100"
      className="overflow-visible"
      style={{ filter: "drop-shadow(0 0 2px rgba(59, 130, 246, 0.6))" }}
    >
      <path
        d={pathData}
        fill="none"
        stroke="rgba(59, 130, 246, 0.8)"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

export function CampaignBucketCard({
  name,
  role,
  lastActive,
  mentions,
  pendingRequests = 0,
  userRole,
  themeColor,
  campaignId,
  onClick,
  onDelete,
  isArchived = false,
  onArchive,
  onRestore,
  onTerminate,
  isTerminating = false,
}: CampaignBucketCardProps) {
  const [isConfirmingDelete, setIsConfirmingDelete] = useState(false);
  const [isConfirmingTerminate, setIsConfirmingTerminate] = useState(false);
  const [mounted, setMounted] = useState(false);
  const cardRef = useRef<HTMLDivElement>(null);
  const { theme } = useTheme();
  const isLightMode = mounted && theme === "light";

  // Prevent hydration mismatch
  useEffect(() => {
    setMounted(true);
  }, []);

  const href = campaignId ? `/campaign/${campaignId}` : "/campaign/clean-water";

  const handleDeleteClick = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    
    if (!onDelete || !campaignId) return;
    
    if (!isConfirmingDelete) {
      setIsConfirmingDelete(true);
      setTimeout(() => setIsConfirmingDelete(false), 3000);
    } else {
      onDelete(campaignId);
      setIsConfirmingDelete(false);
    }
  };

  const handleArchiveClick = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (onArchive && campaignId) {
      onArchive(campaignId);
    }
  };

  const handleRestoreClick = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (onRestore && campaignId) {
      onRestore(campaignId);
    }
  };

  const handleTerminateClick = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    
    if (!onTerminate || !campaignId) return;
    
    if (!isConfirmingTerminate) {
      setIsConfirmingTerminate(true);
      setTimeout(() => setIsConfirmingTerminate(false), 3000);
    } else {
      onTerminate(campaignId);
      setIsConfirmingTerminate(false);
    }
  };

  const handleEnterClick = (e: React.MouseEvent) => {
    if (onClick) {
      onClick();
    }
  };

  const cardContent = (
    <motion.div
      ref={cardRef}
      style={
        {
          "--accent": themeColor,
        } as CSSProperties
      }
      className={cn(
        "group relative flex flex-col items-stretch justify-between overflow-visible",
        isLightMode
          ? "bg-white border-2 border-black shadow-[4px_4px_0_0_#000]"
          : "bg-transparent border border-slate-800 transition-colors duration-200",
        "rounded-xl",
        // High-Intensity Neon Halo (Dark Mode Only)
        !isLightMode && "group-hover:border-amber-400/50 group-hover:shadow-[0_0_30px_rgba(251,191,36,0.4)] group-hover:filter group-hover:drop-shadow-[0_0_12px_rgba(251,191,36,0.6)]",
        isArchived
          ? "opacity-60 grayscale-[0.3] hover:opacity-80 hover:grayscale-0"
          : "",
        "text-left"
      )}
      whileHover={{ scale: 1.02, y: -4 }}
      whileTap={{ scale: 0.98, transition: { duration: 0.05 } }}
      transition={{ duration: 0.15, ease: "easeOut" }}
    >
      {/* Internal Content Wrapper - Prevents Gradient Bleed */}
      <div className={cn(
        "relative h-full w-full rounded-xl overflow-hidden",
        !isLightMode && "bg-gradient-to-br from-slate-900 to-slate-950",
        "px-5 py-4 sm:px-6 sm:py-5"
      )}>
        {/* Top row: lastActive + badges + action buttons */}
      <div className="flex items-start justify-between gap-4 relative z-10">
        <span className={cn(
          "text-xs font-medium",
          isLightMode ? "text-black/60 font-mono" : "text-zinc-500"
        )}>
          {lastActive}
        </span>
        
        <div className="flex items-center gap-2">
          {/* Sparkline Graph (replaces Mentions Badge) */}
          {mentions > 0 && (
            <div className="relative inline-flex items-center gap-1.5 rounded-full bg-sky-500/10 border border-sky-500/30 px-2 py-1">
              <SparklineGraph value={mentions} maxValue={20} />
              <span className="text-xs font-semibold text-sky-400 font-mono">{mentions}</span>
            </div>
          )}
          
          {/* Pending Requests Badge (Only for Leads) */}
          {pendingRequests > 0 && userRole === "Lead" && (
            <div className={cn(
              "relative inline-flex items-center gap-1.5 rounded-full px-2 py-1",
              isLightMode
                ? "bg-amber-100 border border-black text-black"
                : "bg-amber-500/10 border border-amber-500/30 text-amber-400"
            )}>
              <Shield className="h-3 w-3" aria-hidden="true" />
              <span className="text-xs font-semibold font-mono">
                {pendingRequests} Request{pendingRequests !== 1 ? "s" : ""}
              </span>
            </div>
          )}

          {/* Archive Button */}
          {!isArchived && onArchive && campaignId && (
            <button
              type="button"
              onClick={handleArchiveClick}
              className={cn(
                "relative inline-flex items-center gap-1.5 rounded-full px-2 py-1 transition-all duration-200",
                "opacity-0 group-hover:opacity-100",
                isLightMode
                  ? "bg-black/5 border border-black/20 text-black hover:bg-black/10"
                  : "bg-zinc-800/50 border border-zinc-700/50 text-zinc-400 hover:bg-zinc-700/50 hover:text-zinc-300"
              )}
              title="Archive campaign"
            >
              <Archive className="h-3 w-3" aria-hidden="true" />
            </button>
          )}

          {/* Restore Button */}
          {isArchived && onRestore && campaignId && (
            <button
              type="button"
              onClick={handleRestoreClick}
              className={cn(
                "relative inline-flex items-center gap-1.5 rounded-full px-2 py-1 transition-all duration-200",
                "opacity-0 group-hover:opacity-100",
                isLightMode
                  ? "bg-black/5 border border-black/20 text-black hover:bg-black/10"
                  : "bg-zinc-800/50 border border-zinc-700/50 text-zinc-400 hover:bg-zinc-700/50 hover:text-zinc-300"
              )}
              title="Restore campaign"
            >
              <RotateCcw className="h-3 w-3" aria-hidden="true" />
            </button>
          )}

          {/* Terminate Button */}
          {isArchived && onTerminate && campaignId && (
            <button
              type="button"
              onClick={handleTerminateClick}
              disabled={isTerminating}
              className={cn(
                "relative inline-flex items-center gap-1.5 rounded-full px-2 py-1 transition-all duration-200",
                "opacity-0 group-hover:opacity-100",
                isConfirmingTerminate
                  ? "bg-red-500/10 border border-red-500/50 text-red-400 hover:bg-red-500/20"
                  : "bg-red-500/10 border border-red-500/30 text-red-400 hover:bg-red-500/20",
                isTerminating && "opacity-50 cursor-not-allowed"
              )}
              title={isConfirmingTerminate ? "Click again to confirm termination" : "Permanently delete campaign"}
            >
              <Trash2 className="h-3 w-3" aria-hidden="true" />
              {isConfirmingTerminate && (
                <span className="text-xs font-semibold">Confirm?</span>
              )}
            </button>
          )}

          {/* Delete Button (Legacy) */}
          {!isArchived && onDelete && campaignId && (
            <button
              type="button"
              onClick={handleDeleteClick}
              className={cn(
                "relative inline-flex items-center gap-1.5 rounded-full px-2 py-1 transition-all duration-200",
                "opacity-0 group-hover:opacity-100",
                isConfirmingDelete
                  ? "bg-red-500/10 border border-red-500/50 text-red-400 hover:bg-red-500/20"
                  : isLightMode
                  ? "bg-black/5 border border-black/20 text-black hover:bg-black/10"
                  : "bg-zinc-800/50 border border-zinc-700/50 text-zinc-400 hover:bg-zinc-700/50 hover:text-zinc-300"
              )}
              title={isConfirmingDelete ? "Click again to confirm deletion" : "Delete campaign"}
            >
              <Trash2 className="h-3 w-3" aria-hidden="true" />
              {isConfirmingDelete && (
                <span className="text-xs font-semibold">Confirm?</span>
              )}
            </button>
          )}
        </div>
      </div>

      {/* Title & role */}
      <div className="mt-4 space-y-1.5 relative z-10">
        <h3 className={cn(
          "text-lg font-bold line-clamp-1 transition-colors",
          isLightMode ? "text-black" : "text-zinc-100 group-hover:text-amber-400"
        )}>
          {name}
        </h3>
        <p className={cn(
          "text-xs sm:text-sm",
          isLightMode ? "text-black/70" : "text-zinc-400"
        )}>
          Operating as{" "}
          <span className={cn(
            "font-mono",
            isLightMode ? "text-black font-semibold" : "text-zinc-300"
          )}>
            {role}
          </span>
        </p>
      </div>

      {/* Bottom strip: card metadata */}
      <div className={cn(
        "mt-4 flex items-center justify-between gap-3 border-t pt-3 text-[0.7rem] sm:text-xs relative z-10",
        isLightMode ? "border-black/20 text-black/60" : "border-zinc-800 text-zinc-400"
      )}>
        <div className="flex items-center gap-2">
          <span className={cn(
            "rounded-full px-2 py-0.5 font-mono text-[0.6rem] uppercase tracking-[0.16em]",
            isLightMode
              ? "bg-black/5 border border-black/20 text-black"
              : "bg-zinc-800 text-zinc-300"
          )}>
            ADVOCACY BUCKET
          </span>
          <span className={cn(
            "hidden sm:inline font-mono text-[0.6rem]",
            isLightMode ? "text-black/50" : "text-zinc-500"
          )}>
            Click to view impact and collaborators
          </span>
        </div>
        <span className={cn(
          "inline-flex items-center gap-1 font-mono text-[0.6rem]",
          isLightMode ? "text-black/60" : "text-zinc-500"
        )}>
          <span
            className={cn(
              "h-1 w-5 rounded-full",
              isLightMode
                ? "bg-black"
                : "bg-gradient-to-r from-[color:var(--accent)]/80 via-[color:var(--accent)] to-[color:var(--accent)]/60"
            )}
            aria-hidden="true"
          />
          {isArchived ? "ARCHIVED" : "ACTIVE"}
        </span>
      </div>
      </div>
    </motion.div>
  );

  if (onClick) {
    return (
      <button type="button" onClick={handleEnterClick} className="block w-full text-left">
        {cardContent}
      </button>
    );
  }

  return (
    <Link 
      href={href} 
      className="block w-full"
      onClick={handleEnterClick}
    >
      {cardContent}
    </Link>
  );
}
