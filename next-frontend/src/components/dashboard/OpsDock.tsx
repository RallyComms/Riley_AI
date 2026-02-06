"use client";

import { Plus, Globe, Sparkles } from "lucide-react";
import Link from "next/link";
import { cn } from "@app/lib/utils";

interface OpsDockProps {
  onInitialize: () => void;
  onDirectory: () => void;
}

export function OpsDock({ onInitialize, onDirectory }: OpsDockProps) {
  return (
    <div className="fixed left-6 top-1/2 -translate-y-1/2 z-40">
      <div className="flex flex-col gap-3 rounded-2xl border border-zinc-800 bg-zinc-950/60 backdrop-blur-xl p-3 shadow-2xl">
        {/* Initialize Button */}
        <button
          type="button"
          onClick={onInitialize}
          className={cn(
            "group relative flex h-12 w-12 items-center justify-center",
            "rounded-xl border border-zinc-800 bg-zinc-900/40",
            "text-zinc-300 transition-all duration-200",
            "hover:border-amber-400/50 hover:bg-amber-400/10 hover:text-amber-400",
            "focus:outline-none focus:ring-2 focus:ring-amber-400/50 focus:ring-offset-2 focus:ring-offset-zinc-950"
          )}
          aria-label="New Mission"
        >
          <Plus className="h-5 w-5" />
          {/* Tooltip */}
          <div className="absolute left-full ml-3 opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none">
            <div className="rounded-lg border border-zinc-800 bg-zinc-900/95 backdrop-blur-sm px-3 py-1.5 text-xs font-medium text-zinc-100 whitespace-nowrap shadow-lg">
              New Mission
            </div>
          </div>
        </button>

        {/* Directory Button */}
        <button
          type="button"
          onClick={onDirectory}
          className={cn(
            "group relative flex h-12 w-12 items-center justify-center",
            "rounded-xl border border-zinc-800 bg-zinc-900/40",
            "text-zinc-300 transition-all duration-200",
            "hover:border-blue-400/50 hover:bg-blue-400/10 hover:text-blue-400",
            "focus:outline-none focus:ring-2 focus:ring-blue-400/50 focus:ring-offset-2 focus:ring-offset-zinc-950"
          )}
          aria-label="Firm Knowledge Base"
        >
          <Globe className="h-5 w-5" />
          {/* Tooltip */}
          <div className="absolute left-full ml-3 opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none">
            <div className="rounded-lg border border-zinc-800 bg-zinc-900/95 backdrop-blur-sm px-3 py-1.5 text-xs font-medium text-zinc-100 whitespace-nowrap shadow-lg">
              Firm Knowledge Base
            </div>
          </div>
        </button>

        {/* Riley AI Button */}
        <Link
          href="/riley"
          className={cn(
            "group relative flex h-12 w-12 items-center justify-center",
            "rounded-xl border border-zinc-800 bg-zinc-900/40",
            "text-zinc-300 transition-all duration-200",
            "hover:border-amber-400/50 hover:bg-amber-400/10 hover:text-amber-400",
            "focus:outline-none focus:ring-2 focus:ring-amber-400/50 focus:ring-offset-2 focus:ring-offset-zinc-950"
          )}
          aria-label="Talk to Global HQ"
        >
          <Sparkles className="h-5 w-5" />
          {/* Tooltip */}
          <div className="absolute left-full ml-3 opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none">
            <div className="rounded-lg border border-zinc-800 bg-zinc-900/95 backdrop-blur-sm px-3 py-1.5 text-xs font-medium text-zinc-100 whitespace-nowrap shadow-lg">
              Talk to Global HQ
            </div>
          </div>
        </Link>
      </div>
    </div>
  );
}

