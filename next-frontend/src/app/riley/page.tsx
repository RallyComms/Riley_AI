"use client";

import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { RileyStudio } from "@app/components/chat/RileyStudio";

export default function GlobalRileyPage() {
  return (
    <div className="h-screen flex flex-col bg-zinc-950">
      {/* Minimal Header with Back Button */}
      <header className="h-16 border-b border-zinc-800 flex items-center px-6 shrink-0">
        <Link
          href="/"
          className="flex items-center gap-2 text-sm text-zinc-400 hover:text-zinc-100 transition-colors"
        >
          <ArrowLeft className="h-4 w-4" />
          <span>Back to Dashboard</span>
        </Link>
      </header>

      {/* Riley Studio - Full Screen */}
      <div className="flex-1 min-h-0">
        <RileyStudio contextName="Rally Global Brain" tenantId="global" />
      </div>
    </div>
  );
}
