"use client";

import Link from "next/link";
import { ArrowLeft } from "lucide-react";
import { RileyStudio } from "@app/components/chat/RileyStudio";

export default function GlobalRileyPage() {
  return (
    <div className="flex h-screen flex-col bg-[#f8f5ef]">
      <header className="h-16 shrink-0 border-b border-[#e3dac8] bg-[#fbf8f2] flex items-center px-6">
        <Link
          href="/"
          className="flex items-center gap-2 text-sm text-[#6f788a] transition-colors hover:text-[#1f2a44]"
        >
          <ArrowLeft className="h-4 w-4" />
          <span>Back to Dashboard</span>
        </Link>
      </header>

      <div className="flex-1 min-h-0 p-4">
        <RileyStudio contextName="Rally Global Brain" tenantId="global" />
      </div>
    </div>
  );
}
