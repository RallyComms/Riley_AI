"use client";

import Link from "next/link";
import { ChevronLeft } from "lucide-react";
import { RileyStudio } from "@app/components/chat/RileyStudio";

export default function GlobalRileyPage() {
  return (
    <div className="flex h-screen flex-col bg-[#f8f5ef]">
      <header className="border-b border-[#e6dece] bg-[#fbf8f2]">
        <div className="mx-auto flex w-full max-w-6xl items-center px-8 py-3">
          <Link
            href="/"
            className="inline-flex items-center gap-1 rounded-md border border-[#d9d4c8] bg-white px-3 py-1.5 text-sm font-medium text-[#4d5871] hover:bg-[#f4efe5]"
          >
            <ChevronLeft className="h-4 w-4" />
            Back
          </Link>
        </div>
      </header>
      <div className="flex-1 min-h-0">
        <RileyStudio contextName="Rally Global Brain" tenantId="global" />
      </div>
    </div>
  );
}
