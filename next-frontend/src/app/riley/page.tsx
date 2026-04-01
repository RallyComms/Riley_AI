"use client";

import { RileyStudio } from "@app/components/chat/RileyStudio";

export default function GlobalRileyPage() {
  return (
    <div className="flex h-screen flex-col bg-[#f8f5ef]">
      <div className="flex-1 min-h-0">
        <RileyStudio contextName="Rally Global Brain" tenantId="global" />
      </div>
    </div>
  );
}
