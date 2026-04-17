"use client";

import { RileyStudio } from "@app/components/chat/RileyStudio";

export default function GlobalRileyPage() {
  return (
    <div className="mx-auto flex h-[calc(100vh-13rem)] min-h-[640px] w-full max-w-[1400px]">
      <section className="flex min-w-0 flex-1 overflow-hidden rounded-2xl border border-[#e2d8c4] bg-[#f8f5ef] shadow-[0_1px_3px_rgba(31,42,68,0.08)]">
        <div className="min-w-0 flex-1">
          <RileyStudio contextName="Rally Global Brain" tenantId="global" />
        </div>
      </section>
    </div>
  );
}
