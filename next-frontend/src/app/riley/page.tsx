"use client";

import { RileyStudio } from "@app/components/chat/RileyStudio";

export default function GlobalRileyPage() {
  return (
    <div className="-mb-8 -ml-6 -mr-6 -mt-6 h-[calc(100vh-8.5rem)] min-h-[640px] lg:-ml-8 lg:-mr-8">
      <RileyStudio contextName="Rally Global Brain" tenantId="global" />
    </div>
  );
}
