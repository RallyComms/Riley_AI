"use client";

import { useParams } from "next/navigation";
import { RileyStudio } from "@app/components/chat/RileyStudio";

export default function CampaignRileyPage() {
  const params = useParams();
  const campaignId = params.id as string;

  // In production, fetch campaign name from API
  const contextName = campaignId || "Campaign";

  return (
    <div className="flex h-full relative">
      <div className="flex-1 flex flex-col overflow-hidden bg-transparent">
        <RileyStudio contextName={contextName} tenantId={campaignId} />
      </div>
    </div>
  );
}
