"use client";

import { useParams } from "next/navigation";
import { RileyStudio } from "@app/components/chat/RileyStudio";
import { useCampaignName } from "@app/lib/useCampaignName";

export default function CampaignRileyPage() {
  const params = useParams();
  const campaignId = params.id as string;
  const { name } = useCampaignName(campaignId);
  const contextName = name || "Campaign";

  return (
    <div className="flex h-full relative">
      <div className="flex-1 flex flex-col overflow-hidden bg-transparent">
        <RileyStudio contextName={contextName} tenantId={campaignId} />
      </div>
    </div>
  );
}
