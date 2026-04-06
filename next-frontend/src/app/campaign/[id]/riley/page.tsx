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
    <div className="relative flex h-full min-h-0 bg-[#f7f5ef]">
      <div className="flex min-h-0 flex-1 flex-col overflow-hidden bg-[#f7f5ef]">
        <RileyStudio contextName={contextName} tenantId={campaignId} />
      </div>
    </div>
  );
}
