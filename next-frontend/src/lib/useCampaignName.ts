import { useState, useEffect } from "react";
import { useAuth } from "@clerk/nextjs";
import { apiFetch } from "@app/lib/api";

// Backend campaign shape
interface BackendCampaign {
  id: string;
  name: string;
  description: string | null;
  role: "Lead" | "Member";
}

interface CampaignsResponse {
  campaigns: BackendCampaign[];
}

interface UseCampaignNameResult {
  name: string;
  displayName: string;
  isLoading: boolean;
}

/**
 * Hook to fetch and return the campaign name for a given campaign ID.
 * Falls back to shortened ID (first 8 chars) if campaign not found or fetch fails.
 * 
 * @param campaignId - The campaign ID from route params
 * @returns Object with `name` (actual name or empty), `displayName` (name or fallback), and `isLoading` state
 */
export function useCampaignName(campaignId: string | undefined): UseCampaignNameResult {
  const { getToken, isLoaded } = useAuth();
  const [name, setName] = useState<string>("");
  const [isLoading, setIsLoading] = useState<boolean>(true);

  useEffect(() => {
    async function fetchCampaignName() {
      if (!isLoaded || !campaignId) {
        setIsLoading(false);
        return;
      }

      try {
        const token = await getToken();
        if (!token) {
          // Fallback to empty name if no token
          setName("");
          setIsLoading(false);
          return;
        }

        const data: CampaignsResponse = await apiFetch("/api/v1/campaigns", {
          token,
          method: "GET",
        });

        // Find campaign with matching ID
        const campaign = data.campaigns.find((c) => c.id === campaignId);
        if (campaign) {
          setName(campaign.name);
        } else {
          // Campaign not found - leave name empty (will use fallback in displayName)
          setName("");
        }
      } catch (err) {
        console.error("Error fetching campaign name:", err);
        // On error, leave name empty (will use fallback in displayName)
        setName("");
      } finally {
        setIsLoading(false);
      }
    }

    fetchCampaignName();
  }, [isLoaded, campaignId, getToken]);

  // displayName falls back to first 8 chars of ID if name is not available
  const displayName = name || (campaignId ? campaignId.slice(0, 8) : "");

  return { name, displayName, isLoading };
}
