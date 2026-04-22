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

const campaignNameCache = new Map<string, string>();
const campaignNameInFlight = new Map<string, Promise<string>>();

/**
 * Hook to fetch and return the campaign name for a given campaign ID.
 * Falls back to shortened ID (first 8 chars) if campaign not found or fetch fails.
 * 
 * @param campaignId - The campaign ID from route params
 * @returns Object with `name` (actual name or empty), `displayName` (name or fallback), and `isLoading` state
 */
export function useCampaignName(campaignId: string | undefined): UseCampaignNameResult {
  const { getToken, isLoaded } = useAuth();
  const [name, setName] = useState<string>(() => {
    if (!campaignId) return "";
    return campaignNameCache.get(campaignId) ?? "";
  });
  const [isLoading, setIsLoading] = useState<boolean>(() => {
    if (!campaignId) return false;
    return !campaignNameCache.has(campaignId);
  });

  useEffect(() => {
    let mounted = true;

    async function fetchCampaignName() {
      if (!campaignId) {
        setName("");
        setIsLoading(false);
        return;
      }

      const cachedName = campaignNameCache.get(campaignId);
      const hasCachedName = cachedName !== undefined;
      if (hasCachedName) {
        setName(cachedName);
        setIsLoading(false);
      } else {
        setName("");
        setIsLoading(isLoaded);
      }

      if (!isLoaded) return;

      try {
        const token = await getToken();
        if (!token) {
          if (mounted && !hasCachedName) {
            // Fallback to empty name if no token
            setName("");
          }
          return;
        }

        let request = campaignNameInFlight.get(campaignId);
        if (!request) {
          const nextRequest = (async () => {
            const data: CampaignsResponse = await apiFetch("/api/v1/campaigns", {
              token,
              method: "GET",
            });
            // Find campaign with matching ID
            const campaign = data.campaigns.find((c) => c.id === campaignId);
            const nextName = campaign?.name || "";
            campaignNameCache.set(campaignId, nextName);
            return nextName;
          })();
          campaignNameInFlight.set(campaignId, nextRequest);
          void nextRequest.finally(() => {
            if (campaignNameInFlight.get(campaignId) === nextRequest) {
              campaignNameInFlight.delete(campaignId);
            }
          });
          request = nextRequest;
        }

        const nextName = await request;
        if (mounted) {
          setName(nextName);
        }
      } catch (err) {
        console.error("Error fetching campaign name:", err);
        // On error, keep cached value if present, otherwise fallback to empty.
        if (mounted && !hasCachedName) {
          setName("");
        }
      } finally {
        if (mounted) {
          setIsLoading(false);
        }
      }
    }

    void fetchCampaignName();
    return () => {
      mounted = false;
    };
  }, [isLoaded, campaignId, getToken]);

  // displayName falls back to first 8 chars of ID if name is not available
  const displayName = name || (campaignId ? campaignId.slice(0, 8) : "");

  return { name, displayName, isLoading };
}
