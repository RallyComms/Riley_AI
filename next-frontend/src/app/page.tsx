"use client";

import { useState, useEffect } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { CampaignBucketCard } from "@app/components/campaign/CampaignBucketCard";
import { GlobalMenu } from "@app/components/layout/GlobalMenu";
import { OpsDock } from "@app/components/dashboard/OpsDock";
import { CreateCampaignModal } from "@app/components/dashboard/CreateCampaignModal";
import { CampaignDirectory } from "@app/components/dashboard/CampaignDirectory";
import Greeting from "@app/components/ui/Greeting";

// Backend campaign response shape
interface BackendCampaign {
  id: string;
  name: string;
  description: string | null;
  role: "Lead" | "Member";
}

interface CampaignsResponse {
  campaigns: BackendCampaign[];
}

// Frontend campaign shape for CampaignBucketCard
interface CampaignBucket {
  name: string;
  role: string;
  lastActive: string;
  mentions: number;
  pendingRequests: number;
  userRole: "Lead" | "Member";
  themeColor: string;
  campaignId: string;
}

// Theme colors for campaigns (rotating assignment)
const THEME_COLORS = [
  "#0f766e", // Teal
  "#4f46e5", // Indigo
  "#e11d48", // Rose
  "#059669", // Emerald
  "#7c3aed", // Violet
  "#facc15", // Amber
];

// Map backend campaign to frontend shape
function mapBackendToFrontend(
  backendCampaign: BackendCampaign,
  index: number
): CampaignBucket {
  // Generate role display text based on user role
  const roleDisplay =
    backendCampaign.role === "Lead"
      ? "Lead Strategist"
      : backendCampaign.role === "Member"
      ? "Team Member"
      : "Member";

  // Assign theme color based on index (consistent per campaign)
  const themeColor = THEME_COLORS[index % THEME_COLORS.length];

  return {
    name: backendCampaign.name,
    role: roleDisplay,
    lastActive: "Recently active", // TODO: Add lastActive from backend if available
    mentions: 0, // TODO: Add mentions from backend if available
    pendingRequests: 0, // TODO: Add pendingRequests from backend if available
    userRole: backendCampaign.role,
    themeColor,
    campaignId: backendCampaign.id,
  };
}

export default function Dashboard() {
  const { user } = useUser();
  const { getToken, isLoaded } = useAuth();
  const [campaigns, setCampaigns] = useState<CampaignBucket[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [isDirectoryOpen, setIsDirectoryOpen] = useState(false);
  const [terminatingCampaignId, setTerminatingCampaignId] = useState<string | null>(null);

  // Fetch campaigns from API
  useEffect(() => {
    async function fetchCampaigns() {
      // Only fetch when Clerk is loaded and user is authenticated
      if (!isLoaded) {
        return;
      }

      const userId = user?.id;
      if (!userId) {
        setIsLoading(false);
        return;
      }

      try {
        setIsLoading(true);
        setError(null);

        const token = await getToken();
        if (!token) {
          throw new Error("No authentication token available");
        }

        const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
        const response = await fetch(`${apiUrl}/api/v1/campaigns`, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        });

        if (!response.ok) {
          if (response.status === 401) {
            throw new Error("Authentication failed. Please sign in again.");
          } else if (response.status === 403) {
            throw new Error("Access denied. You don't have permission to view campaigns.");
          } else {
            throw new Error(`Failed to load campaigns: ${response.status}`);
          }
        }

        const data: CampaignsResponse = await response.json();
        const mappedCampaigns = data.campaigns.map((campaign, index) =>
          mapBackendToFrontend(campaign, index)
        );
        setCampaigns(mappedCampaigns);
      } catch (err) {
        console.error("Error fetching campaigns:", err);
        setError(
          err instanceof Error
            ? err.message
            : "Unable to load campaigns. Please refresh."
        );
      } finally {
        setIsLoading(false);
      }
    }

    fetchCampaigns();
  }, [isLoaded, user?.id, getToken]);

  const handleCampaignCreated = (newCampaign: CampaignBucket) => {
    // Optimistically add the new campaign to the list
    setCampaigns((prev) => [newCampaign, ...prev]);
    setIsCreateModalOpen(false);
    
    // Optionally re-fetch in background to ensure consistency
    // (The new campaign is already in state, so this is just for safety)
    setTimeout(async () => {
      try {
        const token = await getToken();
        if (!token) return;

        const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
        const response = await fetch(`${apiUrl}/api/v1/campaigns`, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        });

        if (response.ok) {
          const data: CampaignsResponse = await response.json();
          const mappedCampaigns = data.campaigns.map((campaign, index) =>
            mapBackendToFrontend(campaign, index)
          );
          setCampaigns(mappedCampaigns);
        }
      } catch (err) {
        console.error("Background refresh failed:", err);
        // Don't show error to user - optimistic update already succeeded
      }
    }, 1000);
  };

  const handleEnterCampaign = (campaignId: string) => {
    // Navigate to campaign page
    window.location.href = `/campaign/${campaignId}`;
  };

  const handleRequestAccess = (campaignId: string) => {
    // TODO: Implement access request API call
    console.log("Requesting access to campaign:", campaignId);
  };

  const handleArchive = (campaignId: string) => {
    // Archive functionality removed - campaigns are managed by backend
    console.log("Archive not implemented - campaigns are managed by backend");
  };

  const handleRestore = (campaignId: string) => {
    // Restore functionality removed - campaigns are managed by backend
    console.log("Restore not implemented - campaigns are managed by backend");
  };

  const handleTerminate = async (campaignId: string) => {
    if (
      !confirm(
        `Are you sure you want to permanently terminate this campaign? This will delete ALL data and cannot be undone.`
      )
    ) {
      return;
    }

    setTerminatingCampaignId(campaignId);

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }

      const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
      const response = await fetch(
        `${apiUrl}/api/v1/campaign/${campaignId}`,
        {
          method: "DELETE",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        }
      );

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({
          detail: "Termination failed",
        }));

        if (response.status === 403) {
          throw new Error(
            "Only campaign leads can terminate missions."
          );
        } else {
          throw new Error(
            errorData.detail ||
              `Termination failed with status ${response.status}`
          );
        }
      }

      // Optimistically remove from UI
      setCampaigns((prev) => prev.filter((c) => c.campaignId !== campaignId));
    } catch (err) {
      console.error("Termination error:", err);
      alert(
        `Failed to terminate campaign: ${
          err instanceof Error ? err.message : "Unknown error"
        }`
      );
    } finally {
      setTerminatingCampaignId(null);
    }
  };

  return (
    <div className="relative min-h-screen flex flex-col items-center pt-24 pb-12">
      {/* Ops Dock - Left Navigation */}
      <OpsDock
        onInitialize={() => setIsCreateModalOpen(true)}
        onDirectory={() => setIsDirectoryOpen(true)}
      />

      {/* Global Menu - Top Right */}
      <GlobalMenu />

      {/* Header - Top Center */}
      <div className="mb-8 text-center">
        <h1 className="text-4xl sm:text-5xl font-bold tracking-widest text-zinc-100 dark:text-zinc-100 light:text-zinc-900">
          RILEY | <span className="text-amber-400">PLATFORM</span>
        </h1>
        <p className="mt-6 text-xl sm:text-2xl font-light text-zinc-300">
          Welcome back, <Greeting />.
        </p>
      </div>

      {/* Loading State */}
      {isLoading && (
        <div className="w-full max-w-7xl px-6">
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {[1, 2, 3].map((i) => (
              <div
                key={i}
                className="h-64 rounded-lg border border-zinc-800 bg-zinc-900/50 animate-pulse"
              />
            ))}
          </div>
          <p className="mt-6 text-center text-zinc-400">Loading missions...</p>
        </div>
      )}

      {/* Error State */}
      {!isLoading && error && (
        <div className="w-full max-w-7xl px-6">
          <div className="rounded-lg border border-red-500/50 bg-red-500/10 p-6 text-center">
            <p className="text-red-400 font-medium">{error}</p>
            <button
              onClick={() => window.location.reload()}
              className="mt-4 rounded-lg bg-amber-400 px-4 py-2 text-sm font-semibold text-zinc-900 hover:bg-amber-500 transition-colors"
            >
              Refresh Page
            </button>
          </div>
        </div>
      )}

      {/* Empty State */}
      {!isLoading && !error && campaigns.length === 0 && (
        <div className="w-full max-w-7xl px-6">
          <div className="rounded-lg border border-zinc-800 bg-zinc-900/50 p-12 text-center">
            <p className="text-xl text-zinc-300 mb-4">
              No campaigns yet.
            </p>
            <p className="text-zinc-400 mb-6">
              Create your first mission to get started.
            </p>
            <button
              onClick={() => setIsCreateModalOpen(true)}
              className="rounded-lg bg-amber-400 px-6 py-3 text-sm font-semibold text-zinc-900 hover:bg-amber-500 transition-colors"
            >
              Create Campaign
            </button>
          </div>
        </div>
      )}

      {/* Campaigns Grid */}
      {!isLoading && !error && campaigns.length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 w-full max-w-7xl px-6">
          {campaigns.map((campaign) => (
            <CampaignBucketCard
              key={campaign.campaignId}
              name={campaign.name}
              role={campaign.role}
              lastActive={campaign.lastActive}
              mentions={campaign.mentions}
              pendingRequests={campaign.pendingRequests}
              userRole={campaign.userRole}
              themeColor={campaign.themeColor}
              campaignId={campaign.campaignId}
              isArchived={false}
              onArchive={handleArchive}
              onRestore={undefined}
              onTerminate={handleTerminate}
              isTerminating={terminatingCampaignId === campaign.campaignId}
            />
          ))}
        </div>
      )}

      {/* Modals */}
      <CreateCampaignModal
        isOpen={isCreateModalOpen}
        onClose={() => setIsCreateModalOpen(false)}
        onCampaignCreated={handleCampaignCreated}
      />
      <CampaignDirectory
        isOpen={isDirectoryOpen}
        onClose={() => setIsDirectoryOpen(false)}
        userCampaigns={campaigns}
        archivedCampaigns={[]}
        onEnterCampaign={handleEnterCampaign}
        onRequestAccess={handleRequestAccess}
        onRestore={handleRestore}
        onTerminate={handleTerminate}
        terminatingCampaignId={terminatingCampaignId}
      />
    </div>
  );
}
