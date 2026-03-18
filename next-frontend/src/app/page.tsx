"use client";

import { useState, useEffect } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { CampaignBucketCard } from "@app/components/campaign/CampaignBucketCard";
import { GlobalMenu } from "@app/components/layout/GlobalMenu";
import { OpsDock } from "@app/components/dashboard/OpsDock";
import { CreateCampaignModal } from "@app/components/dashboard/CreateCampaignModal";
import { CampaignDirectory } from "@app/components/dashboard/CampaignDirectory";
import Greeting from "@app/components/ui/Greeting";
import { apiFetch } from "@app/lib/api";

// Backend campaign response shape
interface BackendCampaign {
  id: string;
  name: string;
  description: string | null;
  role?: "Lead" | "Member";
  access: "member" | "requestable";
  status?: "active" | "archived";
  archived_at?: string | null;
  created_at?: string | null;
  owner_id?: string | null;
  owner_name?: string | null;
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

interface UserCampaign {
  name: string;
  role: string;
  lastActive: string;
  mentions: number;
  pendingRequests?: number;
  userRole?: "Lead" | "Member";
  themeColor: string;
  campaignId: string;
  access: "member" | "requestable";
  ownerName?: string;
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

// Map backend campaign to directory shape
function mapBackendToUserCampaign(
  backendCampaign: BackendCampaign,
  index: number
): UserCampaign {
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
    access: backendCampaign.access,
    ownerName: backendCampaign.owner_name ?? undefined,
  };
}

function mapUserCampaignToBucket(campaign: UserCampaign): CampaignBucket {
  return {
    name: campaign.name,
    role: campaign.role,
    lastActive: campaign.lastActive,
    mentions: campaign.mentions,
    pendingRequests: campaign.pendingRequests ?? 0,
    userRole: campaign.userRole ?? "Member",
    themeColor: campaign.themeColor,
    campaignId: campaign.campaignId,
  };
}

export default function Dashboard() {
  const { user } = useUser();
  const { getToken, isLoaded } = useAuth();
  const [campaigns, setCampaigns] = useState<CampaignBucket[]>([]);
  const [directoryCampaigns, setDirectoryCampaigns] = useState<UserCampaign[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isCreateModalOpen, setIsCreateModalOpen] = useState(false);
  const [isDirectoryOpen, setIsDirectoryOpen] = useState(false);
  const [terminatingCampaignId, setTerminatingCampaignId] = useState<string | null>(null);
  const [archiveTarget, setArchiveTarget] = useState<{ id: string; name: string } | null>(null);
  const [isArchiving, setIsArchiving] = useState(false);
  const [campaignsVersion, setCampaignsVersion] = useState(0);

  // Fetch campaigns from API
  useEffect(() => {
    async function fetchCampaigns() {
      // Only fetch when Clerk is loaded
      if (!isLoaded) {
        return;
      }

      // If user is not authenticated, show empty state (not error)
      const userId = user?.id;
      if (!userId) {
        setIsLoading(false);
        setError(null);
        setCampaigns([]);
        setDirectoryCampaigns([]);
        return;
      }

      try {
        setIsLoading(true);
        setError(null);

        const token = await getToken();
        if (!token) {
          throw new Error("No authentication token available");
        }

        const data: CampaignsResponse = await apiFetch("/api/v1/campaigns", {
          token,
          method: "GET",
        });
        
        const mappedCampaigns = data.campaigns.map((campaign, index) =>
          mapBackendToUserCampaign(campaign, index)
        );
        setDirectoryCampaigns(mappedCampaigns);
        setCampaigns(mappedCampaigns.map(mapUserCampaignToBucket));
      } catch (err) {
        console.error("Error fetching campaigns:", err);
        // Preserve exact error message from apiFetch (includes status codes if available)
        const errorMessage = err instanceof Error 
          ? err.message 
          : "Unable to load campaigns. Please refresh.";
        setError(errorMessage);
        // Clear campaigns on error to ensure we don't show stale data
        setCampaigns([]);
        setDirectoryCampaigns([]);
      } finally {
        setIsLoading(false);
      }
    }

    fetchCampaigns();
  }, [isLoaded, user?.id, getToken]);

  const handleCampaignCreated = (newCampaign: CampaignBucket) => {
    // Optimistically add the new campaign to the list
    setCampaigns((prev) => [newCampaign, ...prev]);
    setDirectoryCampaigns((prev) => [
      {
        name: newCampaign.name,
        role: newCampaign.role,
        lastActive: newCampaign.lastActive,
        mentions: newCampaign.mentions,
        pendingRequests: newCampaign.pendingRequests,
        userRole: newCampaign.userRole,
        themeColor: newCampaign.themeColor,
        campaignId: newCampaign.campaignId,
        access: "member",
      },
      ...prev,
    ]);
    setIsCreateModalOpen(false);
    
    // Optionally re-fetch in background to ensure consistency
    // (The new campaign is already in state, so this is just for safety)
    setTimeout(async () => {
      try {
        const token = await getToken();
        if (!token) return;

        try {
          const data: CampaignsResponse = await apiFetch("/api/v1/campaigns", {
            token,
            method: "GET",
          });
          const mappedCampaigns = data.campaigns.map((campaign, index) =>
            mapBackendToUserCampaign(campaign, index)
          );
          setDirectoryCampaigns(mappedCampaigns);
          setCampaigns(mappedCampaigns.map(mapUserCampaignToBucket));
        } catch {
          // Silently fail - optimistic update already succeeded
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

  const handleRequestAccess = async (campaignId: string, message?: string) => {
    try {
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }
      await apiFetch(`/api/v1/campaigns/${encodeURIComponent(campaignId)}/access-requests`, {
        token,
        method: "POST",
        body: { message: message || null },
      });
      alert("Access request submitted.");
    } catch (err) {
      console.error("Request access error:", err);
      alert(
        `Failed to submit access request: ${
          err instanceof Error ? err.message : "Unknown error"
        }`
      );
      throw err;
    }
  };

  const handleArchive = (campaignId: string) => {
    console.log("archive_parent_handler_called", { campaignId });
    const campaign = directoryCampaigns.find((c) => c.campaignId === campaignId);
    console.log("archive_modal_state_set", { campaignId });
    setArchiveTarget({
      id: campaignId,
      name: campaign?.name || "this campaign",
    });
  };

  const handleConfirmArchive = async () => {
    if (!archiveTarget) return;
    console.log("archive_confirm_clicked", { campaignId: archiveTarget.id });
    setIsArchiving(true);
    try {
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }
      const response = await apiFetch(`/api/v1/campaigns/${encodeURIComponent(archiveTarget.id)}/archive`, {
        token,
        method: "PATCH",
      });
      console.log("archive_confirm_patch_result", {
        campaignId: archiveTarget.id,
        ok: true,
        response,
      });
      setCampaigns((prev) => prev.filter((c) => c.campaignId !== archiveTarget.id));
      setDirectoryCampaigns((prev) => prev.filter((c) => c.campaignId !== archiveTarget.id));
      setCampaignsVersion((prev) => prev + 1);
      setArchiveTarget(null);
    } catch (err) {
      console.log("archive_confirm_patch_result", {
        campaignId: archiveTarget.id,
        ok: false,
        error: err instanceof Error ? err.message : String(err),
      });
      console.error("Archive campaign error:", err);
      alert(`Failed to archive campaign: ${err instanceof Error ? err.message : "Unknown error"}`);
    } finally {
      setIsArchiving(false);
    }
  };

  useEffect(() => {
    if (archiveTarget) {
      console.log("archive_modal_rendered", { campaignId: archiveTarget.id });
    }
  }, [archiveTarget]);

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

      await apiFetch(`/api/v1/campaign/${campaignId}`, {
        token,
        method: "DELETE",
      });

      // Optimistically remove from UI
      setCampaigns((prev) => prev.filter((c) => c.campaignId !== campaignId));
      setDirectoryCampaigns((prev) => prev.filter((c) => c.campaignId !== campaignId));
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

      {/* Error State - Show exact error message from API */}
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

      {/* Empty State - Only show when not loading, no error, and truly empty */}
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

      {/* Success State - Render campaign cards */}
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
        userCampaigns={directoryCampaigns}
        archivedCampaigns={[]}
        onEnterCampaign={handleEnterCampaign}
        onRequestAccess={handleRequestAccess}
        onArchive={handleArchive}
        onRestore={handleRestore}
        onTerminate={handleTerminate}
        terminatingCampaignId={terminatingCampaignId}
        campaignsVersion={campaignsVersion}
      />

      {archiveTarget && (
        <div className="fixed inset-0 z-[70] flex items-center justify-center p-4">
          <button
            type="button"
            aria-label="Close archive confirmation"
            className="absolute inset-0 bg-black/60 backdrop-blur-sm"
            onClick={() => (isArchiving ? undefined : setArchiveTarget(null))}
          />
          <div className="relative z-10 w-full max-w-md rounded-xl border border-zinc-800 bg-zinc-900 p-6 shadow-2xl">
            <h2 className="text-lg font-semibold text-zinc-100">Archive Campaign</h2>
            <p className="mt-2 text-sm text-zinc-300">
              Are you sure you want to archive this campaign?
            </p>
            <p className="mt-1 text-xs text-zinc-500">{archiveTarget.name}</p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                type="button"
                onClick={() => setArchiveTarget(null)}
                disabled={isArchiving}
                className="rounded-md border border-zinc-700 px-3 py-1.5 text-sm text-zinc-300 hover:bg-zinc-800 disabled:opacity-60"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleConfirmArchive}
                disabled={isArchiving}
                className="rounded-md bg-amber-400 px-3 py-1.5 text-sm font-semibold text-zinc-900 hover:bg-amber-500 disabled:opacity-60"
              >
                {isArchiving ? "Archiving..." : "Archive"}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
