"use client";

import { useState, useMemo, useEffect } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { X, Users, Crown, Globe, ArrowRight, Search, Loader2 } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import Link from "next/link";
import { GlobalDocsList } from "./GlobalDocsList";
import { CampaignBucketCard } from "@app/components/campaign/CampaignBucketCard";
import { AccessRequestModal } from "./AccessRequestModal";
import { Asset } from "@app/lib/types";
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

// Map backend campaign to frontend shape
function mapBackendToFrontend(
  backendCampaign: BackendCampaign,
  index: number
): UserCampaign {
  const roleDisplay =
    backendCampaign.role === "Lead"
      ? "Lead Strategist"
      : backendCampaign.role === "Member"
      ? "Team Member"
      : "Member";

  const themeColor = THEME_COLORS[index % THEME_COLORS.length];

  return {
    name: backendCampaign.name,
    role: roleDisplay,
    lastActive: "Recently active",
    mentions: 0,
    pendingRequests: 0,
    userRole: backendCampaign.role,
    themeColor,
    campaignId: backendCampaign.id,
    access: backendCampaign.access,
    ownerName: backendCampaign.owner_name ?? undefined,
  };
}

interface CampaignDirectoryProps {
  isOpen: boolean;
  onClose: () => void;
  userCampaigns?: UserCampaign[];
  archivedCampaigns?: UserCampaign[];
  onEnterCampaign?: (campaignId: string) => void;
  onRequestAccess?: (campaignId: string, message?: string) => Promise<void>;
  onArchive?: (campaignId: string) => void;
  onRestore?: (campaignId: string) => void;
  onTerminate?: (campaignId: string) => Promise<void>;
  terminatingCampaignId?: string | null;
  campaignsVersion?: number;
  onViewDocument?: (file: Asset) => void;
}

export function CampaignDirectory({
  isOpen,
  onClose,
  userCampaigns = [],
  archivedCampaigns = [],
  onEnterCampaign,
  onRequestAccess,
  onArchive,
  onRestore,
  onTerminate,
  terminatingCampaignId = null,
  campaignsVersion = 0,
  onViewDocument,
}: CampaignDirectoryProps) {
  const { user } = useUser();
  const { getToken, isLoaded } = useAuth();
  const [searchQuery, setSearchQuery] = useState("");
  const [activeTab, setActiveTab] = useState<"campaigns" | "archive" | "documents">("campaigns");
  const [campaignScope, setCampaignScope] = useState<"my" | "all">("my");
  
  // State for fetched campaigns
  const [campaigns, setCampaigns] = useState<UserCampaign[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [requestedCampaignIds, setRequestedCampaignIds] = useState<Set<string>>(new Set());
  const [requestingCampaign, setRequestingCampaign] = useState<UserCampaign | null>(null);

  // Fetch campaigns from API
  useEffect(() => {
    async function fetchCampaigns() {
      if (!isOpen || !isLoaded) {
        return;
      }

      const userId = user?.id;
      if (!userId) {
        setIsLoading(false);
        setError(null);
        setCampaigns([]);
        return;
      }

      try {
        setIsLoading(true);
        setError(null);

        const token = await getToken();
        if (!token) {
          throw new Error("No authentication token available");
        }

        const effectiveStatus = activeTab === "archive" ? "archived" : "active";
        const data: CampaignsResponse = await apiFetch(
          `/api/v1/campaigns?scope=${campaignScope}&status=${effectiveStatus}`,
          {
          token,
          method: "GET",
          }
        );
        
        const mappedCampaigns = data.campaigns.map((campaign, index) =>
          mapBackendToFrontend(campaign, index)
        );
        setCampaigns(mappedCampaigns);
      } catch (err) {
        console.error("Error fetching campaigns:", err);
        const errorMessage = err instanceof Error 
          ? err.message 
          : "Unable to load campaigns. Please refresh.";
        setError(errorMessage);
        setCampaigns([]);
      } finally {
        setIsLoading(false);
      }
    }

    fetchCampaigns();
  }, [isOpen, isLoaded, user?.id, getToken, campaignScope, activeTab, campaignsVersion]);

  // Filter campaigns by search query
  const filteredCampaigns = useMemo(() => {
    if (!searchQuery.trim()) return campaigns;
    
    const query = searchQuery.toLowerCase();
    return campaigns.filter(
      (campaign) =>
        campaign.name.toLowerCase().includes(query) ||
        campaign.role.toLowerCase().includes(query)
    );
  }, [campaigns, searchQuery]);

  const handleEnterCampaign = (campaignId: string) => {
    if (onEnterCampaign) {
      onEnterCampaign(campaignId);
    }
  };

  const handleRequestAccess = async (message: string) => {
    if (!requestingCampaign || !onRequestAccess) return;
    await onRequestAccess(requestingCampaign.campaignId, message);
    setRequestedCampaignIds((prev) => new Set(prev).add(requestingCampaign.campaignId));
    setRequestingCampaign(null);
  };

  const handleArchiveCampaign = (campaignId: string) => {
    if (onArchive) {
      onArchive(campaignId);
    }
  };

  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        {/* Backdrop */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          className="absolute inset-0 bg-black/60 backdrop-blur-sm"
          onClick={onClose}
        />

        {/* Directory Panel */}
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.95 }}
          transition={{ duration: 0.2 }}
          className="relative w-full max-w-6xl max-h-[90vh] rounded-2xl border border-zinc-800 bg-zinc-900/95 backdrop-blur-xl shadow-2xl flex flex-col"
        >
          {/* Header */}
          <div className="flex items-center justify-between border-b border-zinc-800 px-6 py-4">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-blue-400/10 border border-blue-400/20">
                <Globe className="h-5 w-5 text-blue-400" />
              </div>
              <div>
                <h2 className="text-lg font-semibold text-zinc-100">Firm Directory</h2>
                <p className="text-xs text-zinc-400">
                  {activeTab === "campaigns"
                    ? isLoading 
                      ? "Loading campaigns..."
                      : error
                      ? "Error loading campaigns"
                      : `${filteredCampaigns.length} campaign${filteredCampaigns.length !== 1 ? "s" : ""} available`
                    : activeTab === "archive"
                    ? `${filteredCampaigns.length} archived campaign${filteredCampaigns.length !== 1 ? "s" : ""}`
                    : "Global firm archive"}
                </p>
              </div>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg p-1.5 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Tab Switcher */}
          <div className="flex items-center gap-2 border-b border-zinc-800 px-6">
            <button
              type="button"
              onClick={() => setActiveTab("campaigns")}
              className={cn(
                "px-4 py-3 text-sm font-medium transition-colors border-b-2",
                activeTab === "campaigns"
                  ? "text-amber-400 border-amber-400"
                  : "text-zinc-400 border-transparent hover:text-zinc-300"
              )}
            >
              Active Campaigns
            </button>
            <button
              type="button"
              onClick={() => setActiveTab("archive")}
              className={cn(
                "px-4 py-3 text-sm font-medium transition-colors border-b-2",
                activeTab === "archive"
                  ? "text-amber-400 border-amber-400"
                  : "text-zinc-400 border-transparent hover:text-zinc-300"
              )}
            >
              Firm Archive
            </button>
            <button
              type="button"
              onClick={() => setActiveTab("documents")}
              className={cn(
                "px-4 py-3 text-sm font-medium transition-colors border-b-2",
                activeTab === "documents"
                  ? "text-amber-400 border-amber-400"
                  : "text-zinc-400 border-transparent hover:text-zinc-300"
              )}
            >
              Firm Documents
            </button>
          </div>

          {/* Content - Conditional Rendering */}
          {activeTab === "campaigns" ? (
            <>
              {/* Search Bar */}
              <div className="px-6 py-4 border-b border-zinc-800">
                <div className="flex items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-3">
                  <Search className="h-4 w-4 text-zinc-500" />
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    placeholder="Search campaigns..."
                    className="flex-1 bg-transparent text-zinc-100 placeholder:text-zinc-500 focus:outline-none"
                  />
                </div>
                <div className="mt-3 inline-flex rounded-lg border border-zinc-800 bg-zinc-950/50 p-1">
                  <button
                    type="button"
                    onClick={() => setCampaignScope("my")}
                    className={cn(
                      "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                      campaignScope === "my" ? "bg-amber-400 text-zinc-900" : "text-zinc-400 hover:text-zinc-200"
                    )}
                  >
                    My Campaigns
                  </button>
                  <button
                    type="button"
                    onClick={() => setCampaignScope("all")}
                    className={cn(
                      "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                      campaignScope === "all" ? "bg-amber-400 text-zinc-900" : "text-zinc-400 hover:text-zinc-200"
                    )}
                  >
                    All Campaigns
                  </button>
                </div>
              </div>

              {/* Content - Scrollable Grid */}
              <div className="flex-1 overflow-y-auto p-6">
                {/* Loading State */}
                {isLoading && (
                  <div className="flex flex-col items-center justify-center py-12">
                    <Loader2 className="h-8 w-8 text-amber-400 animate-spin mb-4" />
                    <p className="text-sm text-zinc-400">Loading campaigns...</p>
                  </div>
                )}

                {/* Error State */}
                {!isLoading && error && (
                  <div className="flex flex-col items-center justify-center py-12 text-center">
                    <div className="rounded-lg border border-red-500/50 bg-red-500/10 p-6 max-w-md">
                      <p className="text-red-400 font-medium mb-4">{error}</p>
                      <button
                        onClick={() => window.location.reload()}
                        className="rounded-lg bg-amber-400 px-4 py-2 text-sm font-semibold text-zinc-900 hover:bg-amber-500 transition-colors"
                      >
                        Refresh Page
                      </button>
                    </div>
                  </div>
                )}

                {/* Empty State */}
                {!isLoading && !error && filteredCampaigns.length === 0 && (
                  <div className="flex flex-col items-center justify-center py-12 text-center">
                    <p className="text-sm text-zinc-400">
                      {searchQuery 
                        ? "No campaigns match your search." 
                        : "No campaigns available."}
                    </p>
                  </div>
                )}

                {/* Success State - Campaign Cards */}
                {!isLoading && !error && filteredCampaigns.length > 0 && (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                    {filteredCampaigns.map((campaign) => (
                      <div key={campaign.campaignId} className="space-y-2">
                        <CampaignBucketCard
                          name={campaign.name}
                          role={campaign.access === "member" ? campaign.role : "Access request required"}
                          lastActive={campaign.lastActive}
                          mentions={campaign.mentions}
                          pendingRequests={campaign.pendingRequests}
                          userRole={campaign.userRole}
                          themeColor={campaign.themeColor}
                          campaignId={campaign.campaignId}
                          isArchived={false}
                          onClick={
                            campaign.access === "member"
                              ? () => handleEnterCampaign(campaign.campaignId)
                              : () => {}
                          }
                          onArchive={campaign.access === "member" ? handleArchiveCampaign : undefined}
                          onRestore={undefined}
                          onTerminate={onTerminate}
                          isTerminating={terminatingCampaignId === campaign.campaignId}
                        />
                        {campaign.access !== "member" && (
                          <button
                            type="button"
                            disabled={requestedCampaignIds.has(campaign.campaignId)}
                            onClick={() => setRequestingCampaign(campaign)}
                            className={cn(
                              "w-full rounded-lg border px-3 py-2 text-sm font-medium transition-colors",
                              requestedCampaignIds.has(campaign.campaignId)
                                ? "cursor-not-allowed border-zinc-700 bg-zinc-800/50 text-zinc-400"
                                : "border-amber-500/40 bg-amber-500/15 text-amber-300 hover:bg-amber-500/25"
                            )}
                          >
                            {requestedCampaignIds.has(campaign.campaignId) ? "Requested" : "Request access"}
                          </button>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </>
          ) : activeTab === "archive" ? (
            <>
              {/* Search Bar for Archive */}
              <div className="px-6 py-4 border-b border-zinc-800">
                <div className="flex items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-3">
                  <Search className="h-4 w-4 text-zinc-500" />
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    placeholder="Search archived campaigns..."
                    className="flex-1 bg-transparent text-zinc-100 placeholder:text-zinc-500 focus:outline-none"
                  />
                </div>
              </div>

              {/* Archived Campaigns Grid */}
              <div className="flex-1 overflow-y-auto p-6">
                {filteredCampaigns.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-12 text-center">
                    <p className="text-sm text-zinc-400">
                      {searchQuery ? "No archived campaigns match your search." : "No archived campaigns."}
                    </p>
                  </div>
                ) : (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                    {filteredCampaigns.map((campaign) => (
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
                          isArchived={true}
                          onArchive={undefined}
                          onRestore={onRestore}
                          onTerminate={onTerminate}
                          isTerminating={terminatingCampaignId === campaign.campaignId}
                        />
                      ))}
                  </div>
                )}
              </div>
            </>
          ) : (
            <GlobalDocsList onViewDocument={onViewDocument} />
          )}
        </motion.div>
        <AccessRequestModal
          isOpen={!!requestingCampaign}
          campaignName={requestingCampaign?.name || ""}
          onClose={() => setRequestingCampaign(null)}
          onSubmit={handleRequestAccess}
        />
      </div>
    </AnimatePresence>
  );
}
