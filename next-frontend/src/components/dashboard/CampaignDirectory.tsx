"use client";

import { useState, useMemo, useEffect } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { X, Globe, Search, Loader2 } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import { GlobalDocsList } from "./GlobalDocsList";
import { AccessRequestModal } from "./AccessRequestModal";
import { Asset } from "@app/lib/types";
import { apiFetch } from "@app/lib/api";

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
  owner_username?: string | null;
  owner_email?: string | null;
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

const THEME_COLORS = [
  "#0f766e",
  "#4f46e5",
  "#e11d48",
  "#059669",
  "#7c3aed",
  "#facc15",
];

type DirectoryUserLabelCandidate = {
  display_name?: string | null;
  username?: string | null;
  email?: string | null;
  user_id?: string | null;
};

function resolveUserLabel(candidate: DirectoryUserLabelCandidate): string {
  const displayName = String(candidate.display_name || "").trim();
  if (displayName) return displayName;

  const username = String(candidate.username || "").trim();
  if (username) return username;

  const email = String(candidate.email || "").trim();
  if (email) {
    const prefix = email.split("@")[0]?.trim();
    if (prefix) return prefix;
  }

  const userId = String(candidate.user_id || "").trim();
  if (userId) return userId;

  return "Unknown user";
}

function mapBackendToFrontend(backendCampaign: BackendCampaign, index: number): UserCampaign {
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
    ownerName: resolveUserLabel({
      display_name: backendCampaign.owner_name,
      username: backendCampaign.owner_username,
      email: backendCampaign.owner_email,
      user_id: backendCampaign.owner_id,
    }),
  };
}

interface CampaignDirectoryProps {
  isOpen: boolean;
  onClose: () => void;
  variant?: "modal" | "embedded";
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
  variant = "modal",
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
  const [campaigns, setCampaigns] = useState<UserCampaign[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [requestedCampaignIds, setRequestedCampaignIds] = useState<Set<string>>(new Set());
  const [requestingCampaign, setRequestingCampaign] = useState<UserCampaign | null>(null);

  useEffect(() => {
    async function fetchCampaigns() {
      if (!isOpen || !isLoaded) return;

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
        if (!token) throw new Error("No authentication token available");

        const effectiveStatus = activeTab === "archive" ? "archived" : "active";
        const data: CampaignsResponse = await apiFetch(
          `/api/v1/campaigns?scope=${campaignScope}&status=${effectiveStatus}`,
          { token, method: "GET" },
        );
        setCampaigns((data.campaigns || []).map((campaign, index) => mapBackendToFrontend(campaign, index)));
      } catch (err) {
        const errorMessage =
          err instanceof Error ? err.message : "Unable to load campaigns. Please refresh.";
        setError(errorMessage);
        setCampaigns([]);
      } finally {
        setIsLoading(false);
      }
    }

    void fetchCampaigns();
  }, [activeTab, campaignScope, campaignsVersion, getToken, isLoaded, isOpen, user?.id]);

  const filteredCampaigns = useMemo(() => {
    if (!searchQuery.trim()) return campaigns;
    const query = searchQuery.toLowerCase();
    return campaigns.filter(
      (campaign) =>
        campaign.name.toLowerCase().includes(query) || campaign.role.toLowerCase().includes(query),
    );
  }, [campaigns, searchQuery]);

  const handleEnterCampaign = (campaignId: string) => {
    if (onEnterCampaign) onEnterCampaign(campaignId);
  };

  const handleRequestAccess = async (message: string) => {
    if (!requestingCampaign || !onRequestAccess) return;
    await onRequestAccess(requestingCampaign.campaignId, message);
    setRequestedCampaignIds((prev) => new Set(prev).add(requestingCampaign.campaignId));
    setRequestingCampaign(null);
  };

  const statusLabel = (campaign: UserCampaign): "Active" | "Archive" | "Request" => {
    if (activeTab === "archive") return "Archive";
    if (campaign.access !== "member") return "Request";
    return "Active";
  };

  const statusPillClasses = (label: "Active" | "Archive" | "Request"): string => {
    if (label === "Active") return "bg-[#e6d8ad] text-[#3a3f2a]";
    if (label === "Archive") return "bg-[#e9edf5] text-[#41516c]";
    return "bg-[#f3e3c2] text-[#6a4b20]";
  };

  if (variant === "modal" && !isOpen) return null;

  const shell = (
    <div
      className={
        variant === "modal"
          ? "fixed inset-0 z-50 bg-[#f8f5ef]"
          : "w-full bg-transparent"
      }
    >
      <motion.div
        initial={{ opacity: 0, y: 6 }}
        animate={{ opacity: 1, y: 0 }}
        exit={{ opacity: 0, y: 6 }}
        transition={{ duration: 0.2 }}
        className={variant === "modal" ? "h-full overflow-y-auto" : "h-full"}
      >
        <div
          className={cn(
            "mx-auto flex w-full max-w-6xl flex-col",
            variant === "modal" ? "h-full px-8 py-6" : "px-0 py-0",
          )}
        >
            <div className="flex items-center justify-between border-b border-[#e3dac8] pb-4">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-[#ece4d4] text-[#2a3d64]">
                  <Globe className="h-5 w-5" />
                </div>
                <div>
                  <h2 className="text-lg font-semibold text-[#1f2a44]">Firm Directory</h2>
                  <p className="text-xs text-[#6f788a]">
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
              {variant === "modal" ? (
                <button
                  type="button"
                  onClick={onClose}
                  className="rounded-md p-1.5 text-[#6f788a] transition hover:bg-[#efe6d7] hover:text-[#1f2a44]"
                >
                  <X className="h-5 w-5" />
                </button>
              ) : null}
            </div>

            <div className="mt-4 flex items-center gap-2 border-b border-[#e3dac8]">
              <button
                type="button"
                onClick={() => setActiveTab("campaigns")}
                className={cn(
                  "border-b-2 px-3 py-3 text-sm font-medium transition-colors",
                  activeTab === "campaigns"
                    ? "border-[#d4ad47] text-[#1f2a44]"
                    : "border-transparent text-[#6f788a] hover:text-[#324968]",
                )}
              >
                Active Campaigns
              </button>
              <button
                type="button"
                onClick={() => setActiveTab("archive")}
                className={cn(
                  "border-b-2 px-3 py-3 text-sm font-medium transition-colors",
                  activeTab === "archive"
                    ? "border-[#d4ad47] text-[#1f2a44]"
                    : "border-transparent text-[#6f788a] hover:text-[#324968]",
                )}
              >
                Firm Archive
              </button>
              <button
                type="button"
                onClick={() => setActiveTab("documents")}
                className={cn(
                  "border-b-2 px-3 py-3 text-sm font-medium transition-colors",
                  activeTab === "documents"
                    ? "border-[#d4ad47] text-[#1f2a44]"
                    : "border-transparent text-[#6f788a] hover:text-[#324968]",
                )}
              >
                Firm Documents
              </button>
            </div>

            {activeTab === "campaigns" || activeTab === "archive" ? (
              <>
                <div
                  className={cn(
                    "mt-5",
                    variant === "modal"
                      ? "rounded-xl bg-white p-4 shadow-[0_1px_3px_rgba(31,42,68,0.08)]"
                      : "border-b border-[#e3dac8] pb-4",
                  )}
                >
                  <div className="flex items-center gap-2 rounded-lg border border-[#e3dac8] bg-[#fcfaf6] px-3 py-2.5">
                    <Search className="h-4 w-4 text-[#8a90a0]" />
                    <input
                      type="text"
                      value={searchQuery}
                      onChange={(event) => setSearchQuery(event.target.value)}
                      placeholder={activeTab === "archive" ? "Search archived campaigns..." : "Search campaigns..."}
                      className="flex-1 bg-transparent text-sm text-[#1f2a44] placeholder:text-[#8a90a0] focus:outline-none"
                    />
                  </div>
                  {activeTab === "campaigns" ? (
                    <div className="mt-3 inline-flex rounded-lg border border-[#e3dac8] bg-[#fcfaf6] p-1">
                      <button
                        type="button"
                        onClick={() => setCampaignScope("my")}
                        className={cn(
                          "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                          campaignScope === "my"
                            ? "bg-[#d4ad47] text-[#1f2a44]"
                            : "text-[#6f788a] hover:text-[#324968]",
                        )}
                      >
                        My Campaigns
                      </button>
                      <button
                        type="button"
                        onClick={() => setCampaignScope("all")}
                        className={cn(
                          "rounded-md px-3 py-1.5 text-xs font-medium transition-colors",
                          campaignScope === "all"
                            ? "bg-[#d4ad47] text-[#1f2a44]"
                            : "text-[#6f788a] hover:text-[#324968]",
                        )}
                      >
                        All Campaigns
                      </button>
                    </div>
                  ) : null}
                </div>

                <div className={cn("mt-6 pb-8", variant === "modal" ? "flex-1 overflow-y-auto" : "")}>
                  {isLoading ? (
                    <div className="flex flex-col items-center justify-center py-12">
                      <Loader2 className="mb-4 h-8 w-8 animate-spin text-[#d4ad47]" />
                      <p className="text-sm text-[#6f788a]">Loading campaigns...</p>
                    </div>
                  ) : error ? (
                    <div className="mx-auto max-w-md rounded-xl bg-[#fff0f0] p-6 text-center">
                      <p className="mb-4 text-sm font-medium text-[#9e3434]">{error}</p>
                      <button
                        onClick={() => window.location.reload()}
                        className="rounded-lg bg-[#d4ad47] px-4 py-2 text-sm font-semibold text-[#1f2a44] hover:bg-[#bf993b]"
                      >
                        Refresh Page
                      </button>
                    </div>
                  ) : filteredCampaigns.length === 0 ? (
                    <div className="py-12 text-center">
                      <p className="text-sm text-[#6f788a]">
                        {searchQuery ? "No campaigns match your search." : "No campaigns available."}
                      </p>
                    </div>
                  ) : (
                    <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
                      {filteredCampaigns.map((campaign) => {
                        const label = statusLabel(campaign);
                        const clientName = campaign.ownerName || campaign.role || "No client or owner listed";

                        return (
                          <div key={campaign.campaignId} className="space-y-2">
                            <button
                              type="button"
                              onClick={
                                campaign.access === "member"
                                  ? () => handleEnterCampaign(campaign.campaignId)
                                  : undefined
                              }
                              className={cn(
                                "group w-full rounded-lg bg-[#fbf8f2] p-5 text-left shadow-[0_1px_2px_rgba(31,42,68,0.08),0_10px_24px_rgba(31,42,68,0.06)] transition-all",
                                campaign.access === "member"
                                  ? "hover:-translate-y-0.5 hover:shadow-[0_2px_6px_rgba(31,42,68,0.12),0_14px_28px_rgba(31,42,68,0.08)]"
                                  : "cursor-default opacity-95",
                              )}
                            >
                              <div className="mb-3 flex items-start justify-between">
                                <span
                                  className={cn(
                                    "rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-wider",
                                    statusPillClasses(label),
                                  )}
                                >
                                  {label}
                                </span>
                                <span className="text-[11px] text-[#8a90a0]">{campaign.lastActive}</span>
                              </div>
                              <h4 className="mb-1 font-serif text-base font-semibold leading-snug text-[#1f2a44] group-hover:text-[#22386a]">
                                {campaign.name}
                              </h4>
                              <p className="text-xs text-[#5b6578]">{clientName}</p>
                            </button>

                            {campaign.access !== "member" ? (
                              <button
                                type="button"
                                disabled={requestedCampaignIds.has(campaign.campaignId)}
                                onClick={() => setRequestingCampaign(campaign)}
                                className={cn(
                                  "w-full rounded-lg border px-3 py-2 text-sm font-medium transition-colors",
                                  requestedCampaignIds.has(campaign.campaignId)
                                    ? "cursor-not-allowed border-[#ddd4c3] bg-[#f5f1e8] text-[#9aa1b0]"
                                    : "border-[#d9c8a0] bg-[#f4ead1] text-[#5a4a25] hover:bg-[#ebdebe]",
                                )}
                              >
                                {requestedCampaignIds.has(campaign.campaignId) ? "Requested" : "Request access"}
                              </button>
                            ) : null}

                            {activeTab === "campaigns" && campaign.access === "member" && onArchive ? (
                              <button
                                type="button"
                                onClick={() => onArchive(campaign.campaignId)}
                                className="w-full rounded-lg border border-[#ddd4c3] px-3 py-2 text-sm font-medium text-[#5f6778] transition hover:bg-[#efe6d7]"
                              >
                                Archive
                              </button>
                            ) : null}

                            {activeTab === "archive" && onRestore ? (
                              <div className="flex items-center gap-2">
                                <button
                                  type="button"
                                  onClick={() => onRestore(campaign.campaignId)}
                                  className="flex-1 rounded-lg border border-[#ddd4c3] px-3 py-2 text-sm font-medium text-[#5f6778] transition hover:bg-[#efe6d7]"
                                >
                                  Restore
                                </button>
                                {onTerminate ? (
                                  <button
                                    type="button"
                                    onClick={() => {
                                      const confirmed = window.confirm(
                                        "Permanently delete this archived campaign? This cannot be undone.",
                                      );
                                      if (!confirmed) return;
                                      void onTerminate(campaign.campaignId);
                                    }}
                                    disabled={terminatingCampaignId === campaign.campaignId}
                                    className="flex-1 rounded-lg border border-[#e7b8b8] bg-[#fff3f3] px-3 py-2 text-sm font-medium text-[#9e3434] transition hover:bg-[#ffe9e9] disabled:cursor-not-allowed disabled:opacity-60"
                                  >
                                    {terminatingCampaignId === campaign.campaignId ? "Deleting..." : "Delete"}
                                  </button>
                                ) : null}
                              </div>
                            ) : null}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              </>
            ) : (
              <div
                className={cn(
                  "mt-6",
                  variant === "modal"
                    ? "flex-1 overflow-y-auto rounded-xl bg-white p-4 shadow-[0_1px_3px_rgba(31,42,68,0.08)]"
                    : "",
                )}
              >
                <GlobalDocsList onViewDocument={onViewDocument} />
              </div>
            )}
        </div>
      </motion.div>

      <AccessRequestModal
        isOpen={!!requestingCampaign}
        campaignName={requestingCampaign?.name || ""}
        onClose={() => setRequestingCampaign(null)}
        onSubmit={handleRequestAccess}
      />
    </div>
  );

  if (variant === "embedded") return shell;
  return <AnimatePresence>{shell}</AnimatePresence>;
}
