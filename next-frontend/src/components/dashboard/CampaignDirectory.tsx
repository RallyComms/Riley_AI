"use client";

import { useState, useMemo } from "react";
import { X, Users, Crown, Lock, Globe, ArrowRight, Clock, Search, FileText } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import Link from "next/link";
import { GlobalDocsList } from "./GlobalDocsList";
import { CampaignBucketCard } from "@app/components/campaign/CampaignBucketCard";
import { Asset } from "@app/lib/types";

interface Campaign {
  id: string;
  name: string;
  description: string;
  visibility: "internal" | "stealth";
  memberCount: number;
  leadName: string;
  leadAvatar: string;
  userStatus: "member" | "non-member" | "pending";
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
}

// Mock firm campaigns (these are separate from user campaigns)
const MOCK_FIRM_CAMPAIGNS: Campaign[] = [
  {
    id: "redwood-initiative",
    name: "Redwood Initiative",
    description: "Forest conservation and sustainable logging practices across the Pacific Northwest.",
    visibility: "internal",
    memberCount: 15,
    leadName: "Sarah Jones",
    leadAvatar: "SJ",
    userStatus: "non-member",
  },
  {
    id: "tech-policy-2025",
    name: "Tech Policy 2025",
    description: "Advocating for digital privacy rights and AI regulation frameworks.",
    visibility: "internal",
    memberCount: 22,
    leadName: "Michael Chen",
    leadAvatar: "MC",
    userStatus: "non-member",
  },
  {
    id: "healthcare-access-coalition",
    name: "Healthcare Access Coalition",
    description: "Expanding Medicaid and reducing prescription drug costs.",
    visibility: "internal",
    memberCount: 31,
    leadName: "Alex Rivera",
    leadAvatar: "AR",
    userStatus: "pending",
  },
  {
    id: "stealth-operation-alpha",
    name: "Stealth Operation Alpha",
    description: "Confidential strategic initiative.",
    visibility: "stealth",
    memberCount: 5,
    leadName: "John Martinez",
    leadAvatar: "JM",
    userStatus: "member", // User is a member, so they can see it
  },
  {
    id: "stealth-hidden-campaign",
    name: "Stealth Hidden Campaign",
    description: "This should be completely hidden from non-members.",
    visibility: "stealth",
    memberCount: 3,
    leadName: "Sarah Lee",
    leadAvatar: "SL",
    userStatus: "non-member", // This should NOT appear in the directory
  },
];

interface CampaignDirectoryProps {
  isOpen: boolean;
  onClose: () => void;
  userCampaigns?: UserCampaign[];
  archivedCampaigns?: UserCampaign[];
  onEnterCampaign?: (campaignId: string) => void;
  onRequestAccess?: (campaignId: string) => void;
  onRestore?: (campaignId: string) => void;
  onTerminate?: (campaignId: string) => Promise<void>;
  terminatingCampaignId?: string | null;
  onViewDocument?: (file: Asset) => void;
}

export function CampaignDirectory({
  isOpen,
  onClose,
  userCampaigns = [],
  archivedCampaigns = [],
  onEnterCampaign,
  onRequestAccess,
  onRestore,
  onTerminate,
  terminatingCampaignId = null,
  onViewDocument,
}: CampaignDirectoryProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [pendingIds, setPendingIds] = useState<string[]>([]);
  const [activeTab, setActiveTab] = useState<"campaigns" | "archive" | "documents">("campaigns");

  // Convert user campaigns to Directory format
  const convertedUserCampaigns: Campaign[] = useMemo(() => {
    return userCampaigns.map((uc) => ({
      id: uc.campaignId,
      name: uc.name,
      description: `Operating as ${uc.role}. Last active ${uc.lastActive}.`,
      visibility: "internal" as const, // Default to internal for user-created campaigns
      memberCount: 1, // Will be updated when team is added
      leadName: "You", // User is the lead
      leadAvatar: "AY", // Anova Youngers
      userStatus: "member" as const, // User is always a member of their own campaigns
    }));
  }, [userCampaigns]);

  // Merge logic: Start with firm campaigns, filter out duplicates, prepend user campaigns
  const displayCampaigns: Campaign[] = useMemo(() => {
    const userCampaignIds = new Set(convertedUserCampaigns.map((uc) => uc.id));
    const userCampaignIdSet = new Set(userCampaigns.map((uc) => uc.campaignId));

    // Filter out firm campaigns that match user campaign IDs
    const filteredFirmCampaigns = MOCK_FIRM_CAMPAIGNS.filter(
      (campaign) => !userCampaignIds.has(campaign.id)
    );

    // STRICT SECURITY FILTER: Hide stealth campaigns unless user is a member
    const visibleFirmCampaigns = filteredFirmCampaigns.filter((campaign) => {
      // If stealth, only show if user is a member (check userCampaigns directly)
      if (campaign.visibility === "stealth") {
        return userCampaignIdSet.has(campaign.id);
      }
      // Internal campaigns are always visible
      return true;
    });

    // Prepend user campaigns (they always come first)
    return [...convertedUserCampaigns, ...visibleFirmCampaigns];
  }, [convertedUserCampaigns, userCampaigns]);

  // Check if user is a member of a campaign
  const isUserMember = (campaignId: string): boolean => {
    return userCampaigns.some((uc) => uc.campaignId === campaignId);
  };

  // Filter by search query
  const filteredCampaigns = useMemo(() => {
    return displayCampaigns.filter(
      (campaign) =>
        campaign.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        campaign.description.toLowerCase().includes(searchQuery.toLowerCase()) ||
        campaign.leadName.toLowerCase().includes(searchQuery.toLowerCase())
    );
  }, [displayCampaigns, searchQuery]);

  const handleEnterCampaign = (campaignId: string) => {
    if (onEnterCampaign) {
      onEnterCampaign(campaignId);
    }
  };

  const handleRequestAccess = (campaignId: string) => {
    // Add to pending list
    setPendingIds((prev) => {
      if (prev.includes(campaignId)) return prev;
      return [...prev, campaignId];
    });

    // Call parent handler if provided
    if (onRequestAccess) {
      onRequestAccess(campaignId);
    }

    // Show feedback
    console.log("Access requested sent to Campaign Lead.");
    // In production, you could use a toast notification here
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
                    ? `${filteredCampaigns.length} campaign${filteredCampaigns.length !== 1 ? "s" : ""} available`
                    : activeTab === "archive"
                    ? `${archivedCampaigns.length} archived campaign${archivedCampaigns.length !== 1 ? "s" : ""}`
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
              </div>

              {/* Content - Scrollable Grid */}
              <div className="flex-1 overflow-y-auto p-6">
                {filteredCampaigns.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-12 text-center">
                    <p className="text-sm text-zinc-400">
                      {searchQuery ? "No campaigns match your search." : "No campaigns available."}
                    </p>
                  </div>
                ) : (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    {filteredCampaigns.map((campaign) => {
                      const isMember = isUserMember(campaign.id);
                      const isPending = pendingIds.includes(campaign.id) || campaign.userStatus === "pending";

                      return (
                        <div
                          key={campaign.id}
                          className="rounded-xl border border-zinc-800 bg-zinc-950/50 p-5 hover:border-zinc-700 transition-colors"
                        >
                          {/* Header */}
                          <div className="flex items-start justify-between mb-3">
                            <div className="flex-1">
                              <div className="flex items-center gap-2 mb-1">
                                <h3 className="text-base font-semibold text-zinc-100">{campaign.name}</h3>
                                {campaign.visibility === "stealth" && (
                                  <Lock className="h-4 w-4 text-zinc-500" />
                                )}
                              </div>
                              <p className="text-xs text-zinc-400 line-clamp-2">{campaign.description}</p>
                            </div>
                          </div>

                          {/* Metadata */}
                          <div className="flex items-center gap-4 mb-4 text-xs text-zinc-500">
                            <div className="flex items-center gap-1.5">
                              <Users className="h-3.5 w-3.5" />
                              <span>{campaign.memberCount}</span>
                            </div>
                            <div className="flex items-center gap-1.5">
                              <Crown className="h-3.5 w-3.5" />
                              <span>{campaign.leadName}</span>
                            </div>
                          </div>

                          {/* Action Button - Smart Logic */}
                          {isMember ? (
                            <Link
                              href={`/campaign/${campaign.id}`}
                              onClick={() => handleEnterCampaign(campaign.id)}
                              className="flex w-full items-center justify-center gap-2 rounded-lg bg-emerald-500/10 border border-emerald-500/30 px-4 py-2.5 text-sm font-medium text-emerald-400 hover:bg-emerald-500/20 transition-colors"
                            >
                              Enter Campaign
                              <ArrowRight className="h-4 w-4" />
                            </Link>
                          ) : isPending ? (
                            <button
                              type="button"
                              disabled
                              className="flex w-full items-center justify-center gap-2 rounded-lg bg-amber-500/10 border border-amber-500/30 px-4 py-2.5 text-sm font-medium text-amber-400 cursor-not-allowed"
                            >
                              <Clock className="h-4 w-4" />
                              Awaiting Approval
                            </button>
                          ) : (
                            <button
                              type="button"
                              onClick={() => handleRequestAccess(campaign.id)}
                              className="flex w-full items-center justify-center gap-2 rounded-lg border border-zinc-700 bg-zinc-800/50 px-4 py-2.5 text-sm font-medium text-zinc-300 hover:border-amber-400/50 hover:bg-amber-400/10 hover:text-amber-400 transition-colors"
                            >
                              Request Access
                            </button>
                          )}
                        </div>
                      );
                    })}
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
                {archivedCampaigns.length === 0 ? (
                  <div className="flex flex-col items-center justify-center py-12 text-center">
                    <p className="text-sm text-zinc-400">
                      {searchQuery ? "No archived campaigns match your search." : "No archived campaigns."}
                    </p>
                  </div>
                ) : (
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                    {archivedCampaigns
                      .filter((campaign) =>
                        campaign.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
                        campaign.role.toLowerCase().includes(searchQuery.toLowerCase())
                      )
                      .map((campaign) => (
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
      </div>
    </AnimatePresence>
  );
}
