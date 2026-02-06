"use client";

import { useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { X, Shield } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence } from "framer-motion";

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

interface CreateCampaignModalProps {
  isOpen: boolean;
  onClose: () => void;
  onCampaignCreated: (campaign: CampaignBucket) => void;
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

export function CreateCampaignModal({ isOpen, onClose, onCampaignCreated }: CreateCampaignModalProps) {
  const { getToken } = useAuth();
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  // Reset form when modal closes
  const handleClose = () => {
    if (!isSubmitting) {
      setName("");
      setDescription("");
      setSubmitError(null);
      onClose();
    }
  };

  const handleSubmit = async () => {
    if (!name.trim()) {
      setSubmitError("Campaign name is required");
      return;
    }

    setIsSubmitting(true);
    setSubmitError(null);

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }

      const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
      const response = await fetch(`${apiUrl}/api/v1/campaigns`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          name: name.trim(),
          description: description.trim() || null,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({
          detail: "Failed to create campaign",
        }));
        throw new Error(
          errorData.detail || `Failed to create campaign: ${response.status}`
        );
      }

      const createdCampaign = await response.json();

      // Map backend response to frontend CampaignBucket shape
      // Use a random index for theme color (will be consistent per campaign ID)
      const colorIndex = Math.abs(
        createdCampaign.id.split("").reduce((acc: number, char: string) => acc + char.charCodeAt(0), 0)
      ) % THEME_COLORS.length;
      const themeColor = THEME_COLORS[colorIndex];

      const frontendCampaign: CampaignBucket = {
        name: createdCampaign.name,
        role: "Lead Strategist", // Creator is always Lead
        lastActive: "Just now",
        mentions: 0,
        pendingRequests: 0,
        userRole: "Lead",
        themeColor,
        campaignId: createdCampaign.id,
      };

      // Call onCampaignCreated with the new campaign
      onCampaignCreated(frontendCampaign);

      // Reset form
      setName("");
      setDescription("");
      setSubmitError(null);
      onClose();
    } catch (err) {
      console.error("Error creating campaign:", err);
      setSubmitError(
        err instanceof Error ? err.message : "Failed to create campaign"
      );
    } finally {
      setIsSubmitting(false);
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
          onClick={handleClose}
        />

        {/* Modal */}
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.95 }}
          transition={{ duration: 0.2 }}
          className="relative w-full max-w-2xl rounded-2xl border border-zinc-800 bg-zinc-900/95 backdrop-blur-xl shadow-2xl"
        >
          {/* Header */}
          <div className="flex items-center justify-between border-b border-zinc-800 px-6 py-4">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-amber-400/10 border border-amber-400/20">
                <Shield className="h-5 w-5 text-amber-400" />
              </div>
              <div>
                <h2 className="text-lg font-semibold text-zinc-100">Create Campaign</h2>
                <p className="text-xs text-zinc-400">Start a new mission</p>
              </div>
            </div>
            <button
              type="button"
              onClick={handleClose}
              disabled={isSubmitting}
              className="rounded-lg p-1.5 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors disabled:opacity-50"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Content */}
          <div className="p-6">
            <div className="space-y-6">
              {submitError && (
                <div className="rounded-lg border border-red-500/50 bg-red-500/10 p-3">
                  <p className="text-sm text-red-400">{submitError}</p>
                </div>
              )}

              <div>
                <label className="block text-sm font-medium text-zinc-300 mb-2">
                  Campaign Name
                </label>
                <input
                  type="text"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  placeholder="e.g., Clean Water Initiative 2025"
                  className="w-full rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-3 text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-amber-400/50 focus:border-amber-400/50"
                  disabled={isSubmitting}
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-zinc-300 mb-2">
                  Description (Optional)
                </label>
                <textarea
                  value={description}
                  onChange={(e) => setDescription(e.target.value)}
                  placeholder="Brief description of the campaign..."
                  rows={3}
                  className="w-full rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-3 text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-amber-400/50 focus:border-amber-400/50 resize-none"
                  disabled={isSubmitting}
                />
              </div>

              <div className="rounded-lg border border-amber-400/20 bg-amber-400/10 p-4">
                <p className="text-sm text-amber-300">
                  You will be added as the Lead of this campaign. Team members can be added later.
                </p>
              </div>
                <div>
                  <label className="block text-sm font-medium text-zinc-300 mb-2">
                    Add Team Members
                  </label>
                  <div className="relative">
                    <div className="flex items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-3">
                      <Search className="h-4 w-4 text-zinc-500" />
                      <input
                        type="text"
                        value={searchQuery}
                        onChange={(e) => handleSearch(e.target.value)}
                        placeholder="Search by name or email (e.g., 'Sar')"
                        className="flex-1 bg-transparent text-zinc-100 placeholder:text-zinc-500 focus:outline-none"
                      />
                    </div>

                    {/* Search Results Dropdown */}
                    {showSearch && searchResults.length > 0 && (
                      <div className="absolute z-10 mt-2 w-full rounded-lg border border-zinc-800 bg-zinc-900/95 backdrop-blur-xl shadow-xl overflow-hidden">
                        {searchResults
                          .filter((user) => !team.some((member) => member.id === user.id))
                          .map((user) => (
                            <button
                              key={user.id}
                              type="button"
                              onClick={() => handleAddMember(user)}
                              className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-zinc-800/50 transition-colors"
                            >
                              <div className="flex h-8 w-8 items-center justify-center rounded-full bg-zinc-700 text-xs font-medium text-zinc-100">
                                {user.avatar}
                              </div>
                              <div className="flex-1">
                                <p className="text-sm font-medium text-zinc-100">{user.name}</p>
                                <p className="text-xs text-zinc-400">{user.role}</p>
                              </div>
                            </button>
                          ))}
                      </div>
                    )}
                  </div>
                </div>

                {/* Team List */}
                {team.length > 0 && (
                  <div>
                    <label className="block text-sm font-medium text-zinc-300 mb-3">
                      Squad ({team.length})
                    </label>
                    <div className="space-y-2">
                      {team.map((member) => (
                        <div
                          key={member.id}
                          className="flex items-center justify-between rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-3"
                        >
                          <div className="flex items-center gap-3">
                            <div className="relative">
                              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-zinc-700 text-sm font-medium text-zinc-100">
                                {member.avatar}
                              </div>
                              {member.isLead && (
                                <div className="absolute -top-1 -right-1 flex h-5 w-5 items-center justify-center rounded-full bg-amber-400/20 border border-amber-400/30">
                                  <Crown className="h-3 w-3 text-amber-400" />
                                </div>
                              )}
                            </div>
                            <div>
                              <p className="text-sm font-medium text-zinc-100">{member.name}</p>
                              <p className="text-xs text-zinc-400">{member.role}</p>
                            </div>
                          </div>
                          <div className="flex items-center gap-3">
                            <label className="flex items-center gap-2 cursor-pointer">
                              <input
                                type="checkbox"
                                checked={member.isLead}
                                onChange={() => handleToggleLead(member.id)}
                                className="sr-only"
                              />
                              <div
                                className={cn(
                                  "flex h-5 w-9 items-center rounded-full px-1 transition-colors",
                                  member.isLead ? "bg-amber-400/20" : "bg-zinc-800"
                                )}
                              >
                                <div
                                  className={cn(
                                    "h-3 w-3 rounded-full bg-zinc-400 transition-transform",
                                    member.isLead && "translate-x-4 bg-amber-400"
                                  )}
                                />
                              </div>
                              <span className="text-xs text-zinc-400">Lead</span>
                            </label>
                            <button
                              type="button"
                              onClick={() => handleRemoveMember(member.id)}
                              className="rounded-lg p-1.5 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors"
                            >
                              <X className="h-4 w-4" />
                            </button>
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
            </div>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between border-t border-zinc-800 px-6 py-4">
            <button
              type="button"
              onClick={handleClose}
              disabled={isSubmitting}
              className="rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-2 text-sm font-medium text-zinc-300 hover:bg-zinc-800 transition-colors disabled:opacity-50"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              disabled={isSubmitting || !name.trim()}
              className={cn(
                "rounded-lg bg-amber-400 px-4 py-2 text-sm font-semibold text-zinc-900 transition-colors",
                isSubmitting || !name.trim()
                  ? "opacity-50 cursor-not-allowed"
                  : "hover:bg-amber-500"
              )}
            >
              {isSubmitting ? "Creating..." : "Launch Campaign"}
            </button>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
}
