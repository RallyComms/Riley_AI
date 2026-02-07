"use client";

import { useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { X, Shield, CheckCircle } from "lucide-react";
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

interface Member {
  id: string;
  email: string;
  role: string;
}

export function CreateCampaignModal({ isOpen, onClose, onCampaignCreated }: CreateCampaignModalProps) {
  const { getToken, userId } = useAuth();
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  
  // Member invitation state
  const [createdCampaignId, setCreatedCampaignId] = useState<string | null>(null);
  const [memberEmail, setMemberEmail] = useState("");
  const [memberRole, setMemberRole] = useState<"Member" | "Lead">("Member");
  const [members, setMembers] = useState<Member[]>([]);
  const [addMemberError, setAddMemberError] = useState<string | null>(null);
  const [addMemberSuccess, setAddMemberSuccess] = useState<string | null>(null);
  const [isAddingMember, setIsAddingMember] = useState(false);

  // Reset form when modal closes
  const handleClose = () => {
    if (!isSubmitting && !isAddingMember) {
      setName("");
      setDescription("");
      setSubmitError(null);
      setCreatedCampaignId(null);
      setMemberEmail("");
      setMemberRole("Member");
      setMembers([]);
      setAddMemberError(null);
      setAddMemberSuccess(null);
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

      // Store campaign ID and show invite section
      setCreatedCampaignId(createdCampaign.id);
      
      // Fetch initial members list (includes creator)
      try {
        const membersResponse = await fetch(`${apiUrl}/api/v1/campaigns/${createdCampaign.id}/members`, {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
        });
        
        if (membersResponse.ok) {
          const membersData = await membersResponse.json();
          setMembers(membersData.members || []);
        }
      } catch (err) {
        // If fetching members fails, continue anyway - user can still add members
        console.error("Error fetching initial members:", err);
      }
      
      // Don't close modal yet - show invite section
      setIsSubmitting(false);
    } catch (err) {
      console.error("Error creating campaign:", err);
      setSubmitError(
        err instanceof Error ? err.message : "Failed to create campaign"
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleAddMember = async () => {
    if (!createdCampaignId || !memberEmail.trim()) {
      setAddMemberError("Email is required");
      return;
    }

    // Basic email validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(memberEmail.trim())) {
      setAddMemberError("Please enter a valid email address");
      return;
    }

    setIsAddingMember(true);
    setAddMemberError(null);
    setAddMemberSuccess(null);

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }

      const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
      const response = await fetch(`${apiUrl}/api/v1/campaigns/${createdCampaignId}/members`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          email: memberEmail.trim(),
          role: memberRole,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({
          detail: "Failed to add member",
        }));
        
        if (response.status === 404) {
          throw new Error("User not found — ask them to sign up first");
        } else if (response.status === 403) {
          throw new Error("Only Lead members can add members to a campaign");
        } else {
          throw new Error(
            errorData.detail || `Failed to add member: ${response.status}`
          );
        }
      }

      const data = await response.json();
      
      // Update members list from response
      setMembers(data.members || []);
      setAddMemberSuccess(`${memberEmail.trim()} added successfully`);
      setMemberEmail("");
      setMemberRole("Member");
      
      // Clear success message after 3 seconds
      setTimeout(() => setAddMemberSuccess(null), 3000);
    } catch (err) {
      console.error("Error adding member:", err);
      setAddMemberError(
        err instanceof Error ? err.message : "Failed to add member"
      );
    } finally {
      setIsAddingMember(false);
    }
  };

  const handleDone = () => {
    if (!createdCampaignId) return;

    // Map backend response to frontend CampaignBucket shape
    const colorIndex = Math.abs(
      createdCampaignId.split("").reduce((acc: number, char: string) => acc + char.charCodeAt(0), 0)
    ) % THEME_COLORS.length;
    const themeColor = THEME_COLORS[colorIndex];

    const frontendCampaign: CampaignBucket = {
      name: name,
      role: "Lead Strategist", // Creator is always Lead
      lastActive: "Just now",
      mentions: 0,
      pendingRequests: 0,
      userRole: "Lead",
      themeColor,
      campaignId: createdCampaignId,
    };

    // Call onCampaignCreated with the new campaign
    onCampaignCreated(frontendCampaign);

    // Reset everything and close
    setName("");
    setDescription("");
    setSubmitError(null);
    setCreatedCampaignId(null);
    setMemberEmail("");
    setMemberRole("Member");
    setMembers([]);
    setAddMemberError(null);
    setAddMemberSuccess(null);
    onClose();
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

              {!createdCampaignId ? (
                <>
                  <div className="rounded-lg border border-amber-400/20 bg-amber-400/10 p-4">
                    <p className="text-sm text-amber-300">
                      You will be added as the Lead of this campaign. Team members can be added after creation.
                    </p>
                  </div>
                </>
              ) : (
                <>
                  {/* Success message for campaign creation */}
                  <div className="rounded-lg border border-emerald-500/50 bg-emerald-500/10 p-4">
                    <div className="flex items-center gap-2">
                      <CheckCircle className="h-5 w-5 text-emerald-400" />
                      <p className="text-sm text-emerald-300">
                        Campaign created successfully! Invite team members below.
                      </p>
                    </div>
                  </div>

                  {/* Invite Team Members Section */}
                  <div>
                    <label className="block text-sm font-medium text-zinc-300 mb-3">
                      Invite Team Members
                    </label>
                    
                    {addMemberError && (
                      <div className="mb-3 rounded-lg border border-red-500/50 bg-red-500/10 p-3">
                        <p className="text-sm text-red-400">{addMemberError}</p>
                      </div>
                    )}
                    
                    {addMemberSuccess && (
                      <div className="mb-3 rounded-lg border border-emerald-500/50 bg-emerald-500/10 p-3">
                        <div className="flex items-center gap-2">
                          <CheckCircle className="h-4 w-4 text-emerald-400" />
                          <p className="text-sm text-emerald-300">{addMemberSuccess}</p>
                        </div>
                      </div>
                    )}

                    <div className="flex gap-2 mb-4">
                      <input
                        type="email"
                        value={memberEmail}
                        onChange={(e) => setMemberEmail(e.target.value)}
                        placeholder="Enter email address"
                        className="flex-1 rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-2 text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-amber-400/50 focus:border-amber-400/50"
                        disabled={isAddingMember}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" && !isAddingMember) {
                            handleAddMember();
                          }
                        }}
                      />
                      <select
                        value={memberRole}
                        onChange={(e) => setMemberRole(e.target.value as "Member" | "Lead")}
                        className="rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-2 text-zinc-100 focus:outline-none focus:ring-2 focus:ring-amber-400/50 focus:border-amber-400/50"
                        disabled={isAddingMember}
                      >
                        <option value="Member">Member</option>
                        <option value="Lead">Lead</option>
                      </select>
                      <button
                        type="button"
                        onClick={handleAddMember}
                        disabled={isAddingMember || !memberEmail.trim()}
                        className={cn(
                          "rounded-lg bg-amber-400 px-4 py-2 text-sm font-semibold text-zinc-900 transition-colors whitespace-nowrap",
                          isAddingMember || !memberEmail.trim()
                            ? "opacity-50 cursor-not-allowed"
                            : "hover:bg-amber-500"
                        )}
                      >
                        {isAddingMember ? "Adding..." : "Add Member"}
                      </button>
                    </div>
                  </div>

                  {/* Team Members List */}
                  {members.length > 0 && (
                    <div>
                      <label className="block text-sm font-medium text-zinc-300 mb-3">
                        Team Members ({members.length})
                      </label>
                      <div className="space-y-2">
                        {members.map((member) => (
                          <div
                            key={member.id}
                            className="flex items-center justify-between rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-3"
                          >
                            <div className="flex items-center gap-3">
                              <div className="flex h-10 w-10 items-center justify-center rounded-full bg-zinc-700 text-sm font-medium text-zinc-100">
                                {member.email.charAt(0).toUpperCase()}
                              </div>
                              <div>
                                <p className="text-sm font-medium text-zinc-100">{member.email}</p>
                                <p className="text-xs text-zinc-400">{member.role}</p>
                              </div>
                            </div>
                            {member.role === "Lead" && (
                              <div className="flex items-center gap-1 rounded-full bg-amber-400/20 border border-amber-400/30 px-2 py-1">
                                <span className="text-xs font-medium text-amber-400">Lead</span>
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-between border-t border-zinc-800 px-6 py-4">
            {!createdCampaignId ? (
              <>
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
                  {isSubmitting ? "Creating..." : "Create Campaign"}
                </button>
              </>
            ) : (
              <>
                <button
                  type="button"
                  onClick={handleClose}
                  disabled={isAddingMember}
                  className="rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-2 text-sm font-medium text-zinc-300 hover:bg-zinc-800 transition-colors disabled:opacity-50"
                >
                  Skip
                </button>
                <button
                  type="button"
                  onClick={handleDone}
                  disabled={isAddingMember}
                  className={cn(
                    "rounded-lg bg-amber-400 px-4 py-2 text-sm font-semibold text-zinc-900 transition-colors",
                    isAddingMember
                      ? "opacity-50 cursor-not-allowed"
                      : "hover:bg-amber-500"
                  )}
                >
                  Done
                </button>
              </>
            )}
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
}
