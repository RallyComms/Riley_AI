"use client";

import { useEffect, useMemo, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { CheckCircle2, Search, Shield, UserPlus, X } from "lucide-react";
import { AnimatePresence, motion } from "framer-motion";
import { cn } from "@app/lib/utils";
import { apiFetch } from "@app/lib/api";

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

interface Member {
  id: string;
  email: string;
  role: "Lead" | "Member" | string;
}

interface DirectoryUserResult {
  id: string;
  email: string;
  display_name: string;
}

interface UserSearchResponse {
  users: DirectoryUserResult[];
}

const THEME_COLORS = ["#0f766e", "#4f46e5", "#e11d48", "#059669", "#7c3aed", "#facc15"];

export function CreateCampaignModal({ isOpen, onClose, onCampaignCreated }: CreateCampaignModalProps) {
  const { getToken } = useAuth();

  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const [createdCampaignId, setCreatedCampaignId] = useState<string | null>(null);
  const [members, setMembers] = useState<Member[]>([]);

  const [memberSearchQuery, setMemberSearchQuery] = useState("");
  const [memberSearchResults, setMemberSearchResults] = useState<DirectoryUserResult[]>([]);
  const [memberSearchError, setMemberSearchError] = useState<string | null>(null);
  const [isSearchingMembers, setIsSearchingMembers] = useState(false);

  const [selectedMembers, setSelectedMembers] = useState<
    Array<DirectoryUserResult & { role: "Lead" | "Member" }>
  >([]);
  const [isApplyingSelections, setIsApplyingSelections] = useState(false);
  const [selectionError, setSelectionError] = useState<string | null>(null);
  const [selectionSuccess, setSelectionSuccess] = useState<string | null>(null);

  const isBusy = isSubmitting || isSearchingMembers || isApplyingSelections;

  const resetState = () => {
    setName("");
    setDescription("");
    setSubmitError(null);
    setIsSubmitting(false);
    setCreatedCampaignId(null);
    setMembers([]);
    setMemberSearchQuery("");
    setMemberSearchResults([]);
    setMemberSearchError(null);
    setIsSearchingMembers(false);
    setSelectedMembers([]);
    setIsApplyingSelections(false);
    setSelectionError(null);
    setSelectionSuccess(null);
  };

  const handleClose = () => {
    if (isBusy) return;
    resetState();
    onClose();
  };

  const fetchMembers = async (campaignId: string, token: string) => {
    const membersData = await apiFetch<{ members: Member[] }>(
      `/api/v1/campaigns/${encodeURIComponent(campaignId)}/members`,
      { token, method: "GET" },
    );
    setMembers(membersData.members || []);
  };

  const handleSubmitCreate = async () => {
    if (!name.trim()) {
      setSubmitError("Campaign name is required");
      return;
    }
    setSubmitError(null);
    setSelectionError(null);
    setSelectionSuccess(null);
    setIsSubmitting(true);
    try {
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");

      const createdCampaign = await apiFetch<{ id: string; name: string; description: string | null }>(
        "/api/v1/campaigns",
        {
          token,
          method: "POST",
          body: {
            name: name.trim(),
            description: description.trim() || null,
          },
        },
      );

      const colorIndex =
        Math.abs(
          createdCampaign.id
            .split("")
            .reduce((acc: number, char: string) => acc + char.charCodeAt(0), 0),
        ) % THEME_COLORS.length;

      onCampaignCreated({
        name: createdCampaign.name,
        role: "Lead Strategist",
        lastActive: "Just now",
        mentions: 0,
        pendingRequests: 0,
        userRole: "Lead",
        themeColor: THEME_COLORS[colorIndex],
        campaignId: createdCampaign.id,
      });

      setCreatedCampaignId(createdCampaign.id);
      await fetchMembers(createdCampaign.id, token);
      setSelectionSuccess("Mission created. Add team members and roles below.");
    } catch (err) {
      setSubmitError(err instanceof Error ? err.message : "Failed to create campaign.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const existingMemberIds = useMemo(() => new Set((members || []).map((m) => m.id)), [members]);
  const selectedMemberIds = useMemo(() => new Set(selectedMembers.map((m) => m.id)), [selectedMembers]);

  useEffect(() => {
    if (!createdCampaignId) {
      setMemberSearchResults([]);
      return;
    }
    const query = memberSearchQuery.trim();
    if (query.length < 2) {
      setMemberSearchResults([]);
      setMemberSearchError(null);
      return;
    }
    const handle = window.setTimeout(async () => {
      try {
        setIsSearchingMembers(true);
        setMemberSearchError(null);
        const token = await getToken();
        if (!token) return;
        const data = await apiFetch<UserSearchResponse>(
          `/api/v1/campaigns/${encodeURIComponent(createdCampaignId)}/users/search?query=${encodeURIComponent(query)}&limit=10`,
          { token, method: "GET" },
        );
        const filtered = (data.users || []).filter(
          (candidate) => !existingMemberIds.has(candidate.id) && !selectedMemberIds.has(candidate.id),
        );
        setMemberSearchResults(filtered);
      } catch (err) {
        setMemberSearchResults([]);
        setMemberSearchError(err instanceof Error ? err.message : "Failed to search users.");
      } finally {
        setIsSearchingMembers(false);
      }
    }, 250);
    return () => window.clearTimeout(handle);
  }, [createdCampaignId, existingMemberIds, getToken, memberSearchQuery, selectedMemberIds]);

  const addSearchCandidate = (candidate: DirectoryUserResult) => {
    setSelectedMembers((prev) => {
      if (prev.some((member) => member.id === candidate.id)) return prev;
      return [...prev, { ...candidate, role: "Member" }];
    });
    setMemberSearchResults((prev) => prev.filter((item) => item.id !== candidate.id));
    setMemberSearchQuery("");
  };

  const removeSelectedCandidate = (candidateId: string) => {
    setSelectedMembers((prev) => prev.filter((member) => member.id !== candidateId));
  };

  const updateSelectedRole = (candidateId: string, role: "Lead" | "Member") => {
    setSelectedMembers((prev) =>
      prev.map((member) => (member.id === candidateId ? { ...member, role } : member)),
    );
  };

  const applySelectedMembers = async (): Promise<boolean> => {
    if (!createdCampaignId || selectedMembers.length === 0) return true;

    setIsApplyingSelections(true);
    setSelectionError(null);
    setSelectionSuccess(null);
    try {
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");

      for (const member of selectedMembers) {
        await apiFetch(`/api/v1/campaigns/${encodeURIComponent(createdCampaignId)}/members`, {
          token,
          method: "POST",
          body: {
            email: member.email,
            user_id: member.id,
            role: member.role,
          },
        });
      }

      await fetchMembers(createdCampaignId, token);
      setSelectionSuccess(
        `${selectedMembers.length} member${selectedMembers.length === 1 ? "" : "s"} added successfully.`,
      );
      setSelectedMembers([]);
      return true;
    } catch (err) {
      setSelectionError(err instanceof Error ? err.message : "Failed to add selected members.");
      return false;
    } finally {
      setIsApplyingSelections(false);
    }
  };

  const handleDone = async () => {
    if (!createdCampaignId) return;
    const success = await applySelectedMembers();
    if (!success) return;
    resetState();
    onClose();
  };

  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          className="absolute inset-0 bg-slate-900/30 backdrop-blur-sm"
          onClick={handleClose}
        />

        <motion.div
          initial={{ opacity: 0, scale: 0.97, y: 8 }}
          animate={{ opacity: 1, scale: 1, y: 0 }}
          exit={{ opacity: 0, scale: 0.97, y: 8 }}
          transition={{ duration: 0.18 }}
          className="relative w-full max-w-3xl overflow-hidden rounded-2xl bg-[#fbf8f2] shadow-[0_6px_20px_rgba(15,23,42,0.14)]"
        >
          <div className="flex items-center justify-between border-b border-[#e7dfcf] px-6 py-4">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-[#efe6d7] text-[#2a3d64]">
                <Shield className="h-5 w-5" />
              </div>
              <div>
                <h2 className="text-lg font-semibold text-[#1f2a44]">Create Mission</h2>
                <p className="text-xs text-[#6d7688]">Set campaign details and assign your team</p>
              </div>
            </div>
            <button
              type="button"
              onClick={handleClose}
              disabled={isBusy}
              className="rounded-md p-1.5 text-[#687287] transition hover:bg-[#efe6d7] hover:text-[#1f2a44] disabled:opacity-50"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          <div className="max-h-[68vh] overflow-y-auto px-6 py-6">
            <div className="space-y-6">
              {submitError ? (
                <div className="rounded-xl bg-[#fff0f0] px-4 py-3 text-sm text-[#9e3434]">{submitError}</div>
              ) : null}
              {selectionError ? (
                <div className="rounded-xl bg-[#fff0f0] px-4 py-3 text-sm text-[#9e3434]">{selectionError}</div>
              ) : null}
              {selectionSuccess ? (
                <div className="flex items-center gap-2 rounded-xl bg-[#edf7ee] px-4 py-3 text-sm text-[#2f6d3a]">
                  <CheckCircle2 className="h-4 w-4" />
                  {selectionSuccess}
                </div>
              ) : null}

              <section className="space-y-4">
                <h3 className="text-sm font-semibold uppercase tracking-wide text-[#5f6778]">Campaign Details</h3>
                <div>
                  <label className="mb-2 block text-sm font-medium text-[#1f2a44]">Mission Name</label>
                  <input
                    type="text"
                    value={name}
                    onChange={(event) => setName(event.target.value)}
                    placeholder="e.g., Clean Water Initiative 2025"
                    disabled={isSubmitting || !!createdCampaignId}
                    className="w-full rounded-xl border border-[#e2d8c4] bg-white px-4 py-3 text-sm text-[#1f2a44] placeholder:text-[#8a90a0] focus:outline-none focus:ring-2 focus:ring-[#d5c59b] disabled:bg-[#f3efe6] disabled:text-[#8a90a0]"
                  />
                </div>
                <div>
                  <label className="mb-2 block text-sm font-medium text-[#1f2a44]">Description (Optional)</label>
                  <textarea
                    value={description}
                    onChange={(event) => setDescription(event.target.value)}
                    placeholder="Brief description of the campaign..."
                    rows={3}
                    disabled={isSubmitting || !!createdCampaignId}
                    className="w-full resize-none rounded-xl border border-[#e2d8c4] bg-white px-4 py-3 text-sm text-[#1f2a44] placeholder:text-[#8a90a0] focus:outline-none focus:ring-2 focus:ring-[#d5c59b] disabled:bg-[#f3efe6] disabled:text-[#8a90a0]"
                  />
                </div>
                <div className="rounded-xl bg-[#f2ecdf] px-4 py-3 text-sm text-[#4f5970]">
                  You are added automatically as <span className="font-semibold">Lead</span>.
                </div>
              </section>

              <section className="space-y-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-semibold uppercase tracking-wide text-[#5f6778]">Team Members</h3>
                  {createdCampaignId ? (
                    <span className="text-xs text-[#6d7688]">Search and add users with roles</span>
                  ) : (
                    <span className="text-xs text-[#8a90a0]">Create mission first to enable user search</span>
                  )}
                </div>

                <div className="rounded-xl border border-[#e2d8c4] bg-white px-3 py-2">
                  <div className="flex items-center gap-2">
                    <Search className="h-4 w-4 text-[#8a90a0]" />
                    <input
                      type="text"
                      value={memberSearchQuery}
                      onChange={(event) => setMemberSearchQuery(event.target.value)}
                      placeholder={
                        createdCampaignId
                          ? "Search users by name or email..."
                          : "Create mission first to search users..."
                      }
                      disabled={!createdCampaignId || isSearchingMembers}
                      className="w-full bg-transparent py-1 text-sm text-[#1f2a44] placeholder:text-[#8a90a0] focus:outline-none disabled:text-[#9aa1b0]"
                    />
                  </div>
                </div>

                {memberSearchError ? <p className="text-xs text-[#9e3434]">{memberSearchError}</p> : null}

                {createdCampaignId && (isSearchingMembers || memberSearchResults.length > 0) ? (
                  <div className="max-h-40 space-y-1 overflow-y-auto rounded-xl border border-[#e2d8c4] bg-white p-2">
                    {isSearchingMembers ? (
                      <p className="px-2 py-1.5 text-xs text-[#8a90a0]">Searching users...</p>
                    ) : (
                      memberSearchResults.map((candidate) => (
                        <button
                          key={candidate.id}
                          type="button"
                          onClick={() => addSearchCandidate(candidate)}
                          className="flex w-full items-center justify-between rounded-lg px-2 py-2 text-left transition hover:bg-[#f5f0e5]"
                        >
                          <div>
                            <p className="text-sm text-[#1f2a44]">{candidate.display_name || candidate.email}</p>
                            <p className="text-xs text-[#7b8395]">{candidate.email}</p>
                          </div>
                          <span className="inline-flex items-center gap-1 text-xs font-medium text-[#2a3d64]">
                            <UserPlus className="h-3.5 w-3.5" />
                            Add
                          </span>
                        </button>
                      ))
                    )}
                  </div>
                ) : null}

                {selectedMembers.length > 0 ? (
                  <div className="space-y-2">
                    <p className="text-xs font-medium text-[#5f6778]">Selected members</p>
                    {selectedMembers.map((member) => (
                      <div
                        key={member.id}
                        className="flex items-center justify-between rounded-xl border border-[#e2d8c4] bg-white px-3 py-2"
                      >
                        <div>
                          <p className="text-sm text-[#1f2a44]">{member.display_name || member.email}</p>
                          <p className="text-xs text-[#7b8395]">{member.email}</p>
                        </div>
                        <div className="flex items-center gap-2">
                          <select
                            value={member.role}
                            onChange={(event) =>
                              updateSelectedRole(member.id, event.target.value as "Lead" | "Member")
                            }
                            className="rounded-md border border-[#ded4c2] bg-white px-2 py-1 text-xs text-[#1f2a44] focus:outline-none focus:ring-2 focus:ring-[#d5c59b]"
                          >
                            <option value="Member">Member</option>
                            <option value="Lead">Lead</option>
                          </select>
                          <button
                            type="button"
                            onClick={() => removeSelectedCandidate(member.id)}
                            className="rounded-md p-1 text-[#8a90a0] hover:bg-[#efe6d7] hover:text-[#4d5871]"
                            aria-label={`Remove ${member.email}`}
                          >
                            <X className="h-4 w-4" />
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : null}

                {members.length > 0 ? (
                  <div className="space-y-2">
                    <p className="text-xs font-medium text-[#5f6778]">Current team ({members.length})</p>
                    <div className="space-y-1">
                      {members.map((member) => (
                        <div key={member.id} className="flex items-center justify-between rounded-lg bg-[#f5f0e5] px-3 py-2">
                          <span className="text-sm text-[#2a3d64]">{member.email}</span>
                          <span className="text-xs font-medium text-[#5f6778]">{member.role}</span>
                        </div>
                      ))}
                    </div>
                  </div>
                ) : null}
              </section>
            </div>
          </div>

          <div className="flex items-center justify-between border-t border-[#e7dfcf] px-6 py-4">
            {!createdCampaignId ? (
              <>
                <button
                  type="button"
                  onClick={handleClose}
                  disabled={isBusy}
                  className="rounded-lg border border-[#ded4c2] px-4 py-2 text-sm font-medium text-[#4d5871] transition hover:bg-[#f0ebde] disabled:opacity-50"
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={handleSubmitCreate}
                  disabled={isSubmitting || !name.trim()}
                  className={cn(
                    "rounded-lg px-4 py-2 text-sm font-semibold text-[#1f2a44] transition",
                    isSubmitting || !name.trim()
                      ? "cursor-not-allowed bg-[#e8dcc0] opacity-60"
                      : "bg-[#f1cf63] hover:bg-[#e6c257]",
                  )}
                >
                  {isSubmitting ? "Creating..." : "Create Mission"}
                </button>
              </>
            ) : (
              <>
                <button
                  type="button"
                  onClick={handleClose}
                  disabled={isBusy}
                  className="rounded-lg border border-[#ded4c2] px-4 py-2 text-sm font-medium text-[#4d5871] transition hover:bg-[#f0ebde] disabled:opacity-50"
                >
                  Cancel
                </button>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={applySelectedMembers}
                    disabled={isApplyingSelections || selectedMembers.length === 0}
                    className={cn(
                      "rounded-lg border px-4 py-2 text-sm font-medium transition",
                      isApplyingSelections || selectedMembers.length === 0
                        ? "cursor-not-allowed border-[#ddd4c3] text-[#9aa1b0] opacity-70"
                        : "border-[#d5c59b] text-[#2a3d64] hover:bg-[#f5efdf]",
                    )}
                  >
                    {isApplyingSelections
                      ? "Adding..."
                      : `Add Selected${selectedMembers.length > 0 ? ` (${selectedMembers.length})` : ""}`}
                  </button>
                  <button
                    type="button"
                    onClick={handleDone}
                    disabled={isBusy}
                    className={cn(
                      "rounded-lg px-4 py-2 text-sm font-semibold text-[#1f2a44] transition",
                      isBusy ? "cursor-not-allowed bg-[#e8dcc0] opacity-60" : "bg-[#f1cf63] hover:bg-[#e6c257]",
                    )}
                  >
                    Done
                  </button>
                </div>
              </>
            )}
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
}
