"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import { useAuth, useUser } from "@clerk/nextjs";
import { apiFetch } from "@app/lib/api";
import { useCampaignName } from "@app/lib/useCampaignName";
import { Lock, Plus, Send, Users } from "lucide-react";

type ConversationParticipant = {
  user_id: string;
  display_name: string;
};

type CampaignMember = {
  user_id: string;
  display_name: string;
};

type Conversation = {
  id: string;
  type: "public" | "private";
  campaign_id: string;
  created_by: string;
  created_at: string;
  name?: string | null;
  joined_at?: string | null;
  history_access?: "full" | "from_join" | null;
  unread_count?: number;
  participants?: ConversationParticipant[];
};

type ConversationMessage = {
  id: string;
  conversation_id: string;
  content: string;
  created_at: string;
  author_id: string;
  author_display_name: string;
};

export default function ConversationsPage() {
  const params = useParams<{ id: string }>();
  const campaignId = String(params?.id || "");
  const { user } = useUser();
  const { getToken, isLoaded } = useAuth();
  const { displayName: campaignDisplayName } = useCampaignName(campaignId);

  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [selectedConversationId, setSelectedConversationId] = useState<string>("");
  const [messages, setMessages] = useState<ConversationMessage[]>([]);
  const [draft, setDraft] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const [campaignMembers, setCampaignMembers] = useState<CampaignMember[]>([]);

  // New Conversation modal state
  const [showNewConversation, setShowNewConversation] = useState(false);
  const [newConvName, setNewConvName] = useState("");
  const [newConvSelectedIds, setNewConvSelectedIds] = useState<Set<string>>(new Set());
  const [newConvLoading, setNewConvLoading] = useState(false);

  // Add People panel state
  const [showAddPeople, setShowAddPeople] = useState(false);
  const [addPeopleSelectedIds, setAddPeopleSelectedIds] = useState<Set<string>>(new Set());
  const [addPeopleHistoryAccess, setAddPeopleHistoryAccess] = useState<"full" | "from_join">("from_join");
  const [addPeopleLoading, setAddPeopleLoading] = useState(false);

  const currentUserId = String(user?.id || "");

  const publicConversation = useMemo(
    () => conversations.find((c) => c.type === "public") || null,
    [conversations],
  );

  const selectedConversation = useMemo(
    () => conversations.find((c) => c.id === selectedConversationId) || null,
    [conversations, selectedConversationId],
  );

  const conversationLabel = useCallback(
    (conversation: Conversation | null): string => {
      if (!conversation) return "Conversation";
      const explicitName = String(conversation.name || "").trim();
      if (explicitName) return explicitName;
      if (conversation.type === "public") return "Public Feed";

      const participants = (conversation.participants || []).filter((p) => p?.user_id);
      const others = participants.filter((p) => p.user_id !== currentUserId);

      if (others.length === 1) {
        return others[0].display_name || "Unknown user";
      }
      if (others.length > 1) {
        const first = others[0].display_name || "Unknown user";
        const second = others[1].display_name || "Unknown user";
        const remaining = others.length - 2;
        return remaining > 0 ? `${first}, ${second} +${remaining}` : `${first}, ${second}`;
      }
      return "Private Conversation";
    },
    [currentUserId],
  );

  const fetchCampaignMembers = useCallback(async () => {
    if (!campaignId || !isLoaded) return;
    try {
      const token = await getToken();
      if (!token) return;
      const data = await apiFetch<{ members: CampaignMember[] }>(
        `/api/v1/campaigns/${encodeURIComponent(campaignId)}/members?mentions_only=true`,
        { token, method: "GET" },
      );
      setCampaignMembers(data.members || []);
    } catch {
      setCampaignMembers([]);
    }
  }, [campaignId, getToken, isLoaded]);

  const fetchConversations = useCallback(async () => {
    if (!campaignId || !isLoaded) return;
    const token = await getToken();
    if (!token) return;
    const data = await apiFetch<{ conversations: Conversation[] }>(
      `/api/v1/conversations?campaign_id=${encodeURIComponent(campaignId)}`,
      { token, method: "GET" },
    );
    const next = data.conversations || [];
    setConversations(next);
    setSelectedConversationId((prev) => {
      if (prev && next.some((c) => c.id === prev)) return prev;
      const pub = next.find((c) => c.type === "public");
      return pub?.id || next[0]?.id || "";
    });
  }, [campaignId, getToken, isLoaded]);

  const fetchMessages = useCallback(async () => {
    if (!campaignId || !selectedConversationId || !isLoaded) return;
    const token = await getToken();
    if (!token) return;
    const data = await apiFetch<{ messages: ConversationMessage[] }>(
      `/api/v1/conversations/${encodeURIComponent(selectedConversationId)}/messages?campaign_id=${encodeURIComponent(campaignId)}&limit=100`,
      { token, method: "GET" },
    );
    setMessages(data.messages || []);
    await fetchConversations();
  }, [campaignId, fetchConversations, getToken, isLoaded, selectedConversationId]);

  useEffect(() => {
    void fetchConversations();
    void fetchCampaignMembers();
  }, [fetchConversations, fetchCampaignMembers]);

  useEffect(() => {
    void fetchMessages();
  }, [fetchMessages]);

  const handlePost = async (event: FormEvent) => {
    event.preventDefault();
    if (!selectedConversationId || !draft.trim()) return;
    try {
      setLoading(true);
      setError(null);
      const token = await getToken();
      if (!token) throw new Error("Authentication required.");
      await apiFetch(`/api/v1/conversations/${encodeURIComponent(selectedConversationId)}/messages`, {
        token,
        method: "POST",
        body: { campaign_id: campaignId, content: draft.trim(), mention_user_ids: [] },
      });
      setDraft("");
      await fetchMessages();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to send message.");
    } finally {
      setLoading(false);
    }
  };

  // --- New Conversation ---
  const openNewConversation = () => {
    setNewConvName("");
    setNewConvSelectedIds(new Set());
    setShowNewConversation(true);
    setError(null);
  };

  const toggleNewConvMember = (id: string) => {
    setNewConvSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleCreateConversation = async () => {
    if (newConvSelectedIds.size === 0) return;
    try {
      setNewConvLoading(true);
      setError(null);
      const token = await getToken();
      if (!token) throw new Error("Authentication required.");
      const created = await apiFetch<Conversation>("/api/v1/conversations", {
        token,
        method: "POST",
        body: {
          type: "private",
          campaign_id: campaignId,
          participants: Array.from(newConvSelectedIds),
          name: newConvName.trim() || undefined,
        },
      });
      setShowNewConversation(false);
      await fetchConversations();
      if (created?.id) setSelectedConversationId(created.id);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create conversation.");
    } finally {
      setNewConvLoading(false);
    }
  };

  // --- Add People ---
  const existingParticipantIds = useMemo(() => {
    const ids = new Set<string>();
    for (const p of selectedConversation?.participants || []) {
      if (p?.user_id) ids.add(p.user_id);
    }
    return ids;
  }, [selectedConversation]);

  const addableCampaignMembers = useMemo(
    () => campaignMembers.filter((m) => !existingParticipantIds.has(m.user_id)),
    [campaignMembers, existingParticipantIds],
  );

  const openAddPeople = () => {
    setAddPeopleSelectedIds(new Set());
    setAddPeopleHistoryAccess("from_join");
    setShowAddPeople(true);
    setError(null);
  };

  const toggleAddPeopleMember = (id: string) => {
    setAddPeopleSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleAddPeople = async () => {
    if (addPeopleSelectedIds.size === 0 || !selectedConversationId) return;
    try {
      setAddPeopleLoading(true);
      setError(null);
      const token = await getToken();
      if (!token) throw new Error("Authentication required.");
      for (const userId of addPeopleSelectedIds) {
        await apiFetch(
          `/api/v1/conversations/${encodeURIComponent(selectedConversationId)}/add-user`,
          {
            token,
            method: "POST",
            body: { user_id: userId, history_access: addPeopleHistoryAccess },
          },
        );
      }
      setShowAddPeople(false);
      await fetchConversations();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to add people.");
    } finally {
      setAddPeopleLoading(false);
    }
  };

  // --- Leave Conversation ---
  const [leaveLoading, setLeaveLoading] = useState(false);

  const handleLeaveConversation = async () => {
    if (!selectedConversationId || selectedConversation?.type !== "private") return;
    try {
      setLeaveLoading(true);
      setError(null);
      const token = await getToken();
      if (!token) throw new Error("Authentication required.");
      await apiFetch(
        `/api/v1/conversations/${encodeURIComponent(selectedConversationId)}/leave`,
        { token, method: "POST" },
      );
      setConversations((prev) => prev.filter((c) => c.id !== selectedConversationId));
      setSelectedConversationId(publicConversation?.id || "");
      setMessages([]);
      setShowAddPeople(false);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to leave conversation.");
    } finally {
      setLeaveLoading(false);
    }
  };

  // --- Member picker (shared UI) ---
  const renderMemberPicker = (
    members: CampaignMember[],
    selectedIds: Set<string>,
    onToggle: (id: string) => void,
    emptyLabel: string,
  ) => (
    <div className="max-h-48 space-y-1 overflow-y-auto">
      {members.length === 0 ? (
        <p className="py-2 text-xs text-[#6b7285]">{emptyLabel}</p>
      ) : (
        members.map((m) => (
          <label
            key={m.user_id}
            className="flex cursor-pointer items-center gap-2 rounded-lg px-2 py-1.5 text-[#1f2a44] transition hover:bg-[#efe7d7]"
          >
            <input
              type="checkbox"
              checked={selectedIds.has(m.user_id)}
              onChange={() => onToggle(m.user_id)}
              className="accent-[#1f2a44]"
            />
            <span className="text-sm">{m.display_name || "Unknown user"}</span>
          </label>
        ))
      )}
    </div>
  );

  // Only show members other than self for new conversation picker
  const newConvMembers = useMemo(
    () => campaignMembers.filter((m) => m.user_id !== currentUserId),
    [campaignMembers, currentUserId],
  );

  const privateConversations = useMemo(
    () => conversations.filter((c) => c.type === "private"),
    [conversations],
  );

  const directMessageConversations = useMemo(
    () =>
      privateConversations.filter((conversation) => {
        const participants = (conversation.participants || []).filter((p) => p.user_id !== currentUserId);
        return participants.length === 1;
      }),
    [privateConversations, currentUserId],
  );

  const groupConversations = useMemo(
    () =>
      privateConversations.filter((conversation) => {
        const participants = (conversation.participants || []).filter((p) => p.user_id !== currentUserId);
        return participants.length !== 1;
      }),
    [privateConversations, currentUserId],
  );
  const hasPrivateConversations = privateConversations.length > 0;

  const getInitials = useCallback((value: string): string => {
    const words = String(value || "")
      .trim()
      .split(/\s+/)
      .filter(Boolean);
    if (words.length === 0) return "PC";
    if (words.length === 1) return words[0].slice(0, 2).toUpperCase();
    return `${words[0][0] || ""}${words[1][0] || ""}`.toUpperCase();
  }, []);

  const formatMessageTimestamp = useCallback((timestamp: string): string => {
    const value = new Date(timestamp);
    if (Number.isNaN(value.getTime())) return "";
    return value.toLocaleString([], {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });
  }, []);

  const privateConversationPreview = useCallback(
    (conversation: Conversation): string => {
      const participants = (conversation.participants || [])
        .filter((p) => p.user_id && p.user_id !== currentUserId)
        .map((p) => p.display_name || "Unknown user");
      if (participants.length === 0) return "Private thread";
      if (participants.length === 1) return `With ${participants[0]}`;
      return `${participants.length} participants`;
    },
    [currentUserId],
  );

  return (
    <div className="flex h-full min-h-0 flex-col bg-[#f7f5ef] text-[#1f2a44]">
      <header className="border-b border-[#e5ddce] px-6 py-5 lg:px-8">
        <h1 className="text-2xl font-semibold tracking-tight text-[#1f2a44]">
          {campaignDisplayName || "Campaign"} Conversations
        </h1>
        <p className="mt-1 text-sm text-[#6b7285]">
          Public forum is shared across the campaign. Private conversations are listed in the right panel.
        </p>
      </header>

      {error ? (
        <div className="border-b border-[#efd3d3] bg-[#fff5f5] px-6 py-2 text-sm text-[#9f3a38] lg:px-8">{error}</div>
      ) : null}

      <div className="flex min-h-0 flex-1 flex-col lg:flex-row">
        <section className="flex min-h-0 flex-1 flex-col border-b border-[#e5ddce] lg:border-b-0 lg:border-r lg:border-[#e5ddce]">
          {showNewConversation ? (
            <div className="space-y-5 px-6 py-6 lg:px-8">
              <div className="flex items-center justify-between gap-3">
                <p className="text-base font-semibold text-[#1f2a44]">New Private Conversation</p>
                <button
                  type="button"
                  onClick={() => setShowNewConversation(false)}
                  className="rounded-md px-2 py-1 text-xs text-[#6b7285] transition hover:bg-[#f0eadf] hover:text-[#1f2a44]"
                >
                  Cancel
                </button>
              </div>
              <input
                value={newConvName}
                onChange={(e) => setNewConvName(e.target.value)}
                placeholder="Conversation name (optional)"
                className="w-full rounded-xl border border-[#dfd6c5] bg-[#fbfaf6] px-3 py-2.5 text-sm text-[#1f2a44] placeholder:text-[#8a93a5] outline-none focus:border-[#d4c7ad] focus:bg-white"
              />
              <div>
                <p className="mb-2 text-xs font-medium uppercase tracking-wide text-[#6b7285]">Select participants</p>
                {renderMemberPicker(
                  newConvMembers,
                  newConvSelectedIds,
                  toggleNewConvMember,
                  "No other campaign members found.",
                )}
              </div>
              <button
                type="button"
                onClick={handleCreateConversation}
                disabled={newConvLoading || newConvSelectedIds.size === 0}
                className="rounded-lg bg-[#1f2a44] px-4 py-2.5 text-sm font-medium text-[#f7f5ef] transition hover:bg-[#273657] disabled:opacity-50"
              >
                {newConvLoading ? "Creating..." : "Create Conversation"}
              </button>
            </div>
          ) : showAddPeople && selectedConversation?.type === "private" ? (
            <div className="space-y-5 px-6 py-6 lg:px-8">
              <div className="flex items-center justify-between gap-3">
                <p className="text-base font-semibold text-[#1f2a44]">
                  Add People to {conversationLabel(selectedConversation)}
                </p>
                <button
                  type="button"
                  onClick={() => setShowAddPeople(false)}
                  className="rounded-md px-2 py-1 text-xs text-[#6b7285] transition hover:bg-[#f0eadf] hover:text-[#1f2a44]"
                >
                  Cancel
                </button>
              </div>
              <div>
                <p className="mb-2 text-xs font-medium uppercase tracking-wide text-[#6b7285]">Select members to add</p>
                {renderMemberPicker(
                  addableCampaignMembers,
                  addPeopleSelectedIds,
                  toggleAddPeopleMember,
                  "All campaign members are already in this conversation.",
                )}
              </div>
              <div>
                <p className="mb-2 text-xs font-medium uppercase tracking-wide text-[#6b7285]">History access</p>
                <div className="flex flex-wrap gap-4">
                  <label className="flex cursor-pointer items-center gap-1.5 text-sm text-[#1f2a44]">
                    <input
                      type="radio"
                      name="history_access"
                      checked={addPeopleHistoryAccess === "from_join"}
                      onChange={() => setAddPeopleHistoryAccess("from_join")}
                      className="accent-[#1f2a44]"
                    />
                    From now on
                  </label>
                  <label className="flex cursor-pointer items-center gap-1.5 text-sm text-[#1f2a44]">
                    <input
                      type="radio"
                      name="history_access"
                      checked={addPeopleHistoryAccess === "full"}
                      onChange={() => setAddPeopleHistoryAccess("full")}
                      className="accent-[#1f2a44]"
                    />
                    Full history
                  </label>
                </div>
              </div>
              <button
                type="button"
                onClick={handleAddPeople}
                disabled={addPeopleLoading || addPeopleSelectedIds.size === 0}
                className="rounded-lg bg-[#1f2a44] px-4 py-2.5 text-sm font-medium text-[#f7f5ef] transition hover:bg-[#273657] disabled:opacity-50"
              >
                {addPeopleLoading ? "Adding..." : "Add People"}
              </button>
            </div>
          ) : (
            <>
              <div className="flex items-start justify-between gap-4 border-b border-[#ece4d4] px-6 py-4 lg:px-8">
                <div className="min-w-0">
                  <div className="flex items-center gap-2.5">
                    <Users className="h-4 w-4 text-[#6b7285]" />
                    <p className="text-base font-semibold text-[#1f2a44]">
                      {selectedConversation?.type === "public"
                        ? "Public Forum"
                        : conversationLabel(selectedConversation)}
                    </p>
                    <span
                      className={`rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${
                        selectedConversation?.type === "public"
                          ? "bg-[#f0e8d4] text-[#6a5830]"
                          : "bg-[#e9edf4] text-[#455975]"
                      }`}
                    >
                      {selectedConversation?.type === "public" ? "Campaign-wide" : "Private"}
                    </span>
                  </div>
                  <p className="mt-1 line-clamp-2 text-xs text-[#6b7285]">
                    {selectedConversation?.type === "public"
                      ? "Visible to everyone in this campaign."
                      : (selectedConversation?.participants || [])
                          .map((p) => p.display_name || "Unknown user")
                          .join(", ")}
                  </p>
                </div>
                {selectedConversation?.type === "private" ? (
                  <div className="flex shrink-0 items-center gap-2">
                    <button
                      type="button"
                      onClick={openAddPeople}
                      className="rounded-md border border-[#d8d0c0] bg-[#f7f2e8] px-2.5 py-1.5 text-xs font-medium text-[#1f2a44] transition hover:bg-[#f0e8d8]"
                    >
                      Add People
                    </button>
                    <button
                      type="button"
                      onClick={handleLeaveConversation}
                      disabled={leaveLoading}
                      className="rounded-md border border-[#e6c6c6] bg-[#fff3f3] px-2.5 py-1.5 text-xs font-medium text-[#a13d3a] transition hover:bg-[#ffeaea] disabled:opacity-50"
                    >
                      {leaveLoading ? "Leaving..." : "Leave"}
                    </button>
                  </div>
                ) : null}
              </div>

              <div className="flex-1 space-y-6 overflow-y-auto px-6 py-5 lg:px-8">
                {messages.length === 0 ? (
                  <div className="rounded-xl border border-dashed border-[#d9cfbd] bg-[#fbf8f2] px-4 py-5 text-sm text-[#6b7285]">
                    {selectedConversation?.type === "public"
                      ? "No public messages yet. Start the campaign-wide conversation here."
                      : "No private messages yet. Start this thread with a quick update."}
                  </div>
                ) : (
                  messages.map((message) => (
                    <article key={message.id} className="flex items-start gap-3">
                      <div className="mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-[#ece5d7] text-xs font-semibold text-[#34445f]">
                        {getInitials(message.author_display_name || "Unknown user")}
                      </div>
                      <div className="min-w-0 max-w-3xl">
                        <div className="flex flex-wrap items-baseline gap-2">
                          <p className="text-sm font-medium text-[#1f2a44]">
                            {message.author_display_name || "Unknown user"}
                          </p>
                          <p className="text-[11px] text-[#7f8798]">{formatMessageTimestamp(message.created_at)}</p>
                        </div>
                        <p className="mt-0.5 whitespace-pre-wrap text-sm leading-relaxed text-[#2a3650]">{message.content}</p>
                      </div>
                    </article>
                  ))
                )}
              </div>

              <form onSubmit={handlePost} className="mt-auto border-t border-[#ece4d4] px-6 pb-5 pt-3 lg:px-8">
                <div className="flex items-center gap-3 rounded-xl border border-[#ddd3c2] bg-[#fbfaf6] px-3 py-2.5">
                  <input
                    value={draft}
                    onChange={(e) => setDraft(e.target.value)}
                    placeholder={
                      selectedConversation?.type === "public"
                        ? "Message the campaign public forum..."
                        : "Message this private conversation..."
                    }
                    className="h-8 flex-1 bg-transparent text-sm text-[#1f2a44] placeholder:text-[#8a93a5] outline-none"
                  />
                  <button
                    type="submit"
                    disabled={loading || !selectedConversationId || !draft.trim()}
                    className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-[#e0c469] text-[#1f2a44] transition hover:bg-[#d6bb62] disabled:opacity-50"
                  >
                    <Send className="h-3.5 w-3.5" />
                  </button>
                </div>
              </form>
            </>
          )}
        </section>

        <aside className="flex min-h-0 w-full shrink-0 flex-col bg-[#f3eee4] lg:w-[336px]">
          <div className="flex items-center justify-between border-b border-[#e5ddce] px-5 py-4">
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <Lock className="h-3.5 w-3.5 text-[#6b7285]" />
                <p className="text-xs font-semibold uppercase tracking-[0.12em] text-[#6b7285]">Private Conversations</p>
              </div>
              <p className="mt-1 text-[11px] text-[#7b8292]">Direct and group threads only visible to participants</p>
            </div>
            <button
              type="button"
              onClick={openNewConversation}
              className="inline-flex h-8 items-center justify-center gap-1.5 rounded-md border border-[#d8d0c0] bg-[#fdfbf7] px-2.5 text-xs font-medium text-[#52607a] transition hover:bg-white"
              title="New private conversation"
              aria-label="New private conversation"
            >
              <Plus className="h-3.5 w-3.5" />
              New
            </button>
          </div>

          <div className="flex-1 space-y-5 overflow-y-auto px-3 py-3">
            {!hasPrivateConversations ? (
              <div className="rounded-lg border border-dashed border-[#d7cdbb] bg-[#f8f3e9] px-3 py-2.5 text-xs text-[#6b7285]">
                No private threads yet. Use <span className="font-medium text-[#1f2a44]">New</span> to start one.
              </div>
            ) : null}
            <div className="space-y-2">
              <p className="px-2 text-[11px] font-medium uppercase tracking-wide text-[#7b8292]">Public</p>
              {publicConversation ? (
                <button
                  type="button"
                  onClick={() => {
                    setSelectedConversationId(publicConversation.id);
                    setShowNewConversation(false);
                    setShowAddPeople(false);
                  }}
                  className={`w-full rounded-xl px-3 py-2.5 text-left transition ${
                    publicConversation.id === selectedConversationId
                      ? "bg-[#f7f2e8] ring-1 ring-[#dacdaf]"
                      : "hover:bg-[#efe7d7]"
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <p className="truncate text-sm font-medium text-[#1f2a44]">Public Forum</p>
                    {(publicConversation.unread_count || 0) > 0 ? (
                      <span className="rounded-full bg-[#e0c469] px-2 py-0.5 text-xs font-semibold text-[#1f2a44] shadow-[0_0_0_1px_rgba(31,42,68,0.05)]">
                        {publicConversation.unread_count}
                      </span>
                    ) : null}
                  </div>
                  <p className="text-xs text-[#6b7285]">Visible to everyone</p>
                </button>
              ) : null}
            </div>

            <div className="space-y-2">
              <p className="px-2 text-[11px] font-medium uppercase tracking-wide text-[#7b8292]">Direct Messages</p>
              {directMessageConversations.length === 0 ? (
                <p className="px-2 text-xs text-[#6b7285]">No direct messages yet.</p>
              ) : (
                directMessageConversations.map((conversation) => (
                  <button
                    key={conversation.id}
                    type="button"
                    onClick={() => {
                      setSelectedConversationId(conversation.id);
                      setShowNewConversation(false);
                      setShowAddPeople(false);
                    }}
                    className={`w-full rounded-xl px-3 py-2.5 text-left transition ${
                      conversation.id === selectedConversationId
                        ? "bg-[#f7f2e8] ring-1 ring-[#dacdaf]"
                        : "hover:bg-[#efe7d7]"
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <p className="truncate text-sm font-medium text-[#1f2a44]">{conversationLabel(conversation)}</p>
                      {(conversation.unread_count || 0) > 0 ? (
                        <span className="rounded-full bg-[#e0c469] px-2 py-0.5 text-xs font-semibold text-[#1f2a44] shadow-[0_0_0_1px_rgba(31,42,68,0.05)]">
                          {conversation.unread_count}
                        </span>
                      ) : null}
                    </div>
                    <p className="mt-0.5 truncate text-[11px] text-[#6b7285]">{privateConversationPreview(conversation)}</p>
                  </button>
                ))
              )}
            </div>

            <div className="space-y-2">
              <p className="px-2 text-[11px] font-medium uppercase tracking-wide text-[#7b8292]">Group Conversations</p>
              {groupConversations.length === 0 ? (
                <p className="px-2 text-xs text-[#6b7285]">No group conversations yet.</p>
              ) : (
                groupConversations.map((conversation) => (
                  <button
                    key={conversation.id}
                    type="button"
                    onClick={() => {
                      setSelectedConversationId(conversation.id);
                      setShowNewConversation(false);
                      setShowAddPeople(false);
                    }}
                    className={`w-full rounded-xl px-3 py-2.5 text-left transition ${
                      conversation.id === selectedConversationId
                        ? "bg-[#f7f2e8] ring-1 ring-[#dacdaf]"
                        : "hover:bg-[#efe7d7]"
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <p className="truncate text-sm font-medium text-[#1f2a44]">{conversationLabel(conversation)}</p>
                      {(conversation.unread_count || 0) > 0 ? (
                        <span className="rounded-full bg-[#e0c469] px-2 py-0.5 text-xs font-semibold text-[#1f2a44] shadow-[0_0_0_1px_rgba(31,42,68,0.05)]">
                          {conversation.unread_count}
                        </span>
                      ) : null}
                    </div>
                    <p className="mt-0.5 truncate text-[11px] text-[#6b7285]">{privateConversationPreview(conversation)}</p>
                  </button>
                ))
              )}
            </div>
          </div>
        </aside>
      </div>
    </div>
  );
}
