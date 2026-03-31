"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { useAuth, useUser } from "@clerk/nextjs";
import { apiFetch } from "@app/lib/api";
import { useCampaignName } from "@app/lib/useCampaignName";

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
        return others[0].display_name || others[0].user_id;
      }
      if (others.length > 1) {
        const first = others[0].display_name || others[0].user_id;
        const second = others[1].display_name || others[1].user_id;
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
        <p className="py-2 text-xs text-zinc-500">{emptyLabel}</p>
      ) : (
        members.map((m) => (
          <label
            key={m.user_id}
            className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 hover:bg-zinc-800"
          >
            <input
              type="checkbox"
              checked={selectedIds.has(m.user_id)}
              onChange={() => onToggle(m.user_id)}
              className="accent-blue-500"
            />
            <span className="text-sm text-zinc-200">{m.display_name || m.user_id}</span>
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

  return (
    <div className="min-h-screen bg-zinc-950 p-6 text-zinc-100">
      <div className="mx-auto max-w-[1400px] space-y-4">
        <header className="flex items-start justify-between gap-4">
          <div>
            <h1 className="text-2xl font-semibold">{campaignDisplayName}</h1>
            <p className="text-lg font-medium text-zinc-100">Public Forum</p>
            <p className="text-sm text-zinc-400">Visible to everyone in this campaign</p>
          </div>
          <Link
            href={`/campaign/${campaignId}`}
            className="rounded border border-zinc-700 px-3 py-1.5 text-sm hover:bg-zinc-800"
          >
            Back
          </Link>
        </header>

        {error ? <p className="text-sm text-red-300">{error}</p> : null}

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
          <section className="flex h-[78vh] flex-col overflow-hidden rounded border border-zinc-800 bg-zinc-900">
            {showNewConversation ? (
              <div className="space-y-4 p-4">
                <div className="flex items-center justify-between">
                  <p className="text-base font-medium">New Conversation</p>
                  <button
                    type="button"
                    onClick={() => setShowNewConversation(false)}
                    className="text-xs text-zinc-400 hover:text-zinc-200"
                  >
                    Cancel
                  </button>
                </div>
                <input
                  value={newConvName}
                  onChange={(e) => setNewConvName(e.target.value)}
                  placeholder="Conversation name (optional)"
                  className="w-full rounded border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm"
                />
                <div>
                  <p className="mb-1 text-xs font-medium text-zinc-400">Select participants</p>
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
                  className="rounded border border-blue-600/50 bg-blue-500/10 px-4 py-2 text-sm text-blue-300 hover:bg-blue-500/20 disabled:opacity-50"
                >
                  {newConvLoading ? "Creating..." : "Create Conversation"}
                </button>
              </div>
            ) : showAddPeople && selectedConversation?.type === "private" ? (
              <div className="space-y-4 p-4">
                <div className="flex items-center justify-between">
                  <p className="text-base font-medium">
                    Add People to {conversationLabel(selectedConversation)}
                  </p>
                  <button
                    type="button"
                    onClick={() => setShowAddPeople(false)}
                    className="text-xs text-zinc-400 hover:text-zinc-200"
                  >
                    Cancel
                  </button>
                </div>
                <div>
                  <p className="mb-1 text-xs font-medium text-zinc-400">Select members to add</p>
                  {renderMemberPicker(
                    addableCampaignMembers,
                    addPeopleSelectedIds,
                    toggleAddPeopleMember,
                    "All campaign members are already in this conversation.",
                  )}
                </div>
                <div>
                  <p className="mb-1 text-xs font-medium text-zinc-400">History access</p>
                  <div className="flex gap-3">
                    <label className="flex cursor-pointer items-center gap-1.5 text-sm text-zinc-200">
                      <input
                        type="radio"
                        name="history_access"
                        checked={addPeopleHistoryAccess === "from_join"}
                        onChange={() => setAddPeopleHistoryAccess("from_join")}
                        className="accent-blue-500"
                      />
                      From now on
                    </label>
                    <label className="flex cursor-pointer items-center gap-1.5 text-sm text-zinc-200">
                      <input
                        type="radio"
                        name="history_access"
                        checked={addPeopleHistoryAccess === "full"}
                        onChange={() => setAddPeopleHistoryAccess("full")}
                        className="accent-blue-500"
                      />
                      Full history
                    </label>
                  </div>
                </div>
                <button
                  type="button"
                  onClick={handleAddPeople}
                  disabled={addPeopleLoading || addPeopleSelectedIds.size === 0}
                  className="rounded border border-blue-600/50 bg-blue-500/10 px-4 py-2 text-sm text-blue-300 hover:bg-blue-500/20 disabled:opacity-50"
                >
                  {addPeopleLoading ? "Adding..." : "Add People"}
                </button>
              </div>
            ) : (
              <>
                <div className="flex items-center justify-between border-b border-zinc-800 px-4 py-3">
                  <div>
                    <p className="text-base font-medium">
                      {selectedConversation?.type === "public"
                        ? "Public Forum"
                        : conversationLabel(selectedConversation)}
                    </p>
                    <p className="mt-0.5 text-xs text-zinc-500">
                      {selectedConversation?.type === "public"
                        ? "Visible to everyone in this campaign"
                        : (selectedConversation?.participants || [])
                            .map((p) => p.display_name || p.user_id)
                            .join(", ")}
                    </p>
                  </div>
                  {selectedConversation?.type === "private" && (
                    <div className="flex items-center gap-2">
                      <button
                        type="button"
                        onClick={openAddPeople}
                        className="rounded border border-zinc-700 bg-zinc-800 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-700"
                      >
                        Add People
                      </button>
                      <button
                        type="button"
                        onClick={handleLeaveConversation}
                        disabled={leaveLoading}
                        className="rounded border border-red-800/50 bg-red-500/10 px-2.5 py-1 text-xs text-red-300 hover:bg-red-500/20 disabled:opacity-50"
                      >
                        {leaveLoading ? "Leaving..." : "Leave"}
                      </button>
                    </div>
                  )}
                </div>

                <div className="flex-1 space-y-2 overflow-y-auto px-4 py-3">
                  {messages.length === 0 ? (
                    <p className="text-sm text-zinc-500">
                      No messages yet. Start the conversation with your team.
                    </p>
                  ) : (
                    messages.map((message) => (
                      <div key={message.id} className="rounded border border-zinc-800 bg-zinc-950/60 px-3 py-2">
                        <p className="text-sm font-medium text-zinc-200">{message.author_display_name}</p>
                        <p className="text-sm text-zinc-300">{message.content}</p>
                        <p className="text-xs text-zinc-500">{new Date(message.created_at).toLocaleString()}</p>
                      </div>
                    ))
                  )}
                </div>

                <form onSubmit={handlePost} className="mt-auto border-t border-zinc-800 p-3">
                  <div className="flex items-center gap-2">
                    <input
                      value={draft}
                      onChange={(e) => setDraft(e.target.value)}
                      placeholder="Message everyone in this campaign..."
                      className="flex-1 rounded border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm"
                    />
                    <button
                      type="submit"
                      disabled={loading || !selectedConversationId || !draft.trim()}
                      className="rounded border border-emerald-600/40 bg-emerald-500/10 px-3 py-2 text-sm text-emerald-300 disabled:opacity-50"
                    >
                      Send
                    </button>
                  </div>
                </form>
              </>
            )}
          </section>

          <aside className="flex h-[78vh] flex-col overflow-hidden rounded border border-zinc-800 bg-zinc-900">
            <div className="flex items-center justify-between border-b border-zinc-800 px-4 py-3">
              <p className="text-sm text-zinc-300">Conversations</p>
              <button
                type="button"
                onClick={openNewConversation}
                className="rounded border border-blue-600/50 bg-blue-500/10 px-2.5 py-1 text-xs text-blue-300 hover:bg-blue-500/20"
              >
                + New Conversation
              </button>
            </div>

            <div className="flex-1 space-y-5 overflow-y-auto p-3">
              <div className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-wide text-zinc-500">Public</p>
                {publicConversation ? (
                  <button
                    type="button"
                    onClick={() => {
                      setSelectedConversationId(publicConversation.id);
                      setShowNewConversation(false);
                      setShowAddPeople(false);
                    }}
                    className={`w-full rounded border px-3 py-2 text-left text-sm ${
                      publicConversation.id === selectedConversationId
                        ? "border-blue-600/50 bg-blue-500/10 text-blue-200"
                        : "border-zinc-800 bg-zinc-950/40 text-zinc-300 hover:bg-zinc-800"
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <p className="font-medium">Public Forum</p>
                      {(publicConversation.unread_count || 0) > 0 ? (
                        <span className="rounded-full bg-blue-500/20 px-2 py-0.5 text-xs font-semibold text-blue-300">
                          {publicConversation.unread_count}
                        </span>
                      ) : null}
                    </div>
                    <p className="text-xs text-zinc-500">Visible to everyone</p>
                  </button>
                ) : null}
              </div>

              <div className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-wide text-zinc-500">
                  Direct Messages
                </p>
                {directMessageConversations.length === 0 ? (
                  <p className="px-1 text-xs text-zinc-600">No direct messages yet.</p>
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
                      className={`w-full rounded border px-3 py-2 text-left text-sm ${
                        conversation.id === selectedConversationId
                          ? "border-blue-600/50 bg-blue-500/10 text-blue-200"
                          : "border-zinc-800 bg-zinc-950/40 text-zinc-300 hover:bg-zinc-800"
                      }`}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <p className="font-medium">{conversationLabel(conversation)}</p>
                        {(conversation.unread_count || 0) > 0 ? (
                          <span className="rounded-full bg-blue-500/20 px-2 py-0.5 text-xs font-semibold text-blue-300">
                            {conversation.unread_count}
                          </span>
                        ) : null}
                      </div>
                    </button>
                  ))
                )}
              </div>

              <div className="space-y-2">
                <p className="text-xs font-semibold uppercase tracking-wide text-zinc-500">
                  Group Conversations
                </p>
                {groupConversations.length === 0 ? (
                  <p className="px-1 text-xs text-zinc-600">No group conversations yet.</p>
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
                      className={`w-full rounded border px-3 py-2 text-left text-sm ${
                        conversation.id === selectedConversationId
                          ? "border-blue-600/50 bg-blue-500/10 text-blue-200"
                          : "border-zinc-800 bg-zinc-950/40 text-zinc-300 hover:bg-zinc-800"
                      }`}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <p className="font-medium">{conversationLabel(conversation)}</p>
                        {(conversation.unread_count || 0) > 0 ? (
                          <span className="rounded-full bg-blue-500/20 px-2 py-0.5 text-xs font-semibold text-blue-300">
                            {conversation.unread_count}
                          </span>
                        ) : null}
                      </div>
                    </button>
                  ))
                )}
              </div>
            </div>
          </aside>
        </div>
      </div>
    </div>
  );
}
