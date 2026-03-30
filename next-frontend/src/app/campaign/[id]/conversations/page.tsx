"use client";

import { FormEvent, useCallback, useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { useAuth, useUser } from "@clerk/nextjs";
import { apiFetch } from "@app/lib/api";

type ConversationParticipant = {
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

  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [selectedConversationId, setSelectedConversationId] = useState<string>("");
  const [messages, setMessages] = useState<ConversationMessage[]>([]);
  const [draft, setDraft] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  const publicConversation = useMemo(
    () => conversations.find((c) => c.type === "public") || null,
    [conversations]
  );

  const selectedConversation = useMemo(
    () => conversations.find((c) => c.id === selectedConversationId) || null,
    [conversations, selectedConversationId]
  );

  const conversationLabel = useCallback(
    (conversation: Conversation | null): string => {
      if (!conversation) return "Conversation";
      const explicitName = String(conversation.name || "").trim();
      if (explicitName) return explicitName;
      if (conversation.type === "public") return "Public Feed";

      const participants = (conversation.participants || []).filter((p) => p?.user_id);
      const currentUserId = String(user?.id || "");
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
    [user?.id]
  );

  const fetchConversations = useCallback(async () => {
    if (!campaignId || !isLoaded) return;
    const token = await getToken();
    if (!token) return;
    const data = await apiFetch<{ conversations: Conversation[] }>(
      `/api/v1/conversations?campaign_id=${encodeURIComponent(campaignId)}`,
      { token, method: "GET" }
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
      { token, method: "GET" }
    );
    setMessages(data.messages || []);
    // Fetching messages marks conversation as seen server-side.
    await fetchConversations();
  }, [campaignId, fetchConversations, getToken, isLoaded, selectedConversationId]);

  useEffect(() => {
    void fetchConversations();
  }, [fetchConversations]);

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
        body: {
          campaign_id: campaignId,
          content: draft.trim(),
          mention_user_ids: [],
        },
      });
      setDraft("");
      await fetchMessages();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to send message.");
    } finally {
      setLoading(false);
    }
  };

  const handleCreateConversation = async () => {
    try {
      setError(null);
      const participantsRaw = window.prompt("Participant user IDs (comma-separated):", "");
      if (participantsRaw === null) return;
      const nameRaw = window.prompt("Conversation name (optional):", "");
      const participants = participantsRaw
        .split(",")
        .map((id) => id.trim())
        .filter(Boolean);

      const token = await getToken();
      if (!token) throw new Error("Authentication required.");

      await apiFetch("/api/v1/conversations", {
        token,
        method: "POST",
        body: {
          type: "private",
          campaign_id: campaignId,
          participants,
          name: nameRaw?.trim() || undefined,
        },
      });
      await fetchConversations();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to create conversation.");
    }
  };

  return (
    <div className="min-h-screen bg-zinc-950 p-6 text-zinc-100">
      <div className="mx-auto max-w-7xl space-y-4">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-semibold">Campaign Conversations</h1>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setSelectedConversationId(publicConversation?.id || "")}
              className="rounded border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-sm hover:bg-zinc-800"
            >
              Post to Campaign
            </button>
            <button
              type="button"
              onClick={handleCreateConversation}
              className="rounded border border-blue-600/50 bg-blue-500/10 px-3 py-1.5 text-sm text-blue-300 hover:bg-blue-500/20"
            >
              New Conversation
            </button>
            <Link href={`/campaign/${campaignId}`} className="rounded border border-zinc-700 px-3 py-1.5 text-sm hover:bg-zinc-800">
              Back
            </Link>
          </div>
        </div>

        {error ? <p className="text-sm text-red-300">{error}</p> : null}

        <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
          <section className="lg:col-span-2 rounded border border-zinc-800 bg-zinc-900">
            <div className="border-b border-zinc-800 px-4 py-3">
              <p className="text-sm text-zinc-400">Public Feed + Messages</p>
              <p className="text-base font-medium">
                {conversationLabel(selectedConversation)}
              </p>
            </div>
            <div className="max-h-[60vh] space-y-2 overflow-y-auto px-4 py-3">
              {messages.length === 0 ? (
                <p className="text-sm text-zinc-500">No messages yet.</p>
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
            <form onSubmit={handlePost} className="border-t border-zinc-800 p-3">
              <div className="flex items-center gap-2">
                <input
                  value={draft}
                  onChange={(e) => setDraft(e.target.value)}
                  placeholder="Write a message..."
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
          </section>

          <aside className="rounded border border-zinc-800 bg-zinc-900">
            <div className="border-b border-zinc-800 px-4 py-3">
              <p className="text-sm text-zinc-400">Conversations</p>
            </div>
            <div className="space-y-2 p-3">
              {conversations.length === 0 ? (
                <div className="rounded border border-zinc-800 bg-zinc-950/40 p-3">
                  <p className="text-sm font-medium text-zinc-100">Start a conversation</p>
                  <p className="mt-1 text-xs text-zinc-500">No conversations yet. Create one or post to the campaign feed.</p>
                  <div className="mt-3 flex gap-2">
                    <button
                      type="button"
                      onClick={handleCreateConversation}
                      className="rounded border border-blue-600/50 bg-blue-500/10 px-2.5 py-1.5 text-xs text-blue-300 hover:bg-blue-500/20"
                    >
                      Start a conversation
                    </button>
                    <button
                      type="button"
                      onClick={() => setSelectedConversationId(publicConversation?.id || "")}
                      className="rounded border border-zinc-700 bg-zinc-900 px-2.5 py-1.5 text-xs hover:bg-zinc-800"
                    >
                      Post to campaign
                    </button>
                  </div>
                </div>
              ) : (
                conversations.map((conversation) => (
                  <button
                    key={conversation.id}
                    type="button"
                    onClick={() => setSelectedConversationId(conversation.id)}
                    className={`w-full rounded border px-3 py-2 text-left text-sm ${
                      conversation.id === selectedConversationId
                        ? "border-blue-600/50 bg-blue-500/10 text-blue-200"
                        : "border-zinc-800 bg-zinc-950/40 text-zinc-300 hover:bg-zinc-800"
                    }`}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <p className="font-medium">
                        {conversationLabel(conversation)}
                      </p>
                      {(conversation.unread_count || 0) > 0 ? (
                        <span className="rounded-full bg-blue-500/20 px-2 py-0.5 text-xs font-semibold text-blue-300">
                          {conversation.unread_count}
                        </span>
                      ) : null}
                    </div>
                    <p className="text-xs uppercase tracking-wide text-zinc-500">{conversation.type}</p>
                  </button>
                ))
              )}
            </div>
          </aside>
        </div>
      </div>
    </div>
  );
}
