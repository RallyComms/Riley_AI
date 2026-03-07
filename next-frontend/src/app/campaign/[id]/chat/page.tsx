"use client";

import { useState, useRef, useEffect, useMemo } from "react";
import { useParams } from "next/navigation";
import { useAuth, useUser } from "@clerk/nextjs";
import { Check, ChevronLeft, ChevronRight, MoreHorizontal, Plus, Send, Users, X } from "lucide-react";
import { cn } from "@app/lib/utils";
import { apiFetch } from "@app/lib/api";
import { useCampaignName } from "@app/lib/useCampaignName";

// Helper function to get initials from name
function getInitials(name: string): string {
  return name
    .split(" ")
    .map((n) => n[0])
    .join("")
    .toUpperCase()
    .slice(0, 2);
}

// Helper function to format time
function formatTime(date: Date): string {
  return date.toLocaleTimeString("en-US", {
    hour: "numeric",
    minute: "2-digit",
    hour12: true,
  });
}

// Backend message shape
interface BackendMessage {
  id: string;
  content: string;
  timestamp: string; // ISO string
  author_id: string;
  edited_at?: string | null;
  deleted_at?: string | null;
}

// Frontend message shape
interface TeamMessage {
  id: string;
  author: string;
  authorInitials: string;
  content: string;
  timestamp: Date;
  author_id: string;
  edited_at?: Date | null;
  deleted_at?: Date | null;
  mentions?: Array<{ user_id: string; display_name: string }>;
}

interface MentionMember {
  user_id: string;
  display_name: string;
  avatar_url?: string | null;
  role?: string | null;
}

interface ChatThread {
  thread_id: string;
  tenant_id: string;
  name?: string | null;
  is_private: boolean;
  created_by_user_id: string;
  created_at: string;
}

interface MentionContext {
  start: number;
  end: number;
  query: string;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function extractMentionMetadata(
  text: string,
  members: MentionMember[]
): Array<{ user_id: string; display_name: string }> {
  const found: Array<{ user_id: string; display_name: string }> = [];
  for (const member of members) {
    const pattern = new RegExp(`(^|\\s)@${escapeRegExp(member.display_name)}(?=\\s|$)`, "i");
    if (pattern.test(text)) {
      found.push({ user_id: member.user_id, display_name: member.display_name });
    }
  }
  return found;
}

// TeamMessageBubble component
function TeamMessageBubble({
  message,
  isOwnMessage,
  isEditing,
  editDraft,
  isMutating,
  onStartEdit,
  onCancelEdit,
  onEditDraftChange,
  onSaveEdit,
  onDelete,
}: {
  message: TeamMessage;
  isOwnMessage: boolean;
  isEditing: boolean;
  editDraft: string;
  isMutating: boolean;
  onStartEdit: (message: TeamMessage) => void;
  onCancelEdit: () => void;
  onEditDraftChange: (value: string) => void;
  onSaveEdit: (messageId: string) => void;
  onDelete: (messageId: string) => void;
}) {
  const isDeleted = Boolean(message.deleted_at);
  const [isActionsOpen, setIsActionsOpen] = useState(false);
  const actionsRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!isActionsOpen) return;

    const onMouseDown = (event: MouseEvent) => {
      if (!actionsRef.current) return;
      const target = event.target as Node;
      if (!actionsRef.current.contains(target)) {
        setIsActionsOpen(false);
      }
    };

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsActionsOpen(false);
      }
    };

    document.addEventListener("mousedown", onMouseDown);
    document.addEventListener("keydown", onKeyDown);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
      document.removeEventListener("keydown", onKeyDown);
    };
  }, [isActionsOpen]);

  // Parse content and highlight mentions
  const renderContent = (text: string) => {
    // Split by spaces but keep the spaces
    const parts = text.split(/(\s+)/);
    
    return parts.map((part, idx) => {
      // Check if part is a mention (starts with @ and followed by word characters)
      if (part.startsWith("@") && /^@\w+/.test(part)) {
        return (
          <span
            key={idx}
            className="text-amber-400 font-medium"
          >
            {part}
          </span>
        );
      }
      return <span key={idx}>{part}</span>;
    });
  };

  return (
    <div className="flex items-start gap-3 px-4 py-3 hover:bg-zinc-900/30 transition-colors">
      {/* Avatar */}
      <div className="flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-full border-2 border-zinc-800 bg-zinc-700 text-sm font-medium text-zinc-100">
        {message.authorInitials}
      </div>
      
      {/* Message Content */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="text-sm font-medium text-zinc-100">
            {message.author}
          </span>
          <span className="text-xs text-zinc-500">
            {formatTime(message.timestamp)}
          </span>
          {message.edited_at && !isDeleted && (
            <span className="text-xs text-zinc-500">(edited)</span>
          )}
          {isOwnMessage && !isDeleted && !isEditing && (
            <div ref={actionsRef} className="relative ml-2">
              <button
                type="button"
                onClick={() => setIsActionsOpen((prev) => !prev)}
                disabled={isMutating}
                className="rounded p-1 text-zinc-500 hover:text-zinc-300 hover:bg-zinc-800/60 transition-colors disabled:opacity-50"
                aria-label="Message actions"
                title="Message actions"
              >
                <MoreHorizontal className="h-3.5 w-3.5" />
              </button>
              {isActionsOpen && (
                <div className="absolute right-0 top-full z-30 mt-1 w-28 overflow-hidden rounded-md border border-zinc-700 bg-zinc-900 shadow-lg">
                  <button
                    type="button"
                    onClick={() => {
                      setIsActionsOpen(false);
                      onStartEdit(message);
                    }}
                    className="w-full px-3 py-2 text-left text-xs text-zinc-200 hover:bg-zinc-800"
                  >
                    Edit
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setIsActionsOpen(false);
                      onDelete(message.id);
                    }}
                    className="w-full px-3 py-2 text-left text-xs text-red-300 hover:bg-zinc-800"
                  >
                    Delete
                  </button>
                </div>
              )}
            </div>
          )}
        </div>
        {isEditing ? (
          <div className="space-y-2">
            <textarea
              value={editDraft}
              onChange={(e) => onEditDraftChange(e.target.value)}
              rows={3}
              className="w-full resize-y rounded-md border border-zinc-700 bg-zinc-900/70 px-3 py-2 text-sm text-zinc-100 focus:outline-none focus:ring-2 focus:ring-amber-400/50"
            />
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => onSaveEdit(message.id)}
                disabled={isMutating || editDraft.trim().length === 0}
                className="inline-flex items-center gap-1 rounded-md bg-amber-500 px-2 py-1 text-xs font-medium text-zinc-900 hover:bg-amber-400 disabled:opacity-50"
              >
                <Check className="h-3.5 w-3.5" />
                Save
              </button>
              <button
                type="button"
                onClick={onCancelEdit}
                disabled={isMutating}
                className="inline-flex items-center gap-1 rounded-md border border-zinc-700 px-2 py-1 text-xs text-zinc-300 hover:bg-zinc-800/60 disabled:opacity-50"
              >
                <X className="h-3.5 w-3.5" />
                Cancel
              </button>
            </div>
          </div>
        ) : (
          <p
            className={cn(
              "text-sm whitespace-pre-wrap break-words",
              isDeleted ? "text-zinc-500 italic" : "text-zinc-300"
            )}
          >
            {isDeleted ? "Message deleted" : renderContent(message.content)}
          </p>
        )}
      </div>
    </div>
  );
}

export default function TeamChatPage() {
  const MAIN_THREAD_ID = "__team_comms__";
  const params = useParams();
  const campaignId = params.id as string;
  const { user } = useUser();
  const { getToken, isLoaded } = useAuth();
  const [messages, setMessages] = useState<TeamMessage[]>([]);
  const [threads, setThreads] = useState<ChatThread[]>([]);
  const [currentThreadId, setCurrentThreadId] = useState<string>(MAIN_THREAD_ID);
  const [inputValue, setInputValue] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isPosting, setIsPosting] = useState(false);
  const [lastTimestamp, setLastTimestamp] = useState<string | null>(null);
  const [editingMessageId, setEditingMessageId] = useState<string | null>(null);
  const [editDraft, setEditDraft] = useState("");
  const [mutatingMessageId, setMutatingMessageId] = useState<string | null>(null);
  const [campaignMembers, setCampaignMembers] = useState<MentionMember[]>([]);
  const [isThreadsPanelOpen, setIsThreadsPanelOpen] = useState(true);
  const [isMembersPanelOpen, setIsMembersPanelOpen] = useState(true);
  const [isNewThreadOpen, setIsNewThreadOpen] = useState(false);
  const [newThreadName, setNewThreadName] = useState("");
  const [selectedThreadMemberIds, setSelectedThreadMemberIds] = useState<string[]>([]);
  const [isCreatingThread, setIsCreatingThread] = useState(false);
  const [mentionContext, setMentionContext] = useState<MentionContext | null>(null);
  const [isMentionOpen, setIsMentionOpen] = useState(false);
  const [selectedMentionIndex, setSelectedMentionIndex] = useState(0);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null);
  const composerRef = useRef<HTMLDivElement>(null);
  
  // Fetch campaign name using shared hook
  const { displayName } = useCampaignName(campaignId);

  // Map backend message to frontend shape
  const mapBackendToFrontend = (
    backendMsg: BackendMessage,
    userMap: Map<string, { name: string; initials: string }>
  ): TeamMessage => {
    const authorInfo = userMap.get(backendMsg.author_id) || {
      name: "Unknown User",
      initials: "??",
    };

    return {
      id: backendMsg.id,
      author: authorInfo.name,
      authorInitials: authorInfo.initials,
      content: backendMsg.content,
      timestamp: new Date(backendMsg.timestamp),
      author_id: backendMsg.author_id,
      edited_at: backendMsg.edited_at ? new Date(backendMsg.edited_at) : null,
      deleted_at: backendMsg.deleted_at ? new Date(backendMsg.deleted_at) : null,
      mentions: extractMentionMetadata(backendMsg.content, campaignMembers),
    };
  };

  const filteredMentionMembers = useMemo(() => {
    if (!mentionContext) return [];
    const query = mentionContext.query.trim().toLowerCase();
    return campaignMembers.filter((member) => {
      if (!query) return true;
      return member.display_name.toLowerCase().includes(query);
    });
  }, [campaignMembers, mentionContext]);

  const getThreadLabel = (thread: ChatThread): string => {
    if (thread.name && thread.name.trim()) return thread.name;
    return "Private Chat";
  };

  const closeMentionAutocomplete = () => {
    setMentionContext(null);
    setIsMentionOpen(false);
    setSelectedMentionIndex(0);
  };

  const updateMentionAutocomplete = (value: string, caretPosition: number | null) => {
    if (caretPosition === null || caretPosition < 0) {
      closeMentionAutocomplete();
      return;
    }

    const beforeCaret = value.slice(0, caretPosition);
    const match = beforeCaret.match(/(^|\s)@([a-zA-Z0-9._-]*)$/);
    if (!match) {
      closeMentionAutocomplete();
      return;
    }

    const prefix = match[1] || "";
    const start = caretPosition - match[0].length + prefix.length;
    const query = match[2] || "";
    setMentionContext({
      start,
      end: caretPosition,
      query,
    });
    setIsMentionOpen(true);
    setSelectedMentionIndex(0);
  };

  const handleSelectMention = (member: MentionMember) => {
    if (!mentionContext) return;
    const insert = `@${member.display_name} `;
    const nextValue =
      inputValue.slice(0, mentionContext.start) +
      insert +
      inputValue.slice(mentionContext.end);

    setInputValue(nextValue);
    closeMentionAutocomplete();

    const cursorPosition = mentionContext.start + insert.length;
    requestAnimationFrame(() => {
      if (!textareaRef.current) return;
      textareaRef.current.focus();
      textareaRef.current.setSelectionRange(cursorPosition, cursorPosition);
    });
  };

  const fetchCampaignMembers = async () => {
    if (!isLoaded || !campaignId) return;
    try {
      const token = await getToken();
      if (!token) return;

      const data = await apiFetch<{ members: MentionMember[] }>(
        `/api/v1/campaigns/${campaignId}/members?mentions_only=true`,
        {
          token,
          method: "GET",
        }
      );
      setCampaignMembers(data.members || []);
    } catch (err) {
      console.error("Error fetching campaign members for mentions:", err);
      setCampaignMembers([]);
    }
  };

  const fetchThreads = async () => {
    if (!isLoaded || !campaignId) return;
    try {
      const token = await getToken();
      if (!token) return;

      const data = await apiFetch<{ threads: ChatThread[] }>(
        `/api/v1/campaigns/${campaignId}/chat-threads`,
        {
          token,
          method: "GET",
        }
      );
      setThreads(data.threads || []);
    } catch (err) {
      console.error("Error fetching chat threads:", err);
      setThreads([]);
    }
  };

  // Fetch messages from API
  const fetchMessages = async (
    since: string | null = null,
    threadId: string = currentThreadId
  ) => {
    if (!isLoaded) return;

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }

      const path =
        threadId === MAIN_THREAD_ID
          ? `/api/v1/campaigns/${campaignId}/chat?limit=50${since ? `&since=${encodeURIComponent(since)}` : ""}`
          : `/api/v1/campaigns/${campaignId}/chat-threads/${threadId}/messages?limit=50${since ? `&since=${encodeURIComponent(since)}` : ""}`;
      const data = await apiFetch<{ messages: BackendMessage[] }>(path, {
        token,
        method: "GET",
      });
      const backendMessages: BackendMessage[] = data.messages || [];

      // Build user map from campaign members (authorized users only).
      const userMap = new Map<string, { name: string; initials: string }>();
      for (const member of campaignMembers) {
        userMap.set(member.user_id, {
          name: member.display_name,
          initials: getInitials(member.display_name),
        });
      }
      const currentUserName =
        user?.firstName ||
        user?.emailAddresses[0]?.emailAddress ||
        "You";
      const currentUserInitials = getInitials(currentUserName);

      // Add current user to map
      if (user?.id) {
        userMap.set(user.id, {
          name: currentUserName,
          initials: currentUserInitials,
        });
      }

      // Map backend messages to frontend shape
      const newMessages = backendMessages.map((msg) =>
        mapBackendToFrontend(msg, userMap)
      );

      // Update last timestamp (newest message timestamp)
      if (backendMessages.length > 0) {
        const newestMsg = backendMessages[backendMessages.length - 1];
        setLastTimestamp(newestMsg.timestamp);
      }

      // If since was provided, append new messages; otherwise replace all
      if (since) {
        setMessages((prev) => {
          // Avoid duplicates
          const existingIds = new Set(prev.map((m) => m.id));
          const uniqueNew = newMessages.filter((m) => !existingIds.has(m.id));
          return [...prev, ...uniqueNew];
        });
      } else {
        setMessages(newMessages);
      }

      setError(null);
    } catch (err) {
      console.error("Error fetching messages:", err);
      setError(
        err instanceof Error
          ? err.message
          : "Unable to load messages. Please refresh."
      );
    } finally {
      setIsLoading(false);
    }
  };

  // Initial fetch on mount
  useEffect(() => {
    if (isLoaded && campaignId) {
      fetchCampaignMembers();
      fetchThreads();
    }
    if (isLoaded && campaignId && user?.id) {
      fetchMessages(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoaded, campaignId, user?.id, currentThreadId]);

  // Set up polling (every 12 seconds)
  useEffect(() => {
    if (!isLoaded || !campaignId || isLoading || error || !user?.id) return;

    // Clear any existing interval
    if (pollingIntervalRef.current) {
      clearInterval(pollingIntervalRef.current);
    }

    // Poll for new messages
    pollingIntervalRef.current = setInterval(() => {
      if (lastTimestamp) {
        fetchMessages(lastTimestamp, currentThreadId);
      } else {
        // If no lastTimestamp yet, fetch all messages
        fetchMessages(null, currentThreadId);
      }
    }, 12000); // 12 seconds

    return () => {
      if (pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoaded, campaignId, lastTimestamp, isLoading, error, user?.id, currentThreadId]);

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = "auto";
      textareaRef.current.style.height = `${textareaRef.current.scrollHeight}px`;
    }
  }, [inputValue]);

  // Keep mention navigation index in range.
  useEffect(() => {
    if (selectedMentionIndex < filteredMentionMembers.length) return;
    setSelectedMentionIndex(0);
  }, [filteredMentionMembers.length, selectedMentionIndex]);

  // Close mention dropdown when clicking outside composer area.
  useEffect(() => {
    if (!isMentionOpen) return;

    const onMouseDown = (event: MouseEvent) => {
      if (!composerRef.current) return;
      const target = event.target as Node;
      if (!composerRef.current.contains(target)) {
        closeMentionAutocomplete();
      }
    };

    document.addEventListener("mousedown", onMouseDown);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
    };
  }, [isMentionOpen]);

  const handleComposerChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const value = e.target.value;
    setInputValue(value);
    updateMentionAutocomplete(value, e.target.selectionStart);
  };

  const handleComposerSelectionChange = () => {
    if (!textareaRef.current) return;
    updateMentionAutocomplete(textareaRef.current.value, textareaRef.current.selectionStart);
  };

  const handleSend = async () => {
    if (!inputValue.trim() || !user || isPosting) return;

    const messageText = inputValue.trim();
    setIsPosting(true);
    setError(null);

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }

      const sendPath =
        currentThreadId === MAIN_THREAD_ID
          ? `/api/v1/campaigns/${campaignId}/chat`
          : `/api/v1/campaigns/${campaignId}/chat-threads/${currentThreadId}/messages`;

      await apiFetch(sendPath, {
        token,
        method: "POST",
        body: {
          content: messageText,
        },
      });

      // Clear input
      setInputValue("");
      closeMentionAutocomplete();

      // Immediately re-fetch messages since lastTimestamp to get the new message
      if (lastTimestamp) {
        await fetchMessages(lastTimestamp, currentThreadId);
      } else {
        // If no lastTimestamp, fetch all messages
        await fetchMessages(null, currentThreadId);
      }

      // Check for mentions and dispatch notification event
      const mentionPattern = /@(Anova|Me|anova|me)\b/i;
      if (mentionPattern.test(messageText)) {
        window.dispatchEvent(
          new CustomEvent("riley-notification", {
            detail: {
              type: "mention",
              message: messageText,
              author: user.firstName || user.emailAddresses[0]?.emailAddress || "You",
            },
          })
        );
      }
    } catch (err) {
      console.error("Error sending message:", err);
      setError(
        err instanceof Error
          ? err.message
          : "Failed to send message. Please try again."
      );
    } finally {
      setIsPosting(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (isMentionOpen) {
      if (e.key === "Escape") {
        e.preventDefault();
        closeMentionAutocomplete();
        return;
      }

      if (e.key === "ArrowDown") {
        e.preventDefault();
        if (filteredMentionMembers.length === 0) return;
        setSelectedMentionIndex((prev) =>
          (prev + 1) % filteredMentionMembers.length
        );
        return;
      }

      if (e.key === "ArrowUp") {
        e.preventDefault();
        if (filteredMentionMembers.length === 0) return;
        setSelectedMentionIndex((prev) =>
          prev === 0 ? filteredMentionMembers.length - 1 : prev - 1
        );
        return;
      }

      if (e.key === "Enter" && filteredMentionMembers.length > 0) {
        e.preventDefault();
        const selected = filteredMentionMembers[selectedMentionIndex];
        if (selected) {
          handleSelectMention(selected);
        }
        return;
      }
    }

    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleStartEdit = (message: TeamMessage) => {
    setEditingMessageId(message.id);
    setEditDraft(message.content);
    setError(null);
  };

  const handleCancelEdit = () => {
    setEditingMessageId(null);
    setEditDraft("");
  };

  const handleSaveEdit = async (messageId: string) => {
    const nextContent = editDraft.trim();
    if (!nextContent) return;

    try {
      setMutatingMessageId(messageId);
      setError(null);
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }

      const updated = await apiFetch<BackendMessage>(
        `/api/v1/campaigns/${campaignId}/messages/${messageId}`,
        {
          token,
          method: "PATCH",
          body: {
            content: nextContent,
          },
        }
      );

      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === messageId
            ? {
                ...msg,
                content: updated.content,
                edited_at: updated.edited_at ? new Date(updated.edited_at) : new Date(),
                mentions: extractMentionMetadata(updated.content, campaignMembers),
              }
            : msg
        )
      );

      setEditingMessageId(null);
      setEditDraft("");
    } catch (err) {
      console.error("Error updating message:", err);
      setError(
        err instanceof Error
          ? err.message
          : "Failed to update message. Please try again."
      );
    } finally {
      setMutatingMessageId(null);
    }
  };

  const handleDeleteMessage = async (messageId: string) => {
    if (currentThreadId !== MAIN_THREAD_ID) return;
    if (!window.confirm("Delete this message?")) {
      return;
    }

    try {
      setMutatingMessageId(messageId);
      setError(null);
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }

      const deleted = await apiFetch<BackendMessage>(
        `/api/v1/campaigns/${campaignId}/messages/${messageId}`,
        {
          token,
          method: "DELETE",
        }
      );

      setMessages((prev) =>
        prev.map((msg) =>
          msg.id === messageId
            ? {
                ...msg,
                content: deleted.content,
                deleted_at: deleted.deleted_at ? new Date(deleted.deleted_at) : new Date(),
                mentions: [],
              }
            : msg
        )
      );

      if (editingMessageId === messageId) {
        setEditingMessageId(null);
        setEditDraft("");
      }
    } catch (err) {
      console.error("Error deleting message:", err);
      setError(
        err instanceof Error
          ? err.message
          : "Failed to delete message. Please try again."
      );
    } finally {
      setMutatingMessageId(null);
    }
  };

  const handleSelectThread = (threadId: string) => {
    if (threadId === currentThreadId) return;
    setCurrentThreadId(threadId);
    setMessages([]);
    setLastTimestamp(null);
    setIsLoading(true);
    setError(null);
    setEditingMessageId(null);
    setEditDraft("");
    closeMentionAutocomplete();
  };

  const toggleThreadMember = (memberId: string) => {
    setSelectedThreadMemberIds((prev) =>
      prev.includes(memberId)
        ? prev.filter((id) => id !== memberId)
        : [...prev, memberId]
    );
  };

  const handleCreatePrivateThread = async () => {
    try {
      setIsCreatingThread(true);
      setError(null);
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }

      const created = await apiFetch<ChatThread>(
        `/api/v1/campaigns/${campaignId}/chat-threads`,
        {
          token,
          method: "POST",
          body: {
            name: newThreadName.trim() || null,
            member_user_ids: selectedThreadMemberIds,
          },
        }
      );

      setThreads((prev) => [created, ...prev.filter((t) => t.thread_id !== created.thread_id)]);
      setIsNewThreadOpen(false);
      setNewThreadName("");
      setSelectedThreadMemberIds([]);
      handleSelectThread(created.thread_id);
    } catch (err) {
      console.error("Error creating private thread:", err);
      setError(
        err instanceof Error
          ? err.message
          : "Failed to create private chat. Please try again."
      );
    } finally {
      setIsCreatingThread(false);
    }
  };

  return (
    <div className="flex h-full relative">
      {isThreadsPanelOpen && (
        <aside className="w-64 border-r border-white/5 bg-slate-900/40 backdrop-blur-md flex flex-col">
          <div className="px-4 py-3 border-b border-white/5 flex items-center justify-between">
            <h2 className="text-sm font-semibold text-zinc-100">Chats</h2>
            <button
              type="button"
              onClick={() => setIsThreadsPanelOpen(false)}
              className="rounded p-1 text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/60 transition-colors"
              aria-label="Close chats panel"
              title="Close chats panel"
            >
              <ChevronLeft className="h-4 w-4" />
            </button>
          </div>
          <div className="p-3 space-y-2">
            <button
              type="button"
              onClick={() => handleSelectThread(MAIN_THREAD_ID)}
              className={cn(
                "w-full rounded-md px-3 py-2 text-left text-sm transition-colors",
                currentThreadId === MAIN_THREAD_ID
                  ? "bg-amber-500/20 text-amber-200 border border-amber-500/30"
                  : "text-zinc-200 border border-zinc-700 hover:bg-zinc-800/60"
              )}
            >
              Team Comms
            </button>
            {threads.map((thread) => (
              <button
                key={thread.thread_id}
                type="button"
                onClick={() => handleSelectThread(thread.thread_id)}
                className={cn(
                  "w-full rounded-md px-3 py-2 text-left text-sm transition-colors",
                  currentThreadId === thread.thread_id
                    ? "bg-amber-500/20 text-amber-200 border border-amber-500/30"
                    : "text-zinc-200 border border-zinc-700 hover:bg-zinc-800/60"
                )}
              >
                <span className="block truncate">{getThreadLabel(thread)}</span>
              </button>
            ))}
          </div>
          <div className="p-3 border-t border-white/5 mt-auto">
            <button
              type="button"
              onClick={() => setIsNewThreadOpen(true)}
              className="w-full rounded-md border border-zinc-700 bg-zinc-900/50 px-3 py-2 text-sm text-zinc-200 hover:bg-zinc-800/70 transition-colors inline-flex items-center justify-center gap-2"
            >
              <Plus className="h-4 w-4" />
              New Private Chat
            </button>
          </div>
        </aside>
      )}

      {/* Main Chat Area */}
      <div className="flex-1 flex flex-col overflow-hidden bg-transparent">
        {/* Header */}
        <header className="flex items-center justify-between p-6 border-b border-white/5 bg-slate-900/50 backdrop-blur-md">
          <h1 className="text-2xl font-bold text-zinc-100">
            {currentThreadId === MAIN_THREAD_ID ? "Team Comms" : "Private Chat"} |{" "}
            <span className="text-amber-400">{displayName}</span>
          </h1>
          <div className="inline-flex items-center gap-2">
            {!isThreadsPanelOpen && (
              <button
                type="button"
                onClick={() => setIsThreadsPanelOpen(true)}
                className="inline-flex items-center gap-2 rounded-md border border-zinc-700 bg-zinc-900/50 px-3 py-1.5 text-sm text-zinc-200 hover:bg-zinc-800/70 transition-colors"
                aria-label="Show chats panel"
              >
                <Users className="h-4 w-4" />
                <span>Chats</span>
              </button>
            )}
            <button
              type="button"
              onClick={() => setIsMembersPanelOpen((prev) => !prev)}
              className="inline-flex items-center gap-2 rounded-md border border-zinc-700 bg-zinc-900/50 px-3 py-1.5 text-sm text-zinc-200 hover:bg-zinc-800/70 transition-colors"
              aria-label={isMembersPanelOpen ? "Hide members panel" : "Show members panel"}
            >
              <span>Members</span>
              {isMembersPanelOpen ? <ChevronRight className="h-4 w-4" /> : <Users className="h-4 w-4" />}
            </button>
          </div>
        </header>

        {/* Loading State */}
        {isLoading && (
          <div className="flex-1 flex items-center justify-center">
            <p className="text-zinc-400">Loading messages...</p>
          </div>
        )}

        {/* Error State */}
        {!isLoading && error && (
          <div className="flex-1 flex items-center justify-center p-6">
            <div className="rounded-lg border border-red-500/50 bg-red-500/10 p-4 text-center max-w-md">
              <p className="text-red-400 font-medium mb-2">{error}</p>
              <button
                onClick={() => fetchMessages(null, currentThreadId)}
                className="rounded-lg bg-amber-400 px-4 py-2 text-sm font-semibold text-zinc-900 hover:bg-amber-500 transition-colors"
              >
                Retry
              </button>
            </div>
          </div>
        )}

        {/* Message List - Scrollable */}
        {!isLoading && !error && (
          <div className="flex-1 overflow-y-auto bg-transparent">
            <div className="py-4">
              {messages.length === 0 ? (
                <div className="flex items-center justify-center h-full">
                  <p className="text-zinc-400">No messages yet. Start the conversation!</p>
                </div>
              ) : (
                messages.map((message) => (
                  <TeamMessageBubble
                    key={message.id}
                    message={message}
                    isOwnMessage={Boolean(currentThreadId === MAIN_THREAD_ID && user?.id && message.author_id === user.id)}
                    isEditing={Boolean(currentThreadId === MAIN_THREAD_ID && editingMessageId === message.id)}
                    editDraft={editingMessageId === message.id ? editDraft : message.content}
                    isMutating={mutatingMessageId === message.id}
                    onStartEdit={handleStartEdit}
                    onCancelEdit={handleCancelEdit}
                    onEditDraftChange={setEditDraft}
                    onSaveEdit={handleSaveEdit}
                    onDelete={handleDeleteMessage}
                  />
                ))
              )}
              <div ref={messagesEndRef} />
            </div>
          </div>
        )}

        {/* Input Area - Fixed Bottom */}
        <div className="flex-shrink-0 border-t border-white/5 bg-slate-900/50 backdrop-blur-md px-4 py-4">
          <div ref={composerRef} className="flex items-end gap-3 max-w-4xl mx-auto">
            <div className="flex-1 relative">
              {isMentionOpen && (
                <div className="absolute bottom-full left-0 right-0 mb-2 z-30 overflow-hidden rounded-md border border-zinc-700 bg-zinc-900 shadow-lg">
                  {filteredMentionMembers.length === 0 ? (
                    <div className="px-3 py-2 text-sm text-zinc-400">
                      No members found
                    </div>
                  ) : (
                    filteredMentionMembers.slice(0, 8).map((member, idx) => (
                      <button
                        key={member.user_id}
                        type="button"
                        onMouseDown={(e) => {
                          e.preventDefault();
                          handleSelectMention(member);
                        }}
                        className={cn(
                          "flex w-full items-center justify-between px-3 py-2 text-left text-sm transition-colors",
                          idx === selectedMentionIndex
                            ? "bg-amber-500/20 text-amber-100"
                            : "text-zinc-200 hover:bg-zinc-800"
                        )}
                      >
                        <span className="truncate">@{member.display_name}</span>
                        {member.role && (
                          <span className="ml-3 text-xs text-zinc-400">{member.role}</span>
                        )}
                      </button>
                    ))
                  )}
                </div>
              )}
              <textarea
                ref={textareaRef}
                value={inputValue}
                onChange={handleComposerChange}
                onKeyDown={handleKeyDown}
                onClick={handleComposerSelectionChange}
                onKeyUp={handleComposerSelectionChange}
                placeholder="Type a message... (Use @Name to mention someone)"
                disabled={isPosting || isLoading || !!error}
                className={cn(
                  "w-full resize-none rounded-lg border border-zinc-700 bg-zinc-900/50 px-4 py-3",
                  "text-sm text-zinc-100 placeholder:text-zinc-500",
                  "focus:outline-none focus:ring-2 focus:ring-amber-400/50 focus:border-amber-400/50",
                  "max-h-32 overflow-y-auto",
                  "disabled:opacity-50 disabled:cursor-not-allowed"
                )}
                rows={1}
              />
            </div>
            <button
              onClick={handleSend}
              disabled={!inputValue.trim() || isPosting || isLoading || !!error}
              className={cn(
                "flex-shrink-0 flex items-center justify-center",
                "h-11 w-11 rounded-lg",
                "bg-amber-500 hover:bg-amber-600",
                "disabled:bg-zinc-800 disabled:text-zinc-500 disabled:cursor-not-allowed",
                "text-white transition-colors",
                "focus:outline-none focus:ring-2 focus:ring-amber-400/50"
              )}
              aria-label="Send message"
            >
              <Send className="h-5 w-5" />
            </button>
          </div>
        </div>
      </div>

      {isNewThreadOpen && (
        <div className="absolute inset-0 z-40 flex items-center justify-center bg-black/50">
          <div className="w-full max-w-md rounded-lg border border-zinc-700 bg-zinc-900 p-4 shadow-xl">
            <h3 className="text-lg font-semibold text-zinc-100">New Private Chat</h3>
            <p className="mt-1 text-xs text-zinc-400">Only campaign members can be added.</p>
            <div className="mt-4 space-y-3">
              <div>
                <label className="text-xs text-zinc-400">Name (optional)</label>
                <input
                  type="text"
                  value={newThreadName}
                  onChange={(e) => setNewThreadName(e.target.value)}
                  className="mt-1 w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 focus:outline-none focus:ring-2 focus:ring-amber-400/50"
                  placeholder="e.g., Creative Review"
                />
              </div>
              <div>
                <label className="text-xs text-zinc-400">Members</label>
                <div className="mt-1 max-h-44 overflow-y-auto rounded-md border border-zinc-700 bg-zinc-950">
                  {campaignMembers.filter((member) => member.user_id !== user?.id).length === 0 ? (
                    <div className="px-3 py-2 text-sm text-zinc-400">No other campaign members found</div>
                  ) : (
                    campaignMembers
                      .filter((member) => member.user_id !== user?.id)
                      .map((member) => (
                        <label
                          key={member.user_id}
                          className="flex items-center gap-2 px-3 py-2 text-sm text-zinc-200 hover:bg-zinc-800/50"
                        >
                          <input
                            type="checkbox"
                            checked={selectedThreadMemberIds.includes(member.user_id)}
                            onChange={() => toggleThreadMember(member.user_id)}
                            className="h-4 w-4 rounded border-zinc-600 bg-zinc-900"
                          />
                          <span className="truncate">{member.display_name}</span>
                        </label>
                      ))
                  )}
                </div>
              </div>
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => {
                  if (isCreatingThread) return;
                  setIsNewThreadOpen(false);
                  setNewThreadName("");
                  setSelectedThreadMemberIds([]);
                }}
                className="rounded-md border border-zinc-700 px-3 py-1.5 text-sm text-zinc-300 hover:bg-zinc-800/60"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleCreatePrivateThread}
                disabled={isCreatingThread}
                className="rounded-md bg-amber-500 px-3 py-1.5 text-sm font-medium text-zinc-900 hover:bg-amber-400 disabled:opacity-50"
              >
                {isCreatingThread ? "Creating..." : "Create"}
              </button>
            </div>
          </div>
        </div>
      )}

      {isMembersPanelOpen && (
        <aside className="w-72 border-l border-white/5 bg-slate-900/40 backdrop-blur-md flex flex-col">
          <div className="px-4 py-3 border-b border-white/5">
            <h2 className="text-sm font-semibold text-zinc-100">Campaign Members</h2>
            <p className="text-xs text-zinc-400 mt-0.5">{campaignMembers.length} authorized</p>
          </div>
          <div className="flex-1 overflow-y-auto">
            {campaignMembers.length === 0 ? (
              <div className="px-4 py-4 text-sm text-zinc-400">No members found</div>
            ) : (
              <ul className="py-2">
                {campaignMembers.map((member) => (
                  <li key={member.user_id} className="px-3 py-2">
                    <div className="flex items-center gap-3 rounded-md px-2 py-1.5 hover:bg-zinc-800/40 transition-colors">
                      {member.avatar_url ? (
                        <img
                          src={member.avatar_url}
                          alt={member.display_name}
                          className="h-8 w-8 rounded-full object-cover border border-zinc-700"
                        />
                      ) : (
                        <div className="h-8 w-8 rounded-full border border-zinc-700 bg-zinc-800 flex items-center justify-center text-xs font-medium text-zinc-200">
                          {getInitials(member.display_name)}
                        </div>
                      )}
                      <div className="min-w-0 flex-1">
                        <p className="text-sm text-zinc-200 truncate">{member.display_name}</p>
                      </div>
                      {member.role && (
                        <span className="rounded-full border border-amber-400/30 bg-amber-500/10 px-2 py-0.5 text-[10px] font-medium text-amber-300">
                          {member.role}
                        </span>
                      )}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </aside>
      )}
    </div>
  );
}
