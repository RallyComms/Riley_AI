"use client";

import { useState, useRef, useEffect } from "react";
import { useParams } from "next/navigation";
import { useAuth, useUser } from "@clerk/nextjs";
import { Send } from "lucide-react";
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
}

// Frontend message shape
interface TeamMessage {
  id: string;
  author: string;
  authorInitials: string;
  content: string;
  timestamp: Date;
  author_id: string;
}

// TeamMessageBubble component
function TeamMessageBubble({ message }: { message: TeamMessage }) {
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
        </div>
        <p className="text-sm text-zinc-300 whitespace-pre-wrap break-words">
          {renderContent(message.content)}
        </p>
      </div>
    </div>
  );
}

export default function TeamChatPage() {
  const params = useParams();
  const campaignId = params.id as string;
  const { user } = useUser();
  const { getToken, isLoaded } = useAuth();
  const [messages, setMessages] = useState<TeamMessage[]>([]);
  const [inputValue, setInputValue] = useState("");
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isPosting, setIsPosting] = useState(false);
  const [lastTimestamp, setLastTimestamp] = useState<string | null>(null);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const pollingIntervalRef = useRef<NodeJS.Timeout | null>(null);
  
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
    };
  };

  // Fetch messages from API
  const fetchMessages = async (since: string | null = null) => {
    if (!isLoaded) return;

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("No authentication token available");
      }

      const path = `/api/v1/campaigns/${campaignId}/chat?limit=50${since ? `&since=${encodeURIComponent(since)}` : ""}`;
      const data = await apiFetch<{ messages: BackendMessage[] }>(path, {
        token,
        method: "GET",
      });
      const backendMessages: BackendMessage[] = data.messages || [];

      // Build user map (for now, use current user info for all messages)
      // In production, you might want to fetch user details from Clerk or backend
      const userMap = new Map<string, { name: string; initials: string }>();
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
    if (isLoaded && campaignId && user?.id) {
      fetchMessages(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoaded, campaignId, user?.id]);

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
        fetchMessages(lastTimestamp);
      } else {
        // If no lastTimestamp yet, fetch all messages
        fetchMessages(null);
      }
    }, 12000); // 12 seconds

    return () => {
      if (pollingIntervalRef.current) {
        clearInterval(pollingIntervalRef.current);
      }
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isLoaded, campaignId, lastTimestamp, isLoading, error, user?.id]);

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

      await apiFetch(`/api/v1/campaigns/${campaignId}/chat`, {
        token,
        method: "POST",
        body: {
          content: messageText,
        },
      });

      // Clear input
      setInputValue("");

      // Immediately re-fetch messages since lastTimestamp to get the new message
      if (lastTimestamp) {
        await fetchMessages(lastTimestamp);
      } else {
        // If no lastTimestamp, fetch all messages
        await fetchMessages(null);
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
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className="flex h-full relative">
      {/* Main Chat Area */}
      <div className="flex-1 flex flex-col overflow-hidden bg-transparent">
        {/* Header */}
        <header className="flex items-center justify-between p-6 border-b border-white/5 bg-slate-900/50 backdrop-blur-md">
          <h1 className="text-2xl font-bold text-zinc-100">
            Team Comms | <span className="text-amber-400">{displayName}</span>
          </h1>
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
                onClick={() => fetchMessages(null)}
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
                  <TeamMessageBubble key={message.id} message={message} />
                ))
              )}
              <div ref={messagesEndRef} />
            </div>
          </div>
        )}

        {/* Input Area - Fixed Bottom */}
        <div className="flex-shrink-0 border-t border-white/5 bg-slate-900/50 backdrop-blur-md px-4 py-4">
          <div className="flex items-end gap-3 max-w-4xl mx-auto">
            <div className="flex-1 relative">
              <textarea
                ref={textareaRef}
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                onKeyDown={handleKeyDown}
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
    </div>
  );
}
