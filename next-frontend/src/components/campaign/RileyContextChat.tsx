"use client";

import { useState, useRef, useEffect } from "react";
import { useUser, useAuth } from "@clerk/nextjs";
import { Send, Loader2, Trash2, FileText } from "lucide-react";
import ReactMarkdown from "react-markdown";
import { cn } from "@app/lib/utils";
import { TypewriterMarkdown } from "@app/components/ui/TypewriterMarkdown";
import { apiFetch } from "@app/lib/api";

type Message = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  status?: "thinking"; // For placeholder messages
};

interface RileyContextChatProps {
  mode: "messaging" | "research" | "strategy" | "pitch";
  contextKey: string;
  campaignId: string;
  onViewAsset?: (filename: string) => void;
}

export function RileyContextChat({ mode, contextKey, campaignId, onViewAsset }: RileyContextChatProps) {
  // All hooks must be called unconditionally before any early returns
  const { user, isLoaded } = useUser();
  const { getToken, isLoaded: authLoaded } = useAuth();
  
  // Derive user display name: username ?? firstName ?? primaryEmail ?? "there"
  const userDisplayName = user?.username ?? user?.firstName ?? user?.primaryEmailAddress?.emailAddress ?? "there";
  
  // Check for missing requirements (but still render UI with error banner)
  const missingAuth = !authLoaded || !isLoaded || !user;
  const missingCampaignId = !campaignId;
  
  const getSystemMessage = () => {
    switch (mode) {
      case "messaging":
        return "I've analyzed your active columns. I can help you refine messaging, suggest edits, or review drafts. What would you like to work on?";
      case "research":
        return "I've reviewed your research data. I can help you analyze trends, identify insights, or answer questions about your findings. What would you like to explore?";
      case "strategy":
        return "I've reviewed your strategy documents. I can help you develop tactics, analyze positioning, or refine your approach. What would you like to focus on?";
      case "pitch":
        return "I've analyzed your pitch materials. I can help you refine presentations, strengthen arguments, or prepare for client meetings. What would you like to improve?";
      default:
        return "How can I help you today?";
    }
  };

  const getTitle = () => {
    switch (mode) {
      case "messaging":
        return "Riley Messaging";
      case "research":
        return "Riley Research";
      case "strategy":
        return "Riley Strategy";
      case "pitch":
        return "Riley Pitch";
      default:
        return "Riley Strategy";
    }
  };

  const [messages, setMessages] = useState<Message[]>([
    {
      id: "system-1",
      role: "system",
      content: getSystemMessage(),
    },
  ]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [isClearing, setIsClearing] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Derive sessionId with user ID to prevent collisions (after hooks, but handle conditionally)
  const sessionId = isLoaded && user && campaignId 
    ? `session_${campaignId}_${contextKey}_${user.id}` 
    : null;

  // Load chat history on mount and when sessionId changes
  useEffect(() => {
    // Only load if we have a valid sessionId
    if (!sessionId || !user || !campaignId) {
      return;
    }

    // Only load history if messages are empty or only contain system message
    // This prevents overwriting optimistic updates
    setMessages((prev) => {
      const hasUserOrAssistantMessages = prev.some(
        (msg) => msg.role === "user" || msg.role === "assistant"
      );
      // If we have user/assistant messages, don't load history (preserve optimistic updates)
      if (hasUserOrAssistantMessages) {
        return prev;
      }
      // Otherwise, keep current messages (might be system message)
      return prev;
    });

    async function loadHistory() {
      try {
        const token = await getToken();
        if (!token) return;
        
        const history = await apiFetch<Array<{ role: string; content: string }>>(
          `/api/v1/chat/history/${sessionId}`,
          {
            token,
            method: "GET",
          }
        );
        
        // Guard: Only update if history exists and messages are still empty/only system
        setMessages((prev) => {
          const hasUserOrAssistantMessages = prev.some(
            (msg) => msg.role === "user" || msg.role === "assistant"
          );
          // Don't overwrite if user/assistant messages exist
          if (hasUserOrAssistantMessages) {
            return prev;
          }
          
          if (history && Array.isArray(history) && history.length > 0) {
            // Convert history to Message format
            const historyMessages: Message[] = history.map(
              (msg: { role: string; content: string }, idx: number) => ({
                id: `history-${Date.now()}-${idx}`,
                role: msg.role === "user" ? "user" : "assistant",
                content: msg.content,
              })
            );
            // Prepend system message
            return [
              {
                id: "system-1",
                role: "system",
                content: getSystemMessage(),
              },
              ...historyMessages,
            ];
          } else {
            // Empty history - keep system message only if messages are still empty
            return prev.length === 0 || (prev.length === 1 && prev[0].role === "system")
              ? [
                  {
                    id: "system-1",
                    role: "system",
                    content: getSystemMessage(),
                  },
                ]
              : prev;
          }
        });
      } catch (error) {
        // If history fetch fails (404 or other), continue with default system message
        // Guard: Only set system message if messages are still empty
        setMessages((prev) => {
          const hasUserOrAssistantMessages = prev.some(
            (msg) => msg.role === "user" || msg.role === "assistant"
          );
          if (hasUserOrAssistantMessages) {
            return prev;
          }
          
          if (error instanceof Error && error.message.includes("404")) {
            // No history exists yet, keep default system message
            return [
              {
                id: "system-1",
                role: "system",
                content: getSystemMessage(),
              },
            ];
          } else {
            console.error("Failed to load chat history:", error);
            return [
              {
                id: "system-1",
                role: "system",
                content: getSystemMessage(),
              },
            ];
          }
        });
      }
    }

    loadHistory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId, user?.id, campaignId]); // Reload when sessionId, user, or campaignId changes

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Check for missing requirements (but still render UI with error banner)
  const missingAuth = !isLoaded || !user;
  const missingCampaignId = !campaignId;
  
  // Check if send is enabled and why it might be disabled
  const canSend = input.trim().length > 0 && !isLoading && !missingAuth && !missingCampaignId;
  const sendDisabledReason = missingAuth 
    ? "Not authenticated" 
    : missingCampaignId 
    ? "Campaign ID missing" 
    : input.trim().length === 0 
    ? "Enter a message" 
    : isLoading 
    ? "Sending..." 
    : null;

  async function handleSend() {
    if (!canSend || !sessionId) return;

    // Step 1: Capture user input immediately
    const userInput = input.trim();
    const userMessage: Message = {
      id: `user-${Date.now()}`,
      role: "user",
      content: userInput,
    };

    // Step 2: Create thinking placeholder for assistant response (store ID for later replacement)
    const thinkingId = `thinking-${Date.now()}`;
    const thinkingMessage: Message = {
      id: thinkingId,
      role: "assistant",
      content: "",
      status: "thinking",
    };

    // Step 3: OPTIMISTIC UPDATE - Immediately append user message + thinking placeholder
    // This makes the message appear instantly in the UI BEFORE any await
    setMessages((prev) => [...prev, userMessage, thinkingMessage]);
    
    // Step 4: Clear input field immediately for better UX
    setInput("");
    
    // Step 5: Set loading state
    setIsLoading(true);

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }

      const data = await apiFetch<{ response: string }>("/api/v1/chat", {
        token,
        method: "POST",
        body: {
          query: userInput,
          tenant_id: campaignId,
          mode: "fast",
          session_id: sessionId,
          user_display_name: userDisplayName,
        },
      });

      // Step 6: Replace thinking placeholder with actual assistant response
      const assistantMessage: Message = {
        id: `assistant-${Date.now()}`,
        role: "assistant",
        content: data.response,
      };
      
      // Replace thinking message with actual response
      setMessages((prev) => {
        return prev.map((msg) =>
          msg.id === thinkingId ? assistantMessage : msg
        );
      });
    } catch (error) {
      // Log error to console for debugging
      console.error("Riley chat error:", error);
      
      // Replace thinking placeholder with error message
      const errorText = error instanceof Error 
        ? error.message 
        : "Unknown error occurred";
      
      const errorMessage: Message = {
        id: `error-${Date.now()}`,
        role: "assistant",
        content: `Error: ${errorText}`,
      };
      
      // Replace thinking message with error message
      setMessages((prev) => {
        return prev.map((msg) =>
          msg.id === thinkingId ? errorMessage : msg
        );
      });
    } finally {
      setIsLoading(false);
    }
  }

  async function handleClearContext() {
    setIsClearing(true);
    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }
      
      await apiFetch(`/api/v1/chat/history/${sessionId}`, {
        token,
        method: "DELETE",
      });
      
      // Reset messages to just the system message
      setMessages([
        {
          id: "system-1",
          role: "system",
          content: getSystemMessage(),
        },
      ]);
    } catch (error) {
      console.error("Failed to clear chat history:", error);
      // Optionally show a toast/notification to the user
    } finally {
      setIsClearing(false);
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  // Render content with interactive citation badges
  function renderContent(text: string): React.ReactNode {
    if (!text) return null;

    // Regex to match [[Source: Filename.pdf]] patterns
    const citationRegex = /(\[\[Source: .*?\]\])/g;
    const parts: Array<{ type: "text" | "citation"; content: string }> = [];
    let lastIndex = 0;
    let match;

    // Find all citation matches
    while ((match = citationRegex.exec(text)) !== null) {
      // Add text before the citation
      if (match.index > lastIndex) {
        parts.push({
          type: "text",
          content: text.substring(lastIndex, match.index),
        });
      }

      // Extract filename from citation (remove [[Source: ]] wrapper)
      const citationText = match[1];
      const filenameMatch = citationText.match(/\[\[Source: (.+?)\]\]/);
      const filename = filenameMatch ? filenameMatch[1] : citationText;

      parts.push({
        type: "citation",
        content: filename,
      });

      lastIndex = match.index + match[0].length;
    }

    // Add remaining text after last citation
    if (lastIndex < text.length) {
      parts.push({
        type: "text",
        content: text.substring(lastIndex),
      });
    }

    // If no citations found, return plain text
    if (parts.length === 0) {
      return <span>{text}</span>;
    }

    // Render parts with citation badges
    return (
      <>
        {parts.map((part, idx) => {
          if (part.type === "citation") {
            return (
              <button
                key={`citation-${idx}`}
                type="button"
                onClick={() => {
                  if (onViewAsset) {
                    onViewAsset(part.content);
                  }
                }}
                className={cn(
                  "inline-flex items-center gap-1.5",
                  "bg-zinc-800/50 border border-zinc-700/50 rounded-md",
                  "px-2 py-0.5 text-[10px] text-amber-500/90",
                  "hover:bg-amber-500/10 hover:border-amber-500/30 hover:text-amber-400",
                  "transition-all mx-1 align-baseline cursor-pointer"
                )}
              >
                <FileText className="h-2.5 w-2.5" />
                <span>{part.content}</span>
              </button>
            );
          }
          return <span key={`text-${idx}`}>{part.content}</span>;
        })}
      </>
    );
  }

  return (
    <div className="h-full flex flex-col">
      {/* Error Banner */}
      {(missingAuth || missingCampaignId) && (
        <div className="flex-shrink-0 bg-red-500/10 border-b border-red-500/30 px-4 py-2">
          <p className="text-sm text-red-400">
            {missingAuth ? "Not authenticated. Please sign in." : "Campaign ID missing. Cannot send messages."}
          </p>
        </div>
      )}
      
      {/* Header with Clear Context Button */}
      <div className="flex-shrink-0 flex items-center justify-end px-4 py-2 border-b border-zinc-800">
        <button
          type="button"
          onClick={handleClearContext}
          disabled={isClearing}
          className={cn(
            "flex items-center gap-1.5 px-2 py-1 text-xs text-zinc-400 hover:text-zinc-200 transition-colors rounded",
            "hover:bg-zinc-800/50 disabled:opacity-50 disabled:cursor-not-allowed"
          )}
          aria-label="Clear context"
        >
          {isClearing ? (
            <Loader2 className="h-3 w-3 animate-spin" />
          ) : (
            <Trash2 className="h-3 w-3" />
          )}
          <span>Clear Context</span>
        </button>
      </div>

      {/* Chat Area - Scrollable */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4 scrollbar-thin">
        {messages.map((message, index) => {
          const isLastMessage = index === messages.length - 1;
          const isLastAssistant = message.role === "assistant" && isLastMessage;
          
          return (
            <div
              key={message.id}
              className={cn(
                "flex flex-col gap-1",
                message.role === "user" ? "items-end" : "items-start"
              )}
            >
              <div
                className={cn(
                  "rounded-lg px-3 py-2 max-w-[85%] text-sm",
                  message.role === "user"
                    ? "bg-amber-500/10 border border-amber-500/20 text-amber-100"
                    : message.role === "system"
                    ? "bg-zinc-800/50 border border-zinc-700/50 text-zinc-300 italic"
                    : "bg-zinc-900/50 border border-zinc-800/50 text-zinc-100"
                )}
              >
                {message.status === "thinking" ? (
                  // Thinking placeholder
                  <div className="flex items-center gap-2 text-zinc-400">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    <span>Riley is thinking...</span>
                  </div>
                ) : isLastAssistant ? (
                  // Typewriter effect for last assistant message
                  <TypewriterMarkdown content={message.content} />
                ) : message.role === "assistant" ? (
                  // Static markdown for previous assistant messages
                  <div className="riley-md">
                    <ReactMarkdown>{message.content}</ReactMarkdown>
                  </div>
                ) : (
                  // User and system messages (plain text)
                  message.content
                )}
              </div>
            </div>
          );
        })}

        {/* Loading Indicator */}
        {isLoading && (
          <div className="flex items-start gap-2">
            <div className="rounded-lg px-3 py-2 bg-zinc-900/50 border border-zinc-800/50">
              <Loader2 className="h-4 w-4 animate-spin text-zinc-400" />
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input Area - Fixed Bottom */}
      <div className="flex-shrink-0 border-t border-zinc-800 p-4">
        {sendDisabledReason && !isLoading && (
          <div className="mb-2 text-xs text-zinc-500 text-center">
            {sendDisabledReason}
          </div>
        )}
        <div className="flex items-center gap-2">
          <input
            ref={inputRef}
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask Riley..."
            disabled={isLoading || missingAuth || missingCampaignId}
            className="flex-1 rounded-lg border border-zinc-800 bg-zinc-900/50 px-3 py-2 text-sm text-zinc-100 placeholder:text-zinc-600 focus:outline-none focus:ring-2 focus:ring-amber-500/50 disabled:opacity-50 disabled:cursor-not-allowed"
          />
          <button
            type="button"
            onClick={handleSend}
            disabled={!canSend}
            className={cn(
              "flex-shrink-0 p-2 rounded-lg transition-colors",
              canSend
                ? "bg-amber-500/10 border border-amber-500/20 text-amber-400 hover:bg-amber-500/20"
                : "bg-zinc-800/50 border border-zinc-700/50 text-zinc-600 cursor-not-allowed"
            )}
            aria-label="Send message"
            title={sendDisabledReason || "Send message"}
          >
            {isLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Send className="h-4 w-4" />
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

// Export helper function for pages to use
export function getRileyTitle(mode: "messaging" | "research" | "strategy" | "pitch"): string {
  switch (mode) {
    case "messaging":
      return "Riley Messaging";
    case "research":
      return "Riley Research";
    case "strategy":
      return "Riley Strategy";
    case "pitch":
      return "Riley Pitch";
    default:
      return "Riley Strategy";
  }
}
