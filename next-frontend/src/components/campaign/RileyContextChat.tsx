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
  const { getToken } = useAuth();
  
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
            setMessages([
              {
                id: "system-1",
                role: "system",
                content: getSystemMessage(),
              },
              ...historyMessages,
            ]);
          } else {
            // No history found, keep default system message
            setMessages([
              {
                id: "system-1",
                role: "system",
                content: getSystemMessage(),
              },
            ]);
          }
      } catch (error) {
        // If history fetch fails (404 or other), continue with default system message
        if (error instanceof Error && error.message.includes("404")) {
          // No history exists yet, keep default system message
          setMessages([
            {
              id: "system-1",
              role: "system",
              content: getSystemMessage(),
            },
          ]);
        } else {
          console.error("Failed to load chat history:", error);
        setMessages([
          {
            id: "system-1",
            role: "system",
            content: getSystemMessage(),
          },
        ]);
      }
    }

    loadHistory();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sessionId, user?.id, campaignId]); // Reload when sessionId, user, or campaignId changes

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const canSend = input.trim().length > 0 && !isLoading;

  async function handleSend() {
    if (!canSend) return;

    const userMessage: Message = {
      id: `user-${Date.now()}`,
      role: "user",
      content: input.trim(),
    };

    setMessages((prev) => [...prev, userMessage]);
    const userInput = input.trim();
    setInput("");
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
        },
      });

      const assistantMessage: Message = {
        id: `assistant-${Date.now()}`,
        role: "assistant",
        content: data.response,
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      const errorMessage: Message = {
        id: `error-${Date.now()}`,
        role: "assistant",
        content: `Error: ${error instanceof Error ? error.message : "Unknown error occurred"}`,
      };
      setMessages((prev) => [...prev, errorMessage]);
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

  // Guard clause: Don't render if user or campaignId is not available
  // This must come AFTER all hooks to comply with Rules of Hooks
  if (!isLoaded || !user || !campaignId) {
    return null;
  }

  return (
    <div className="h-full flex flex-col">
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
                {isLastAssistant ? (
                  // Typewriter effect for last assistant message
                  <TypewriterMarkdown content={message.content} />
                ) : message.role === "assistant" ? (
                  // Static markdown for previous assistant messages
                  <div className="prose prose-invert prose-sm max-w-none prose-p:mb-4 prose-p:leading-7 prose-ul:my-4 prose-ul:list-disc prose-ul:pl-4 prose-li:mb-2 prose-strong:text-amber-400 prose-strong:font-semibold">
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
        <div className="flex items-center gap-2">
          <input
            ref={inputRef}
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask Riley..."
            disabled={isLoading}
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
