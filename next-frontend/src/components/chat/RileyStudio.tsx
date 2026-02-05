"use client";

import { useState, useRef, useEffect } from "react";
import { useAuth } from "@clerk/nextjs";
import { Send, Loader2, Sparkles, Zap, Brain, FileText, Pencil, Check, X, Search, MessageSquare, BarChart3 } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkBreaks from "remark-breaks";
import { cn } from "@app/lib/utils";
import { TypewriterMarkdown } from "@app/components/ui/TypewriterMarkdown";

type Message = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  sourcesCount?: number;
};

type Conversation = {
  id: string;
  title: string;
  lastMessage: string;
  timestamp: Date;
};

interface RileyStudioProps {
  contextName: string;
  tenantId: string;
  mode?: "fast" | "deep";
}

export function RileyStudio({ contextName, tenantId, mode: initialMode = "fast" }: RileyStudioProps) {
  const { getToken, userId } = useAuth();
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [activeConversationId, setActiveConversationId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [mode, setMode] = useState<"fast" | "deep">(initialMode);
  const [renamingSessionId, setRenamingSessionId] = useState<string | null>(null);
  const [renameInput, setRenameInput] = useState("");
  const [isRenaming, setIsRenaming] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const renameInputRef = useRef<HTMLInputElement>(null);
  
  // Check if this is Global Riley (Imperial Amber theme)
  const isGlobal = tenantId === "global";

  // Generate collision-proof, scope-aware session ID
  const createNewSessionId = (): string => {
    // Ensure tenantId is always included (either "global" or campaignId)
    const safeTenantId = tenantId || "global";
    // Use userId from Clerk, fallback to "anonymous" if not available
    const safeUserId = userId || "anonymous";
    // Generate timestamp
    const timestamp = Date.now();
    // Format: session_{tenantId}_{userId}_{timestamp}
    return `session_${safeTenantId}_${safeUserId}_${timestamp}`;
  };

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Auto-resize textarea
  useEffect(() => {
    if (inputRef.current) {
      inputRef.current.style.height = "auto";
      inputRef.current.style.height = `${Math.min(inputRef.current.scrollHeight, 200)}px`;
    }
  }, [input]);

  // Load chat history when conversation is selected
  useEffect(() => {
    if (!activeConversationId) {
      // Clear messages when no conversation is selected
      setMessages([]);
      return;
    }

    // Clear messages immediately when switching sessions
    setMessages([]);
    setIsLoading(true);

    async function loadHistory() {
      try {
        const token = await getToken();
        const response = await fetch(
          `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/v1/chat/history/${activeConversationId}`,
          {
            headers: {
              "Authorization": `Bearer ${token}`,
            },
          }
        );
        
        if (response.ok) {
          const history = await response.json();
          if (history && Array.isArray(history) && history.length > 0) {
            const historyMessages: Message[] = history.map(
              (msg: { role: string; content: string }, idx: number) => ({
                id: `history-${Date.now()}-${idx}`,
                role: msg.role === "user" ? "user" : "assistant",
                content: msg.content,
              })
            );
            setMessages([
              {
                id: "system-1",
                role: "system",
                content: `Hi, I'm Riley. I have access to ${contextName}. How can I help you today?`,
              },
              ...historyMessages,
            ]);
          } else {
            setMessages([
              {
                id: "system-1",
                role: "system",
                content: `Hi, I'm Riley. I have access to ${contextName}. How can I help you today?`,
              },
            ]);
          }
        }
      } catch (error) {
        console.error("Failed to load chat history:", error);
        setMessages([
          {
            id: "system-1",
            role: "system",
            content: `Hi, I'm Riley. I have access to ${contextName}. How can I help you today?`,
          },
        ]);
      } finally {
        setIsLoading(false);
      }
    }

    loadHistory();
  }, [activeConversationId, contextName, getToken]);

  const canSend = input.trim().length > 0 && !isLoading;

  const handleNewConversation = () => {
    // Generate collision-proof, scope-aware session ID
    const newId = createNewSessionId();
    setActiveConversationId(newId);
    setMessages([
      {
        id: "system-1",
        role: "system",
        content: `Hi, I'm Riley. I have access to ${contextName}. How can I help you today?`,
      },
    ]);
    setInput("");
  };

  const handleRenameStart = (e: React.MouseEvent, conv: Conversation) => {
    e.stopPropagation();
    setRenamingSessionId(conv.id);
    setRenameInput(conv.title);
    // Focus input after state update
    setTimeout(() => {
      renameInputRef.current?.focus();
      renameInputRef.current?.select();
    }, 0);
  };

  const handleRenameCancel = (e?: React.MouseEvent) => {
    if (e) e.stopPropagation();
    setRenamingSessionId(null);
    setRenameInput("");
  };

  const handleRenameSave = async (e: React.MouseEvent | React.KeyboardEvent, sessionId: string) => {
    e.stopPropagation();
    
    const newTitle = renameInput.trim();
    if (!newTitle || newTitle === conversations.find(c => c.id === sessionId)?.title) {
      handleRenameCancel();
      return;
    }

    setIsRenaming(true);
    try {
      const token = await getToken();
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/v1/sessions/${sessionId}`,
        {
          method: "PATCH",
          headers: {
            "Content-Type": "application/json",
            "Authorization": `Bearer ${token}`,
          },
          body: JSON.stringify({ title: newTitle }),
        }
      );

      if (response.ok) {
        // Update the conversation in the list
        setConversations((prev) =>
          prev.map((conv) =>
            conv.id === sessionId ? { ...conv, title: newTitle } : conv
          )
        );
        handleRenameCancel();
      } else {
        console.error("Failed to rename session");
      }
    } catch (error) {
      console.error("Error renaming session:", error);
    } finally {
      setIsRenaming(false);
    }
  };

  const handleRenameKeyDown = (e: React.KeyboardEvent<HTMLInputElement>, sessionId: string) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleRenameSave(e, sessionId);
    } else if (e.key === "Escape") {
      e.preventDefault();
      handleRenameCancel();
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

    // Render parts with citation badges and markdown for text
    return (
      <>
        {parts.map((part, idx) => {
          if (part.type === "citation") {
            return (
              <button
                key={`citation-${idx}`}
                type="button"
                onClick={() => {
                  // For global chat, we might not have onViewAsset, so just show alert
                  alert(`Citation: ${part.content}\n\nNote: File viewing not available in global chat.`);
                }}
                className="inline-flex items-center gap-1.5 bg-zinc-800/50 border border-zinc-700/50 rounded-md px-2 py-0.5 text-[10px] transition-all mx-1 align-baseline cursor-pointer text-amber-500/90 hover:bg-amber-500/10 hover:border-amber-500/30 hover:text-amber-400"
              >
                <FileText className="h-2.5 w-2.5" />
                <span>{part.content}</span>
              </button>
            );
          }
          // Render text parts (markdown will be handled by parent container)
          return <span key={`text-${idx}`}>{part.content}</span>;
        })}
      </>
    );
  }

  const handleSend = async () => {
    if (!canSend) return;

    // Step 1: Capture user input immediately
    const userInput = input.trim();
    const userMessage: Message = {
      id: `user-${Date.now()}`,
      role: "user",
      content: userInput,
    };

    // Step 2: OPTIMISTIC UPDATE - Immediately append user message to state
    // This makes the message appear instantly in the UI
    setMessages((prev) => {
      // If this is the first message (only system message exists), keep system + add user
      if (prev.length === 1 && prev[0].role === "system") {
        return [prev[0], userMessage];
      }
      // Otherwise, append to existing messages
      return [...prev, userMessage];
    });
    
    // Step 3: Clear input field immediately for better UX
    setInput("");

    // Step 4: Set loading state to show "Thinking" indicator
    setIsLoading(true);

    try {
      // Step 5: Get auth token
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication token not available");
      }

      // Step 6: Ensure we have a session ID (create new conversation if needed)
      // Generate collision-proof, scope-aware session ID if needed
      let sessionId = activeConversationId;
      if (!sessionId) {
        const newId = createNewSessionId();
        setActiveConversationId(newId);
        sessionId = newId;
      }

      // Step 7: Perform the API call
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/v1/chat`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Authorization": `Bearer ${token}`,
        },
        body: JSON.stringify({
          query: userInput,
          tenant_id: tenantId,
          mode: mode,
          session_id: sessionId,
        }),
      });

      if (!response.ok) {
        const text = await response.text().catch(() => "");
        throw new Error(`Chat failed (${response.status}). ${text ? `Details: ${text}` : ""}`.trim());
      }

      const data = await response.json();

      // Step 8: Create assistant message from response
      const assistantMessage: Message = {
        id: `assistant-${Date.now()}`,
        role: "assistant",
        content: data.response,
        sourcesCount: data.sources_count,
      };

      // Step 9: Append assistant response to messages
      setMessages((prev) => [...prev, assistantMessage]);

      // Step 10: Update conversation list if this is a new conversation
      if (sessionId && !conversations.find((c) => c.id === sessionId)) {
        setConversations((prev) => [
          {
            id: sessionId,
            title: userMessage.content.slice(0, 30) + (userMessage.content.length > 30 ? "..." : ""),
            lastMessage: userMessage.content,
            timestamp: new Date(),
          },
          ...prev,
        ]);
      }
    } catch (error) {
      // On error, append error message
      const errorMessage: Message = {
        id: `error-${Date.now()}`,
        role: "assistant",
        content: `Error: ${error instanceof Error ? error.message : "Unknown error occurred"}`,
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      // Step 11: Clear loading state (hides "Thinking" indicator)
      setIsLoading(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const formatTime = (date: Date) => {
    const now = new Date();
    const diff = now.getTime() - date.getTime();
    const hours = Math.floor(diff / (1000 * 60 * 60));
    const days = Math.floor(hours / 24);

    if (days > 0) return `${days}d ago`;
    if (hours > 0) return `${hours}h ago`;
    return "Just now";
  };

  const promptStarters = [
    { text: "What are the key insights from recent research?", icon: Search, color: "text-cyan-400" },
    { text: "Help me refine the messaging strategy", icon: MessageSquare, color: "text-purple-400" },
    { text: "Analyze the current campaign performance", icon: BarChart3, color: "text-emerald-400" },
    { text: "Generate ideas for the next phase", icon: Zap, color: "text-orange-400" },
  ];

  const isEmpty = messages.length === 0 || (messages.length === 1 && messages[0].role === "system");

  return (
    <div 
      className={cn(
        "flex h-full text-white overflow-hidden",
        !isGlobal && "bg-slate-950/50 backdrop-blur-sm"
      )}
      style={isGlobal ? {
        background: "radial-gradient(ellipse at center, rgba(120, 53, 15, 0.2) 0%, rgb(2, 6, 23) 50%, rgb(2, 6, 23) 100%)"
      } : undefined}
    >
      {/* Left Sidebar - Chat History */}
      <aside className="w-64 bg-zinc-900/50 border-r border-zinc-800 flex flex-col shrink-0">
        {/* New Conversation Button */}
        <div className="p-4 border-b border-zinc-800">
          <button
            type="button"
            onClick={handleNewConversation}
            className={cn(
              "w-full flex items-center justify-center gap-2 px-4 py-3 rounded-lg transition-colors font-medium",
              isGlobal
                ? "bg-amber-500/10 border border-amber-500/20 text-amber-400 hover:bg-amber-500/20"
                : "bg-amber-500/10 border border-amber-500/20 text-amber-400 hover:bg-amber-500/20"
            )}
          >
            <Sparkles className="h-4 w-4" />
            <span>New Conversation</span>
          </button>
        </div>

        {/* Conversation List */}
        <div className="flex-1 overflow-y-auto p-2 space-y-1">
          {conversations.map((conv) => {
            const isActive = activeConversationId === conv.id;
            const isRenaming = renamingSessionId === conv.id;

            return (
              <div
                key={conv.id}
                className={cn(
                  "w-full rounded-lg transition-colors relative group",
                  isActive
                    ? "bg-zinc-800/50 border border-amber-500/30"
                    : "hover:bg-zinc-800/30 border border-transparent"
                )}
              >
                <div
                  role="button"
                  tabIndex={0}
                  onClick={() => {
                    setActiveConversationId(conv.id);
                  }}
                  onKeyDown={(e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      setActiveConversationId(conv.id);
                    }
                  }}
                  className="w-full text-left p-3 cursor-pointer"
                >
                  {isRenaming ? (
                    <div className="flex items-center gap-2">
                      <input
                        ref={renameInputRef}
                        type="text"
                        value={renameInput}
                        onChange={(e) => setRenameInput(e.target.value)}
                        onKeyDown={(e) => handleRenameKeyDown(e, conv.id)}
                        onClick={(e) => e.stopPropagation()}
                        className="flex-1 bg-zinc-900/50 border border-zinc-700 rounded px-2 py-1 text-sm text-zinc-100 focus:outline-none focus:ring-1 focus:ring-amber-500/50"
                        disabled={isRenaming}
                      />
                      <button
                        type="button"
                        onClick={(e) => handleRenameSave(e, conv.id)}
                        disabled={isRenaming}
                        className="p-1 rounded transition-colors text-amber-400 hover:bg-amber-500/10"
                      >
                        <Check className="h-4 w-4" />
                      </button>
                      <button
                        type="button"
                        onClick={(e) => handleRenameCancel(e)}
                        disabled={isRenaming}
                        className="p-1 rounded text-zinc-500 hover:bg-zinc-800/50 transition-colors"
                      >
                        <X className="h-4 w-4" />
                      </button>
                    </div>
                  ) : (
                    <>
                      <div className="flex items-start justify-between gap-2">
                        <div className="flex-1 min-w-0">
                          <div className="font-medium text-sm text-zinc-100 truncate">{conv.title}</div>
                          <div className="text-xs text-zinc-500 mt-1 truncate">{conv.lastMessage}</div>
                          <div className="text-xs text-zinc-600 mt-1">{formatTime(conv.timestamp)}</div>
                        </div>
                        {isActive && (
                          <button
                            type="button"
                            onClick={(e) => handleRenameStart(e, conv)}
                        className="opacity-0 group-hover:opacity-100 p-1.5 rounded transition-all text-amber-400 hover:bg-amber-500/10"
                            title="Rename conversation"
                          >
                            <Pencil className="h-3.5 w-3.5" />
                          </button>
                        )}
                      </div>
                    </>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </aside>

      {/* Main Area */}
      <main className="flex-1 flex flex-col min-w-0">
        {/* Header with Mode Toggle */}
        <header className="h-16 border-b border-zinc-800 flex items-center justify-between px-6 shrink-0">
          <div className="flex items-center gap-2">
            <Sparkles className={cn(
              "h-5 w-5",
              isGlobal ? "text-amber-400 drop-shadow-[0_0_15px_rgba(251,191,36,0.4)]" : "text-amber-400"
            )} />
            <h1 className="text-lg font-bold text-white">Riley</h1>
            <span className="text-zinc-600">|</span>
            <span className={cn(
              "text-sm font-medium",
              isGlobal ? "text-amber-400 drop-shadow-[0_0_10px_rgba(251,191,36,0.3)]" : "text-amber-500/90"
            )}>
              {isGlobal ? "Rally Global Brain" : contextName}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={() => setMode("fast")}
              className={cn(
                "flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-colors",
                mode === "fast"
                  ? "bg-amber-500/10 border border-amber-500/20 text-amber-400"
                  : "bg-zinc-800/50 border border-zinc-700/50 text-zinc-500 hover:bg-zinc-800"
              )}
            >
              <Zap className="h-4 w-4" />
              <span>Fast</span>
            </button>
            <button
              type="button"
              onClick={() => setMode("deep")}
              className={cn(
                "flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-colors",
                mode === "deep"
                  ? "bg-amber-500/10 border border-amber-500/20 text-amber-400"
                  : "bg-zinc-800/50 border border-zinc-700/50 text-zinc-500 hover:bg-zinc-800"
              )}
            >
              <Brain className="h-4 w-4" />
              <span>Deep</span>
            </button>
          </div>
        </header>

        {/* Message Area */}
        <div className="flex-1 overflow-y-auto">
          {isEmpty ? (
            /* Empty State */
            <div className="h-full flex flex-col items-center justify-center px-6 py-12">
              <div className="max-w-2xl w-full text-center">
                <div className="mb-8 flex justify-center">
                  <div className={cn(
                    "h-20 w-20 rounded-full border flex items-center justify-center",
                    "bg-amber-500/10 border-amber-500/20"
                  )}>
                    <Sparkles className={cn(
                      "h-10 w-10 text-amber-400",
                      isGlobal && "drop-shadow-[0_0_15px_rgba(251,191,36,0.4)]"
                    )} />
                  </div>
                </div>
                <h2 className="text-3xl font-bold text-white mb-2">
                  Hi, I'm Riley.
                </h2>
                <p className="text-zinc-400 mb-8">
                  I have access to <strong className={cn(
                    isGlobal ? "text-amber-400" : "text-amber-400"
                  )}>{isGlobal ? "Rally Global Brain" : contextName}</strong>. Ask me anything about strategy, messaging, or historical data.
                </p>

                {/* Prompt Starters */}
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  {promptStarters.map((prompt, idx) => {
                    const Icon = prompt.icon;
                    return (
                      <button
                        key={idx}
                        type="button"
                        onClick={() => {
                          setInput(prompt.text);
                          inputRef.current?.focus();
                        }}
                        className={cn(
                          "p-6 md:p-8 text-left rounded-lg border transition-all duration-200 ease-out text-sm",
                          "bg-slate-900/50 border-white/5",
                          "hover:border-amber-500/30 hover:bg-amber-500/5 hover:scale-[1.02] hover:shadow-[0_0_20px_rgba(251,191,36,0.1)]"
                        )}
                      >
                        <div className="flex items-start gap-3">
                          <Icon className={cn("h-5 w-5 flex-shrink-0 mt-0.5", prompt.color)} />
                          <span className="text-zinc-300 font-medium">{prompt.text}</span>
                        </div>
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>
          ) : (
            /* Message Stream */
            <div className="max-w-3xl mx-auto w-full px-6 py-8 space-y-6">
              {messages.map((message, index) => {
                const isLastMessage = index === messages.length - 1;
                const isLastAssistant = message.role === "assistant" && isLastMessage;

                return (
                  <div
                    key={message.id}
                    className={cn(
                      "flex gap-4",
                      message.role === "user" ? "justify-end" : "justify-start"
                    )}
                  >
                    {message.role !== "user" && (
                      <div className="flex-shrink-0 h-8 w-8 rounded-full border flex items-center justify-center bg-amber-500/10 border-amber-500/20">
                        <Sparkles className="h-4 w-4 text-amber-400" />
                      </div>
                    )}
                    <div
                      className={cn(
                        "rounded-lg px-4 py-3 max-w-[85%]",
                        message.role === "user"
                          ? "bg-amber-500/10 border border-amber-500/20 text-amber-100"
                          : message.role === "system"
                          ? "bg-zinc-800/50 border border-zinc-700/50 text-zinc-300 italic"
                          : isGlobal
                          ? "bg-zinc-900/50 border border-amber-500/10 text-zinc-100"
                          : "bg-zinc-900/50 border border-zinc-800/50 text-zinc-100"
                      )}
                    >
                      {isLastAssistant ? (
                        // Typewriter effect for last assistant message
                        <TypewriterMarkdown content={message.content} />
                      ) : message.role === "assistant" ? (
                        // Static markdown for previous assistant messages
                        <div className="text-sm prose prose-invert prose-sm max-w-none leading-loose prose-p:mb-8 prose-p:leading-7 prose-headings:mt-8 prose-headings:mb-4 prose-ul:my-4 prose-ul:list-disc prose-ul:pl-4 prose-li:mb-4 prose-strong:font-semibold prose-strong:text-amber-400">
                          <ReactMarkdown remarkPlugins={[remarkBreaks]}>
                            {message.content.replace(/<br\s*\/?>/gi, '\n\n')}
                          </ReactMarkdown>
                        </div>
                      ) : (
                        // User and system messages (plain text)
                        <div className="text-sm whitespace-pre-wrap">{message.content}</div>
                      )}
                      {message.role === "assistant" && message.sourcesCount !== undefined && (
                        <div className="mt-2 flex items-center gap-1.5 border-t border-zinc-800 pt-2 text-xs text-zinc-500">
                          <span>📚</span>
                          <span>Analyzed {message.sourcesCount} document{message.sourcesCount !== 1 ? "s" : ""}</span>
                        </div>
                      )}
                    </div>
                    {message.role === "user" && (
                      <div className="flex-shrink-0 h-8 w-8 rounded-full bg-zinc-800 flex items-center justify-center text-xs font-medium text-zinc-300">
                        A
                      </div>
                    )}
                  </div>
                );
              })}

              {/* Thinking Indicator - Shows when isLoading is true */}
              {isLoading && (
                <div className="flex gap-4 justify-start">
                  <div className="flex-shrink-0 h-8 w-8 rounded-full border flex items-center justify-center bg-amber-500/10 border-amber-500/20">
                    <Sparkles className="h-4 w-4 text-amber-400" />
                  </div>
                  <div className="rounded-lg px-4 py-3 bg-zinc-900/50 border border-zinc-800/50">
                    <div className="flex items-center gap-2">
                    <Loader2 className="h-4 w-4 animate-spin text-zinc-400" />
                      <span className="text-sm text-zinc-400">Riley is thinking...</span>
                    </div>
                  </div>
                </div>
              )}

              <div ref={messagesEndRef} />
            </div>
          )}
        </div>

        {/* Input Area - Fixed Bottom */}
        <div className="flex-shrink-0 border-t border-zinc-800 p-4 bg-slate-950/50 backdrop-blur-sm">
          <div className="max-w-3xl mx-auto w-full">
            <div className="flex items-end gap-3">
              <textarea
                ref={inputRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Message Riley..."
                disabled={isLoading}
                rows={1}
                className="flex-1 resize-none rounded-lg border border-zinc-800 bg-zinc-900/50 px-4 py-3 text-sm text-zinc-100 placeholder:text-zinc-600 focus:outline-none focus:ring-2 focus:ring-amber-500/50 disabled:opacity-50 disabled:cursor-not-allowed max-h-[200px]"
              />
              <button
                type="button"
                onClick={handleSend}
                disabled={!canSend}
                className={cn(
                  "flex-shrink-0 p-3 rounded-lg transition-colors",
                  canSend
                    ? "bg-amber-500/10 border border-amber-500/20 text-amber-400 hover:bg-amber-500/20"
                    : "bg-zinc-800/50 border border-zinc-700/50 text-zinc-600 cursor-not-allowed"
                )}
                aria-label="Send message"
              >
                {isLoading ? (
                  <Loader2 className="h-5 w-5 animate-spin" />
                ) : (
                  <Send className="h-5 w-5" />
                )}
              </button>
            </div>
            <p className="text-xs text-zinc-600 mt-2 text-center">
              {mode === "fast" ? "⚡ Fast mode: Quick responses" : "🧠 Deep mode: Comprehensive analysis"}
            </p>
          </div>
        </div>
      </main>
    </div>
  );
}
