"use client";

import { useRef, useState, useEffect } from "react";
import { useAuth } from "@clerk/nextjs";
import { Loader2, Send, Zap, Brain } from "lucide-react";
import { TypewriterMarkdown } from "@app/components/TypewriterMarkdown";
import { cn } from "@app/lib/utils";
import { detectPersona } from "@app/lib/personas";
import { apiFetch } from "@app/lib/api";

type Message = {
  id: string;
  role: "user" | "assistant";
  content: string;
  sourcesCount?: number;
  modelUsed?: string;
};

type ChatMode = "fast" | "deep";

type ChatResponse = {
  response: string;
  model_used: string;
  sources_count: number;
};

interface ChatInterfaceProps {
  tenantId?: string;
}

export function ChatInterface({ tenantId = "rally" }: ChatInterfaceProps) {
  const { getToken } = useAuth();
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [mode, setMode] = useState<ChatMode>("fast");
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const chatContainerRef = useRef<HTMLDivElement>(null);

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
    setInput("");
    setIsLoading(true);

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }

      const data = await apiFetch<ChatResponse>("/api/v1/chat", {
        token,
        method: "POST",
        body: {
          query: userMessage.content,
          tenant_id: tenantId,
          mode: mode,
        },
      });

      const assistantMessage: Message = {
        id: `assistant-${Date.now()}`,
        role: "assistant",
        content: data.response,
        sourcesCount: data.sources_count,
        modelUsed: data.model_used,
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (e) {
      const errorMessage: Message = {
        id: `error-${Date.now()}`,
        role: "assistant",
        content: `Error: ${e instanceof Error ? e.message : "Unknown error occurred"}`,
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <section className="flex flex-col rounded-2xl border border-slate-200 bg-white shadow-sm">
      {/* Brain Toggle Header */}
      <div className="flex items-center justify-between border-b border-slate-200 bg-slate-50/50 px-5 py-3 sm:px-6">
        <div className="flex flex-col gap-1">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
            <Brain className="h-4 w-4 text-slate-700" aria-hidden="true" />
            Strategic Chat
          </div>
          <p className="text-xs text-slate-500">
            Format: Clean, conversational, minimal headers.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setMode("fast")}
            className={cn(
              "inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-medium transition-all",
              "border",
              mode === "fast"
                ? "border-slate-300 bg-white text-slate-900 shadow-sm"
                : "border-transparent bg-transparent text-slate-500 hover:text-slate-700"
            )}
          >
            <Zap className="h-3.5 w-3.5" aria-hidden="true" />
            Speed (Gemini 2.5 Flash)
          </button>
          <button
            type="button"
            onClick={() => setMode("deep")}
            className={cn(
              "inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-medium transition-all",
              "border",
              mode === "deep"
                ? "border-amber-400 bg-amber-400 text-slate-900 shadow-sm"
                : "border-transparent bg-transparent text-slate-500 hover:text-slate-700"
            )}
          >
            <Brain className="h-3.5 w-3.5" aria-hidden="true" />
            Deep Strategy (Gemini 2.5 Pro)
          </button>
        </div>
      </div>

      {/* Chat Window */}
      <div
        ref={chatContainerRef}
        className="flex h-[500px] flex-col overflow-y-auto px-4 py-4 sm:px-6"
      >
        {messages.length === 0 ? (
          <div className="flex h-full items-center justify-center">
            <div className="text-center">
              <p className="text-sm font-medium text-slate-900">
                Start a conversation with Riley
              </p>
              <p className="mt-1 text-xs text-slate-500">
                Ask questions about your client's documents and get strategic
                insights.
              </p>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-4">
            {messages.map((msg, index) => {
              const isNewestMessage = index === messages.length - 1;
              const shouldAnimate = msg.role === "assistant" && isNewestMessage;

              // Detect persona in assistant messages
              const detectedPersona = msg.role === "assistant" ? detectPersona(msg.content) : null;

              return (
                <div
                  key={msg.id}
                  className={cn(
                    "flex w-full flex-col",
                    msg.role === "user" ? "items-end" : "items-start"
                  )}
                >
                  {/* Persona Detection Badge */}
                  {detectedPersona && (
                    <div className="mb-1.5 flex items-center gap-1.5 rounded-lg bg-amber-50 border border-amber-200 px-2.5 py-1 text-xs text-amber-800">
                      <span>🎯</span>
                      <span className="font-medium">Target Persona Identified:</span>
                      <span className="font-semibold">{detectedPersona}</span>
                    </div>
                  )}
                  <div
                    className={cn(
                      "max-w-[85%] rounded-2xl px-4 py-3 text-sm",
                      msg.role === "user"
                        ? "bg-slate-900 text-white"
                        : "border border-slate-100 bg-white text-slate-900"
                    )}
                  >
                    {msg.role === "user" ? (
                      <div className="whitespace-pre-wrap break-words">
                        {msg.content}
                      </div>
                    ) : (
                      <TypewriterMarkdown
                        content={msg.content}
                        shouldAnimate={shouldAnimate}
                      />
                    )}
                    {msg.role === "assistant" && msg.sourcesCount !== undefined && (
                      <div className="mt-2 flex items-center gap-1.5 border-t border-slate-100 pt-2 text-xs text-slate-500">
                        <span>📚</span>
                        <span>Analyzed {msg.sourcesCount} document{msg.sourcesCount !== 1 ? "s" : ""}</span>
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
            {isLoading && (
              <div className="flex justify-start">
                <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3">
                  <div className="flex items-center gap-2 text-sm text-slate-500">
                    <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
                    <span>Riley is thinking...</span>
                  </div>
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        )}
      </div>

      {/* Input Area */}
      <div className="border-t border-slate-200 bg-slate-50/50 p-4 sm:p-5">
        <div className="flex gap-3">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                handleSend();
              }
            }}
            placeholder="Ask Riley about your documents..."
            className={cn(
              "flex-1 rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-900",
              "placeholder:text-slate-400 shadow-sm",
              "focus:outline-none focus:ring-2 focus:ring-slate-300 focus:border-slate-300"
            )}
          />
          <button
            type="button"
            onClick={handleSend}
            disabled={!canSend}
            className={cn(
              "inline-flex items-center justify-center gap-2 rounded-xl px-5 py-3 text-sm font-semibold",
              "border border-amber-400 bg-amber-400 text-slate-900 shadow-sm",
              "hover:bg-amber-500 disabled:cursor-not-allowed disabled:opacity-60",
              "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-amber-400 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-50"
            )}
          >
            {isLoading ? (
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
            ) : (
              <>
                <Send className="h-4 w-4" aria-hidden="true" />
                Send
              </>
            )}
          </button>
        </div>
      </div>
    </section>
  );
}

