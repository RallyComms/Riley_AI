"use client";

import { useState, useRef, useEffect, useMemo } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { Send, Loader2, Sparkles, Zap, Brain, FileText, Check, X, Search, MessageSquare, BarChart3, ChevronLeft, Users, FolderPlus, Folder, MoreHorizontal, ChevronDown, ChevronRight, Copy, CheckCheck, Download, ClipboardList, AlertTriangle } from "lucide-react";
import ReactMarkdown from "react-markdown";
import remarkBreaks from "remark-breaks";
import { cn } from "@app/lib/utils";
import { TypewriterMarkdown } from "@app/components/ui/TypewriterMarkdown";
import { apiFetch } from "@app/lib/api";
import { toAsset } from "@app/lib/files";
import { Asset } from "@app/lib/types";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";

type Message = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  sourcesCount?: number;
  sources?: MessageSource[];
  status?: "thinking"; // For placeholder messages
  reportDownloadUrl?: string;
  reportTitle?: string;
  reportSuggestionPrompt?: string;
  reportSuggestionType?: "strategy_memo" | "audience_analysis" | "narrative_brief" | "opposition_framing_brief";
};

type SourceCitation = {
  key: string;
  label: string;
};

type MessageSource = {
  id: string;
  filename: string;
  location?: string;
};

type Conversation = {
  id: string;
  title: string;
  lastMessage: string;
  timestamp: Date;
  projectId: string | null;
};

type RileyProject = {
  id: string;
  name: string;
  createdAt: Date;
  updatedAt: Date;
};

interface RileyStudioProps {
  contextName: string;
  tenantId: string;
  mode?: "fast" | "deep";
}

type PersistedConversation = {
  id: string;
  title: string;
  project_id?: string | null;
  last_message?: string | null;
  last_message_at?: string | null;
  created_at?: string | null;
};

type PersistedProject = {
  id: string;
  name: string;
  created_at?: string | null;
  updated_at?: string | null;
};

type RileyIndexSummary = {
  total_documents: number;
  indexed_count: number;
  processing_count: number;
  failed_count: number;
  low_text_count: number;
  ocr_needed_count: number;
  counts_by_file_type: Record<string, number>;
  recent_uploads: Array<{
    filename: string;
    file_type: string;
    ingestion_status: string;
    upload_date: string;
  }>;
};

type PersistedReportJob = {
  report_job_id: string;
  tenant_id: string;
  user_id: string;
  conversation_id?: string | null;
  report_type: string;
  title: string;
  status: "queued" | "processing" | "complete" | "failed";
  created_at?: string | null;
  started_at?: string | null;
  completed_at?: string | null;
  error_message?: string | null;
  output_file_id?: string | null;
  output_url?: string | null;
  summary_text?: string | null;
  query: string;
  mode: "normal" | "deep";
  report_body?: string | null;
};

type ReportJob = {
  reportJobId: string;
  tenantId: string;
  userId: string;
  conversationId: string | null;
  reportType: string;
  title: string;
  status: "queued" | "processing" | "complete" | "failed";
  createdAt: Date;
  startedAt?: Date;
  completedAt?: Date;
  errorMessage?: string | null;
  outputFileId?: string | null;
  outputUrl?: string | null;
  summaryText?: string | null;
  query: string;
  mode: "normal" | "deep";
};

export function RileyStudio({ contextName, tenantId, mode: initialMode = "fast" }: RileyStudioProps) {
  const { getToken, isLoaded: authLoaded } = useAuth();
  const { user, isLoaded: userLoaded } = useUser();
  
  // Derive user display name: username ?? firstName ?? primaryEmail ?? "there"
  const userDisplayName = user?.username ?? user?.firstName ?? user?.primaryEmailAddress?.emailAddress ?? "there";
  
  // Check for missing requirements
  const missingAuth = !authLoaded || !userLoaded || !user;
  const missingTenantId = !tenantId;
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [projects, setProjects] = useState<RileyProject[]>([]);
  const [activeConversationId, setActiveConversationId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [isLoadingConversations, setIsLoadingConversations] = useState(true);
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [mode, setMode] = useState<"fast" | "deep">(initialMode);
  const [renamingSessionId, setRenamingSessionId] = useState<string | null>(null);
  const [renameInput, setRenameInput] = useState("");
  const [isRenamingRequest, setIsRenamingRequest] = useState(false);
  const [isCreatingProject, setIsCreatingProject] = useState(false);
  const [projectInput, setProjectInput] = useState("");
  const [openConversationMenuId, setOpenConversationMenuId] = useState<string | null>(null);
  const [collapsedProjectIds, setCollapsedProjectIds] = useState<Record<string, boolean>>({});
  const [assetByFilename, setAssetByFilename] = useState<Record<string, Asset>>({});
  const [selectedSourceAsset, setSelectedSourceAsset] = useState<Asset | null>(null);
  const [copiedMessageId, setCopiedMessageId] = useState<string | null>(null);
  const [openSourcesByMessageId, setOpenSourcesByMessageId] = useState<Record<string, boolean>>({});
  const [indexSummary, setIndexSummary] = useState<RileyIndexSummary | null>(null);
  const [reportJobs, setReportJobs] = useState<ReportJob[]>([]);
  const [isReportsLoading, setIsReportsLoading] = useState(false);
  const [isReportModalOpen, setIsReportModalOpen] = useState(false);
  const [isSubmittingReport, setIsSubmittingReport] = useState(false);
  const [reportType, setReportType] = useState<"strategy_memo" | "audience_analysis" | "narrative_brief" | "opposition_framing_brief">("strategy_memo");
  const [reportTitle, setReportTitle] = useState("");
  const [reportPrompt, setReportPrompt] = useState("");
  const [reportDeepMode, setReportDeepMode] = useState(true);
  const knownReportStatusRef = useRef<Record<string, "queued" | "processing" | "complete" | "failed">>({});
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const renameInputRef = useRef<HTMLInputElement>(null);
  
  // Check if this is Global Riley (Imperial Amber theme)
  const isGlobal = tenantId === "global";
  const selectedConversationStorageKey =
    tenantId && user?.id ? `rileySelectedConversation:${tenantId}:${user.id}` : null;
  const collapsedProjectsStorageKey =
    tenantId && user?.id ? `rileyCollapsedProjects:${tenantId}:${user.id}` : null;

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  // Load persisted conversation list for this tenant/user scope.
  useEffect(() => {
    async function loadConversations() {
      if (!authLoaded || !userLoaded || !user || !tenantId) return;
      setIsLoadingConversations(true);
      try {
        const token = await getToken();
        if (!token) return;

        const data = await apiFetch<{ conversations: PersistedConversation[] }>(
          `/api/v1/riley/conversations?tenant_id=${encodeURIComponent(tenantId)}`,
          {
            token,
            method: "GET",
          }
        );

        const mapped: Conversation[] = (data.conversations || []).map((conv) => ({
          id: conv.id,
          title: conv.title || "New Conversation",
          projectId: conv.project_id || null,
          lastMessage: conv.last_message || "",
          timestamp: conv.last_message_at
            ? new Date(conv.last_message_at)
            : conv.created_at
            ? new Date(conv.created_at)
            : new Date(),
        }));

        setConversations(mapped);
        if (mapped.length > 0) {
          const savedConversationId = selectedConversationStorageKey
            ? localStorage.getItem(selectedConversationStorageKey)
            : null;
          const preferredId =
            savedConversationId && mapped.some((conv) => conv.id === savedConversationId)
              ? savedConversationId
              : mapped[0].id;
          setActiveConversationId((prev) =>
            prev && mapped.some((conv) => conv.id === prev) ? prev : preferredId
          );
        } else {
          setActiveConversationId(null);
          setMessages([]);
          if (selectedConversationStorageKey) {
            localStorage.removeItem(selectedConversationStorageKey);
          }
        }
      } catch (error) {
        console.error("Failed to load Riley conversations:", error);
      } finally {
        setIsLoadingConversations(false);
      }
    }

    loadConversations();
  }, [authLoaded, userLoaded, user, tenantId, getToken, selectedConversationStorageKey]);

  useEffect(() => {
    async function loadProjects() {
      if (!authLoaded || !userLoaded || !user || !tenantId) return;
      try {
        const token = await getToken();
        if (!token) return;

        const data = await apiFetch<{ projects: PersistedProject[] }>(
          `/api/v1/riley/projects?tenant_id=${encodeURIComponent(tenantId)}`,
          {
            token,
            method: "GET",
          }
        );

        const mapped: RileyProject[] = (data.projects || []).map((project) => ({
          id: project.id,
          name: project.name || "Untitled Project",
          createdAt: project.created_at ? new Date(project.created_at) : new Date(),
          updatedAt: project.updated_at ? new Date(project.updated_at) : new Date(),
        }));
        setProjects(mapped);
      } catch (error) {
        console.error("Failed to load Riley projects:", error);
        setProjects([]);
      }
    }

    loadProjects();
  }, [authLoaded, userLoaded, user, tenantId, getToken]);

  useEffect(() => {
    if (!selectedConversationStorageKey) return;
    if (!activeConversationId) {
      localStorage.removeItem(selectedConversationStorageKey);
      return;
    }
    localStorage.setItem(selectedConversationStorageKey, activeConversationId);
  }, [selectedConversationStorageKey, activeConversationId]);

  useEffect(() => {
    if (!collapsedProjectsStorageKey) return;
    try {
      const raw = localStorage.getItem(collapsedProjectsStorageKey);
      if (!raw) return;
      const parsed = JSON.parse(raw) as Record<string, boolean>;
      setCollapsedProjectIds(parsed || {});
    } catch {
      setCollapsedProjectIds({});
    }
  }, [collapsedProjectsStorageKey]);

  useEffect(() => {
    if (!collapsedProjectsStorageKey) return;
    localStorage.setItem(collapsedProjectsStorageKey, JSON.stringify(collapsedProjectIds));
  }, [collapsedProjectsStorageKey, collapsedProjectIds]);

  useEffect(() => {
    if (!openConversationMenuId) return;
    const handleWindowClick = () => setOpenConversationMenuId(null);
    window.addEventListener("click", handleWindowClick);
    return () => {
      window.removeEventListener("click", handleWindowClick);
    };
  }, [openConversationMenuId]);

  useEffect(() => {
    if (!tenantId || isGlobal || !authLoaded || !userLoaded || !user) return;
    let cancelled = false;
    let intervalId: number | null = null;

    const loadIndexSummary = async () => {
      try {
        const token = await getToken();
        if (!token) return;
        const summary = await apiFetch<RileyIndexSummary>(
          `/api/v1/riley/index-summary?tenant_id=${encodeURIComponent(tenantId)}`,
          {
            token,
            method: "GET",
          }
        );
        if (!cancelled) {
          setIndexSummary(summary);
        }
      } catch (error) {
        if (!cancelled) {
          console.error("Failed to load Riley index summary:", error);
        }
      }
    };

    void loadIndexSummary();
    intervalId = window.setInterval(() => {
      void loadIndexSummary();
    }, 15000);

    return () => {
      cancelled = true;
      if (intervalId) window.clearInterval(intervalId);
    };
  }, [tenantId, isGlobal, authLoaded, userLoaded, user, getToken]);

  // Build local filename -> asset map for source opening.
  useEffect(() => {
    if (!tenantId || !authLoaded || !userLoaded || !user) return;

    async function loadAssets() {
      try {
        const token = await getToken();
        if (!token) return;

        const data = await apiFetch<{ files: any[] }>(
          `/api/v1/list?tenant_id=${encodeURIComponent(tenantId)}`,
          {
            token,
            method: "GET",
          }
        );

        const next: Record<string, Asset> = {};
        for (const file of data.files || []) {
          const asset = toAsset(file, { status: "ready" });
          next[asset.name.toLowerCase()] = asset;
        }
        setAssetByFilename(next);
      } catch (error) {
        console.error("Failed to load source assets:", error);
        setAssetByFilename({});
      }
    }

    loadAssets();
  }, [tenantId, authLoaded, userLoaded, user, getToken]);

  const toReportJob = (job: PersistedReportJob): ReportJob => ({
    reportJobId: job.report_job_id,
    tenantId: job.tenant_id,
    userId: job.user_id,
    conversationId: job.conversation_id ?? null,
    reportType: job.report_type,
    title: job.title || "Untitled report",
    status: job.status,
    createdAt: job.created_at ? new Date(job.created_at) : new Date(),
    startedAt: job.started_at ? new Date(job.started_at) : undefined,
    completedAt: job.completed_at ? new Date(job.completed_at) : undefined,
    errorMessage: job.error_message ?? null,
    outputFileId: job.output_file_id ?? null,
    outputUrl: job.output_url ?? null,
    summaryText: job.summary_text ?? null,
    query: job.query || "",
    mode: job.mode || "deep",
  });

  const loadReportJobs = async (opts?: { silent?: boolean }) => {
    if (!tenantId || isGlobal || !authLoaded || !userLoaded || !user) return;
    if (!opts?.silent) setIsReportsLoading(true);
    try {
      const token = await getToken();
      if (!token) return;
      const data = await apiFetch<{ jobs: PersistedReportJob[] }>(
        `/api/v1/riley/reports?tenant_id=${encodeURIComponent(tenantId)}&limit=50`,
        {
          token,
          method: "GET",
        }
      );
      const mapped = (data.jobs || []).map(toReportJob);
      const previousStatuses = { ...knownReportStatusRef.current };
      const nextStatuses: Record<string, "queued" | "processing" | "complete" | "failed"> = {};
      mapped.forEach((job) => {
        nextStatuses[job.reportJobId] = job.status;
      });
      knownReportStatusRef.current = nextStatuses;
      setReportJobs(mapped);

      // Add concise chat notifications for relevant status transitions.
      for (const job of mapped) {
        const previous = previousStatuses[job.reportJobId];
        if (previous === undefined) continue;
        if (previous === job.status) continue;
        if (job.conversationId && activeConversationId && job.conversationId !== activeConversationId) continue;
        if (job.status === "complete") {
          setMessages((prev) => {
            const messageId = `report-complete-${job.reportJobId}`;
            if (prev.some((msg) => msg.id === messageId)) return prev;
            return [
              ...prev,
              {
                id: messageId,
                role: "assistant",
                content: `Report complete: **${job.title}**${job.summaryText ? `\n\n${job.summaryText}` : ""}`,
                reportDownloadUrl: job.outputUrl || undefined,
                reportTitle: job.title,
              },
            ];
          });
        } else if (job.status === "failed") {
          setMessages((prev) => {
            const messageId = `report-failed-${job.reportJobId}`;
            if (prev.some((msg) => msg.id === messageId)) return prev;
            return [
              ...prev,
              {
                id: messageId,
                role: "system",
                content: `Report failed: ${job.title}${job.errorMessage ? ` — ${job.errorMessage}` : ""}`,
              },
            ];
          });
        }
      }
    } catch (error) {
      console.error("Failed to load Riley report jobs:", error);
    } finally {
      if (!opts?.silent) setIsReportsLoading(false);
    }
  };

  useEffect(() => {
    if (isGlobal || !tenantId || !authLoaded || !userLoaded || !user) return;
    let cancelled = false;
    let intervalId: number | null = null;
    const poll = async () => {
      if (cancelled) return;
      await loadReportJobs({ silent: true });
    };
    void poll();
    intervalId = window.setInterval(() => {
      void poll();
    }, 8000);
    return () => {
      cancelled = true;
      if (intervalId) window.clearInterval(intervalId);
    };
  }, [isGlobal, tenantId, authLoaded, userLoaded, user, getToken, activeConversationId]);

  const extractCitations = (content: string): SourceCitation[] => {
    if (!content) return [];
    const regex = /\[\[Source:\s*(.+?)\]\]/g;
    const found: SourceCitation[] = [];
    const seen = new Set<string>();
    let match: RegExpExecArray | null;

    while ((match = regex.exec(content)) !== null) {
      const label = match[1]?.trim();
      if (!label) continue;
      const key = label.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      found.push({ key, label });
    }

    return found;
  };

  const handleSourceClick = (citation: SourceCitation) => {
    const asset = assetByFilename[citation.key];
    if (!asset) return;
    setSelectedSourceAsset(asset);
  };

  const handleSourceFilenameClick = (filename: string) => {
    const asset = assetByFilename[filename.toLowerCase()];
    if (!asset) return;
    setSelectedSourceAsset(asset);
  };

  const handleCopyAssistantMessage = async (messageId: string, content: string) => {
    try {
      await navigator.clipboard.writeText(content);
      setCopiedMessageId(messageId);
      window.setTimeout(() => {
        setCopiedMessageId((current) => (current === messageId ? null : current));
      }, 1600);
    } catch (error) {
      console.error("Failed to copy Riley response:", error);
    }
  };

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

    setIsLoading(true);
    // Reset to a clean per-conversation state before loading history.
    setMessages([]);
    let cancelled = false;
    const targetConversationId = activeConversationId;

    async function loadHistory() {
      try {
        const token = await getToken();
        if (!token) {
          setIsLoading(false);
          return;
        }
        
        const historyResponse = await apiFetch<{ messages: Array<{ role: string; content: string }> }>(
          `/api/v1/riley/conversations/${targetConversationId}/messages?tenant_id=${encodeURIComponent(tenantId)}`,
          {
            token,
            method: "GET",
          }
        );
        const history = historyResponse.messages || [];
        if (cancelled) return;

        if (history && Array.isArray(history) && history.length > 0) {
          const historyMessages: Message[] = history.map(
            (msg: { role: string; content: string }, idx: number) => ({
              id: `history-${targetConversationId}-${idx}`,
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
      } catch (error) {
        console.error("Failed to load chat history:", error);
        if (cancelled) return;
        setMessages([
          {
            id: "system-1",
            role: "system",
            content: `Hi, I'm Riley. I have access to ${contextName}. How can I help you today?`,
          },
        ]);
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    }

    loadHistory();
    return () => {
      cancelled = true;
    };
  }, [activeConversationId, contextName, getToken, tenantId]);

  // Check if send is enabled and why it might be disabled
  const canSend = input.trim().length > 0 && !isLoading && !missingAuth && !missingTenantId;
  const sendDisabledReason = missingAuth 
    ? "Not authenticated" 
    : missingTenantId 
    ? "Tenant ID missing" 
    : input.trim().length === 0 
    ? "Enter a message" 
    : isLoading 
    ? "Sending..." 
    : null;

  const handleNewConversation = async () => {
    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication token not available");
      }

      const created = await apiFetch<PersistedConversation>("/api/v1/riley/conversations", {
        token,
        method: "POST",
        body: {
          tenant_id: tenantId,
          title: "New Conversation",
        },
      });

      const newConversation: Conversation = {
        id: created.id,
        title: created.title || "New Conversation",
        projectId: null,
        lastMessage: "",
        timestamp: created.created_at ? new Date(created.created_at) : new Date(),
      };

      setConversations((prev) => [newConversation, ...prev.filter((c) => c.id !== newConversation.id)]);
      setActiveConversationId(newConversation.id);
      setMessages([
        {
          id: "system-1",
          role: "system",
          content: `Hi, I'm Riley. I have access to ${contextName}. How can I help you today?`,
        },
      ]);
      setInput("");
    } catch (error) {
      console.error("Failed to create Riley conversation:", error);
    }
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

    setIsRenamingRequest(true);
    try {
      const token = await getToken();
      if (!token) return;
      
      await apiFetch(`/api/v1/riley/conversations/${sessionId}?tenant_id=${encodeURIComponent(tenantId)}`, {
        token,
        method: "PATCH",
        body: { title: newTitle },
      });
      
      // Update the conversation in the list
      setConversations((prev) =>
        prev.map((conv) =>
          conv.id === sessionId ? { ...conv, title: newTitle } : conv
        )
      );
      handleRenameCancel();
    } catch (error) {
      console.error("Error renaming session:", error);
    } finally {
      setIsRenamingRequest(false);
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

  const handleCreateProject = async () => {
    const name = projectInput.trim();
    if (!name) return;

    try {
      const token = await getToken();
      if (!token) return;

      const created = await apiFetch<PersistedProject>("/api/v1/riley/projects", {
        token,
        method: "POST",
        body: {
          tenant_id: tenantId,
          name,
        },
      });

      const newProject: RileyProject = {
        id: created.id,
        name: created.name || name,
        createdAt: created.created_at ? new Date(created.created_at) : new Date(),
        updatedAt: created.updated_at ? new Date(created.updated_at) : new Date(),
      };
      setProjects((prev) => [newProject, ...prev.filter((project) => project.id !== newProject.id)]);
      setProjectInput("");
      setIsCreatingProject(false);
    } catch (error) {
      console.error("Failed to create Riley project:", error);
    }
  };

  const handleAssignProject = async (conversationId: string, projectId: string | null) => {
    try {
      const token = await getToken();
      if (!token) return;

      await apiFetch(`/api/v1/riley/conversations/${conversationId}/project`, {
        token,
        method: "PATCH",
        body: {
          tenant_id: tenantId,
          project_id: projectId,
        },
      });

      setConversations((prev) =>
        prev.map((conv) =>
          conv.id === conversationId
            ? { ...conv, projectId }
            : conv
        )
      );
    } catch (error) {
      console.error("Failed to assign Riley project:", error);
    }
  };

  const handleDeleteConversation = async (conversationId: string) => {
    try {
      const token = await getToken();
      if (!token) return;

      await apiFetch(`/api/v1/riley/conversations/${conversationId}?tenant_id=${encodeURIComponent(tenantId)}`, {
        token,
        method: "DELETE",
      });

      let nextConversationId: string | null = null;
      setConversations((prev) => {
        const remaining = prev.filter((conv) => conv.id !== conversationId);
        nextConversationId = remaining.length > 0 ? remaining[0].id : null;
        return remaining;
      });
      if (activeConversationId === conversationId) {
        setActiveConversationId(nextConversationId);
      }
      setOpenConversationMenuId(null);
    } catch (error) {
      console.error("Failed to delete Riley conversation:", error);
    }
  };

  const toggleProjectCollapsed = (projectId: string) => {
    setCollapsedProjectIds((prev) => ({
      ...prev,
      [projectId]: !prev[projectId],
    }));
  };

  const detectReportIntent = (
    text: string
  ): {
    isReportIntent: boolean;
    suggestedType: "strategy_memo" | "audience_analysis" | "narrative_brief" | "opposition_framing_brief";
  } => {
    const normalized = (text || "").toLowerCase();
    const suggestedType: "strategy_memo" | "audience_analysis" | "narrative_brief" | "opposition_framing_brief" =
      normalized.includes("audience")
        ? "audience_analysis"
        : normalized.includes("opposition")
        ? "opposition_framing_brief"
        : normalized.includes("narrative")
        ? "narrative_brief"
        : "strategy_memo";

    const hasReportKeyword = /\b(report|memo|brief)\b/.test(normalized);
    const hasLongFormKeyword = /\b(full|comprehensive|in-depth|long-form)\b/.test(normalized);
    const hasSynthesisKeyword = /\b(synthesi[sz]e|synthesis|analy[sz]e)\b/.test(normalized);
    const hasCorpusScope = /\b(all|across|these|entire|whole)\b/.test(normalized) && /\b(documents|files|sources|corpus)\b/.test(normalized);
    const docCountMatch = normalized.match(/\b(\d{1,3})\s+(documents|files|sources)\b/);
    const hasManyDocs = docCountMatch ? Number(docCountMatch[1]) >= 8 : false;
    const isReportIntent =
      hasReportKeyword ||
      (hasLongFormKeyword && (hasSynthesisKeyword || hasReportKeyword)) ||
      (hasSynthesisKeyword && hasCorpusScope) ||
      hasManyDocs;

    return { isReportIntent, suggestedType };
  };

  const executeSend = async (userInput: string, options?: { bypassReportIntent?: boolean }) => {
    if (!userInput.trim() || isLoading || missingAuth || missingTenantId) return;

    // Step 1: Ensure we have a session ID BEFORE any async operations
    // This prevents history loading from interfering with optimistic updates
    let sessionId = activeConversationId;
    if (!sessionId) {
      try {
        const token = await getToken();
        if (!token) {
          throw new Error("Authentication token not available");
        }
        const created = await apiFetch<PersistedConversation>("/api/v1/riley/conversations", {
          token,
          method: "POST",
          body: {
            tenant_id: tenantId,
            title: "New Conversation",
          },
        });
        sessionId = created.id;
        setActiveConversationId(sessionId);
      } catch (error) {
        console.error("Failed to initialize conversation:", error);
        return;
      }
    }

    // Step 2: Capture user input immediately
    const trimmedInput = userInput.trim();
    if (!options?.bypassReportIntent) {
      const intent = detectReportIntent(trimmedInput);
      if (intent.isReportIntent) {
        setInput("");
        setMessages((prev) => [
          ...prev,
          {
            id: `report-suggest-${Date.now()}`,
            role: "assistant",
            content:
              "This is best handled as a full report. I can generate a downloadable report for you, or continue in chat if you prefer.",
            reportSuggestionPrompt: trimmedInput,
            reportSuggestionType: intent.suggestedType,
          },
        ]);
        return;
      }
    }

    const userMessage: Message = {
      id: `user-${Date.now()}`,
      role: "user",
      content: trimmedInput,
    };

    // Step 3: Create thinking placeholder for assistant response (store ID for later replacement)
    const thinkingId = `thinking-${Date.now()}`;
    const thinkingMessage: Message = {
      id: thinkingId,
      role: "assistant",
      content: "",
      status: "thinking",
    };

    // Step 4: OPTIMISTIC UPDATE - Immediately append user message + thinking placeholder
    // This makes the message appear instantly in the UI BEFORE any await
    setMessages((prev) => {
      // If this is the first message (only system message exists), keep system + add user + thinking
      if (prev.length === 1 && prev[0].role === "system") {
        return [prev[0], userMessage, thinkingMessage];
      }
      // Otherwise, append to existing messages
      return [...prev, userMessage, thinkingMessage];
    });
    
    // Step 5: Clear input field immediately for better UX
    setInput("");

    // Step 6: Set loading state to show "Thinking" indicator
    setIsLoading(true);

    try {
      // Step 7: Get auth token
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication token not available");
      }

      // Step 8: Perform the API call
      const data = await apiFetch<{
        response: string;
        sources_count?: number;
        sources?: MessageSource[];
      }>("/api/v1/chat", {
        token,
        method: "POST",
        body: {
          query: trimmedInput,
          tenant_id: tenantId,
          mode: mode,
          session_id: sessionId,
          user_display_name: userDisplayName,
        },
      });

      // Step 9: Replace thinking placeholder with actual assistant response
      const assistantMessage: Message = {
        id: `assistant-${Date.now()}`,
        role: "assistant",
        content: data.response,
        sourcesCount: data.sources_count,
        sources: Array.isArray(data.sources) ? data.sources : [],
      };

      // Replace thinking message with actual response
      setMessages((prev) => {
        return prev.map((msg) =>
          msg.id === thinkingId ? assistantMessage : msg
        );
      });

      // Step 10: Upsert conversation list entry and move to top
      if (sessionId) {
        setConversations((prev) => {
          const existing = prev.find((c) => c.id === sessionId);
          const nextTitle = existing?.title && existing.title !== "New Conversation"
            ? existing.title
            : userMessage.content.slice(0, 30) + (userMessage.content.length > 30 ? "..." : "");
          const updated: Conversation = {
            id: sessionId,
            title: nextTitle,
            projectId: existing?.projectId || null,
            lastMessage: userMessage.content,
            timestamp: new Date(),
          };
          return [updated, ...prev.filter((c) => c.id !== sessionId)];
        });
      }
    } catch (error) {
      // Log error to console for debugging
      console.error("Riley chat error:", error);
      
      // On error, replace thinking placeholder with error message
      const errorText = error instanceof Error 
        ? error.message 
        : "Unknown error occurred";
      
      // apiFetch throws errors with format "HTTP <status>: <detail>" or "Network/CORS failure"
      // Display the exact error message to help with debugging
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
      // Step 11: Clear loading state (hides "Thinking" indicator)
      setIsLoading(false);
    }
  };

  const handleSend = async () => {
    if (!canSend) return;
    await executeSend(input.trim());
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleSubmitReportJob = async () => {
    const trimmedPrompt = reportPrompt.trim();
    if (!trimmedPrompt || isSubmittingReport || missingAuth || missingTenantId) return;
    setIsSubmittingReport(true);
    try {
      const token = await getToken();
      if (!token) throw new Error("Authentication token not available");
      const created = await apiFetch<PersistedReportJob>("/api/v1/riley/reports", {
        token,
        method: "POST",
        body: {
          tenant_id: tenantId,
          query: trimmedPrompt,
          conversation_id: activeConversationId,
          report_type: reportType,
          title: reportTitle.trim() || undefined,
          mode: reportDeepMode ? "deep" : "normal",
        },
      });

      const mapped = toReportJob(created);
      setReportJobs((prev) => [mapped, ...prev.filter((job) => job.reportJobId !== mapped.reportJobId)]);
      knownReportStatusRef.current[mapped.reportJobId] = mapped.status;
      setMessages((prev) => [
        ...prev,
        {
          id: `report-queued-${mapped.reportJobId}`,
          role: "system",
          content: `Riley is generating report "${mapped.title}". Track progress and download it from the Reports panel.`,
        },
      ]);
      setIsReportModalOpen(false);
      setReportTitle("");
      setReportPrompt("");
      setReportType("strategy_memo");
      setReportDeepMode(true);
      await loadReportJobs({ silent: true });
    } catch (error) {
      console.error("Failed to create Riley report job:", error);
      setMessages((prev) => [
        ...prev,
        {
          id: `report-create-error-${Date.now()}`,
          role: "system",
          content: "Could not start report generation. Please try again.",
        },
      ]);
    } finally {
      setIsSubmittingReport(false);
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

  const formatReportDate = (date: Date) =>
    date.toLocaleString(undefined, {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    });

  const reportStatusBadgeClass = (status: ReportJob["status"]) => {
    if (status === "queued") return "border-amber-500/30 bg-amber-500/10 text-amber-300";
    if (status === "processing") return "border-blue-500/30 bg-blue-500/10 text-blue-300";
    if (status === "complete") return "border-emerald-500/30 bg-emerald-500/10 text-emerald-300";
    return "border-rose-500/30 bg-rose-500/10 text-rose-300";
  };

  const promptStarters = [
    { text: "What are the key insights from recent research?", icon: Search, color: "text-cyan-400" },
    { text: "Help me refine the messaging strategy", icon: MessageSquare, color: "text-purple-400" },
    { text: "Analyze the current campaign performance", icon: BarChart3, color: "text-emerald-400" },
    { text: "Generate ideas for the next phase", icon: Zap, color: "text-orange-400" },
  ];

  const looseConversations = useMemo(
    () => conversations.filter((conv) => !conv.projectId),
    [conversations]
  );

  const isEmpty = messages.length === 0 || (messages.length === 1 && messages[0].role === "system");
  const hasThinkingPlaceholder = messages.some((msg) => msg.status === "thinking");
  const showWelcome =
    isEmpty &&
    !isLoading &&
    !isLoadingConversations &&
    conversations.length === 0 &&
    !activeConversationId;

  const renderConversationRow = (conv: Conversation, nested: boolean = false) => {
    const isActive = activeConversationId === conv.id;
    const isRowRenaming = renamingSessionId === conv.id;
    const isMenuOpen = openConversationMenuId === conv.id;
    const currentProject = projects.find((project) => project.id === conv.projectId);

    return (
      <div
        key={conv.id}
        className={cn(
          "w-full rounded-lg transition-colors relative group",
          nested && "ml-4",
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
            setOpenConversationMenuId(null);
          }}
          onKeyDown={(e) => {
            if (e.key === "Enter" || e.key === " ") {
              e.preventDefault();
              setActiveConversationId(conv.id);
            }
          }}
          className="w-full text-left p-3 cursor-pointer"
        >
          {isRowRenaming ? (
            <div className="flex items-center gap-2">
              <input
                ref={renameInputRef}
                type="text"
                value={renameInput}
                onChange={(e) => setRenameInput(e.target.value)}
                onKeyDown={(e) => handleRenameKeyDown(e, conv.id)}
                onClick={(e) => e.stopPropagation()}
                className="flex-1 bg-zinc-900/50 border border-zinc-700 rounded px-2 py-1 text-sm text-zinc-100 focus:outline-none focus:ring-1 focus:ring-amber-500/50"
                disabled={isRenamingRequest}
              />
              <button
                type="button"
                onClick={(e) => handleRenameSave(e, conv.id)}
                disabled={isRenamingRequest}
                className="p-1 rounded transition-colors text-amber-400 hover:bg-amber-500/10"
              >
                <Check className="h-4 w-4" />
              </button>
              <button
                type="button"
                onClick={(e) => handleRenameCancel(e)}
                disabled={isRenamingRequest}
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
                <div className="relative">
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      setOpenConversationMenuId((prev) => (prev === conv.id ? null : conv.id));
                    }}
                    className={cn(
                      "p-1.5 rounded transition-all hover:bg-zinc-700/70",
                      isMenuOpen || isActive ? "text-zinc-300" : "text-zinc-500 opacity-0 group-hover:opacity-100"
                    )}
                    title="Conversation actions"
                  >
                    <MoreHorizontal className="h-3.5 w-3.5" />
                  </button>
                  {isMenuOpen && (
                    <div
                      className="absolute right-0 top-8 z-20 min-w-[180px] rounded-lg border border-zinc-700 bg-zinc-900/95 p-1 shadow-xl"
                      onClick={(e) => e.stopPropagation()}
                    >
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          setOpenConversationMenuId(null);
                          setRenamingSessionId(conv.id);
                          setRenameInput(conv.title);
                          setTimeout(() => {
                            renameInputRef.current?.focus();
                            renameInputRef.current?.select();
                          }, 0);
                        }}
                        className="w-full rounded-md px-2 py-1.5 text-left text-xs text-zinc-200 hover:bg-zinc-800"
                      >
                        Rename
                      </button>
                      {projects.length > 0 && (
                        <>
                          <div className="my-1 border-t border-zinc-800" />
                          <div className="px-2 py-1 text-[10px] uppercase tracking-wide text-zinc-500">
                            {conv.projectId ? "Move to Project" : "Add to Project"}
                          </div>
                          {projects.map((project) => (
                            <button
                              key={project.id}
                              type="button"
                              onClick={(e) => {
                                e.stopPropagation();
                                setOpenConversationMenuId(null);
                                void handleAssignProject(conv.id, project.id);
                              }}
                              className={cn(
                                "w-full rounded-md px-2 py-1.5 text-left text-xs hover:bg-zinc-800",
                                conv.projectId === project.id ? "text-amber-300" : "text-zinc-200"
                              )}
                            >
                              {project.name}
                            </button>
                          ))}
                        </>
                      )}
                      {conv.projectId && (
                        <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            setOpenConversationMenuId(null);
                            void handleAssignProject(conv.id, null);
                          }}
                          className="mt-1 w-full rounded-md px-2 py-1.5 text-left text-xs text-zinc-200 hover:bg-zinc-800"
                        >
                          Remove from Project{currentProject ? ` (${currentProject.name})` : ""}
                        </button>
                      )}
                      <div className="my-1 border-t border-zinc-800" />
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          if (!window.confirm("Delete this conversation?")) return;
                          void handleDeleteConversation(conv.id);
                        }}
                        className="w-full rounded-md px-2 py-1.5 text-left text-xs text-rose-300 hover:bg-rose-500/10"
                      >
                        Delete
                      </button>
                    </div>
                  )}
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    );
  };

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
      {isSidebarOpen && (
        <aside className="w-64 bg-zinc-900/50 border-r border-zinc-800 flex flex-col shrink-0">
          {/* Sidebar Header */}
          <div className="p-4 border-b border-zinc-800 flex items-center justify-between">
            <div className="flex-1 mr-2 flex items-center gap-2">
              <div className="flex-1 grid grid-cols-2 gap-2">
                <button
                  type="button"
                  onClick={handleNewConversation}
                  className={cn(
                    "inline-flex items-center justify-center gap-1.5 h-10 rounded-lg border px-3 text-xs font-medium transition-colors",
                    "border-amber-500/20 bg-amber-500/10 text-amber-400 hover:bg-amber-500/20"
                  )}
                >
                  <Sparkles className="h-3.5 w-3.5" />
                  <span>New Conversation</span>
                </button>
                <button
                  type="button"
                  onClick={() => setIsCreatingProject((prev) => !prev)}
                  className={cn(
                    "inline-flex items-center justify-center gap-1.5 h-10 rounded-lg border px-3 text-xs font-medium transition-colors",
                    "border-zinc-700 bg-zinc-900/60 text-zinc-300 hover:bg-zinc-800/70"
                  )}
                >
                  <FolderPlus className="h-3.5 w-3.5" />
                  <span>New Project</span>
                </button>
              </div>
            </div>
            <button
              type="button"
              onClick={() => setIsSidebarOpen(false)}
              className="inline-flex h-10 w-10 items-center justify-center rounded-lg border border-zinc-700 bg-zinc-900/60 text-zinc-300 transition-colors hover:bg-zinc-800/80 hover:text-zinc-100"
              aria-label="Close conversations panel"
              title="Close conversations panel"
            >
              <ChevronLeft className="h-5 w-5" />
            </button>
          </div>

          {/* Conversation List */}
          <div className="flex-1 overflow-y-auto p-2 space-y-3">
            {isCreatingProject && (
              <div className="rounded-lg border border-zinc-700 bg-zinc-900/60 p-2">
                <input
                  type="text"
                  value={projectInput}
                  onChange={(e) => setProjectInput(e.target.value)}
                  placeholder="Project name"
                  className="w-full rounded-md border border-zinc-700 bg-zinc-900/70 px-2 py-1.5 text-sm text-zinc-100 focus:outline-none focus:ring-1 focus:ring-amber-500/50"
                />
                <div className="mt-2 flex items-center gap-2">
                  <button
                    type="button"
                    onClick={handleCreateProject}
                    className="rounded-md bg-amber-500/20 border border-amber-500/30 px-2 py-1 text-xs text-amber-300 hover:bg-amber-500/30 transition-colors"
                  >
                    Create
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setIsCreatingProject(false);
                      setProjectInput("");
                    }}
                    className="rounded-md border border-zinc-700 px-2 py-1 text-xs text-zinc-400 hover:bg-zinc-800/70 transition-colors"
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}

            <div>
              <div className="px-2 pb-1 text-[11px] uppercase tracking-wide text-zinc-500">Projects</div>
              <div className="space-y-2">
                {projects.length === 0 ? (
                  <div className="px-2 py-1 text-xs text-zinc-600">No projects yet.</div>
                ) : (
                  projects.map((project) => {
                    const projectConversations = conversations.filter((conv) => conv.projectId === project.id);
                    const isCollapsed = Boolean(collapsedProjectIds[project.id]);
                    return (
                      <div key={project.id} className="rounded-lg border border-zinc-800/80 bg-zinc-900/30 p-2">
                        <button
                          type="button"
                          onClick={() => toggleProjectCollapsed(project.id)}
                          className="flex w-full items-center gap-2 rounded-md px-1 py-1 text-left hover:bg-zinc-800/40"
                        >
                          {isCollapsed ? (
                            <ChevronRight className="h-3.5 w-3.5 text-zinc-500" />
                          ) : (
                            <ChevronDown className="h-3.5 w-3.5 text-zinc-500" />
                          )}
                          <Folder className="h-3.5 w-3.5 text-zinc-400" />
                          <div className="truncate text-xs font-medium text-zinc-300">{project.name}</div>
                        </button>
                        {!isCollapsed && (
                          <div className="space-y-1">
                            {projectConversations.length === 0 ? (
                              <div className="ml-4 px-2 py-1 text-xs text-zinc-600">No conversations.</div>
                            ) : (
                              projectConversations.map((conv) => renderConversationRow(conv, true))
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            </div>

            <div>
              <div className="px-2 pb-1 text-[11px] uppercase tracking-wide text-zinc-500">Conversations</div>
              <div className="space-y-1">
                {looseConversations.length === 0 ? (
                  <div className="px-2 py-1 text-xs text-zinc-600">No conversations.</div>
                ) : (
                  looseConversations.map((conv) => renderConversationRow(conv))
                )}
              </div>
            </div>
          </div>
        </aside>
      )}

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
            {!isSidebarOpen && (
              <button
                type="button"
                onClick={() => setIsSidebarOpen(true)}
                className="ml-3 inline-flex items-center gap-1.5 rounded-md border border-zinc-700 bg-zinc-900/50 px-2.5 py-1 text-xs text-zinc-300 hover:bg-zinc-800 transition-colors"
                aria-label="Open conversations panel"
              >
                <Users className="h-3.5 w-3.5" />
                <span>Conversations</span>
              </button>
            )}
          </div>
          <div className="flex items-center gap-2">
            {!isGlobal && (
              <button
                type="button"
                onClick={() => setIsReportModalOpen(true)}
                className="flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm bg-zinc-800/60 border border-zinc-700/70 text-zinc-200 hover:bg-zinc-800"
                title="Generate long-form report"
              >
                <ClipboardList className="h-4 w-4" />
                <span>Generate Report</span>
              </button>
            )}
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

        {!isGlobal && indexSummary && (
          <div className="border-b border-zinc-800 bg-zinc-900/40 px-6 py-3">
            <div className="flex flex-wrap items-center gap-3 text-xs">
              <span className="rounded-md border border-zinc-700 bg-zinc-800/40 px-2 py-1 text-zinc-200">
                Documents indexed: {indexSummary.indexed_count} / {indexSummary.total_documents}
              </span>
              <span className="rounded-md border border-blue-500/30 bg-blue-500/10 px-2 py-1 text-blue-300">
                Processing: {indexSummary.processing_count}
              </span>
              <span className="rounded-md border border-rose-500/30 bg-rose-500/10 px-2 py-1 text-rose-300">
                Failed: {indexSummary.failed_count + indexSummary.low_text_count}
              </span>
              <span className="rounded-md border border-purple-500/30 bg-purple-500/10 px-2 py-1 text-purple-300">
                OCR needed: {indexSummary.ocr_needed_count}
              </span>
              <a
                href={`/campaign/${tenantId}/assets`}
                className="rounded-md border border-amber-500/30 bg-amber-500/10 px-2 py-1 text-amber-300 hover:bg-amber-500/20"
              >
                View documents
              </a>
            </div>
            <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-zinc-400">
              {Object.entries(indexSummary.counts_by_file_type || {})
                .sort((a, b) => b[1] - a[1])
                .slice(0, 8)
                .map(([fileType, count]) => (
                  <span
                    key={fileType}
                    className="rounded-full border border-zinc-700 bg-zinc-800/40 px-2 py-0.5"
                  >
                    {fileType}: {count}
                  </span>
                ))}
            </div>
            {(indexSummary.failed_count > 0 ||
              indexSummary.low_text_count > 0 ||
              indexSummary.ocr_needed_count > 0) && (
              <div className="mt-2 text-[11px] text-amber-300">
                Some documents are not fully indexable yet, so Riley results may be incomplete.
              </div>
            )}
          </div>
        )}

        {!isGlobal && (
          <div className="border-b border-zinc-800 bg-zinc-900/30 px-6 py-3">
            <div className="flex items-center justify-between gap-3">
              <div className="flex items-center gap-2 text-sm text-zinc-200">
                <ClipboardList className="h-4 w-4 text-amber-400" />
                <span>Report Jobs</span>
              </div>
              <button
                type="button"
                onClick={() => void loadReportJobs()}
                className="rounded-md border border-zinc-700 bg-zinc-800/40 px-2 py-1 text-xs text-zinc-300 hover:bg-zinc-800"
              >
                Refresh
              </button>
            </div>
            <div className="mt-2 space-y-2">
              {isReportsLoading && reportJobs.length === 0 ? (
                <div className="text-xs text-zinc-500">Loading report jobs...</div>
              ) : reportJobs.length === 0 ? (
                <div className="text-xs text-zinc-500">No report jobs yet. Use Generate Report to start one.</div>
              ) : (
                reportJobs.slice(0, 6).map((job) => (
                  <div key={job.reportJobId} className="rounded-lg border border-zinc-800 bg-zinc-900/50 p-2">
                    <div className="flex items-start justify-between gap-2">
                      <div className="min-w-0">
                        <div className="truncate text-sm text-zinc-100">{job.title}</div>
                        <div className="mt-1 text-[11px] text-zinc-500">
                          {formatReportDate(job.createdAt)}
                        </div>
                      </div>
                      <span className={cn("rounded-full border px-2 py-0.5 text-[11px]", reportStatusBadgeClass(job.status))}>
                        {job.status}
                      </span>
                    </div>
                    {job.summaryText && (
                      <div className="mt-1 text-xs text-zinc-400 line-clamp-2">{job.summaryText}</div>
                    )}
                    {job.status === "failed" && job.errorMessage && (
                      <div className="mt-1 inline-flex items-center gap-1 text-xs text-rose-300">
                        <AlertTriangle className="h-3 w-3" />
                        <span className="line-clamp-1">{job.errorMessage}</span>
                      </div>
                    )}
                    {job.status === "complete" && job.outputUrl && (
                      <div className="mt-2">
                        <a
                          href={job.outputUrl}
                          target="_blank"
                          rel="noreferrer"
                          className="inline-flex items-center gap-1.5 rounded-md border border-emerald-500/30 bg-emerald-500/10 px-2 py-1 text-xs text-emerald-300 hover:bg-emerald-500/20"
                        >
                          <Download className="h-3.5 w-3.5" />
                          <span>Open DOCX</span>
                        </a>
                      </div>
                    )}
                  </div>
                ))
              )}
            </div>
          </div>
        )}

        {/* Message Area */}
        <div className="flex-1 overflow-y-auto">
          {showWelcome ? (
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
                const citations = message.role === "assistant" ? extractCitations(message.content) : [];
                const structuredSources = message.sources || [];
                const fallbackSources: MessageSource[] = citations.map((citation) => ({
                  id: citation.key,
                  filename: citation.label,
                  location: "unknown",
                }));
                const displaySources = structuredSources.length > 0 ? structuredSources : fallbackSources;
                const isSourcesOpen = openSourcesByMessageId[message.id] ?? true;

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
                      {message.status === "thinking" ? (
                        // Thinking placeholder
                        <div className="flex items-center gap-2 text-sm text-zinc-400">
                          <Loader2 className="h-4 w-4 animate-spin" />
                          <span>Riley is thinking...</span>
                        </div>
                      ) : isLastAssistant ? (
                        // Typewriter effect for last assistant message
                        <TypewriterMarkdown content={message.content} />
                      ) : message.role === "assistant" ? (
                        // Static markdown for previous assistant messages
                        <div className="riley-md">
                          <ReactMarkdown remarkPlugins={[remarkBreaks]}>
                            {message.content.replace(/<br\s*\/?>/gi, '\n\n')}
                          </ReactMarkdown>
                        </div>
                      ) : (
                        // User and system messages (plain text)
                        <div className="text-sm whitespace-pre-wrap">{message.content}</div>
                      )}
                      {message.reportDownloadUrl && (
                        <div className="mt-2 flex justify-end">
                          <a
                            href={message.reportDownloadUrl}
                            target="_blank"
                            rel="noreferrer"
                            className="inline-flex items-center gap-1.5 rounded-md border border-emerald-500/30 bg-emerald-500/10 px-2 py-1 text-[11px] text-emerald-300 hover:bg-emerald-500/20"
                            title={message.reportTitle ? `Open ${message.reportTitle}` : "Open report"}
                          >
                            <Download className="h-3.5 w-3.5" />
                            <span>Open Report</span>
                          </a>
                        </div>
                      )}
                      {message.reportSuggestionPrompt && (
                        <div className="mt-2 flex flex-wrap justify-end gap-2">
                          <button
                            type="button"
                            onClick={() => {
                              setReportPrompt(message.reportSuggestionPrompt || "");
                              setReportType(message.reportSuggestionType || "strategy_memo");
                              setReportDeepMode(true);
                              setIsReportModalOpen(true);
                            }}
                            className="inline-flex items-center gap-1.5 rounded-md border border-amber-500/30 bg-amber-500/10 px-2 py-1 text-[11px] text-amber-300 hover:bg-amber-500/20"
                          >
                            <ClipboardList className="h-3.5 w-3.5" />
                            <span>Generate Report</span>
                          </button>
                          <button
                            type="button"
                            onClick={() => void executeSend(message.reportSuggestionPrompt || "", { bypassReportIntent: true })}
                            className="inline-flex items-center gap-1.5 rounded-md border border-zinc-700 bg-zinc-800/40 px-2 py-1 text-[11px] text-zinc-300 hover:bg-zinc-800"
                          >
                            <MessageSquare className="h-3.5 w-3.5" />
                            <span>Continue in chat</span>
                          </button>
                        </div>
                      )}
                      {message.role === "assistant" && message.content && (
                        <div className="mt-2 flex justify-end">
                          <button
                            type="button"
                            onClick={() => handleCopyAssistantMessage(message.id, message.content)}
                            className="inline-flex items-center gap-1.5 rounded-md border border-zinc-700 bg-zinc-800/40 px-2 py-1 text-[11px] text-zinc-300 hover:bg-zinc-800"
                            title="Copy response markdown"
                          >
                            {copiedMessageId === message.id ? (
                              <>
                                <CheckCheck className="h-3.5 w-3.5 text-emerald-400" />
                                <span>Copied</span>
                              </>
                            ) : (
                              <>
                                <Copy className="h-3.5 w-3.5" />
                                <span>Copy</span>
                              </>
                            )}
                          </button>
                        </div>
                      )}
                      {message.role === "assistant" && message.sourcesCount !== undefined && (
                        <div className="mt-2 flex items-center gap-1.5 border-t border-zinc-800 pt-2 text-xs text-zinc-500">
                          <span>📚</span>
                          <span>Analyzed {message.sourcesCount} document{message.sourcesCount !== 1 ? "s" : ""}</span>
                        </div>
                      )}
                      {message.role === "assistant" && displaySources.length > 0 && (
                        <div className="mt-2 border-t border-zinc-800 pt-2">
                          <button
                            type="button"
                            onClick={() =>
                              setOpenSourcesByMessageId((prev) => ({
                                ...prev,
                                [message.id]: !isSourcesOpen,
                              }))
                            }
                            className="mb-1 inline-flex items-center gap-1.5 text-xs text-zinc-400 hover:text-zinc-200"
                          >
                            {isSourcesOpen ? (
                              <ChevronDown className="h-3.5 w-3.5" />
                            ) : (
                              <ChevronRight className="h-3.5 w-3.5" />
                            )}
                            <span>Sources ({displaySources.length})</span>
                          </button>
                          {isSourcesOpen && (
                            <div className="flex flex-col gap-1.5">
                            {displaySources.map((source) => {
                              const lookupKey = source.filename.toLowerCase();
                              const hasAsset = Boolean(assetByFilename[lookupKey]);
                              return (
                                <button
                                  key={`${message.id}-${source.id}`}
                                  type="button"
                                  onClick={() => handleSourceFilenameClick(source.filename)}
                                  disabled={!hasAsset}
                                  className={cn(
                                    "inline-flex items-center justify-between gap-2 rounded-md border px-2 py-1 text-[11px] transition-colors",
                                    hasAsset
                                      ? "border-amber-500/30 bg-amber-500/10 text-amber-300 hover:bg-amber-500/20"
                                      : "border-zinc-700 bg-zinc-800/40 text-zinc-500 cursor-not-allowed"
                                  )}
                                  title={hasAsset ? `Open ${source.filename}` : "Source unavailable"}
                                >
                                  <span className="inline-flex items-center gap-1.5 min-w-0">
                                    <FileText className="h-3 w-3" />
                                    <span className="max-w-[220px] truncate">{source.filename}</span>
                                  </span>
                                  <span className="text-[10px] text-zinc-400">{source.location || "unknown"}</span>
                                </button>
                              );
                            })}
                            </div>
                          )}
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

              {/* Thinking Indicator for non-optimistic loading only */}
              {isLoading && !hasThinkingPlaceholder && (
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
            {sendDisabledReason && !isLoading && (
              <div className="mb-2 text-xs text-zinc-500 text-center">
                {sendDisabledReason}
              </div>
            )}
            <div className="flex items-end gap-3">
              <textarea
                ref={inputRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Message Riley..."
                disabled={isLoading || missingAuth || missingTenantId}
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
                title={sendDisabledReason || "Send message"}
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
      {isReportModalOpen && !isGlobal && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-black/60 p-4">
          <div className="w-full max-w-2xl rounded-xl border border-zinc-700 bg-zinc-900 shadow-2xl">
            <div className="flex items-center justify-between border-b border-zinc-800 px-4 py-3">
              <h3 className="text-sm font-semibold text-zinc-100">Generate Riley Report</h3>
              <button
                type="button"
                onClick={() => setIsReportModalOpen(false)}
                className="rounded-md border border-zinc-700 bg-zinc-800/50 px-2 py-1 text-xs text-zinc-300 hover:bg-zinc-800"
              >
                Close
              </button>
            </div>
            <div className="space-y-4 px-4 py-4">
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                <label className="flex flex-col gap-1 text-xs text-zinc-400">
                  <span>Report Type</span>
                  <select
                    value={reportType}
                    onChange={(e) =>
                      setReportType(
                        e.target.value as
                          | "strategy_memo"
                          | "audience_analysis"
                          | "narrative_brief"
                          | "opposition_framing_brief"
                      )
                    }
                    className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 focus:outline-none focus:ring-1 focus:ring-amber-500/50"
                  >
                    <option value="strategy_memo">Strategy memo</option>
                    <option value="audience_analysis">Audience analysis</option>
                    <option value="narrative_brief">Narrative brief</option>
                    <option value="opposition_framing_brief">Opposition framing brief</option>
                  </select>
                </label>
                <label className="flex flex-col gap-1 text-xs text-zinc-400">
                  <span>Optional Title</span>
                  <input
                    type="text"
                    value={reportTitle}
                    onChange={(e) => setReportTitle(e.target.value)}
                    placeholder="Q3 persuasion strategy memo"
                    className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-1 focus:ring-amber-500/50"
                  />
                </label>
              </div>
              <label className="flex flex-col gap-1 text-xs text-zinc-400">
                <span>Instructions / Prompt</span>
                <textarea
                  value={reportPrompt}
                  onChange={(e) => setReportPrompt(e.target.value)}
                  rows={7}
                  placeholder="Analyze these campaign documents and write a strategy memo focused on message discipline, persuasion risks, and opportunities by audience."
                  className="resize-y rounded-md border border-zinc-700 bg-zinc-900 px-3 py-2 text-sm text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-1 focus:ring-amber-500/50"
                />
              </label>
              <label className="inline-flex items-center gap-2 text-sm text-zinc-300">
                <input
                  type="checkbox"
                  checked={reportDeepMode}
                  onChange={(e) => setReportDeepMode(e.target.checked)}
                  className="h-4 w-4 rounded border-zinc-600 bg-zinc-900 text-amber-500 focus:ring-amber-500/50"
                />
                <span>Deep mode (recommended)</span>
              </label>
            </div>
            <div className="flex items-center justify-end gap-2 border-t border-zinc-800 px-4 py-3">
              <button
                type="button"
                onClick={() => setIsReportModalOpen(false)}
                className="rounded-md border border-zinc-700 bg-zinc-900 px-3 py-1.5 text-xs text-zinc-300 hover:bg-zinc-800"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={() => void handleSubmitReportJob()}
                disabled={isSubmittingReport || reportPrompt.trim().length === 0}
                className={cn(
                  "inline-flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs transition-colors",
                  isSubmittingReport || reportPrompt.trim().length === 0
                    ? "cursor-not-allowed border-zinc-700 bg-zinc-800 text-zinc-500"
                    : "border-amber-500/30 bg-amber-500/10 text-amber-300 hover:bg-amber-500/20"
                )}
              >
                {isSubmittingReport ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <ClipboardList className="h-3.5 w-3.5" />}
                <span>{isSubmittingReport ? "Starting..." : "Generate Report"}</span>
              </button>
            </div>
          </div>
        </div>
      )}
      {selectedSourceAsset && (
        <DocumentViewer
          file={selectedSourceAsset}
          variant="modal"
          onClose={() => setSelectedSourceAsset(null)}
        />
      )}
    </div>
  );
}
