"use client";

import { useState, useRef, useEffect, useMemo } from "react";
import { useAuth, useUser } from "@clerk/nextjs";
import { Send, Loader2, Sparkles, Zap, Brain, FileText, Check, X, Search, MessageSquare, BarChart3, ChevronLeft, FolderPlus, Folder, MoreHorizontal, ChevronDown, ChevronRight, Copy, CheckCheck, Download, ClipboardList, AlertTriangle, Pencil, RotateCcw, Trash2, Square } from "lucide-react";
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
  reportSuggestionType?:
    | "summary"
    | "strategy_memo"
    | "audience_analysis"
    | "narrative_brief"
    | "opposition_framing_brief";
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

type ActiveGeneration = {
  requestId: number;
  prompt: string;
  userMessageId: string;
  thinkingId: string;
};

function generateClientMessageId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `msg-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

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

type PersistedReportJob = {
  report_job_id: string;
  tenant_id: string;
  user_id: string;
  conversation_id?: string | null;
  report_type: string;
  title: string;
  status: "queued" | "processing" | "cancelling" | "cancelled" | "complete" | "failed" | "deleted";
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
  status: "queued" | "processing" | "cancelling" | "cancelled" | "complete" | "failed" | "deleted";
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
  const [editingMessageId, setEditingMessageId] = useState<string | null>(null);
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
  const [isReportModalOpen, setIsReportModalOpen] = useState(false);
  const [isSubmittingReport, setIsSubmittingReport] = useState(false);
  const [hasInFlightReportJobs, setHasInFlightReportJobs] = useState(false);
  const [reportType, setReportType] = useState<
    "summary" | "strategy_memo" | "audience_analysis" | "narrative_brief" | "opposition_framing_brief"
  >("summary");
  const [reportTitle, setReportTitle] = useState("");
  const [reportPrompt, setReportPrompt] = useState("");
  const [reportDeepMode, setReportDeepMode] = useState(true);
  const [animatingAssistantMessageId, setAnimatingAssistantMessageId] = useState<string | null>(null);
  const knownReportStatusRef = useRef<
    Record<string, "queued" | "processing" | "cancelling" | "cancelled" | "complete" | "failed" | "deleted">
  >({});
  const instantScrollRef = useRef(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const renameInputRef = useRef<HTMLInputElement>(null);
  const skipNextHistoryLoadConversationIdRef = useRef<string | null>(null);
  const activeConversationIdRef = useRef<string | null>(null);
  const activeGenerationRef = useRef<ActiveGeneration | null>(null);
  const activeAbortControllerRef = useRef<AbortController | null>(null);
  const generationRequestSeqRef = useRef(0);
  
  // Check if this is Global Riley (Imperial Amber theme)
  const isGlobal = tenantId === "global";
  const selectedConversationStorageKey =
    tenantId && user?.id ? `rileySelectedConversation:${tenantId}:${user.id}` : null;
  const collapsedProjectsStorageKey =
    tenantId && user?.id ? `rileyCollapsedProjects:${tenantId}:${user.id}` : null;
  const sidebarOpenStorageKey =
    tenantId && user?.id ? `rileySidebarOpen:${tenantId}:${user.id}` : null;

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: instantScrollRef.current ? "auto" : "smooth" });
  }, [messages]);

  useEffect(() => {
    activeConversationIdRef.current = activeConversationId;
  }, [activeConversationId]);

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
    if (!sidebarOpenStorageKey) return;
    try {
      const raw = localStorage.getItem(sidebarOpenStorageKey);
      if (raw === "true") setIsSidebarOpen(true);
      if (raw === "false") setIsSidebarOpen(false);
    } catch {
      setIsSidebarOpen(true);
    }
  }, [sidebarOpenStorageKey]);

  useEffect(() => {
    if (!sidebarOpenStorageKey) return;
    localStorage.setItem(sidebarOpenStorageKey, isSidebarOpen ? "true" : "false");
  }, [sidebarOpenStorageKey, isSidebarOpen]);

  useEffect(() => {
    if (!openConversationMenuId) return;
    const handleWindowClick = () => setOpenConversationMenuId(null);
    window.addEventListener("click", handleWindowClick);
    return () => {
      window.removeEventListener("click", handleWindowClick);
    };
  }, [openConversationMenuId]);

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
      const visibleMapped = (data.jobs || []).map(toReportJob);
      const previousStatuses = { ...knownReportStatusRef.current };
      const nextStatuses: Record<
        string,
        "queued" | "processing" | "cancelling" | "cancelled" | "complete" | "failed" | "deleted"
      > = {};
      let nextHasInFlightReportJobs = false;
      visibleMapped.forEach((job) => {
        nextStatuses[job.reportJobId] = job.status;
        if (job.status === "queued" || job.status === "processing" || job.status === "cancelling") {
          nextHasInFlightReportJobs = true;
        }
      });
      knownReportStatusRef.current = nextStatuses;
      setHasInFlightReportJobs(nextHasInFlightReportJobs);
      // Add concise chat notifications for relevant status transitions.
      for (const job of visibleMapped) {
        const previous = previousStatuses[job.reportJobId];
        if (previous === undefined) continue;
        if (previous === job.status) continue;
        if (
          job.conversationId &&
          activeConversationIdRef.current &&
          job.conversationId !== activeConversationIdRef.current
        ) {
          continue;
        }
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
    }
  };

  useEffect(() => {
    if (isGlobal || !tenantId || !authLoaded || !userLoaded || !user) return;
    let cancelled = false;
    let intervalId: number | null = null;

    const stopPolling = () => {
      if (intervalId !== null) {
        window.clearInterval(intervalId);
        intervalId = null;
      }
    };

    const pollOnce = async () => {
      if (cancelled) return;
      await loadReportJobs({ silent: true });
    };

    const startPollingIfNeeded = () => {
      if (!hasInFlightReportJobs) {
        stopPolling();
        return;
      }
      if (document.visibilityState !== "visible") {
        stopPolling();
        return;
      }
      if (intervalId !== null) return;
      // Removed unconditional 8s polling: only poll while report jobs are actively in-flight.
      intervalId = window.setInterval(() => {
        void pollOnce();
      }, 8000);
    };

    if (document.visibilityState === "visible") {
      void pollOnce();
    }
    startPollingIfNeeded();

    const handleVisibilityChange = () => {
      if (document.visibilityState !== "visible") {
        stopPolling();
        return;
      }
      if (!hasInFlightReportJobs) return;
      void pollOnce();
      startPollingIfNeeded();
    };
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      cancelled = true;
      stopPolling();
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [isGlobal, tenantId, authLoaded, userLoaded, user, getToken, hasInFlightReportJobs]);

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
      setAnimatingAssistantMessageId(null);
      setMessages([]);
      return;
    }

    if (skipNextHistoryLoadConversationIdRef.current === activeConversationId) {
      skipNextHistoryLoadConversationIdRef.current = null;
      setIsLoading(false);
      return;
    }

    setIsLoading(true);
    // Reset to a clean per-conversation state before loading history.
    instantScrollRef.current = true;
    setAnimatingAssistantMessageId(null);
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
          setMessages(historyMessages);
          window.setTimeout(() => {
            instantScrollRef.current = false;
          }, 0);
        } else {
          setMessages([]);
          window.setTimeout(() => {
            instantScrollRef.current = false;
          }, 0);
        }
      } catch (error) {
        console.error("Failed to load chat history:", error);
        if (cancelled) return;
        setMessages([]);
        window.setTimeout(() => {
          instantScrollRef.current = false;
        }, 0);
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
  }, [activeConversationId, getToken, tenantId]);

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
      skipNextHistoryLoadConversationIdRef.current = newConversation.id;
      setActiveConversationId(newConversation.id);
      setMessages([]);
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
    // Prevent row-level keyboard handlers from consuming key events while typing.
    e.stopPropagation();
    if (e.key === "Enter") {
      e.preventDefault();
      handleRenameSave(e, sessionId);
    } else if (e.key === "Escape") {
      e.preventDefault();
      handleRenameCancel();
    }
  };

  const handleProjectInputKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    // Keep sidebar/global shortcuts from hijacking text input keys (including Space).
    e.stopPropagation();
    if (e.key === "Enter") {
      e.preventDefault();
      void handleCreateProject();
    } else if (e.key === "Escape") {
      e.preventDefault();
      setIsCreatingProject(false);
      setProjectInput("");
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

  const handleDeleteProject = async (projectId: string) => {
    if (!window.confirm("Delete this project folder? Conversations will remain and be moved out of the folder.")) return;
    try {
      const token = await getToken();
      if (!token) return;
      await apiFetch(`/api/v1/riley/projects/${projectId}?tenant_id=${encodeURIComponent(tenantId)}`, {
        token,
        method: "DELETE",
      });
      setProjects((prev) => prev.filter((project) => project.id !== projectId));
      setConversations((prev) =>
        prev.map((conv) => (conv.projectId === projectId ? { ...conv, projectId: null } : conv))
      );
      setCollapsedProjectIds((prev) => {
        const next = { ...prev };
        delete next[projectId];
        return next;
      });
    } catch (error) {
      console.error("Failed to delete Riley project:", error);
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
    suggestedType: "summary" | "strategy_memo" | "audience_analysis" | "narrative_brief" | "opposition_framing_brief";
  } => {
    const normalized = (text || "").toLowerCase();
    const suggestedType: "summary" | "strategy_memo" | "audience_analysis" | "narrative_brief" | "opposition_framing_brief" =
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
    setAnimatingAssistantMessageId(null);

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
        skipNextHistoryLoadConversationIdRef.current = sessionId;
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

    const clientMessageId = generateClientMessageId();
    const userMessage: Message = {
      id: `user-${clientMessageId}`,
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
      return [...prev, userMessage, thinkingMessage];
    });
    
    // Step 5: Clear input field immediately for better UX
    setInput("");

    // Step 6: Set loading state to show "Thinking" indicator
    setIsLoading(true);
    const requestId = generationRequestSeqRef.current + 1;
    generationRequestSeqRef.current = requestId;
    const abortController = new AbortController();
    activeGenerationRef.current = {
      requestId,
      prompt: trimmedInput,
      userMessageId: userMessage.id,
      thinkingId,
    };
    activeAbortControllerRef.current = abortController;

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
          client_message_id: clientMessageId,
        },
        signal: abortController.signal,
      });
      if (activeGenerationRef.current?.requestId !== requestId) {
        return;
      }

      // Step 9: Replace thinking placeholder with actual assistant response
      const assistantMessage: Message = {
        id: `assistant-${Date.now()}`,
        role: "assistant",
        content: data.response,
        sourcesCount: data.sources_count,
        sources: Array.isArray(data.sources) ? data.sources : [],
      };
      setAnimatingAssistantMessageId(assistantMessage.id);

      // Replace thinking message with actual response
      setMessages((prev) => {
        const hasThinking = prev.some((msg) => msg.id === thinkingId);
        if (!hasThinking) {
          const hasUserMessage = prev.some(
            (msg) => msg.id === userMessage.id
          );
          return hasUserMessage
            ? [...prev, assistantMessage]
            : [...prev, userMessage, assistantMessage];
        }
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
      const isAbortError =
        error instanceof DOMException
          ? error.name === "AbortError"
          : error instanceof Error && error.name === "AbortError";
      if (isAbortError || activeGenerationRef.current?.requestId !== requestId) {
        return;
      }
      
      // On error, replace thinking placeholder with error message
      const errorText = error instanceof Error 
        ? error.message 
        : "Unknown error occurred";
      const normalizedErrorText = errorText.toLowerCase();
      const isUsageLimitError =
        normalizedErrorText.includes("usage limits") ||
        normalizedErrorText.includes("temporarily unavailable due to usage limits") ||
        normalizedErrorText.includes("reached your limit for deep/reports today");
      
      // apiFetch throws errors with format "HTTP <status>: <detail>" or "Network/CORS failure"
      // Display the exact error message to help with debugging
      const errorMessage: Message = {
        id: `error-${Date.now()}`,
        role: "assistant",
        content: isUsageLimitError
          ? "You’ve reached your limit for Deep/Reports today."
          : `Error: ${errorText}`,
      };
      // Replace thinking message with error message
      setMessages((prev) => {
        const hasThinking = prev.some((msg) => msg.id === thinkingId);
        if (!hasThinking) {
          const hasUserMessage = prev.some(
            (msg) => msg.id === userMessage.id
          );
          return hasUserMessage
            ? [...prev, errorMessage]
            : [...prev, userMessage, errorMessage];
        }
        return prev.map((msg) =>
          msg.id === thinkingId ? errorMessage : msg
        );
      });
    } finally {
      if (activeGenerationRef.current?.requestId === requestId) {
        activeGenerationRef.current = null;
        activeAbortControllerRef.current = null;
        // Step 11: Clear loading state (hides "Thinking" indicator)
        setIsLoading(false);
      }
    }
  };

  const handleStopGeneration = () => {
    const activeGeneration = activeGenerationRef.current;
    if (!activeGeneration) return;
    activeAbortControllerRef.current?.abort();
    activeAbortControllerRef.current = null;
    activeGenerationRef.current = null;
    setAnimatingAssistantMessageId(null);
    setMessages((prev) =>
      prev.filter(
        (msg) =>
          msg.id !== activeGeneration.thinkingId &&
          msg.id !== activeGeneration.userMessageId
      )
    );
    setEditingMessageId(null);
    setInput(activeGeneration.prompt);
    setIsLoading(false);
    inputRef.current?.focus();
  };

  const handleSend = async () => {
    if (!canSend) return;
    const prompt = input.trim();
    if (!prompt) return;
    if (editingMessageId) {
      const targetId = editingMessageId;
      setMessages((prev) => {
        const deleteIndices = getDeleteIndicesForMessage(targetId, prev);
        if (deleteIndices.length === 0) return prev;
        const next = [...prev];
        for (const index of deleteIndices) {
          if (index >= 0 && index < next.length) next.splice(index, 1);
        }
        return next;
      });
      setEditingMessageId(null);
    }
    await executeSend(prompt);
  };

  const getPromptForMessage = (messageId: string, snapshot: Message[]): string | null => {
    const idx = snapshot.findIndex((msg) => msg.id === messageId);
    if (idx < 0) return null;
    const target = snapshot[idx];
    if (target.role === "user") {
      const prompt = target.content.trim();
      return prompt || null;
    }
    for (let i = idx - 1; i >= 0; i -= 1) {
      if (snapshot[i].role === "user") {
        const prompt = snapshot[i].content.trim();
        return prompt || null;
      }
    }
    return null;
  };

  const getDeleteIndicesForMessage = (messageId: string, snapshot: Message[]): number[] => {
    const idx = snapshot.findIndex((msg) => msg.id === messageId);
    if (idx < 0) return [];
    const target = snapshot[idx];
    const indices = new Set<number>();
    indices.add(idx);

    if (target.role === "user") {
      for (let i = idx + 1; i < snapshot.length; i += 1) {
        const next = snapshot[i];
        if (next.role === "system") continue;
        if (next.role === "assistant") indices.add(i);
        break;
      }
    } else if (target.role === "assistant") {
      for (let i = idx - 1; i >= 0; i -= 1) {
        const prev = snapshot[i];
        if (prev.role === "system") continue;
        if (prev.role === "user") indices.add(i);
        break;
      }
    }

    return Array.from(indices).sort((a, b) => b - a);
  };

  const handleEditMessage = (messageId: string) => {
    const prompt = getPromptForMessage(messageId, messages);
    if (!prompt) return;
    setEditingMessageId(messageId);
    setInput(prompt);
    inputRef.current?.focus();
  };

  const handleRerunMessage = async (messageId: string) => {
    const prompt = getPromptForMessage(messageId, messages);
    if (!prompt) return;
    await executeSend(prompt, { bypassReportIntent: true });
  };

  const handleDeleteMessage = (messageId: string) => {
    if (editingMessageId === messageId) {
      setEditingMessageId(null);
    }
    setMessages((prev) => {
      const deleteIndices = getDeleteIndicesForMessage(messageId, prev);
      if (deleteIndices.length === 0) return prev;
      const next = [...prev];
      for (const index of deleteIndices) {
        if (index >= 0 && index < next.length) next.splice(index, 1);
      }
      return next;
    });
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
      knownReportStatusRef.current[mapped.reportJobId] = mapped.status;
      setMessages((prev) => [
        ...prev,
        {
          id: `report-queued-${mapped.reportJobId}`,
          role: "system",
          content: `Riley is generating report "${mapped.title}". I'll post a download link here when it's ready.`,
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
      const rawMessage = error instanceof Error ? error.message : "";
      const normalized = rawMessage.toLowerCase();
      const isQuotaError =
        normalized.includes("reached your limit for deep/reports today") ||
        normalized.includes("usage limits");
      setMessages((prev) => [
        ...prev,
        {
          id: `report-create-error-${Date.now()}`,
          role: "system",
          content: isQuotaError
            ? "You’ve reached your limit for Deep/Reports today."
            : "Could not start report generation. Please try again.",
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

  const isEmpty = messages.length === 0;
  const hasThinkingPlaceholder = messages.some((msg) => msg.status === "thinking");
  const isGenerating = isLoading && !!activeGenerationRef.current;
  const showWelcome =
    isEmpty &&
    !isLoading &&
    !isLoadingConversations &&
    conversations.length === 0 &&
    !activeConversationId;
  const showCollapsedRail = !isSidebarOpen;
  const shouldRenderSidebar = true;
  const sidebarWidthClass = isSidebarOpen ? "w-[260px]" : "w-16";
  const collapsedRailConversations = useMemo(
    () => conversations.slice(0, 8),
    [conversations]
  );

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
          isActive
            ? isGlobal
              ? "bg-[#efe7d8] border border-[#e3dac8]"
              : "bg-[#efe7d8] border border-[#d8cb9d]"
            : isGlobal
            ? "hover:bg-[#f2ece0] border border-transparent"
            : "hover:bg-[#f2ece0] border border-transparent"
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
            // Only treat key presses on the row itself as activation.
            // Do not intercept keys from child inputs/buttons.
            if (e.currentTarget !== e.target) return;
            if (e.key === "Enter" || e.key === " ") {
              e.preventDefault();
              setActiveConversationId(conv.id);
            }
          }}
          className={cn("w-full text-left p-3 cursor-pointer", nested && "pl-7")}
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
                className={cn(
                  "flex-1 rounded px-2 py-1 text-sm focus:outline-none focus:ring-1",
                  isGlobal
                    ? "border border-[#d8d0bf] bg-white text-[#1f2a44] focus:ring-[#d4ad47]/40"
                    : "border border-[#d8d0bf] bg-white text-[#1f2a44] focus:ring-[#d4ad47]/40"
                )}
                disabled={isRenamingRequest}
              />
              <button
                type="button"
                onClick={(e) => handleRenameSave(e, conv.id)}
                disabled={isRenamingRequest}
                className="rounded p-1 text-[#7a5f19] transition-colors hover:bg-[#f2ece0]"
              >
                <Check className="h-4 w-4" />
              </button>
              <button
                type="button"
                onClick={(e) => handleRenameCancel(e)}
                disabled={isRenamingRequest}
                className="rounded p-1 text-[#7d8799] transition-colors hover:bg-[#f2ece0]"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
          ) : (
            <>
              <div className="flex items-start justify-between gap-2">
                <div className="flex-1 min-w-0">
                  <div className={cn("truncate text-sm font-medium", isGlobal ? "text-[#1f2a44]" : "text-[#1f2a44]")}>{conv.title}</div>
                  <div className={cn("mt-1 truncate text-xs", isGlobal ? "text-[#5d687f]" : "text-[#5d687f]")}>{conv.lastMessage}</div>
                  <div className={cn("mt-1 text-xs", isGlobal ? "text-[#8a90a0]" : "text-[#8a90a0]")}>{formatTime(conv.timestamp)}</div>
                </div>
                <div className="relative">
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      setOpenConversationMenuId((prev) => (prev === conv.id ? null : conv.id));
                    }}
                    className={cn(
                      "p-1.5 rounded transition-all",
                      isGlobal ? "hover:bg-[#e9e0d0]" : "hover:bg-[#e9e0d0]",
                      isMenuOpen || isActive
                        ? isGlobal
                          ? "text-[#4d5871]"
                          : "text-[#4d5871]"
                        : isGlobal
                        ? "text-[#8a90a0] opacity-0 group-hover:opacity-100"
                        : "text-[#8a90a0] opacity-0 group-hover:opacity-100"
                    )}
                    title="Conversation actions"
                  >
                    <MoreHorizontal className="h-3.5 w-3.5" />
                  </button>
                  {isMenuOpen && (
                    <div
                      className={cn(
                        "absolute right-0 top-8 z-20 min-w-[180px] rounded-lg p-1 shadow-xl",
                        isGlobal
                          ? "border border-[#d8d0bf] bg-white/95"
                          : "border border-[#d8d0bf] bg-white/95"
                      )}
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
                        className={cn(
                          "w-full rounded-md px-2 py-1.5 text-left text-xs",
                          isGlobal ? "text-[#1f2a44] hover:bg-[#f2ece0]" : "text-[#1f2a44] hover:bg-[#f2ece0]"
                        )}
                      >
                        Rename
                      </button>
                      {projects.length > 0 && (
                        <>
                          <div className={cn("my-1 border-t", isGlobal ? "border-[#e6dece]" : "border-[#e6dece]")} />
                          <div className={cn("px-2 py-1 text-[10px] uppercase tracking-wide", isGlobal ? "text-[#8a90a0]" : "text-[#8a90a0]")}>
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
                    "w-full rounded-md px-2 py-1.5 text-left text-xs hover:bg-[#f2ece0]",
                                conv.projectId === project.id
                                  ? isGlobal
                                    ? "text-[#6d560f] bg-[#faf3df]"
                                    : "text-amber-300"
                                  : isGlobal
                                  ? "text-[#1f2a44] hover:bg-[#f2ece0]"
                                  : "text-[#1f2a44]"
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
                          className={cn(
                            "mt-1 w-full rounded-md px-2 py-1.5 text-left text-xs",
                            isGlobal ? "text-[#1f2a44] hover:bg-[#f2ece0]" : "text-[#1f2a44] hover:bg-[#f2ece0]"
                          )}
                        >
                          Remove from Project{currentProject ? ` (${currentProject.name})` : ""}
                        </button>
                      )}
                      <div className={cn("my-1 border-t", isGlobal ? "border-[#e6dece]" : "border-[#e6dece]")} />
                      <button
                        type="button"
                        onClick={(e) => {
                          e.stopPropagation();
                          if (!window.confirm("Delete this conversation?")) return;
                          void handleDeleteConversation(conv.id);
                        }}
                        className={cn(
                          "w-full rounded-md px-2 py-1.5 text-left text-xs",
                          isGlobal ? "text-rose-700 hover:bg-rose-50" : "text-rose-700 hover:bg-rose-50"
                        )}
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
        "riley-layout flex h-full min-w-0 overflow-hidden",
        isGlobal ? "bg-[#f8f5ef] text-[#1f2a44]" : "bg-[#f7f5ef] text-[#1f2a44]"
      )}
    >
      {/* Left Sidebar - Chat History */}
      {shouldRenderSidebar && (
        <aside
          className={cn(
            "riley-sidebar border-r flex flex-col h-full shrink-0 overflow-hidden transition-[width] duration-200 ease-out",
            isGlobal ? "bg-[#f3eee4] border-[#e3dac8]" : "bg-[#eef2e9] border-[#d9dfd2]",
            sidebarWidthClass
          )}
        >
          {showCollapsedRail ? (
            <div className="flex h-full flex-col items-center gap-3 py-3">
              <button
                type="button"
                onClick={() => setIsSidebarOpen(true)}
                className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-[#d8d0bf] bg-white text-[#4d5871] transition-colors hover:bg-[#f7f2e8]"
                aria-label="Expand sidebar"
                title="Expand sidebar"
              >
                <ChevronRight className="h-4 w-4" />
              </button>
              <div className="h-px w-8 bg-[#ddd5c5]" />
              <button
                type="button"
                onClick={handleNewConversation}
                className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-[#d8cb9d] bg-[#faf3df] text-[#6d560f] transition-colors hover:bg-[#f3ebd3]"
                aria-label="New chat"
                title="New chat"
              >
                <Sparkles className="h-4 w-4" />
              </button>
              <button
                type="button"
                onClick={() => setIsCreatingProject(true)}
                className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-[#d8d0bf] bg-white text-[#4d5871] transition-colors hover:bg-[#f7f2e8]"
                aria-label="New project"
                title="New project"
              >
                <FolderPlus className="h-4 w-4" />
              </button>
              <div className="h-px w-8 bg-[#ddd5c5]" />
              <div className="flex min-h-0 w-full flex-1 flex-col items-center gap-2 overflow-y-auto pb-2">
                {collapsedRailConversations.map((conv) => {
                  const isActive = activeConversationId === conv.id;
                  const initial = (conv.title || "C").charAt(0).toUpperCase();
                  return (
                    <button
                      key={conv.id}
                      type="button"
                      onClick={() => setActiveConversationId(conv.id)}
                      className={cn(
                        "inline-flex h-8 w-8 items-center justify-center rounded-md border text-[11px] font-medium transition-colors",
                        isActive
                          ? "border-[#d8cb9d] bg-[#faf3df] text-[#6d560f]"
                          : "border-[#ddd5c5] bg-white text-[#5d687f] hover:bg-[#f7f2e8]"
                      )}
                      title={conv.title}
                      aria-label={`Open ${conv.title}`}
                    >
                      {initial}
                    </button>
                  );
                })}
              </div>
            </div>
          ) : (
          <>
          {/* Sidebar Header */}
          <div className={cn("shrink-0 p-4 border-b flex items-center justify-between", isGlobal ? "border-[#e3dac8]" : "border-[#d9dfd2]")}>
            <div className="flex-1 mr-2 flex items-center gap-2">
              <div className="flex-1 grid grid-cols-2 gap-2">
                <button
                  type="button"
                  onClick={handleNewConversation}
                  className={cn(
                    "inline-flex items-center justify-center gap-1.5 h-10 rounded-lg border px-3 text-xs font-medium transition-colors",
                    isGlobal
                      ? "border-[#d8cb9d] bg-[#faf3df] text-[#6d560f] hover:bg-[#f3ebd3]"
                      : "border-[#d4ad47]/40 bg-[#f8f2df] text-[#7a5f19] hover:bg-[#f1e7ca]"
                  )}
                >
                  <Sparkles className="h-3.5 w-3.5" />
                  <span>New Chat</span>
                </button>
                <button
                  type="button"
                  onClick={() => setIsCreatingProject((prev) => !prev)}
                  className={cn(
                    "inline-flex items-center justify-center gap-1.5 h-10 rounded-lg border px-3 text-xs font-medium transition-colors",
                    isGlobal
                      ? "border-[#d8d0bf] bg-white text-[#4d5871] hover:bg-[#f7f2e8]"
                      : "border-[#d5dccf] bg-[#f8faf6] text-[#5d687f] hover:bg-[#eef2e9]"
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
              className={cn(
                "inline-flex h-10 w-10 items-center justify-center rounded-lg border transition-colors",
                isGlobal
                  ? "border-[#d8d0bf] bg-white text-[#4d5871] hover:bg-[#f7f2e8]"
                  : "border-[#d5dccf] bg-[#f8faf6] text-[#5d687f] hover:bg-[#eef2e9] hover:text-[#1f2a44]"
              )}
              aria-label="Close conversations panel"
              title="Close conversations panel"
            >
              <ChevronLeft className="h-5 w-5" />
            </button>
          </div>

          {/* Conversation List */}
          <div className="min-h-0 min-w-0 flex-1 overflow-y-auto overflow-x-hidden p-3 space-y-4">
            {isCreatingProject && (
              <div className={cn("rounded-lg border p-2", isGlobal ? "border-[#d8d0bf] bg-white" : "border-[#d5dccf] bg-[#f8faf6]")}>
                <input
                  type="text"
                  value={projectInput}
                  onChange={(e) => setProjectInput(e.target.value)}
                  onKeyDown={handleProjectInputKeyDown}
                  placeholder="Project name"
                  className={cn(
                    "w-full rounded-md px-2 py-1.5 text-sm focus:outline-none focus:ring-1",
                    isGlobal
                      ? "border border-[#d8d0bf] bg-white text-[#1f2a44] focus:ring-[#d4ad47]/40"
                      : "border border-[#d5dccf] bg-white text-[#1f2a44] focus:ring-[#d4ad47]/35"
                  )}
                />
                <div className="mt-2 flex items-center gap-2">
                  <button
                    type="button"
                    onClick={handleCreateProject}
                    className={cn(
                      "rounded-md border px-2 py-1 text-xs transition-colors",
                      isGlobal
                        ? "bg-[#faf3df] border-[#d8cb9d] text-[#6d560f] hover:bg-[#f3ebd3]"
                        : "bg-[#f8f2df] border-[#d4ad47]/40 text-[#7a5f19] hover:bg-[#f1e7ca]"
                    )}
                  >
                    Create
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      setIsCreatingProject(false);
                      setProjectInput("");
                    }}
                    className={cn(
                      "rounded-md border px-2 py-1 text-xs transition-colors",
                      isGlobal ? "border-[#d8d0bf] text-[#6f788a] hover:bg-[#f2ece0]" : "border-[#d5dccf] text-[#6f788a] hover:bg-[#eef2e9]"
                    )}
                  >
                    Cancel
                  </button>
                </div>
              </div>
            )}

            <div>
              <div className={cn("px-2 pb-1 text-[11px] uppercase tracking-wide", isGlobal ? "text-[#8a90a0]" : "text-[#7d8799]")}>Projects</div>
              <div className="space-y-2">
                {projects.length === 0 ? (
                  <div className={cn("px-2 py-1 text-xs", isGlobal ? "text-[#8a90a0]" : "text-[#7d8799]")}>No projects yet. Create one to group related chats.</div>
                ) : (
                  projects.map((project) => {
                    const projectConversations = conversations.filter((conv) => conv.projectId === project.id);
                    const isCollapsed = Boolean(collapsedProjectIds[project.id]);
                    return (
                      <div key={project.id} className={cn("rounded-lg border p-2", isGlobal ? "border-[#e3dac8] bg-[#f7f2e8]" : "border-[#d9dfd2] bg-[#f2f6ee]")}>
                        <div className="flex items-center gap-1">
                          <button
                            type="button"
                            onClick={() => toggleProjectCollapsed(project.id)}
                            className={cn(
                              "flex min-w-0 flex-1 items-center gap-2 rounded-md px-1 py-1 text-left",
                              isGlobal ? "hover:bg-[#efe7d8]" : "hover:bg-[#e8efe3]"
                            )}
                          >
                            {isCollapsed ? (
                              <ChevronRight className={cn("h-3.5 w-3.5", isGlobal ? "text-[#8a90a0]" : "text-[#7d8799]")} />
                            ) : (
                              <ChevronDown className={cn("h-3.5 w-3.5", isGlobal ? "text-[#8a90a0]" : "text-[#7d8799]")} />
                            )}
                            <Folder className={cn("h-3.5 w-3.5", isGlobal ? "text-[#6f788a]" : "text-[#6f788a]")} />
                            <div className={cn("truncate text-xs font-medium", isGlobal ? "text-[#1f2a44]" : "text-[#1f2a44]")}>{project.name}</div>
                          </button>
                          <button
                            type="button"
                            onClick={(e) => {
                              e.stopPropagation();
                              void handleDeleteProject(project.id);
                            }}
                            className={cn(
                              "rounded p-1",
                              isGlobal ? "text-[#8a90a0] hover:bg-[#efe7d8] hover:text-rose-700" : "text-[#7d8799] hover:bg-[#e8efe3] hover:text-rose-700"
                            )}
                            title="Delete project"
                            aria-label="Delete project"
                          >
                            <Trash2 className="h-3.5 w-3.5" />
                          </button>
                        </div>
                        {!isCollapsed && (
                          <div className="space-y-1">
                            {projectConversations.length === 0 ? (
                              <div className={cn("ml-4 px-2 py-1 text-xs", isGlobal ? "text-[#8a90a0]" : "text-[#7d8799]")}>No conversations.</div>
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

            <div className={cn("mx-1 h-px", isGlobal ? "hidden" : "bg-[#d9dfd2]")} />
            <div>
              <div className={cn("px-2 pb-1 text-[11px] uppercase tracking-wide", isGlobal ? "text-[#8a90a0]" : "text-[#7d8799]")}>Ungrouped chats</div>
              <div className="space-y-1">
                {looseConversations.length === 0 ? (
                  <div className={cn("px-2 py-1 text-xs", isGlobal ? "text-[#8a90a0]" : "text-[#7d8799]")}>No ungrouped chats yet.</div>
                ) : (
                  looseConversations.map((conv) => renderConversationRow(conv))
                )}
              </div>
            </div>
          </div>
          </>
          )}
        </aside>
      )}

      {/* Main Area */}
      <main className="riley-chat-area flex min-w-0 flex-1 flex-col">
        {!isGlobal && (
          <header
            className={cn(
              "h-16 flex items-center justify-between px-6 shrink-0",
              "h-auto min-h-16 border-b border-[#e5ddce] bg-[#f7f5ef] py-3"
            )}
          >
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-3">
                <div className="flex h-8 w-8 items-center justify-center rounded-full border border-[#d8cb9d] bg-[#eadfb7]">
                  <span className="text-xs font-semibold text-[#6d560f]">R</span>
                </div>
                <div>
                  <h1 className="text-lg font-semibold text-[#1f2a44]">Riley</h1>
                  <p className="text-xs text-[#6f788a]">{contextName || "Campaign Workspace"}</p>
                </div>
              </div>
              {!isSidebarOpen && !showCollapsedRail && (
                <button
                  type="button"
                  onClick={() => setIsSidebarOpen(true)}
                  className="ml-2 inline-flex h-8 w-8 items-center justify-center rounded-md border border-[#d5dccf] bg-[#f8faf6] text-[#5d687f] transition-colors hover:bg-[#eef2e9]"
                  aria-label="Open sidebar"
                  title="Open sidebar"
                >
                  <ChevronRight className="h-4 w-4" />
                </button>
              )}
            </div>
            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={() => setIsReportModalOpen(true)}
                className="flex items-center gap-2 rounded-lg border border-[#d4ad47]/40 bg-[#f8f2df] px-3 py-1.5 text-sm text-[#7a5f19] hover:bg-[#f1e7ca]"
                title="Generate long-form report"
              >
                <ClipboardList className="h-4 w-4" />
                <span>Generate Report</span>
              </button>
              <>
                <button
                  type="button"
                  onClick={() => setMode("fast")}
                  className={cn(
                    "flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-colors",
                    mode === "fast"
                      ? "border border-[#d4ad47]/35 bg-white text-[#6d560f]"
                      : "border border-[#ddd5c5] bg-[#f3f0e8] text-[#6f788a] hover:bg-[#ece6d8]"
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
                      ? "border border-[#d4ad47]/35 bg-white text-[#6d560f]"
                      : "border border-[#ddd5c5] bg-[#f3f0e8] text-[#6f788a] hover:bg-[#ece6d8]"
                  )}
                >
                  <Brain className="h-4 w-4" />
                  <span>Deep</span>
                </button>
              </>
            </div>
          </header>
        )}

        {!isGlobal && (
          <div className="border-b border-[#e5ddce] bg-[#f6f2e8] px-6 py-2.5">
            <div className="text-xs text-[#6f788a]">
              Riley is working with documents and strategy from this campaign.
            </div>
          </div>
        )}

        {/* Message Area */}
        <div className="chat-container flex-1 overflow-y-auto">
          {isGlobal ? (
            <div className="px-6 pt-5 pb-1">
              <h1 className="text-sm font-medium text-[#5d687f]">Riley</h1>
            </div>
          ) : null}
          {showWelcome ? (
            /* Empty State */
            <div className="h-full flex flex-col items-center justify-center px-6 py-12">
              <div className="max-w-2xl w-full text-center">
                <div className="mb-8 flex justify-center">
                  <div className={cn(
                    "h-20 w-20 rounded-full border flex items-center justify-center",
                    "bg-[#eadfb7] border-[#d8cb9d]"
                  )}>
                    <span className="text-3xl font-semibold text-[#6d560f]">R</span>
                  </div>
                </div>
                <h2 className="mb-2 text-3xl font-bold text-[#1f2a44]">
                  Hi, I'm Riley.
                </h2>
                <p className="mb-8 text-[#6f788a]">
                  {isGlobal ? (
                    <>I have access to <strong className="text-[#6d560f]">Rally Global Brain</strong>. Ask me anything about strategy, messaging, or historical data.</>
                  ) : (
                    <>I can help you analyze strategy, messaging, and documents in this campaign.</>
                  )}
                </p>

                {/* Prompt Starters */}
                {!isGlobal && (
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
                          isGlobal
                            ? "bg-white border-[#e3dac8] hover:bg-[#fdfaf3] hover:border-[#d8cb9d]"
                            : "bg-[#f8f4ea] border-[#ddd5c5] hover:bg-[#f2ece0] hover:border-[#d8cb9d]"
                        )}
                      >
                        <div className="flex items-start gap-3">
                          <Icon className={cn("h-5 w-5 flex-shrink-0 mt-0.5", prompt.color)} />
                          <span className="font-medium text-[#1f2a44]">{prompt.text}</span>
                        </div>
                      </button>
                    );
                  })}
                </div>
                )}
              </div>
            </div>
          ) : (
            /* Message Stream */
            <div
              className={cn(
                "chat-content w-full px-6 py-8 space-y-6",
                isGlobal ? "max-w-[700px]" : "mx-auto max-w-[700px]"
              )}
            >
              {messages.map((message, index) => {
                const isLastMessage = index === messages.length - 1;
                const isLiveAnimatingAssistant = message.role === "assistant" && message.id === animatingAssistantMessageId;
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
                      <div
                        className={cn(
                          "flex-shrink-0 h-8 w-8 rounded-full border flex items-center justify-center",
                          "bg-[#eadfb7] border-[#d8cb9d]"
                        )}
                      >
                        <span className="text-xs font-semibold text-[#6d560f]">R</span>
                      </div>
                    )}
                    <div
                      className={cn(
                        "relative group/message px-4 py-3 max-w-[85%] rounded-2xl",
                        message.role !== "system" && message.status !== "thinking" && "pr-20",
                        message.role === "user"
                          ? isGlobal
                            ? "bg-[#4f6386] text-white"
                            : "bg-[#4f6386] text-white"
                          : message.role === "system"
                          ? isGlobal
                            ? "bg-[#f0eadf] text-[#6f788a] italic border border-[#e3dac8]"
                            : "bg-[#f0eadf] border border-[#e3dac8] text-[#6f788a] italic"
                          : isGlobal
                          ? "bg-[#f1eee8] border border-[#e3dac8] text-[#1f2a44]"
                          : "bg-[#f1eee8] border border-[#e3dac8] text-[#1f2a44]"
                      )}
                    >
                      {message.role !== "system" && message.status !== "thinking" && (
                        <div
                          className={cn(
                            "absolute right-2 top-2 flex items-center gap-1 rounded-md border p-1 opacity-0 transition-opacity group-hover/message:opacity-100",
                            isGlobal
                              ? "border-[#d8d0bf] bg-white/90"
                              : "border-[#ddd5c5] bg-white/95"
                          )}
                        >
                          <button
                            type="button"
                            onClick={() => handleEditMessage(message.id)}
                            className={cn(
                              "rounded p-1",
                              isGlobal
                                ? "text-[#6f788a] hover:bg-[#f2ece0] hover:text-[#1f2a44]"
                                : "text-[#6f788a] hover:bg-[#f2ece0] hover:text-[#1f2a44]"
                            )}
                            title="Edit message"
                          >
                            <Pencil className="h-3.5 w-3.5" />
                          </button>
                          <button
                            type="button"
                            onClick={() => void handleRerunMessage(message.id)}
                            className={cn(
                              "rounded p-1",
                              isGlobal
                                ? "text-[#6f788a] hover:bg-[#f2ece0] hover:text-[#1f2a44]"
                                : "text-[#6f788a] hover:bg-[#f2ece0] hover:text-[#1f2a44]"
                            )}
                            title="Resend message"
                          >
                            <RotateCcw className="h-3.5 w-3.5" />
                          </button>
                          <button
                            type="button"
                            onClick={() => handleDeleteMessage(message.id)}
                            className={cn(
                              "rounded p-1",
                              isGlobal
                                ? "text-[#6f788a] hover:bg-[#f2ece0] hover:text-rose-700"
                                : "text-[#6f788a] hover:bg-[#f2ece0] hover:text-rose-700"
                            )}
                            title="Delete message pair"
                          >
                            <Trash2 className="h-3.5 w-3.5" />
                          </button>
                        </div>
                      )}
                      {message.status === "thinking" ? (
                        // Thinking placeholder
                        <div className={cn("flex items-center gap-2 text-sm", isGlobal ? "text-[#4d5871]" : "text-[#6f788a]")}>
                          <Loader2 className="h-4 w-4 animate-spin" />
                          <span>Riley is thinking...</span>
                        </div>
                      ) : isLiveAnimatingAssistant ? (
                        // Typewriter effect only for the currently live assistant response
                        <TypewriterMarkdown
                          content={message.content}
                          className="riley-md-light"
                        />
                      ) : message.role === "assistant" ? (
                        // Static markdown for previous assistant messages
                        <div className="riley-md riley-md-light">
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
                      {message.reportSuggestionPrompt && !isGlobal && (
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
                            className="inline-flex items-center gap-1.5 rounded-md border border-[#ddd5c5] bg-[#f8f4ea] px-2 py-1 text-[11px] text-[#4d5871] hover:bg-[#f2ece0]"
                          >
                            <MessageSquare className="h-3.5 w-3.5" />
                            <span>Continue in chat</span>
                          </button>
                        </div>
                      )}
                      {message.role === "assistant" && message.content && !isGlobal && (
                        <div className="mt-2 flex justify-end">
                          <button
                            type="button"
                            onClick={() => handleCopyAssistantMessage(message.id, message.content)}
                            className="inline-flex items-center gap-1.5 rounded-md border border-[#ddd5c5] bg-[#f8f4ea] px-2 py-1 text-[11px] text-[#4d5871] hover:bg-[#f2ece0]"
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
                        <div
                          className={cn(
                            "mt-2 flex items-center gap-1.5 border-t pt-2 text-xs",
                            isGlobal ? "border-[#ddd5c5] text-[#5d687f]" : "border-[#ddd5c5] text-[#5d687f]"
                          )}
                        >
                          <span>📚</span>
                          <span>Analyzed {message.sourcesCount} document{message.sourcesCount !== 1 ? "s" : ""}</span>
                        </div>
                      )}
                      {message.role === "assistant" && displaySources.length > 0 && (
                        <div className={cn("mt-2 border-t pt-2", isGlobal ? "border-[#ddd5c5]" : "border-[#ddd5c5]")}>
                          <button
                            type="button"
                            onClick={() =>
                              setOpenSourcesByMessageId((prev) => ({
                                ...prev,
                                [message.id]: !isSourcesOpen,
                              }))
                            }
                            className={cn(
                              "mb-1 inline-flex items-center gap-1.5 text-xs",
                              isGlobal ? "text-[#5d687f] hover:text-[#1f2a44]" : "text-[#5d687f] hover:text-[#1f2a44]"
                            )}
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
                                      ? isGlobal
                                        ? "border-[#d8cb9d] bg-[#faf3df] text-[#6d560f] hover:bg-[#f3ebd3]"
                                        : "border-[#d8cb9d] bg-[#faf3df] text-[#6d560f] hover:bg-[#f3ebd3]"
                                      : isGlobal
                                      ? "border-[#ddd5c5] bg-[#f7f2e8] text-[#8a90a0] cursor-not-allowed"
                                      : "border-[#ddd5c5] bg-[#f7f2e8] text-[#8a90a0] cursor-not-allowed"
                                  )}
                                  title={hasAsset ? `Open ${source.filename}` : "Source unavailable"}
                                >
                                  <span className="inline-flex items-center gap-1.5 min-w-0">
                                    <FileText className="h-3 w-3" />
                                    <span className="max-w-[220px] truncate">{source.filename}</span>
                                  </span>
                                  <span className={cn("text-[10px]", isGlobal ? "text-[#6f788a]" : "text-[#6f788a]")}>
                                    {source.location || "unknown"}
                                  </span>
                                </button>
                              );
                            })}
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                    {message.role === "user" && (
                      <div
                        className={cn(
                          "flex-shrink-0 h-8 w-8 rounded-full flex items-center justify-center text-xs font-medium",
                          "bg-[#dfe6f2] text-[#4f6386]"
                        )}
                      >
                        A
                      </div>
                    )}
                  </div>
                );
              })}

              {/* Thinking Indicator for non-optimistic loading only */}
              {isLoading && !hasThinkingPlaceholder && (
                <div className="flex gap-4 justify-start">
                  <div
                    className={cn(
                      "flex-shrink-0 h-8 w-8 rounded-full border flex items-center justify-center",
                      "bg-[#eadfb7] border-[#d8cb9d]"
                    )}
                  >
                    <span className="text-xs font-semibold text-[#6d560f]">R</span>
                  </div>
                  <div
                    className={cn(
                      "rounded-2xl px-4 py-3",
                      "bg-[#f1eee8] border border-[#e3dac8]"
                    )}
                  >
                    <div className="flex items-center gap-2">
                      <Loader2 className="h-4 w-4 animate-spin text-[#7b8395]" />
                      <span className="text-sm text-[#6f788a]">Riley is thinking...</span>
                    </div>
                  </div>
                </div>
              )}

              <div ref={messagesEndRef} />
            </div>
          )}
        </div>

        {/* Input Area - Fixed Bottom */}
        <div
          className={cn(
            "flex-shrink-0 p-4",
            isGlobal
              ? "bg-transparent"
              : "border-t border-[#e5ddce] bg-[#f7f5ef]"
          )}
        >
          <div
            className={cn(
              "chat-content w-full",
              isGlobal ? "max-w-[700px] px-6" : "mx-auto max-w-[700px]"
            )}
          >
            {editingMessageId && (
              <div
                className={cn(
                  "mb-2 flex items-center justify-between rounded-md border px-3 py-2 text-xs",
                  isGlobal
                    ? "border-[#d8d0bf] bg-[#f7f2e8] text-[#4d5871]"
                    : "border-[#ddd5c5] bg-[#f8f4ea] text-[#4d5871]"
                )}
              >
                <span>Editing message. Send to replace and regenerate response.</span>
                <button
                  type="button"
                  onClick={() => {
                    setEditingMessageId(null);
                    setInput("");
                  }}
                  className={cn("rounded px-2 py-1", isGlobal ? "hover:bg-[#efe7d8]" : "hover:bg-[#f2ece0]")}
                >
                  Cancel
                </button>
              </div>
            )}
            {sendDisabledReason && !isLoading && (
              <div className={cn("mb-2 text-xs text-center", isGlobal ? "text-[#8a90a0]" : "text-[#7d8799]")}>
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
                className={cn(
                  "scrollbar-hide flex-1 resize-none overflow-y-auto px-4 py-3 text-sm max-h-[200px] disabled:opacity-50 disabled:cursor-not-allowed focus:outline-none",
                  isGlobal
                    ? "rounded-3xl border border-[#ddd5c5] bg-white text-[#1f2a44] placeholder:text-[#98a1b1] focus:ring-2 focus:ring-[#d4ad47]/40"
                    : "rounded-3xl border border-[#ddd5c5] bg-white text-[#1f2a44] placeholder:text-[#98a1b1] focus:ring-2 focus:ring-[#d4ad47]/35"
                )}
              />
              {isGenerating ? (
                <button
                  type="button"
                  onClick={handleStopGeneration}
                  className={cn(
                    "flex-shrink-0 inline-flex items-center gap-2 rounded-full border px-3 py-2 text-sm transition-colors",
                    isGlobal
                      ? "border-[#d6ccb8] bg-[#efe8d8] text-[#6f4e1f] hover:bg-[#e6dcc8]"
                      : "border-[#d6ccb8] bg-[#efe8d8] text-[#6f4e1f] hover:bg-[#e6dcc8]"
                  )}
                  aria-label="Stop generation"
                  title="Stop generation"
                >
                  <Square className="h-3.5 w-3.5 fill-current" />
                  <span>Stop</span>
                </button>
              ) : (
                <button
                  type="button"
                  onClick={handleSend}
                  disabled={!canSend}
                  className={cn(
                    "flex-shrink-0 p-3 transition-colors",
                    canSend
                      ? isGlobal
                        ? "rounded-full bg-[#e8e2d2] border border-[#d6ccb8] text-[#4f6386] hover:bg-[#ded6c3]"
                        : "rounded-full bg-[#e8e2d2] border border-[#d6ccb8] text-[#4f6386] hover:bg-[#ded6c3]"
                      : isGlobal
                      ? "rounded-full bg-[#f2eee5] border border-[#e3dac8] text-[#a3a9b7] cursor-not-allowed"
                      : "rounded-full bg-[#f2eee5] border border-[#e3dac8] text-[#a3a9b7] cursor-not-allowed"
                  )}
                  aria-label="Send message"
                  title={sendDisabledReason || "Send message"}
                >
                  <Send className="h-5 w-5" />
                </button>
              )}
            </div>
            {!isGlobal && (
              <p className="mt-2 text-center text-xs text-[#7d8799]">
                {mode === "fast" ? "Fast mode: Quick responses" : "Deep mode: Comprehensive analysis"}
              </p>
            )}
          </div>
        </div>
      </main>
      {isReportModalOpen && !isGlobal && (
        <div className="fixed inset-0 z-40 flex items-center justify-center bg-[#1f2a44]/20 p-4 backdrop-blur-[1px]">
          <div className="w-full max-w-2xl rounded-xl border border-[#ddd5c5] bg-[#fbf8f2] shadow-xl">
            <div className="flex items-center justify-between border-b border-[#e5ddce] px-4 py-3">
              <h3 className="text-sm font-semibold text-[#1f2a44]">Generate Riley Report</h3>
              <button
                type="button"
                onClick={() => setIsReportModalOpen(false)}
                className="rounded-md border border-[#ddd5c5] bg-white px-2 py-1 text-xs text-[#4d5871] hover:bg-[#f2ece0]"
              >
                Close
              </button>
            </div>
            <div className="space-y-4 px-4 py-4">
              <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                <label className="flex flex-col gap-1 text-xs text-[#6f788a]">
                  <span>Report Type</span>
                  <select
                    value={reportType}
                    onChange={(e) =>
                      setReportType(
                        e.target.value as
                          | "summary"
                          | "strategy_memo"
                          | "audience_analysis"
                          | "narrative_brief"
                          | "opposition_framing_brief"
                      )
                    }
                    className="rounded-md border border-[#ddd5c5] bg-white px-3 py-2 text-sm text-[#1f2a44] focus:outline-none focus:ring-1 focus:ring-[#d4ad47]/35"
                  >
                    <option value="summary">Summary</option>
                    <option value="strategy_memo">Strategy memo</option>
                    <option value="audience_analysis">Audience analysis</option>
                    <option value="narrative_brief">Narrative brief</option>
                    <option value="opposition_framing_brief">Opposition framing brief</option>
                  </select>
                </label>
                <label className="flex flex-col gap-1 text-xs text-[#6f788a]">
                  <span>Optional Title</span>
                  <input
                    type="text"
                    value={reportTitle}
                    onChange={(e) => setReportTitle(e.target.value)}
                    placeholder="Q3 persuasion strategy memo"
                    className="rounded-md border border-[#ddd5c5] bg-white px-3 py-2 text-sm text-[#1f2a44] placeholder:text-[#98a1b1] focus:outline-none focus:ring-1 focus:ring-[#d4ad47]/35"
                  />
                </label>
              </div>
              <label className="flex flex-col gap-1 text-xs text-[#6f788a]">
                <span>Instructions / Prompt</span>
                <textarea
                  value={reportPrompt}
                  onChange={(e) => setReportPrompt(e.target.value)}
                  rows={7}
                  placeholder="Analyze these campaign documents and write a strategy memo focused on message discipline, persuasion risks, and opportunities by audience."
                  className="resize-y rounded-md border border-[#ddd5c5] bg-white px-3 py-2 text-sm text-[#1f2a44] placeholder:text-[#98a1b1] focus:outline-none focus:ring-1 focus:ring-[#d4ad47]/35"
                />
              </label>
              <label className="inline-flex items-center gap-2 text-sm text-[#4d5871]">
                <input
                  type="checkbox"
                  checked={reportDeepMode}
                  onChange={(e) => setReportDeepMode(e.target.checked)}
                  className="h-4 w-4 rounded border-[#cfc7b8] bg-white text-[#d4ad47] focus:ring-[#d4ad47]/40"
                />
                <span>Deep mode (recommended)</span>
              </label>
            </div>
            <div className="flex items-center justify-end gap-2 border-t border-[#e5ddce] px-4 py-3">
              <button
                type="button"
                onClick={() => setIsReportModalOpen(false)}
                className="rounded-md border border-[#ddd5c5] bg-white px-3 py-1.5 text-xs text-[#4d5871] hover:bg-[#f2ece0]"
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
                    ? "cursor-not-allowed border-[#ddd5c5] bg-[#f2eee5] text-[#a3a9b7]"
                    : "border-[#d4ad47]/35 bg-[#f8f2df] text-[#7a5f19] hover:bg-[#f1e7ca]"
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
