"use client";

import { useState, useRef, useEffect, useCallback, useMemo, memo } from "react";
import { X, Send, FileText, FileType2, Table, Presentation, Image as ImageIcon, Download, CheckCircle, Clock, AlertCircle, PlayCircle, GripVertical, ExternalLink, ZoomIn, ZoomOut, RefreshCw, Loader2 } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import { Asset, KanbanCard } from "@app/lib/types";
import { useAuth } from "@clerk/nextjs";
import { usePathname } from "next/navigation";
import { apiFetch } from "@app/lib/api";
import DocViewer, { DocViewerRenderers } from "@cyntler/react-doc-viewer";
import "@cyntler/react-doc-viewer/dist/index.css";

interface Comment {
  id: string;
  authorUserId: string;
  authorDisplayName: string;
  content: string;
  createdAt: string;
  mentions: string[];
  mentionDisplayNames: string[];
}

interface MentionMember {
  user_id: string;
  display_name: string;
}

// Flexible file interface that works with both Asset and KanbanCard
interface FileData {
  id: string;
  name: string;
  type: Asset["type"];
  url: string;
  // Optional preview metadata from backend (e.g., Office/HTML -> PDF)
  previewUrl?: string | null;
  previewType?: string | null;
  previewStatus?: "complete" | "failed" | "processing" | "queued" | null;
  previewError?: string | null;
  status?: Asset["status"];
  uploader?: string;
  size?: string;
  uploadDate?: string;
}

interface DocumentViewerProps {
  file: FileData;
  onClose: () => void;
  variant?: "drawer" | "modal";
}

function getFileIcon(type: FileData["type"]) {
  switch (type) {
    case "pdf":
      return { icon: FileText, color: "text-red-500" };
    case "docx":
      return { icon: FileType2, color: "text-blue-500" };
    case "xlsx":
      return { icon: Table, color: "text-emerald-500" };
    case "pptx":
      return { icon: Presentation, color: "text-orange-500" };
    case "img":
      return { icon: ImageIcon, color: "text-purple-500" };
    default:
      return { icon: FileText, color: "text-zinc-500" };
  }
}

function getStatusBadge(status?: Asset["status"]) {
  switch (status) {
    case "approved":
      return { icon: CheckCircle, color: "text-emerald-400", bg: "bg-emerald-500/10", border: "border-emerald-500/20", label: "Client Ready" };
    case "in_review":
      return { icon: Clock, color: "text-yellow-400", bg: "bg-yellow-500/10", border: "border-yellow-500/20", label: "In Review" };
    case "needs_review":
      return { icon: AlertCircle, color: "text-red-400", bg: "bg-red-500/10", border: "border-red-500/20", label: "Needs Review" };
    case "in_progress":
      return { icon: PlayCircle, color: "text-blue-400", bg: "bg-blue-500/10", border: "border-blue-500/20", label: "In Progress" };
    default:
      return { icon: Clock, color: "text-zinc-400", bg: "bg-zinc-500/10", border: "border-zinc-500/20", label: "Ready" };
  }
}

function getInitials(name: string): string {
  return name
    .split(" ")
    .map((n) => n[0])
    .join("")
    .toUpperCase()
    .slice(0, 2);
}

// Map our file types to DocViewer's expected file types
function mapFileType(type: FileData["type"]): string {
  switch (type) {
    case "pdf":
      return "pdf";
    case "docx":
      return "docx";
    case "xlsx":
      return "xlsx";
    case "pptx":
      return "pptx";
    case "img":
      return "jpg"; // DocViewer handles images as jpg/png
    default:
      return "pdf";
  }
}

function getRenderableUrl(asset: FileData): string | null {
  const documentTypes: FileData["type"][] = ["pdf", "docx", "xlsx", "pptx"];
  const isDocument = documentTypes.includes(asset.type);
  if (isDocument) {
    return asset.previewUrl || asset.url || null;
  }
  return asset.url || null;
}

function getRenderableType(asset: FileData): FileData["type"] {
  const documentTypes: FileData["type"][] = ["pdf", "docx", "xlsx", "pptx"];
  const isDocument = documentTypes.includes(asset.type);
  if (isDocument && asset.previewUrl) {
    return "pdf";
  }
  return asset.type;
}

// Memoized DocViewer component to prevent re-renders when parent state changes
const StableDocViewer = memo(({ documents, config }: { documents: any[]; config: any }) => {
  return (
    <DocViewer
      documents={documents}
      pluginRenderers={DocViewerRenderers}
      style={{ height: "100%", background: "transparent", minHeight: "100%" }}
      config={config}
    />
  );
}, (prevProps, nextProps) => {
  // Only re-render if documents actually change
  return (
    prevProps.documents[0]?.uri === nextProps.documents[0]?.uri &&
    prevProps.documents[0]?.fileType === nextProps.documents[0]?.fileType
  );
});

StableDocViewer.displayName = "StableDocViewer";

export function DocumentViewer({ file, onClose, variant = "drawer" }: DocumentViewerProps) {
  const [comments, setComments] = useState<Comment[]>([]);
  const [commentInput, setCommentInput] = useState("");
  const [mentionMembers, setMentionMembers] = useState<MentionMember[]>([]);
  const [isLoadingComments, setIsLoadingComments] = useState(false);
  const [isSendingComment, setIsSendingComment] = useState(false);
  const [width, setWidth] = useState(800); // Default width for drawer
  const [isResizing, setIsResizing] = useState(false);
  const [zoom, setZoom] = useState(1); // Zoom level (0.5 to 3)
  const [runtimePreviewUrl, setRuntimePreviewUrl] = useState<string | null>(
    file.previewUrl ?? null
  );
  const [isRefreshingPreview, setIsRefreshingPreview] = useState(false);
  const commentsEndRef = useRef<HTMLDivElement>(null);
  const resizeRef = useRef<HTMLDivElement>(null);
  const { icon: FileIcon, color: iconColor } = getFileIcon(file.type);
  const statusBadge = getStatusBadge(file.status || "ready");
  const { getToken, isLoaded } = useAuth();
  const pathname = usePathname();

  const minWidth = 400;
  const maxWidth = 1200;
  const isModal = variant === "modal";
  // Log preview fields when preview panel opens
  useEffect(() => {
    console.log("asset preview fields", {
      id: file.id,
      preview_status: file.previewStatus,
      preview_url: file.previewUrl,
      preview_type: file.previewType,
      preview_error: file.previewError,
    });
  }, [file.id, file.previewStatus, file.previewUrl, file.previewType, file.previewError]);

  useEffect(() => {
    setRuntimePreviewUrl(file.previewUrl ?? null);
  }, [file.id, file.previewUrl]);

  const campaignIdFromPath = useMemo(() => {
    const match = pathname?.match(/^\/campaign\/([^/]+)/);
    return match?.[1] ?? null;
  }, [pathname]);

  const mentionMemberByToken = useMemo(() => {
    const map = new Map<string, string>();
    for (const member of mentionMembers) {
      const userId = String(member.user_id || "").toLowerCase();
      const displayName = String(member.display_name || "").trim();
      if (!userId || !displayName) continue;
      const emailPrefix = displayName.includes("@")
        ? displayName.split("@")[0].toLowerCase()
        : "";
      map.set(userId, member.user_id);
      map.set(displayName.toLowerCase(), member.user_id);
      map.set(displayName.toLowerCase().replace(/\s+/g, "_"), member.user_id);
      map.set(displayName.toLowerCase().replace(/\s+/g, ""), member.user_id);
      if (emailPrefix) {
        map.set(emailPrefix, member.user_id);
      }
    }
    return map;
  }, [mentionMembers]);

  useEffect(() => {
    const shouldRefreshPreview =
      file.previewType === "pdf" &&
      file.previewStatus === "complete" &&
      !!campaignIdFromPath;
    if (!shouldRefreshPreview || !isLoaded) {
      return;
    }

    let cancelled = false;

    const refreshPreviewUrl = async () => {
      try {
        setIsRefreshingPreview(true);
        const token = await getToken();
        if (!token || !campaignIdFromPath) return;
        const data = await apiFetch<{ file_id: string; preview_url: string }>(
          `/api/v1/files/${encodeURIComponent(file.id)}/preview-url?tenant_id=${encodeURIComponent(campaignIdFromPath)}`,
          {
            token,
            method: "GET",
          }
        );
        if (!cancelled && data?.preview_url) {
          setRuntimePreviewUrl(data.preview_url);
        }
      } catch {
        // Fall back to existing preview URL from payload.
      } finally {
        if (!cancelled) {
          setIsRefreshingPreview(false);
        }
      }
    };

    void refreshPreviewUrl();
    return () => {
      cancelled = true;
    };
  }, [
    file.id,
    file.previewType,
    file.previewStatus,
    campaignIdFromPath,
    getToken,
    isLoaded,
  ]);

  useEffect(() => {
    const loadMembersAndComments = async () => {
      if (!campaignIdFromPath || !isLoaded) return;
      try {
        setIsLoadingComments(true);
        const token = await getToken();
        if (!token) return;

        const [membersData, commentsData] = await Promise.all([
          apiFetch<{ members: MentionMember[] }>(
            `/api/v1/campaigns/${encodeURIComponent(campaignIdFromPath)}/members?mentions_only=true`,
            { token, method: "GET" }
          ),
          apiFetch<{
            comments: Array<{
              id: string;
              author_user_id: string;
              author_display_name: string;
              content: string;
              mentions: string[];
              mention_display_names: string[];
              created_at: string;
            }>;
          }>(
            `/api/v1/files/${encodeURIComponent(file.id)}/comments?tenant_id=${encodeURIComponent(campaignIdFromPath)}`,
            { token, method: "GET" }
          ),
        ]);

        setMentionMembers(membersData.members || []);
        setComments(
          (commentsData.comments || []).map((item) => ({
            id: item.id,
            authorUserId: item.author_user_id,
            authorDisplayName: item.author_display_name,
            content: item.content,
            createdAt: item.created_at,
            mentions: Array.isArray(item.mentions) ? item.mentions : [],
            mentionDisplayNames: Array.isArray(item.mention_display_names)
              ? item.mention_display_names
              : [],
          }))
        );
      } catch (error) {
        console.error("Failed to load document comments or mention members:", error);
        setComments([]);
      } finally {
        setIsLoadingComments(false);
      }
    };
    void loadMembersAndComments();
  }, [campaignIdFromPath, file.id, getToken, isLoaded]);

  // Auto-scroll comments to bottom
  useEffect(() => {
    commentsEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [comments]);

  const formatCommentTimestamp = (iso: string): string => {
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) return "just now";
    return date.toLocaleString();
  };

  const extractMentionedUserIds = (text: string): string[] => {
    const found = text.match(/@([a-zA-Z0-9_.-]+)/g) || [];
    const ids = new Set<string>();
    for (const token of found) {
      const normalized = token.replace("@", "").toLowerCase();
      const userId = mentionMemberByToken.get(normalized);
      if (userId) {
        ids.add(userId);
      }
    }
    return Array.from(ids);
  };

  // Resize handler
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizing(true);
  }, []);

  useEffect(() => {
    if (!isResizing) return;

    const handleMouseMove = (e: MouseEvent) => {
      const newWidth = window.innerWidth - e.clientX;
      const clampedWidth = Math.max(minWidth, Math.min(maxWidth, newWidth));
      setWidth(clampedWidth);
    };

    const handleMouseUp = () => {
      setIsResizing(false);
    };

    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);

    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
    };
  }, [isResizing, minWidth, maxWidth]);

  const handleSendComment = async () => {
    if (!commentInput.trim() || !campaignIdFromPath) return;
    try {
      setIsSendingComment(true);
      const token = await getToken();
      if (!token) return;
      const mentionIds = extractMentionedUserIds(commentInput.trim());
      const data = await apiFetch<{
        comments: Array<{
          id: string;
          author_user_id: string;
          author_display_name: string;
          content: string;
          mentions: string[];
          mention_display_names: string[];
          created_at: string;
        }>;
      }>(
        `/api/v1/files/${encodeURIComponent(file.id)}/comments?tenant_id=${encodeURIComponent(campaignIdFromPath)}`,
        {
          token,
          method: "POST",
          body: {
            content: commentInput.trim(),
            mentions: mentionIds,
          },
        }
      );
      setComments(
        (data.comments || []).map((item) => ({
          id: item.id,
          authorUserId: item.author_user_id,
          authorDisplayName: item.author_display_name,
          content: item.content,
          createdAt: item.created_at,
          mentions: Array.isArray(item.mentions) ? item.mentions : [],
          mentionDisplayNames: Array.isArray(item.mention_display_names)
            ? item.mention_display_names
            : [],
        }))
      );
      setCommentInput("");
    } catch (error) {
      console.error("Failed to send comment:", error);
    } finally {
      setIsSendingComment(false);
    }
  };

  const handleKeyPress = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      void handleSendComment();
    }
  };

  // Decide which URL and type to use for rendering.
  const effectiveUrl = useMemo(() => {
    return getRenderableUrl({
      ...file,
      previewUrl: runtimePreviewUrl,
    });
  }, [file, runtimePreviewUrl]);

  const effectiveType = useMemo<Asset["type"]>(() => {
    return getRenderableType(file);
  }, [file]);

  // Memoize documents array to prevent unnecessary recalculations
  const documents = useMemo(() => [
    {
      uri: effectiveUrl,
      fileType: mapFileType(effectiveType),
      fileName: file.name,
    },
  ], [effectiveUrl, effectiveType, file.name]);

  // Memoize DocViewer config to prevent re-renders
  const docViewerConfig = useMemo(() => ({
    header: {
      disableHeader: true,
      disableFileName: true,
      retainURLParams: false,
    },
    // Enable PDF vertical scrolling by default
    pdfVerticalScrollByDefault: true,
  }), []);

  const handleOpenExternally = () => {
    // Trigger download by creating a temporary anchor element
    const link = document.createElement("a");
    link.href = file.url;
    link.download = file.name;
    link.target = "_blank";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleDownload = () => {
    // Create a temporary anchor element to trigger download
    const link = document.createElement("a");
    link.href = file.url;
    link.download = file.name;
    link.target = "_blank";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleZoomIn = () => {
    setZoom((z) => Math.min(z + 0.2, 3));
  };

  const handleZoomOut = () => {
    setZoom((z) => Math.max(z - 0.2, 0.5));
  };

  return (
    <AnimatePresence>
      <>
        {/* Backdrop */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.2 }}
          className={cn(
            "fixed inset-0 z-40 bg-black/60 backdrop-blur-sm",
            isModal ? "bg-black/80" : "bg-black/60"
          )}
          onClick={onClose}
        />

        {/* Container - Drawer or Modal */}
        <motion.div
          initial={isModal ? { opacity: 0, scale: 0.95 } : { x: "100%" }}
          animate={isModal ? { opacity: 1, scale: 1 } : { x: 0 }}
          exit={isModal ? { opacity: 0, scale: 0.95 } : { x: "100%" }}
          transition={isModal ? { duration: 0.2 } : { type: "spring", damping: 25, stiffness: 200 }}
          style={isModal ? {} : { width: `${width}px` }}
          className={cn(
            "z-50 bg-zinc-950/95 backdrop-blur-xl border border-zinc-800 shadow-2xl flex flex-col",
            isModal
              ? "fixed inset-0 m-auto w-[90vw] h-[90vh] rounded-xl overflow-hidden"
              : "fixed inset-y-0 right-0 border-l"
          )}
          onClick={(e) => e.stopPropagation()}
        >
          {/* Resize Handle - Only for drawer */}
          {!isModal && (
            <div
              ref={resizeRef}
              onMouseDown={handleMouseDown}
              className={cn(
                "absolute left-0 top-0 bottom-0 w-1 cursor-col-resize hover:bg-amber-500/50 transition-colors z-10",
                isResizing && "bg-amber-500"
              )}
            >
              <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2">
                <GripVertical className="h-4 w-4 text-zinc-600" />
              </div>
            </div>
          )}

          {/* Header */}
          <div className="flex items-center justify-between border-b border-zinc-800 px-6 py-4 flex-shrink-0">
            <div className="flex items-center gap-3 min-w-0 flex-1">
              <FileIcon className={cn("h-5 w-5 flex-shrink-0", iconColor)} />
              <div className="min-w-0 flex-1">
                <h2 className="text-lg font-semibold text-zinc-100 truncate">{file.name}</h2>
                <div className="flex items-center gap-2 mt-1">
                  <span className={cn(
                    "inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs font-medium",
                    statusBadge.bg,
                    statusBadge.color,
                    statusBadge.border
                  )}>
                    {(() => {
                      const StatusIcon = statusBadge.icon;
                      return <StatusIcon className="h-3 w-3" />;
                    })()}
                    {statusBadge.label}
                  </span>
                </div>
              </div>
            </div>
            <div className="flex items-center gap-2 ml-4">
              <button
                type="button"
                onClick={handleOpenExternally}
                className="inline-flex items-center gap-2 rounded-lg border border-zinc-700 bg-transparent px-3 py-2 text-sm font-medium text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-zinc-100 flex-shrink-0"
                aria-label="Open in App"
              >
                <ExternalLink className="h-4 w-4" />
                <span>Open in App</span>
              </button>
              <button
                type="button"
                onClick={onClose}
                className="rounded-lg p-2 text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-100 flex-shrink-0"
                aria-label="Close"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
          </div>

          {/* Main Content - 2 Column Grid */}
          {/* Isolated columns to prevent layout shifts */}
          <div className="flex-1 flex overflow-hidden">
            {/* Column 1: Preview (2/3) - Isolated from comment state */}
            <div className="flex-[2] flex flex-col overflow-hidden border-r border-zinc-800 bg-zinc-950 isolate">
              {/* Custom Toolbar with Zoom and Download Controls */}
              <div className="flex items-center justify-between border-b border-zinc-800 px-4 py-2 bg-zinc-900/50 flex-shrink-0">
                <div className="flex items-center gap-2">
                  <span className="text-xs text-zinc-400 font-medium">Zoom: {Math.round(zoom * 100)}%</span>
                </div>
                <div className="flex items-center gap-2">
                  {/* Zoom Out */}
                  <button
                    type="button"
                    onClick={handleZoomOut}
                    disabled={zoom <= 0.5}
                    className="inline-flex items-center justify-center rounded-lg p-1.5 text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-100 disabled:opacity-50 disabled:cursor-not-allowed"
                    aria-label="Zoom Out"
                  >
                    <ZoomOut className="h-4 w-4" />
                  </button>
                  {/* Zoom In */}
                  <button
                    type="button"
                    onClick={handleZoomIn}
                    disabled={zoom >= 3}
                    className="inline-flex items-center justify-center rounded-lg p-1.5 text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-100 disabled:opacity-50 disabled:cursor-not-allowed"
                    aria-label="Zoom In"
                  >
                    <ZoomIn className="h-4 w-4" />
                  </button>
                  {/* Download */}
                  <button
                    type="button"
                    onClick={handleDownload}
                    className="inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-xs font-medium text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-zinc-100 border border-zinc-700"
                    aria-label="Download"
                  >
                    <Download className="h-3.5 w-3.5" />
                    <span>Download</span>
                  </button>
                </div>
              </div>
              {/* Main viewer area: image, PDF (original or preview), or fallback */}
              <div className="flex-1 overflow-auto bg-zinc-950">
                {file.type === "img" && effectiveUrl ? (
                  <div className="flex items-center justify-center h-full p-6">
                    <img
                      src={effectiveUrl}
                      alt={file.name}
                      className="max-h-full max-w-full rounded-lg border border-zinc-800 object-contain"
                    />
                  </div>
                ) : !effectiveUrl ? (
                  isRefreshingPreview ? (
                    <div className="flex h-full items-center justify-center">
                      <div className="flex items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-900/70 px-4 py-2 text-sm text-zinc-300">
                        <Loader2 className="h-4 w-4 animate-spin text-amber-400" />
                        Refreshing preview...
                      </div>
                    </div>
                  ) : (
                  <div className="flex flex-col items-center justify-center h-full p-8 text-center bg-zinc-950">
                    <div className="max-w-md space-y-4">
                      <div className="rounded-full bg-zinc-800/50 p-4 inline-block">
                        <AlertCircle className="h-12 w-12 text-zinc-400" />
                      </div>
                      <div className="space-y-2">
                        <h3 className="text-lg font-semibold text-zinc-100">Preview unavailable</h3>
                        <p className="text-sm text-zinc-500">
                          This file cannot be previewed right now.
                        </p>
                      </div>
                      <div className="flex gap-3 justify-center">
                        <button
                          type="button"
                          onClick={handleDownload}
                          className="inline-flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-800/50 px-4 py-2 text-sm font-medium text-zinc-100 transition-colors hover:bg-zinc-700 hover:border-zinc-600"
                        >
                          <Download className="h-4 w-4" />
                          Download
                        </button>
                        <button
                          type="button"
                          onClick={handleOpenExternally}
                          className="inline-flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-800/50 px-4 py-2 text-sm font-medium text-zinc-100 transition-colors hover:bg-zinc-700 hover:border-zinc-600"
                        >
                          <ExternalLink className="h-4 w-4" />
                          Open in new tab
                        </button>
                      </div>
                    </div>
                  </div>
                  )
                ) : effectiveType === "pdf" ? (
                  <div className="w-full h-full">
                    <iframe
                      src={effectiveUrl}
                      style={{ width: "100%", height: "70vh" }}
                      className="border-0"
                      title={`Preview of ${file.name}`}
                    />
                  </div>
                ) : (
                  // Non-image docs: render with DocViewer (docx/xlsx/pptx or non-pdf formats)
                  <div
                    style={{
                      transform: `scale(${zoom})`,
                      transformOrigin: "top center",
                      width: `${100 / zoom}%`,
                      height: `${100 / zoom}%`,
                    }}
                  >
                    <StableDocViewer documents={documents} config={docViewerConfig} />
                  </div>
                )}
              </div>
            </div>

            {/* Column 2: Collaboration (1/3) - Isolated from viewer state */}
            <div className="flex-1 flex flex-col overflow-hidden bg-zinc-900/30 isolate">
              {/* Comments Header */}
              <div className="border-b border-zinc-800 px-4 py-3 flex-shrink-0">
                <h3 className="text-sm font-semibold text-zinc-100">Team Comments</h3>
                <p className="text-xs text-zinc-500 mt-0.5">{comments.length} comments</p>
              </div>

              {/* Comments List */}
              <div className="flex-1 overflow-y-auto p-4 space-y-4 scrollbar-thin">
                {isLoadingComments ? (
                  <div className="text-sm text-zinc-500">Loading comments...</div>
                ) : null}
                {comments.map((comment) => (
                  <div
                    key={comment.id}
                    className="rounded-lg border border-zinc-800 bg-zinc-900/50 p-3"
                  >
                    <div className="flex items-start gap-3">
                      {/* Avatar */}
                      <div className="flex h-8 w-8 flex-shrink-0 items-center justify-center rounded-full border-2 border-zinc-800 bg-zinc-700 text-xs font-medium text-zinc-100">
                        {getInitials(comment.authorDisplayName)}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1">
                          <span className="text-sm font-medium text-zinc-100">
                            {comment.authorDisplayName}
                          </span>
                          <span className="text-xs text-zinc-500">{formatCommentTimestamp(comment.createdAt)}</span>
                        </div>
                        <p className="text-sm text-zinc-300 whitespace-pre-wrap break-words">
                          {comment.content.split(" ").map((word, idx) => {
                            // Highlight mentions
                            if (word.startsWith("@")) {
                              return (
                                <span
                                  key={idx}
                                  className="text-amber-400 font-medium"
                                >
                                  {word}{" "}
                                </span>
                              );
                            }
                            return <span key={idx}>{word} </span>;
                          })}
                        </p>
                      </div>
                    </div>
                  </div>
                ))}
                <div ref={commentsEndRef} />
              </div>

              {/* Comment Input */}
              <div className="border-t border-zinc-800 p-4 flex-shrink-0">
                <div className="flex items-end gap-2">
                  <textarea
                    value={commentInput}
                    onChange={(e) => setCommentInput(e.target.value)}
                    onKeyDown={handleKeyPress}
                    placeholder="Write a comment... (@ to mention)"
                    rows={2}
                    className="flex-1 rounded-lg border border-zinc-800 bg-zinc-900/50 px-3 py-2 text-sm text-zinc-100 placeholder:text-zinc-600 focus:outline-none focus:ring-2 focus:ring-amber-500/50 resize-none"
                  />
                  <button
                    type="button"
                    onClick={() => void handleSendComment()}
                    disabled={!commentInput.trim() || isSendingComment}
                    className="rounded-lg bg-amber-500/10 border border-amber-500/20 p-2 text-amber-400 transition-colors hover:bg-amber-500/20 disabled:opacity-50 disabled:cursor-not-allowed flex-shrink-0"
                    aria-label="Send comment"
                  >
                    <Send className="h-4 w-4" />
                  </button>
                </div>
                {commentInput.includes("@") && mentionMembers.length > 0 ? (
                  <div className="mt-2 flex flex-wrap gap-1">
                    {mentionMembers.slice(0, 8).map((member) => (
                      <button
                        key={member.user_id}
                        type="button"
                        onClick={() =>
                          setCommentInput((prev) => `${prev} @${member.display_name.replace(/\s+/g, "_")} `)
                        }
                        className="rounded border border-zinc-700 bg-zinc-800/60 px-2 py-1 text-[11px] text-zinc-300 hover:bg-zinc-700/70"
                      >
                        @{member.display_name}
                      </button>
                    ))}
                  </div>
                ) : null}
                <p className="text-xs text-zinc-500 mt-2">
                  Press Enter to send, Shift+Enter for new line
                </p>
              </div>
            </div>
          </div>
        </motion.div>
      </>
    </AnimatePresence>
  );
}
