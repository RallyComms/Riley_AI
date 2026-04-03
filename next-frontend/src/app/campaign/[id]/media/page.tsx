"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Download, Video, Music, Image as ImageIcon, X, Upload, FileText, FileType2, Table, Presentation, Loader2, Link2, ExternalLink, Plus } from "lucide-react";
import { RileyContextChat } from "@app/components/campaign/RileyContextChat";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { Asset } from "@app/lib/types";
import { cn } from "@app/lib/utils";
import { apiFetch, ApiRequestError } from "@app/lib/api";
import { toAsset } from "@app/lib/files";

const MAX_UPLOAD_BATCH_SIZE = 10;
const UPLOAD_STATUS_POLL_INTERVAL_MS = 3000;
const UPLOAD_STATUS_MAX_POLL_MS = 180000;
const TERMINAL_INGESTION_STATUSES = new Set(["indexed", "partial", "failed", "low_text", "ocr_needed"]);
const TRANSIENT_INGESTION_STATUSES = new Set(["queued", "processing", "uploaded"]);

type UploadStatusItem = {
  fileName: string;
  status: "pending" | "uploading" | "queued" | "failed";
  message?: string;
};

type ExternalMediaLink = {
  id: string;
  title: string;
  url: string;
  kind: "video" | "link";
  createdAt: string;
};

function detectExternalLinkKind(url: string): "video" | "link" {
  const normalized = url.toLowerCase();
  if (
    normalized.includes("youtube.com") ||
    normalized.includes("youtu.be") ||
    normalized.includes("vimeo.com") ||
    normalized.includes("loom.com") ||
    normalized.endsWith(".mp4") ||
    normalized.endsWith(".webm") ||
    normalized.endsWith(".mov")
  ) {
    return "video";
  }
  return "link";
}

function hostnameFromUrl(rawUrl: string): string {
  try {
    return new URL(rawUrl).hostname.replace(/^www\./, "");
  } catch {
    return rawUrl;
  }
}

function mediaKindFromAsset(asset: Asset): "video" | "audio" | "image" | "file" {
  const lowerName = asset.name.toLowerCase();
  if (lowerName.match(/\.(mp4|webm|mov|m4v)$/i)) return "video";
  if (lowerName.match(/\.(mp3|wav|ogg|m4a)$/i)) return "audio";
  if (asset.type === "img") return "image";
  return "file";
}

function getFileIcon(type: Asset["type"]) {
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
      return { icon: ImageIcon, color: "text-purple-500" };
  }
}

function ingestionBadgeClass(status?: Asset["ingestionStatus"]): string {
  switch (status) {
    case "queued":
      return "border-amber-500/30 bg-amber-500/10 text-amber-300";
    case "processing":
      return "border-blue-500/30 bg-blue-500/10 text-blue-300";
    case "indexed":
      return "border-emerald-500/30 bg-emerald-500/10 text-emerald-300";
    case "failed":
    case "low_text":
      return "border-rose-500/30 bg-rose-500/10 text-rose-300";
    case "ocr_needed":
      return "border-purple-500/30 bg-purple-500/10 text-purple-300";
    case "partial":
      return "border-orange-500/30 bg-orange-500/10 text-orange-300";
    default:
      return "border-zinc-700 bg-zinc-800/40 text-zinc-400";
  }
}

function multimodalBadgeClass(kind: "ocr_processed" | "vision_processed" | "partial"): string {
  switch (kind) {
    case "ocr_processed":
      return "border-cyan-500/30 bg-cyan-500/10 text-cyan-300";
    case "vision_processed":
      return "border-fuchsia-500/30 bg-fuchsia-500/10 text-fuchsia-300";
    case "partial":
      return "border-orange-500/30 bg-orange-500/10 text-orange-300";
    default:
      return "border-zinc-700 bg-zinc-800/40 text-zinc-400";
  }
}

function formatStatusLabel(value: string): string {
  return value.replaceAll("_", " ");
}

export default function MediaPage() {
  const params = useParams();
  const campaignId = params.id as string;
  const { getToken } = useAuth();
  if (process.env.NODE_ENV !== "production") {
    console.log("[MediaPage] campaignId:", campaignId);
  }
  const [assets, setAssets] = useState<Asset[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isUploading, setIsUploading] = useState(false);
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [selectedFile, setSelectedFile] = useState<Asset | null>(null);
  const [externalLinks, setExternalLinks] = useState<ExternalMediaLink[]>([]);
  const [isAddLinkOpen, setIsAddLinkOpen] = useState(false);
  const [newLinkTitle, setNewLinkTitle] = useState("");
  const [newLinkUrl, setNewLinkUrl] = useState("");
  const [toast, setToast] = useState<{ kind: "success" | "error"; message: string } | null>(null);
  const [uploadStatuses, setUploadStatuses] = useState<UploadStatusItem[]>([]);
  const [recentUploadedNames, setRecentUploadedNames] = useState<string[]>([]);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const uploadPollIntervalRef = useRef<number | null>(null);
  const uploadPollStartedAtRef = useRef<number | null>(null);

  const normalizeFilename = useCallback((name: string) => name.trim().toLowerCase(), []);

  const stopUploadStatusPolling = useCallback(() => {
    if (uploadPollIntervalRef.current !== null) {
      window.clearInterval(uploadPollIntervalRef.current);
      uploadPollIntervalRef.current = null;
    }
    uploadPollStartedAtRef.current = null;
  }, []);

  const showToast = useCallback((kind: "success" | "error", message: string) => {
    setToast({ kind, message });
    window.setTimeout(() => {
      setToast((current) => (current?.message === message ? null : current));
    }, 3200);
  }, []);

  // Fetch files from backend and filter by Media tag
  const fetchFiles = useCallback(async (): Promise<Asset[]> => {
    if (!campaignId) return [];

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }
        
      const data = await apiFetch<{ files: any[] }>(
        `/api/v1/list?tenant_id=${encodeURIComponent(campaignId)}`,
        {
          token,
          method: "GET",
        }
      );

      // Convert backend file list to Asset format and filter by Media tag
      const fetchedAssets: Asset[] = data.files
        .filter((file: any) => {
          const tags = Array.isArray(file.tags) ? file.tags : [];
          return tags.includes("Media");
        })
        .map((file: any) => toAsset(file, { status: "ready" }));

      setAssets(fetchedAssets);
      return fetchedAssets;
    } catch (error) {
      console.error("Error fetching files:", error);
      setAssets([]);
      return [];
    } finally {
      setIsLoading(false);
    }
  }, [campaignId, getToken]);

  useEffect(() => {
    fetchFiles();
  }, [fetchFiles]);

  useEffect(() => {
    if (!campaignId) return;
    try {
      const raw = window.localStorage.getItem(`campaign_media_links_${campaignId}`);
      if (!raw) return;
      const parsed = JSON.parse(raw) as ExternalMediaLink[];
      if (Array.isArray(parsed)) {
        setExternalLinks(
          parsed.filter(
            (item) =>
              item &&
              typeof item.id === "string" &&
              typeof item.title === "string" &&
              typeof item.url === "string" &&
              (item.kind === "video" || item.kind === "link")
          )
        );
      }
    } catch (error) {
      console.warn("Failed to load campaign media links:", error);
    }
  }, [campaignId]);

  useEffect(() => {
    if (!campaignId) return;
    try {
      window.localStorage.setItem(`campaign_media_links_${campaignId}`, JSON.stringify(externalLinks));
    } catch {
      // ignore persistence errors
    }
  }, [campaignId, externalLinks]);

  useEffect(() => {
    return () => {
      stopUploadStatusPolling();
    };
  }, [stopUploadStatusPolling]);

  useEffect(() => {
    if (!isChatOpen) return;
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsChatOpen(false);
      }
    };
    const shouldLockBody = window.innerWidth < 1024;
    const previousOverflow = document.body.style.overflow;
    if (shouldLockBody) {
      document.body.style.overflow = "hidden";
    }
    window.addEventListener("keydown", handleEscape);
    return () => {
      window.removeEventListener("keydown", handleEscape);
      if (shouldLockBody) {
        document.body.style.overflow = previousOverflow;
      }
    };
  }, [isChatOpen]);

  const areRecentUploadsSettled = useCallback(
    (latestAssets: Asset[], trackedNames: string[]): boolean => {
      if (trackedNames.length === 0) return true;
      const byName = new Map<string, Asset>();
      for (const asset of latestAssets) {
        byName.set(normalizeFilename(asset.name), asset);
      }
      for (const trackedName of trackedNames) {
        const asset = byName.get(normalizeFilename(trackedName));
        if (!asset) return false;
        const status = String(asset.ingestionStatus || "uploaded").toLowerCase();
        if (TRANSIENT_INGESTION_STATUSES.has(status)) return false;
        if (!TERMINAL_INGESTION_STATUSES.has(status)) return false;
      }
      return true;
    },
    [normalizeFilename]
  );

  useEffect(() => {
    if (recentUploadedNames.length === 0) {
      stopUploadStatusPolling();
      return;
    }
    if (uploadPollIntervalRef.current !== null) return;

    uploadPollStartedAtRef.current = Date.now();

    const pollOnce = async () => {
      const latestAssets = await fetchFiles();
      if (areRecentUploadsSettled(latestAssets, recentUploadedNames)) {
        stopUploadStatusPolling();
        setRecentUploadedNames([]);
        return;
      }
      if (
        uploadPollStartedAtRef.current !== null &&
        Date.now() - uploadPollStartedAtRef.current > UPLOAD_STATUS_MAX_POLL_MS
      ) {
        stopUploadStatusPolling();
      }
    };

    void pollOnce();
    uploadPollIntervalRef.current = window.setInterval(() => {
      void pollOnce();
    }, UPLOAD_STATUS_POLL_INTERVAL_MS);
  }, [areRecentUploadsSettled, fetchFiles, recentUploadedNames, stopUploadStatusPolling]);

  const handleUploadClick = () => {
    if (!isUploading) {
      fileInputRef.current?.click();
    }
  };

  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    if (process.env.NODE_ENV !== "production") {
      console.log("[MediaPage] file select triggered");
      console.log("[MediaPage] selected files:", event.target.files);
    }
    const files = Array.from(event.target.files || []);
    if (!files.length || !campaignId) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[MediaPage] aborting upload: missing files or campaignId", { files: files.length, campaignId });
      }
      return;
    }
    if (files.length > MAX_UPLOAD_BATCH_SIZE) {
      showToast("error", `You can upload up to ${MAX_UPLOAD_BATCH_SIZE} files at once.`);
      if (fileInputRef.current) fileInputRef.current.value = "";
      return;
    }

    setIsUploading(true);
    setUploadStatuses(files.map((file) => ({ fileName: file.name, status: "pending" })));

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }

      const successfulUploads: string[] = [];
      for (const file of files) {
        setUploadStatuses((prev) =>
          prev.map((item) =>
            item.fileName === file.name
              ? { ...item, status: "uploading", message: "Uploading..." }
              : item
          )
        );
        const formData = new FormData();
        formData.append("file", file);
        formData.append("tenant_id", campaignId);
        formData.append("tags", "Media");

        if (process.env.NODE_ENV !== "production") {
          console.log("[upload] FormData keys:", Array.from(formData.keys()));
          console.log("[upload] selected file:", { name: file.name, size: file.size, type: file.type });
        }

        try {
          await apiFetch<{ id: string; url: string; filename: string; type?: string }>("/api/v1/upload", {
            token,
            method: "POST",
            body: formData,
            headers: { "X-Upload-Batch-Size": String(files.length) },
          });
          successfulUploads.push(file.name);

          setUploadStatuses((prev) =>
            prev.map((item) =>
              item.fileName === file.name
                ? { ...item, status: "queued", message: "Indexing queued" }
                : item
            )
          );
        } catch (error) {
          const message = error instanceof Error ? error.message : "Upload failed";
          setUploadStatuses((prev) =>
            prev.map((item) =>
              item.fileName === file.name
                ? { ...item, status: "failed", message }
                : item
            )
          );
          throw error;
        }
      }

      await fetchFiles();
      if (successfulUploads.length > 0) {
        setRecentUploadedNames((prev) => {
          const merged = new Set([...prev, ...successfulUploads.map((name) => normalizeFilename(name))]);
          return Array.from(merged);
        });
      }
      showToast("success", `Queued indexing for ${files.length} file(s).`);
    } catch (error) {
      if (error instanceof ApiRequestError) {
        console.error("[upload] failed request", {
          status: error.status,
          statusText: error.statusText,
          responseBody: error.responseBody,
        });
      } else {
        console.error("[upload] failed:", error);
      }
      showToast("error", `Upload failed: ${error instanceof Error ? error.message : "Unknown error"}`);
    } finally {
      setIsUploading(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  };

  // Handle viewing an asset from citation badge
  const handleViewAsset = (filename: string) => {
    // Search for the asset in the local assets state
    const asset = assets.find((a) => a.name === filename);
    
    if (asset) {
      setSelectedFile(asset);
    } else {
      // Asset not found in current view (might be Global/Archived)
      alert(`Asset "${filename}" not found in current view (might be Global/Archived).`);
    }
  };

  const handleDownload = (asset: Asset) => {
    // Trigger download
    const link = document.createElement("a");
    link.href = asset.url;
    link.download = asset.name;
    link.target = "_blank";
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const handleCreateExternalLink = () => {
    const title = newLinkTitle.trim();
    const url = newLinkUrl.trim();
    if (!title || !url) {
      showToast("error", "Title and URL are required for a media link.");
      return;
    }
    const normalizedUrl = /^https?:\/\//i.test(url) ? url : `https://${url}`;
    try {
      const parsed = new URL(normalizedUrl);
      if (!parsed.hostname) {
        throw new Error("Invalid URL");
      }
    } catch {
      showToast("error", "Please provide a valid external URL.");
      return;
    }

    const linkItem: ExternalMediaLink = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
      title,
      url: normalizedUrl,
      kind: detectExternalLinkKind(normalizedUrl),
      createdAt: new Date().toISOString(),
    };
    setExternalLinks((prev) => [linkItem, ...prev]);
    setNewLinkTitle("");
    setNewLinkUrl("");
    setIsAddLinkOpen(false);
    showToast("success", "Media link added.");
  };

  const mediaRows = [
    ...externalLinks.map((link) => ({
      id: `link-${link.id}`,
      source: "link" as const,
      kind: link.kind,
      title: link.title,
      sublabel: hostnameFromUrl(link.url),
      dateLabel: new Date(link.createdAt).toLocaleDateString(undefined, { month: "short", day: "numeric" }),
      url: link.url,
      asset: null as Asset | null,
    })),
    ...assets.map((asset) => {
      const kind = mediaKindFromAsset(asset);
      return {
        id: `asset-${asset.id}`,
        source: "asset" as const,
        kind,
        title: asset.name,
        sublabel: `${asset.type.toUpperCase()}${asset.size ? ` • ${asset.size}` : ""}`,
        dateLabel: asset.uploadDate
          ? new Date(asset.uploadDate).toLocaleDateString(undefined, { month: "short", day: "numeric" })
          : "—",
        url: asset.url,
        asset,
      };
    }),
  ];
  const externalLinkCount = mediaRows.filter((item) => item.source === "link").length;
  const uploadedFileCount = mediaRows.filter((item) => item.source === "asset").length;

  return (
    <div className="relative flex h-full min-h-0 bg-[#f7f5ef]">
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <header className="flex flex-wrap items-end justify-between gap-4 border-b border-[#e5ddce] px-6 py-5 lg:px-8">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-[#1f2a44]">Media</h1>
            <p className="mt-1 text-sm text-[#6f788a]">
              Campaign media library for files and external links.
            </p>
            <p className="mt-1 text-xs text-[#8a90a0]">
              Use Media for videos, brochures, websites, and other reference-ready items.
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <input
              ref={fileInputRef}
              type="file"
              className="hidden"
              accept=".pdf,.docx,.doc,.xlsx,.xls,.pptx,.ppt,.png,.jpg,.jpeg,.webp,.gif,.svg,.mp4,.webm,.mov,.mp3,.wav,.ogg"
              multiple
              onChange={handleFileSelect}
            />
            <button
              type="button"
              onClick={handleUploadClick}
              disabled={isUploading}
              className="inline-flex items-center gap-2 rounded-md border border-[#d8d0bf] bg-white px-3 py-1.5 text-sm font-medium text-[#1f2a44] transition-colors hover:bg-[#f1ece2] disabled:opacity-60"
            >
              {isUploading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Upload className="w-4 h-4" />}
              <span>{isUploading ? "Uploading..." : "Upload Media"}</span>
            </button>
            <button
              type="button"
              onClick={() => setIsAddLinkOpen(true)}
              className="inline-flex items-center gap-2 rounded-md border border-[#d8d0bf] bg-white px-3 py-1.5 text-sm font-medium text-[#1f2a44] transition-colors hover:bg-[#f1ece2]"
            >
              <Link2 className="h-4 w-4" />
              Add Link
            </button>
            <button
              onClick={() => setIsChatOpen((prev) => !prev)}
              className={`inline-flex items-center gap-2 rounded-md border px-3 py-1.5 text-sm font-medium transition-colors ${
                isChatOpen
                  ? "border-[#ccb67d] bg-[#f2ebd6] text-[#1f2a44]"
                  : "border-[#d8d0bf] bg-white text-[#1f2a44] hover:bg-[#f1ece2]"
              }`}
              aria-expanded={isChatOpen}
              aria-controls="media-riley-panel"
            >
              <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-[#eadfb7] text-[11px] font-semibold text-[#6d560f]">R</span>
              Ask Riley
            </button>
          </div>
        </header>
        <div className="border-b border-[#e5ddce] bg-[#f6f1e2] px-6 py-2.5 text-xs text-[#6f5d31] lg:px-8">
          External media links are currently saved locally in this browser only. They are not shared campaign-wide yet.
        </div>
        {uploadStatuses.length > 0 && (
          <div className="border-b border-[#e5ddce] bg-white/70 px-6 py-2 lg:px-8">
            <div className="mb-1 text-xs font-medium text-[#6f788a]">Upload Progress</div>
            <div className="space-y-1">
              {uploadStatuses.map((item) => (
                <div key={item.fileName} className="flex items-center justify-between text-xs text-[#1f2a44]">
                  <span className="max-w-[70%] truncate">{item.fileName}</span>
                  <span className="text-[#8a90a0]">{item.message || item.status}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="flex-1 overflow-y-auto px-6 py-5 lg:px-8">
          {isLoading ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-sm text-[#8a90a0]">Loading media...</p>
            </div>
          ) : mediaRows.length === 0 ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-sm text-[#8a90a0]">No media items yet. Upload files or add external links.</p>
            </div>
          ) : (
            <div className="overflow-hidden rounded-xl border border-[#e5ddce] bg-[#fcfbf8]">
              <div className="flex items-center gap-2 border-b border-[#e5ddce] px-4 py-2 text-xs">
                <span className="rounded-full bg-[#ece6d9] px-2 py-0.5 text-[#6f788a]">
                  Uploaded files: {uploadedFileCount}
                </span>
                <span className="rounded-full bg-[#efe8d1] px-2 py-0.5 text-[#6f5d31]">
                  External links: {externalLinkCount}
                </span>
              </div>
              <div className="flex items-center gap-4 border-b border-[#e5ddce] px-4 py-2.5">
                <span className="flex-1 text-[10px] font-semibold uppercase tracking-wider text-[#8a90a0]">Media Item</span>
                <span className="w-24 text-[10px] font-semibold uppercase tracking-wider text-[#8a90a0]">Type</span>
                <span className="w-20 text-right text-[10px] font-semibold uppercase tracking-wider text-[#8a90a0]">Added</span>
                <span className="w-10" />
              </div>
              <div className="divide-y divide-[#efe8da]">
                {mediaRows.map((item) => {
                  const icon =
                    item.kind === "video"
                      ? Video
                      : item.kind === "audio"
                      ? Music
                      : item.kind === "image"
                      ? ImageIcon
                      : item.source === "link"
                      ? Link2
                      : FileText;
                  const Icon = icon;
                  const typeLabel =
                    item.source === "link"
                      ? item.kind === "video"
                        ? "Video link"
                        : "External link"
                      : item.kind === "video"
                      ? "Video file"
                      : item.kind === "audio"
                      ? "Audio file"
                      : item.kind === "image"
                      ? "Image file"
                      : "Document file";

                  return (
                    <div
                      key={item.id}
                      className={`group flex items-center gap-4 px-4 py-3 transition-colors hover:bg-[#f5f1e8] ${
                        item.source === "link" ? "border-l-2 border-[#d4ad47]/70 pl-3.5" : ""
                      }`}
                    >
                      <button
                        type="button"
                        className="flex min-w-0 flex-1 items-center gap-3 text-left"
                        onClick={() => {
                          if (item.source === "link") {
                            window.open(item.url, "_blank", "noopener,noreferrer");
                            return;
                          }
                          if (item.asset) {
                            setSelectedFile(item.asset);
                          }
                        }}
                      >
                        <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-[#ece6d9] text-[#1f2a44]">
                          <Icon className="h-4 w-4" />
                        </div>
                        <div className="min-w-0">
                          <p className="truncate text-sm font-medium text-[#1f2a44]">{item.title}</p>
                          <p className="truncate text-[11px] text-[#8a90a0]">{item.sublabel}</p>
                        </div>
                      </button>
                      <span className="w-24 text-xs text-[#6f788a]">{typeLabel}</span>
                      <span className="w-20 text-right text-xs text-[#8a90a0]">{item.dateLabel}</span>
                      {item.source === "asset" && item.asset ? (
                        <button
                          type="button"
                          onClick={() => handleDownload(item.asset as Asset)}
                          className="rounded-md p-1.5 text-[#8a90a0] opacity-0 transition group-hover:opacity-100 hover:bg-[#ece6d9] hover:text-[#1f2a44]"
                          aria-label="Download media file"
                        >
                          <Download className="h-4 w-4" />
                        </button>
                      ) : (
                        <button
                          type="button"
                          onClick={() => window.open(item.url, "_blank", "noopener,noreferrer")}
                          className="rounded-md p-1.5 text-[#8a90a0] opacity-100 transition hover:bg-[#ece6d9] hover:text-[#1f2a44]"
                          aria-label="Open external media link"
                        >
                          <ExternalLink className="h-4 w-4" />
                        </button>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>

      {isChatOpen && (
        <>
          <button
            type="button"
            className="fixed inset-0 z-30 bg-[#1f2a44]/20 backdrop-blur-[1px] lg:hidden"
            aria-label="Close Ask Riley panel backdrop"
            onClick={() => setIsChatOpen(false)}
          />
          <aside
            id="media-riley-panel"
            className="fixed inset-y-0 right-0 z-40 w-[94vw] max-w-[400px] border-l border-[#d8d0bf] bg-[#fcfbf8] shadow-xl shadow-[#1f2a44]/10 lg:static lg:z-auto lg:h-full lg:w-[360px] lg:max-w-none lg:shadow-none"
          >
          <div className="flex h-full flex-col">
            <div className="flex h-16 items-center justify-between border-b border-[#e5ddce] px-4">
              <span className="inline-flex items-center gap-2 text-sm font-semibold text-[#1f2a44]">
                <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-[#eadfb7] text-[11px] font-semibold text-[#6d560f]">R</span>
                Ask Riley
              </span>
              <button
                onClick={() => setIsChatOpen(false)}
                className="rounded-md p-1.5 text-[#6f788a] transition-colors hover:bg-[#f1ece2] hover:text-[#1f2a44]"
                aria-label="Close Ask Riley panel"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <div className="border-b border-[#e5ddce] bg-white/80 px-4 py-3">
              <p className="text-sm leading-relaxed text-[#1f2a44]">
                Welcome to Riley Media. I can help analyze campaign media files, evaluate external links, and synthesize supporting content in this workspace. I&apos;m getting much stronger in Riley V2.
              </p>
            </div>
            <div className="flex-1 overflow-hidden">
              <RileyContextChat
                mode="research"
                contextKey="media"
                campaignId={campaignId}
                onViewAsset={handleViewAsset}
              />
            </div>
          </div>
          </aside>
        </>
      )}

      {isAddLinkOpen ? (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-[#1f2a44]/30 p-4 backdrop-blur-sm">
          <div className="w-full max-w-md rounded-xl border border-[#d8d0bf] bg-[#fcfbf8] p-4 shadow-xl shadow-[#1f2a44]/10">
            <div className="mb-3 flex items-center justify-between">
              <h3 className="text-sm font-semibold text-[#1f2a44]">Add External Media Link</h3>
              <button
                type="button"
                onClick={() => setIsAddLinkOpen(false)}
                className="rounded p-1 text-[#6f788a] hover:bg-[#f1ece2] hover:text-[#1f2a44]"
                aria-label="Close add-link modal"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <div className="space-y-2">
              <input
                value={newLinkTitle}
                onChange={(e) => setNewLinkTitle(e.target.value)}
                placeholder="Link title"
                className="w-full rounded-md border border-[#d8d0bf] bg-white px-2.5 py-2 text-sm text-[#1f2a44]"
              />
              <input
                value={newLinkUrl}
                onChange={(e) => setNewLinkUrl(e.target.value)}
                placeholder="https://example.com/media"
                className="w-full rounded-md border border-[#d8d0bf] bg-white px-2.5 py-2 text-sm text-[#1f2a44]"
              />
              <p className="text-xs text-[#8a90a0]">
                Store videos, brochures, websites, and other external media links here.
              </p>
              <p className="text-[11px] text-[#9a8a5b]">
                Local-only for now: links are saved in this browser on this device.
              </p>
            </div>
            <div className="mt-4 flex items-center justify-end gap-2">
              <button
                type="button"
                onClick={() => setIsAddLinkOpen(false)}
                className="rounded-md border border-[#d8d0bf] px-3 py-1.5 text-xs font-medium text-[#6f788a] hover:bg-[#f1ece2]"
              >
                Cancel
              </button>
              <button
                type="button"
                onClick={handleCreateExternalLink}
                className="inline-flex items-center gap-1.5 rounded-md bg-[#d4ad47] px-3 py-1.5 text-xs font-medium text-[#1f2a44] hover:bg-[#c89e32]"
              >
                <Plus className="h-3.5 w-3.5" />
                Add Link
              </button>
            </div>
          </div>
        </div>
      ) : null}

      {/* Document Viewer Modal */}
      {selectedFile && (
        <DocumentViewer
          file={selectedFile}
          variant="modal"
          onClose={() => setSelectedFile(null)}
        />
      )}
      {toast && (
        <div
          role="status"
          className={cn(
            "fixed right-6 top-6 z-50 rounded-md border px-4 py-3 text-sm shadow-lg backdrop-blur-md",
            toast.kind === "success"
              ? "border-emerald-500/30 bg-emerald-500/12 text-emerald-700"
              : "border-red-500/30 bg-red-500/12 text-red-700"
          )}
        >
          {toast.message}
        </div>
      )}
    </div>
  );
}
