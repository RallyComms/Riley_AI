"use client";

import { useState, useRef, useEffect, useCallback, useMemo } from "react";
import { useParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Upload, Loader2 } from "lucide-react";
import { Asset, AssetTag } from "@app/lib/types";
import { AssetRow } from "@app/components/campaign/AssetRow";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { RenameAssetModal } from "@app/components/campaign/RenameAssetModal";
import { DeleteFileModal } from "@app/components/campaign/DeleteFileModal";
import { PromoteModal } from "@app/components/campaign/PromoteModal";
import { apiFetch, ApiRequestError } from "@app/lib/api";
import { uploadFileDirectToGcs, DirectUploadError } from "@app/lib/directUpload";
import { cn } from "@app/lib/utils";

const MAX_UPLOAD_BATCH_SIZE = 10;
const DIRECT_UPLOAD_MAX_MB = Number(process.env.NEXT_PUBLIC_DIRECT_UPLOAD_MAX_MB || "250");
const MAX_UPLOAD_MB = DIRECT_UPLOAD_MAX_MB;
const MAX_UPLOAD_BYTES = Math.max(1, Math.floor(MAX_UPLOAD_MB * 1024 * 1024));
const UPLOAD_STATUS_POLL_INTERVAL_MS = 10000;
const TERMINAL_INGESTION_STATUSES = new Set(["indexed", "partial", "failed", "low_text", "ocr_needed"]);
const TRANSIENT_INGESTION_STATUSES = new Set(["queued", "processing", "uploaded"]);
const TRANSIENT_MULTIMODAL_STATUSES = new Set(["pending", "ocr_attempted"]);
const TRANSIENT_AUX_STATUSES = new Set(["queued", "processing"]);

type UploadStatusItem = {
  fileName: string;
  status: "pending" | "uploading" | "queued" | "failed";
  message?: string;
};

function fileTooLargeMessage(): string {
  return `This file is too large. Current limit is ${MAX_UPLOAD_MB} MB.`;
}

// Helper function to determine if file type supports AI processing
function isAISupportedType(type: Asset["type"]): boolean {
  return type === "pdf" || type === "docx" || type === "xlsx" || type === "pptx" || type === "img";
}

// Mock initial data with new fields - using valid public URLs for testing
const initialAssets: Asset[] = [
  {
    id: "1",
    name: "Q3_Polling_Data.pdf",
    type: "pdf",
    url: "https://www.w3.org/WAI/ER/tests/xhtml/testfiles/resources/pdf/dummy.pdf",
    tags: ["Research"],
    uploadDate: "2024-01-15",
    uploader: "Sarah Chen",
    size: "2.4 MB",
    urgency: "medium",
    assignedTo: [],
    comments: 0,
    status: "ready",
    aiEnabled: true, // PDF supports AI
  },
  {
    id: "2",
    name: "Candidate_Bio_Draft.docx",
    type: "docx",
    url: "https://file-examples.com/storage/fe68c7b4c5c0b5a5b5b5b5b/2017/10/file_example_DOCX_10.docx",
    tags: ["Messaging", "Strategy"],
    uploadDate: "2024-01-14",
    uploader: "John Martinez",
    size: "850 KB",
    urgency: "low",
    assignedTo: ["Sarah Chen"],
    comments: 2,
    status: "needs_review",
    aiEnabled: true, // Docx supports AI
  },
  {
    id: "3",
    name: "Strategic_Plan_Q1.xlsx",
    type: "xlsx",
    url: "https://file-examples.com/storage/fe68c7b4c5c0b5a5b5b5b5b/2017/10/file_example_XLSX_10.xlsx",
    tags: ["Strategy"],
    uploadDate: "2024-01-13",
    uploader: "Alex Rivera",
    size: "1.2 MB",
    urgency: "medium",
    assignedTo: [],
    comments: 0,
    status: "ready",
    aiEnabled: false, // Excel files not AI-enabled by default
  },
  {
    id: "4",
    name: "Press_Release_Template.pptx",
    type: "pptx",
    url: "https://file-examples.com/storage/fe68c7b4c5c0b5a5b5b5b5b/2017/10/file_example_PPTX_10.pptx",
    tags: [],
    uploadDate: "2024-01-12",
    uploader: "Sarah Chen",
    size: "3.1 MB",
    urgency: "low",
    assignedTo: [],
    comments: 0,
    status: "ready",
    aiEnabled: false, // PowerPoint not AI-enabled by default
  },
  {
    id: "5",
    name: "Campaign_Photo.jpg",
    type: "img",
    url: "https://images.unsplash.com/photo-1557804506-669a67965ba0?auto=format&fit=crop&w=800&q=80",
    tags: ["Media"],
    uploadDate: "2024-01-16",
    uploader: "John Martinez",
    size: "45.2 MB",
    urgency: "high",
    assignedTo: ["Alex Rivera"],
    comments: 5,
    status: "in_review",
    aiEnabled: false, // Media files disabled for AI
  },
  {
    id: "6",
    name: "Client_Pitch_Deck_2024.pptx",
    type: "pptx",
    url: "https://file-examples.com/storage/fe68c7b4c5c0b5a5b5b5b5b/2017/10/file_example_PPTX_10.pptx",
    tags: ["Pitch"],
    uploadDate: "2024-01-17",
    uploader: "Sarah Chen",
    size: "8.7 MB",
    urgency: "high",
    assignedTo: ["John Martinez", "Alex Rivera"],
    comments: 12,
    status: "approved",
    aiEnabled: false, // PowerPoint not AI-enabled by default
  },
];

function getFileTypeFromExtension(filename: string): Asset["type"] {
  const extension = filename.split(".").pop()?.toLowerCase() || "";
  if (extension === "pdf") return "pdf";
  if (extension === "docx" || extension === "doc") return "docx";
  if (extension === "xlsx" || extension === "xls" || extension === "csv") return "xlsx";
  if (extension === "pptx" || extension === "ppt") return "pptx";
  if (["png", "jpg", "jpeg", "webp", "gif", "svg"].includes(extension)) return "img";
  return "pdf"; // Default
}

export default function CampaignAssetsPage() {
  const params = useParams();
  const { getToken } = useAuth();
  const campaignId = params.id as string;
  const [assets, setAssets] = useState<Asset[]>([]);
  const [isUploading, setIsUploading] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [selectedAsset, setSelectedAsset] = useState<Asset | null>(null);
  const [renamingAsset, setRenamingAsset] = useState<Asset | null>(null);
  const [deletingAsset, setDeletingAsset] = useState<Asset | null>(null);
  const [promotingAsset, setPromotingAsset] = useState<Asset | null>(null);
  const [uploadStatuses, setUploadStatuses] = useState<UploadStatusItem[]>([]);
  const [recentUploadedNames, setRecentUploadedNames] = useState<string[]>([]);
  const [toast, setToast] = useState<{ kind: "success" | "error"; message: string } | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const uploadPollIntervalRef = useRef<number | null>(null);
  const recentUploadedNamesRef = useRef<string[]>([]);

  const normalizeFilename = useCallback((name: string) => name.trim().toLowerCase(), []);

  useEffect(() => {
    recentUploadedNamesRef.current = recentUploadedNames;
  }, [recentUploadedNames]);

  const stopUploadStatusPolling = useCallback(() => {
    if (uploadPollIntervalRef.current !== null) {
      window.clearInterval(uploadPollIntervalRef.current);
      uploadPollIntervalRef.current = null;
    }
  }, []);

  const pauseUploadStatusPolling = useCallback(() => {
    if (uploadPollIntervalRef.current !== null) {
      window.clearInterval(uploadPollIntervalRef.current);
      uploadPollIntervalRef.current = null;
    }
  }, []);

  const showToast = useCallback((kind: "success" | "error", message: string) => {
    setToast({ kind, message });
    window.setTimeout(() => {
      setToast((current) => (current?.message === message ? null : current));
    }, 3200);
  }, []);

  // Fetch files from backend - reusable function
  const fetchFiles = useCallback(async (): Promise<Asset[]> => {
    if (!campaignId) return []; // Don't fetch if no campaign ID
    
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
      
      // Convert backend file list to Asset format
      const fetchedAssets: Asset[] = data.files.map((file: any) => {
        const baseType = getFileTypeFromExtension(file.name);
        const previewType = file.preview_type as string | undefined;

        // If server generated a PDF preview, treat as PDF for viewing
        const effectiveType = previewType === "pdf" ? "pdf" : baseType;

        return {
          id: file.id, // Qdrant UUID (required)
          name: file.name,
          type: effectiveType,
          url: file.url, // original file URL (used for download)
          previewUrl: file.preview_url ?? undefined,
          previewType: previewType ?? undefined,
          previewStatus: file.preview_status ?? undefined,
          previewError: file.preview_error ?? undefined,
          ingestionStatus: file.ingestion_status ?? undefined,
          ocrStatus: file.ocr_status ?? undefined,
          visionStatus: file.vision_status ?? undefined,
          multimodalStatus: file.multimodal_status ?? undefined,
          ocrProcessed: file.ocr_processed ?? false,
          visionProcessed: file.vision_processed ?? false,
          tags: file.tags || [], // Use tags from backend
          uploadDate: new Date(file.date).toISOString().split("T")[0],
          uploader: "System",
          size: file.size,
          urgency: "medium",
          assignedTo: [],
          comments: 0,
          status: "ready",
          aiEnabled: isAISupportedType(effectiveType) ? Boolean(file.ai_enabled) : false,
        };
      });
      
      setAssets(fetchedAssets);
      return fetchedAssets;
    } catch (error) {
      console.error("Error fetching files:", error);
      // Return empty array if fetch fails (Qdrant is source of truth)
      setAssets([]);
      return [];
    } finally {
      setIsLoading(false);
    }
  }, [campaignId, getToken]);

  // Fetch files from backend on mount
  useEffect(() => {
    fetchFiles();
  }, [fetchFiles]);

  useEffect(() => {
    return () => {
      stopUploadStatusPolling();
    };
  }, [stopUploadStatusPolling]);

  const normalizeStatus = useCallback((status: unknown) => String(status || "").trim().toLowerCase(), []);

  const isAssetInProcessingState = useCallback(
    (asset: Asset): boolean => {
      const ingestionStatus = normalizeStatus(asset.ingestionStatus);
      if (TRANSIENT_INGESTION_STATUSES.has(ingestionStatus)) return true;
      if (ingestionStatus && !TERMINAL_INGESTION_STATUSES.has(ingestionStatus)) return true;

      const ocrStatus = normalizeStatus(asset.ocrStatus);
      if (TRANSIENT_AUX_STATUSES.has(ocrStatus)) return true;

      const visionStatus = normalizeStatus(asset.visionStatus);
      if (TRANSIENT_AUX_STATUSES.has(visionStatus)) return true;

      const previewStatus = normalizeStatus(asset.previewStatus);
      if (TRANSIENT_AUX_STATUSES.has(previewStatus)) return true;

      const multimodalStatus = normalizeStatus(asset.multimodalStatus);
      if (TRANSIENT_MULTIMODAL_STATUSES.has(multimodalStatus)) return true;

      return asset.status === "processing";
    },
    [normalizeStatus]
  );

  const hasVisibleAssetsProcessing = useMemo(
    () => assets.some((asset) => isAssetInProcessingState(asset)),
    [assets, isAssetInProcessingState]
  );

  const areRecentUploadsSettled = useCallback(
    (latestAssets: Asset[], trackedNames: string[]): boolean => {
      if (trackedNames.length === 0) return true;
      const byName = new Map<string, Asset>();
      for (const asset of latestAssets) {
        byName.set(normalizeFilename(asset.name), asset);
      }

      for (const rawName of trackedNames) {
        const asset = byName.get(normalizeFilename(rawName));
        if (!asset) return false;
        if (isAssetInProcessingState(asset)) return false;
      }
      return true;
    },
    [isAssetInProcessingState, normalizeFilename]
  );

  useEffect(() => {
    if (recentUploadedNames.length === 0 && !hasVisibleAssetsProcessing) {
      stopUploadStatusPolling();
      return;
    }

    const pollOnce = async (): Promise<boolean> => {
      const latestAssets = await fetchFiles();
      const trackedNames = recentUploadedNamesRef.current;
      const recentUploadsSettled = areRecentUploadsSettled(latestAssets, trackedNames);
      if (trackedNames.length > 0 && recentUploadsSettled) {
        setRecentUploadedNames([]);
      }

      const hasProcessingAssets = latestAssets.some((asset) => isAssetInProcessingState(asset));
      if (!hasProcessingAssets && (trackedNames.length === 0 || recentUploadsSettled)) {
        stopUploadStatusPolling();
        return false;
      }

      return true;
    };

    const startPollingInterval = () => {
      if (uploadPollIntervalRef.current !== null || document.visibilityState !== "visible") {
        return;
      }
      uploadPollIntervalRef.current = window.setInterval(() => {
        void pollOnce();
      }, UPLOAD_STATUS_POLL_INTERVAL_MS);
    };

    if (document.visibilityState === "visible") {
      void pollOnce().then((shouldContinue) => {
        if (!shouldContinue) return;
        startPollingInterval();
      });
    } else {
      pauseUploadStatusPolling();
    }

    const handleVisibilityChange = () => {
      if (document.visibilityState !== "visible") {
        pauseUploadStatusPolling();
        return;
      }
      void pollOnce().then((shouldContinue) => {
        if (!shouldContinue) return;
        startPollingInterval();
      });
    };

    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => {
      document.removeEventListener("visibilitychange", handleVisibilityChange);
      pauseUploadStatusPolling();
    };
  }, [
    areRecentUploadsSettled,
    fetchFiles,
    hasVisibleAssetsProcessing,
    isAssetInProcessingState,
    pauseUploadStatusPolling,
    recentUploadedNames,
    stopUploadStatusPolling,
  ]);

  const handleUpload = () => {
    fileInputRef.current?.click();
  };

  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFiles = Array.from(event.target.files || []);
    if (selectedFiles.length === 0) return;

    if (selectedFiles.length > MAX_UPLOAD_BATCH_SIZE) {
      showToast("error", `You can upload up to ${MAX_UPLOAD_BATCH_SIZE} files at once.`);
      if (fileInputRef.current) fileInputRef.current.value = "";
      return;
    }

    const oversizedFiles = selectedFiles.filter((file) => file.size > MAX_UPLOAD_BYTES);
    const uploadCandidates = selectedFiles.filter((file) => file.size <= MAX_UPLOAD_BYTES);
    if (uploadCandidates.length === 0) {
      setUploadStatuses(
        selectedFiles.map((file) => ({
          fileName: file.name,
          status: "failed",
          message: fileTooLargeMessage(),
        }))
      );
      showToast("error", fileTooLargeMessage());
      if (fileInputRef.current) fileInputRef.current.value = "";
      return;
    }

    setIsUploading(true);
    setUploadStatuses(
      selectedFiles.map((file) => {
        if (file.size > MAX_UPLOAD_BYTES) {
          return { fileName: file.name, status: "failed" as const, message: fileTooLargeMessage() };
        }
        return { fileName: file.name, status: "pending" as const };
      })
    );
    if (oversizedFiles.length > 0) {
      showToast("error", fileTooLargeMessage());
    }

    const uploadFile = async (file: File): Promise<void> => {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }
      await uploadFileDirectToGcs({
        token,
        tenantId: campaignId,
        surface: "assets",
        file,
        onProgress: (uploadedBytes, totalBytes) => {
          if (totalBytes <= 0) return;
          const percent = Math.min(100, Math.round((uploadedBytes / totalBytes) * 100));
          setUploadStatuses((prev) =>
            prev.map((item) =>
              item.fileName === file.name
                ? { ...item, status: "uploading", message: `Uploading… ${percent}%` }
                : item
            )
          );
        },
      });
    };

    try {
      const successfulUploads: string[] = [];
      for (const file of uploadCandidates) {
        setUploadStatuses((prev) =>
          prev.map((item) =>
            item.fileName === file.name
              ? { ...item, status: "uploading", message: "Uploading…" }
              : item
          )
        );

        try {
          await uploadFile(file);
          successfulUploads.push(file.name);
          setUploadStatuses((prev) =>
            prev.map((item) =>
              item.fileName === file.name
                ? { ...item, status: "queued", message: "Uploaded (Riley Memory off by default)" }
                : item
            )
          );
        } catch (error) {
          let errorMessage = error instanceof Error ? error.message : "Upload failed";
          if (
            (error instanceof ApiRequestError && error.status === 413) ||
            (error instanceof DirectUploadError && error.status === 413)
          ) {
            errorMessage = fileTooLargeMessage();
          }
          setUploadStatuses((prev) =>
            prev.map((item) =>
              item.fileName === file.name
                ? { ...item, status: "failed", message: errorMessage }
                : item
            )
          );
        }
      }
      
      // After successful upload, refetch the entire assets list to get complete metadata
      // including preview fields (preview_url, preview_status, preview_error) from Qdrant
      // This ensures the UI sees the latest preview state without waiting
      await fetchFiles();
      if (successfulUploads.length > 0) {
        setRecentUploadedNames((prev) => {
          const merged = new Set(prev.map(normalizeFilename));
          for (const name of successfulUploads) {
            merged.add(normalizeFilename(name));
          }
          return Array.from(merged);
        });
      }
      if (successfulUploads.length > 0) {
        showToast("success", `Uploaded ${successfulUploads.length} file(s). Riley Memory is off by default.`);
      }
    } catch (error) {
      console.error("Upload error:", error);
      const detail = error instanceof Error ? error.message : "Unknown error";
      const normalizedDetail = detail.toLowerCase().includes("failed to fetch")
        ? `${detail}. If this file is large, the request may exceed upload limits.`
        : detail;
      showToast("error", `Upload failed: ${normalizedDetail}`);
    } finally {
      setIsUploading(false);
      // Reset file input
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  };

  const handleTagChange = (assetId: string, tags: AssetTag[]) => {
    setAssets((prev) =>
      prev.map((asset) => (asset.id === assetId ? { ...asset, tags } : asset))
    );
  };

  const handleDelete = async (assetId: string, isGlobal: boolean) => {
    const asset = assets.find((a) => a.id === assetId);
    if (!asset) return;

    try {
      if (isGlobal) {
        // Global delete: Remove from disk, Qdrant, and all tabs
        const token = await getToken();
        if (!token) {
          throw new Error("Authentication required");
        }
        
        await apiFetch(`/api/v1/files/${assetId}?tenant_id=${encodeURIComponent(campaignId)}`, {
          token,
          method: "DELETE",
        });

        // Remove from local state
        setAssets((prev) => prev.filter((a) => a.id !== assetId));
        
        // Close viewer if this asset was open
        if (selectedAsset?.id === assetId) {
          setSelectedAsset(null);
        }
      } else {
        // Local delete: Clear all tags (remove from all workstreams)
        const token = await getToken();
        if (!token) {
          throw new Error("Authentication required");
        }
        
        await apiFetch(`/api/v1/files/${assetId}/untag`, {
          token,
          method: "PATCH",
          body: { tag: "*" }, // "*" means clear all tags
        });

        // Update local state to clear all tags
        setAssets((prev) =>
          prev.map((a) =>
            a.id === assetId
              ? { ...a, tags: [] }
              : a
          )
        );
      }
    } catch (error) {
      // Improved error logging: fetch and log full response JSON on non-2xx
      // This ensures 422 details are properly stringified, not [object Object]
      if (error instanceof Error && error.message.includes("HTTP")) {
        try {
          const token = await getToken();
          if (token) {
            const baseUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
            const url = isGlobal 
              ? `${baseUrl}/api/v1/files/${assetId}?tenant_id=${encodeURIComponent(campaignId)}`
              : `${baseUrl}/api/v1/files/${assetId}/untag`;
            const response = await fetch(url, {
              method: isGlobal ? "DELETE" : "PATCH",
              headers: {
                Authorization: `Bearer ${token}`,
                "Content-Type": "application/json",
              },
              body: !isGlobal ? JSON.stringify({ tag: "*" }) : undefined,
            });
            if (!response.ok) {
              const errorJson = await response.json();
              console.error("Delete error response JSON:", JSON.stringify(errorJson, null, 2));
            }
          }
        } catch (logError) {
          // If logging fails, fall back to basic error logging
          console.error("Failed to log error details:", logError);
        }
      }
      console.error("Delete error:", error);
      throw error; // Re-throw so modal can handle it
    }
  };

  const handleDownload = (assetId: string) => {
    const asset = assets.find((a) => a.id === assetId);
    if (asset) {
      // Mock download - in production, this would trigger actual download
      console.log("Downloading:", asset.name);
      // You could use: window.open(asset.url, '_blank');
    }
  };

  const handleAIEnabledChange = async (assetId: string, enabled: boolean) => {
    // Optimistic update.
    setAssets((prev) =>
      prev.map((asset) => (asset.id === assetId ? { ...asset, aiEnabled: enabled } : asset))
    );
    try {
      const token = await getToken();
      if (!token) throw new Error("Authentication required");
      await apiFetch(`/api/v1/files/${assetId}/ai_enabled?tenant_id=${encodeURIComponent(campaignId)}`, {
        token,
        method: "PATCH",
        body: { ai_enabled: enabled },
      });
      const latestAssets = await fetchFiles();
      const updatedAsset = latestAssets.find((asset) => asset.id === assetId);
      const refreshedIngestionStatus = String(updatedAsset?.ingestionStatus || "").toLowerCase();
      showToast(
        "success",
        enabled
          ? refreshedIngestionStatus === "indexed"
            ? "Already indexed. Riley memory enabled."
            : refreshedIngestionStatus === "queued" || refreshedIngestionStatus === "processing"
              ? "Riley Memory enabled. Ingestion has started for this asset."
              : "Riley memory enabled."
          : "Riley Memory disabled. Riley will exclude this asset going forward."
      );
    } catch (error) {
      // Revert optimistic state.
      setAssets((prev) =>
        prev.map((asset) => (asset.id === assetId ? { ...asset, aiEnabled: !enabled } : asset))
      );
      showToast("error", error instanceof Error ? error.message : "Failed to update Riley Memory setting.");
    }
  };

  const handleAssetClick = (asset: Asset) => {
    setSelectedAsset(asset);
  };

  const handleCloseViewer = () => {
    setSelectedAsset(null);
  };

  const handleRename = async (newName: string) => {
    if (!renamingAsset) return;

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }
      
      const result = await apiFetch<{ new_name: string; new_url: string }>(
        "/api/v1/files/rename",
        {
          token,
          method: "POST",
          body: {
            old_name: renamingAsset.name,
            new_name: newName,
          },
        }
      );

      // Update the asset in state
      setAssets((prev) =>
        prev.map((asset) =>
          asset.id === renamingAsset.id
            ? {
                ...asset,
                name: result.new_name,
                url: result.new_url,
              }
            : asset
        )
      );

      setRenamingAsset(null);
    } catch (error) {
      console.error("Rename error:", error);
      throw error; // Re-throw so the modal can handle it
    }
  };

  const handlePromote = async (isGolden: boolean) => {
    if (!promotingAsset) return;

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }
      
      await apiFetch(`/api/v1/files/${promotingAsset.id}/archive?tenant_id=${encodeURIComponent(campaignId)}`, {
        token,
        method: "POST",
        body: { is_golden: isGolden },
      });

      // Success - modal will close automatically
      setPromotingAsset(null);
    } catch (error) {
      console.error("Promote error:", error);
      throw error; // Re-throw so the modal can handle it
    }
  };

  return (
    <div className="relative flex h-full min-h-0 bg-[#f7f5ef]">
      <main className="flex min-w-0 flex-1 flex-col overflow-hidden bg-[#f7f5ef]">
        <header className="flex flex-wrap items-end justify-between gap-4 border-b border-[#e5ddce] px-6 py-5 lg:px-8">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-[#1f2a44]">
              Assets
            </h1>
            <p className="mt-1 text-sm text-[#6f788a]">
              Campaign document library and Riley Memory controls.
            </p>
            <p className="mt-1 text-xs text-[#8a90a0]">
              Assets is the primary home for uploaded campaign files.
            </p>
          </div>
          <div className="flex items-center gap-3">
              <input
                ref={fileInputRef}
                type="file"
                className="hidden"
                accept=".pdf,.docx,.doc,.xlsx,.xls,.pptx,.ppt,.png,.jpg,.jpeg,.webp"
                multiple
                onChange={handleFileSelect}
              />
              <button
                type="button"
                onClick={handleUpload}
                disabled={isUploading}
                className="inline-flex items-center gap-2 rounded-md border border-[#d8d0bf] bg-white px-3 py-1.5 text-sm font-medium text-[#1f2a44] transition-colors hover:bg-[#f1ece2] focus:outline-none focus:ring-2 focus:ring-[#d4ad47]/40 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {isUploading ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Uploading...
                  </>
                ) : (
                  <>
                    <Upload className="h-4 w-4" />
                    Upload Files
                  </>
                )}
              </button>
          </div>
        </header>
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
          <div className="overflow-hidden rounded-xl border border-[#e5ddce] bg-[#fcfbf8]">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-[#e5ddce]">
                    <th className="px-6 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-[#8a90a0]">
                      Icon
                    </th>
                    <th className="px-6 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-[#8a90a0]">
                      Name
                    </th>
                    <th className="px-6 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-[#8a90a0]">
                      Spotlight Tags
                    </th>
                    <th className="px-6 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-[#8a90a0]">
                      Meta
                    </th>
                    <th className="px-6 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-[#8a90a0]">
                      Riley's Memory
                    </th>
                    <th className="px-6 py-3 text-left text-[10px] font-semibold uppercase tracking-wider text-[#8a90a0]">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {isLoading ? (
                    <tr>
                      <td colSpan={6} className="px-6 py-12 text-center text-sm text-[#8a90a0]">
                        Loading files...
                      </td>
                    </tr>
                  ) : assets.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="px-6 py-12 text-center text-sm text-[#8a90a0]">
                        No assets uploaded yet. Click "Upload Files" to get started.
                      </td>
                    </tr>
                  ) : (
                    assets.map((asset) => (
                      <AssetRow
                        key={asset.id}
                        asset={asset}
                        onTagChange={handleTagChange}
                        onDelete={(id) => {
                          const asset = assets.find((a) => a.id === id);
                          if (asset) {
                            setDeletingAsset(asset);
                          }
                        }}
                        onDownload={handleDownload}
                        onAIEnabledChange={handleAIEnabledChange}
                        onClick={handleAssetClick}
                        onRename={setRenamingAsset}
                        onPromote={setPromotingAsset}
                        campaignId={campaignId}
                      />
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </main>

      {/* Document Viewer */}
      {selectedAsset && (
        <DocumentViewer
          file={selectedAsset}
          onClose={handleCloseViewer}
        />
      )}

      {/* Rename Modal */}
      {renamingAsset && (
        <RenameAssetModal
          isOpen={!!renamingAsset}
          onClose={() => setRenamingAsset(null)}
          onRename={handleRename}
          currentName={renamingAsset.name}
        />
      )}

      {/* Delete Modal */}
      {deletingAsset && (
        <DeleteFileModal
          isOpen={!!deletingAsset}
          onClose={() => {
            setDeletingAsset(null);
          }}
          file={deletingAsset}
          currentContext="assets"
          onDelete={handleDelete}
        />
      )}

      {/* Promote Modal */}
      {promotingAsset && (
        <PromoteModal
          isOpen={!!promotingAsset}
          onClose={() => setPromotingAsset(null)}
          onPromote={handlePromote}
          fileName={promotingAsset.name}
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
