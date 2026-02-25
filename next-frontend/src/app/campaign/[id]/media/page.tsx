"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Download, Video, Music, Image as ImageIcon, Sparkles, X, Upload, FileText, FileType2, Table, Presentation, Loader2 } from "lucide-react";
import { RileyContextChat, getRileyTitle } from "@app/components/campaign/RileyContextChat";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { Asset } from "@app/lib/types";
import { cn } from "@app/lib/utils";
import { apiFetch, ApiRequestError } from "@app/lib/api";
import { toAsset } from "@app/lib/files";

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
  const [toast, setToast] = useState<{ kind: "success" | "error"; message: string } | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const showToast = useCallback((kind: "success" | "error", message: string) => {
    setToast({ kind, message });
    window.setTimeout(() => {
      setToast((current) => (current?.message === message ? null : current));
    }, 3200);
  }, []);

  // Fetch files from backend and filter by Media tag
  const fetchFiles = useCallback(async () => {
    if (!campaignId) return;

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
    } catch (error) {
      console.error("Error fetching files:", error);
      setAssets([]);
    } finally {
      setIsLoading(false);
    }
  }, [campaignId, getToken]);

  useEffect(() => {
    fetchFiles();
  }, [fetchFiles]);

  const handleUploadClick = () => {
    if (!isUploading) {
      fileInputRef.current?.click();
    }
  };

  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    if (process.env.NODE_ENV !== "production") {
      console.log("[MediaPage] file select triggered");
      console.log("[MediaPage] selected file:", event.target.files?.[0]);
    }
    const file = event.target.files?.[0];
    if (!file || !campaignId) {
      if (process.env.NODE_ENV !== "production") {
        console.warn("[MediaPage] aborting upload: missing file or campaignId", { file: !!file, campaignId });
      }
      return;
    }

    setIsUploading(true);

    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }

      const formData = new FormData();
      formData.append("file", file);
      formData.append("tenant_id", campaignId);
      formData.append("tags", "Media");

      if (process.env.NODE_ENV !== "production") {
        console.log("[upload] FormData keys:", Array.from(formData.keys()));
        console.log("[upload] selected file:", { name: file.name, size: file.size, type: file.type });
      }

      const result = await apiFetch<{ id: string; url: string; filename: string; type?: string }>("/api/v1/upload", {
        token,
        method: "POST",
        body: formData,
      });

      if (process.env.NODE_ENV !== "production") {
        console.log("[upload] success response:", result);
      }

      await fetchFiles();
      showToast("success", `Uploaded "${file.name}" successfully.`);
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

  return (
    <div className="flex h-full relative">
      {/* MIDDLE: Media Gallery */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="flex items-center justify-between p-6 border-b border-zinc-800">
          <h1 className="text-2xl font-bold text-zinc-100">Media Assets</h1>
          <div className="flex items-center gap-3">
            <input
              ref={fileInputRef}
              type="file"
              className="hidden"
              accept=".pdf,.docx,.doc,.xlsx,.xls,.pptx,.ppt,.png,.jpg,.jpeg,.webp,.gif,.svg,.mp4,.webm,.mov,.mp3,.wav,.ogg"
              onChange={handleFileSelect}
            />
            {/* Upload Button */}
            <button
              type="button"
              onClick={handleUploadClick}
              disabled={isUploading}
              className="flex items-center gap-2 bg-zinc-800 text-zinc-300 hover:bg-zinc-700 px-4 py-2 rounded-md transition-colors"
            >
              {isUploading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Upload className="w-4 h-4" />}
              <span>{isUploading ? "Uploading..." : "Upload media"}</span>
            </button>
            {/* THE "OPEN" BUTTON - Only visible if chat is closed */}
            {!isChatOpen && (
              <button
                onClick={() => setIsChatOpen(true)}
                className="flex items-center gap-2 text-amber-500 hover:bg-zinc-900 px-4 py-2 rounded-md transition-colors"
              >
                <Sparkles className="w-4 h-4" />
                <span>Ask Riley</span>
              </button>
            )}
            {/* THE "HIDE" BUTTON - Only visible if chat is open */}
            {isChatOpen && (
              <button
                onClick={() => setIsChatOpen(false)}
                className="flex items-center gap-2 bg-amber-500/10 text-amber-500 border border-amber-500/50 hover:bg-amber-500/20 px-4 py-2 rounded-md transition-colors"
              >
                <Sparkles className="w-4 h-4" />
                <span>Hide Riley</span>
              </button>
            )}
          </div>
        </header>

        {/* Media Grid */}
        <div className="flex-1 overflow-y-auto p-6">
          {isLoading ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-zinc-500">Loading files...</p>
            </div>
          ) : assets.length === 0 ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-zinc-500">No files tagged with Media yet.</p>
            </div>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {assets.map((asset) => {
                const { icon: FileIcon, color: iconColor } = getFileIcon(asset.type);
                const isVideo = asset.type === "img" && asset.name.match(/\.(mp4|webm|mov)$/i);
                const isAudio = asset.name.match(/\.(mp3|wav|ogg)$/i);
                const isImage = asset.type === "img" && !isVideo && !isAudio;

                return (
                  <div
                    key={asset.id}
                    className="rounded-xl border border-zinc-800 bg-zinc-900 overflow-hidden hover:border-zinc-700 transition-colors group cursor-pointer"
                    onClick={() => setSelectedFile(asset)}
                  >
                    {/* Media Preview */}
                    <div className="relative aspect-video bg-zinc-950 flex items-center justify-center">
                      {isVideo ? (
                        <div className="w-full h-full flex items-center justify-center">
                          <Video className="h-16 w-16 text-zinc-600" />
                        </div>
                      ) : isAudio ? (
                        <div className="w-full p-6">
                          <div className="flex items-center justify-center mb-4">
                            <div className="h-20 w-20 rounded-full bg-zinc-800 flex items-center justify-center">
                              <Music className="h-10 w-10 text-zinc-400" />
                            </div>
                          </div>
                        </div>
                      ) : isImage ? (
                        <img
                          src={asset.url}
                          alt={asset.name}
                          className="w-full h-full object-cover"
                          onError={(e) => {
                            // Fallback to icon if image fails to load
                            e.currentTarget.style.display = "none";
                            const parent = e.currentTarget.parentElement;
                            if (parent) {
                              parent.innerHTML = `
                                <div class="flex items-center justify-center h-full">
                                  <svg class="h-16 w-16 text-zinc-700" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 16l4.586-4.586a2 2 0 012.828 0L16 16m-2-2l1.586-1.586a2 2 0 012.828 0L20 14m-6-6h.01M6 20h12a2 2 0 002-2V6a2 2 0 00-2-2H6a2 2 0 00-2 2v12a2 2 0 002 2z" />
                                  </svg>
                                </div>
                              `;
                            }
                          }}
                        />
                      ) : (
                        <div className="flex items-center justify-center">
                          <FileIcon className={cn("h-16 w-16", iconColor)} />
                        </div>
                      )}
                    </div>

                    {/* Footer */}
                    <div className="p-4 border-t border-zinc-800 bg-zinc-900/50">
                      <div className="flex items-center justify-between gap-3">
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-medium text-zinc-100 truncate">{asset.name}</p>
                          <div className="flex items-center gap-2 mt-1">
                            {isVideo && <Video className="h-3.5 w-3.5 text-zinc-500" />}
                            {isAudio && <Music className="h-3.5 w-3.5 text-zinc-500" />}
                            {isImage && <ImageIcon className="h-3.5 w-3.5 text-zinc-500" />}
                            <span className="text-xs text-zinc-500">{asset.size}</span>
                          </div>
                        </div>
                        <button
                          type="button"
                          onClick={(e) => {
                            e.stopPropagation();
                            handleDownload(asset);
                          }}
                          className="flex-shrink-0 p-2 rounded-lg text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors opacity-0 group-hover:opacity-100"
                          aria-label="Download"
                        >
                          <Download className="h-4 w-4" />
                        </button>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>

      {/* RIGHT: Riley Chat - Conditionally Rendered */}
      {isChatOpen && (
        <div className="w-[400px] border-l border-zinc-800 bg-zinc-900/50 backdrop-blur-sm flex flex-col h-full shrink-0">
          {/* Header with CLOSE Button */}
          <div className="h-16 flex items-center justify-between px-4 border-b border-zinc-800">
            <span className="font-semibold text-amber-500 flex items-center gap-2">
              <Sparkles className="w-4 h-4" /> {getRileyTitle("research")}
            </span>
            <button
              onClick={() => setIsChatOpen(false)}
              className="text-zinc-500 hover:text-white p-2 transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
          {/* The Chat Content */}
          <div className="flex-1 overflow-hidden">
            <RileyContextChat 
              mode="research" 
              contextKey="media" 
              campaignId={campaignId}
              onViewAsset={handleViewAsset}
            />
          </div>
        </div>
      )}

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
              ? "border-emerald-500/30 bg-emerald-500/15 text-emerald-100"
              : "border-red-500/30 bg-red-500/15 text-red-100"
          )}
        >
          {toast.message}
        </div>
      )}
    </div>
  );
}
