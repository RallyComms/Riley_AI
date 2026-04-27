"use client";

import { useState, useEffect, useMemo } from "react";
import { useAuth } from "@clerk/nextjs";
import { FileText, FileType2, Table, Presentation, Image as ImageIcon, Search, Eye, Trophy, Trash2, AlertTriangle } from "lucide-react";
import { cn } from "@app/lib/utils";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { Asset, AssetTag } from "@app/lib/types";
import { apiFetch } from "@app/lib/api";

interface GlobalFile {
  id: string;
  name: string;
  url: string;
  type: string;
  date: string;
  size: string;
  tags: string[];
  is_golden?: boolean;
  client_id?: string; // Original campaign ID
  source_campaign_id?: string;
  origin_campaign_name?: string;
  preview_url?: string | null;
  preview_type?: string | null;
  preview_status?: "complete" | "failed" | "processing" | "queued" | "skipped" | "not_requested" | null;
  preview_error?: string | null;
  ingestion_status?: string | null;
}

function getFileIcon(type: string) {
  const ext = type.toLowerCase();
  switch (ext) {
    case "pdf":
      return { icon: FileText, color: "text-red-500" };
    case "docx":
    case "doc":
      return { icon: FileType2, color: "text-blue-500" };
    case "xlsx":
    case "xls":
    case "csv":
      return { icon: Table, color: "text-emerald-500" };
    case "pptx":
    case "ppt":
      return { icon: Presentation, color: "text-orange-500" };
    case "img":
    case "png":
    case "jpg":
    case "jpeg":
    case "webp":
      return { icon: ImageIcon, color: "text-purple-500" };
    default:
      return { icon: FileText, color: "text-zinc-500" };
  }
}

function hasValidGlobalRecord(file: GlobalFile): boolean {
  const name = String(file.name || "").trim().toLowerCase();
  const origin = String(file.source_campaign_id || file.client_id || "").trim().toLowerCase();
  const url = String(file.url || "").trim();
  const previewUrl = String(file.preview_url || "").trim();
  if (!name || ["unknown", "untitled", "none", "null", "n/a", "na"].includes(name)) {
    return false;
  }
  if (!origin || ["unknown", "none", "null", "global"].includes(origin)) {
    return false;
  }
  return Boolean(url || previewUrl);
}

interface GlobalDocsListProps {
  onViewDocument?: (file: Asset) => void;
}

export function GlobalDocsList({ onViewDocument }: GlobalDocsListProps) {
  const { getToken } = useAuth();
  const [files, setFiles] = useState<GlobalFile[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  const [selectedFile, setSelectedFile] = useState<Asset | null>(null);
  const [pendingActionFile, setPendingActionFile] = useState<GlobalFile | null>(null);
  const [isSubmittingAction, setIsSubmittingAction] = useState(false);

  const fetchGlobalFiles = async () => {
    try {
      setIsLoading(true);
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication token not available");
      }

      const data = await apiFetch<{ files: GlobalFile[] }>(
        `/api/v1/list?tenant_id=global`,
        {
          token,
          method: "GET",
        }
      );
      const incoming = data.files || [];
      setFiles(incoming.filter(hasValidGlobalRecord));
    } catch (error) {
      console.error("Error fetching global files:", error);
      setFiles([]);
    } finally {
      setIsLoading(false);
    }
  };

  // Fetch global files
  useEffect(() => {
    void fetchGlobalFiles();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [getToken]);

  // Filter files by search query
  const filteredFiles = useMemo(() => {
    if (!searchQuery.trim()) return files;
    const query = searchQuery.toLowerCase();
    return files.filter((file) =>
      file.name.toLowerCase().includes(query)
    );
  }, [files, searchQuery]);

  // Convert string[] tags to AssetTag[]
  const convertTags = (tags: string[]): AssetTag[] => {
    const validTags: AssetTag[] = ["Messaging", "Research", "Strategy", "Media", "Pitch", "Other"];
    return tags
      .filter((tag): tag is AssetTag => validTags.includes(tag as AssetTag))
      .map((tag) => tag as AssetTag);
  };

  // Convert GlobalFile to Asset for DocumentViewer
  const convertToAsset = (file: GlobalFile): Asset => {
    return {
      id: file.id,
      name: file.name,
      type: file.type as Asset["type"],
      url: file.url,
      previewUrl: file.preview_url ?? null,
      previewType: file.preview_type ?? null,
      previewStatus: file.preview_status ?? null,
      previewError: file.preview_error ?? null,
      ingestionStatus: (file.ingestion_status as Asset["ingestionStatus"]) ?? null,
      tags: convertTags(file.tags),
      uploadDate: file.date.split("T")[0],
      uploader: "Firm Archive",
      size: file.size,
      urgency: "medium",
      assignedTo: [],
      comments: 0,
      status: "approved",
      aiEnabled: file.type === "pdf" || file.type === "docx",
    };
  };

  const handleView = (file: GlobalFile) => {
    const asset = convertToAsset(file);
    if (onViewDocument) {
      onViewDocument(asset);
    } else {
      setSelectedFile(asset);
    }
  };

  const handleFirmDocumentAction = async (mode: "archive_only" | "delete_everywhere") => {
    if (!pendingActionFile || isSubmittingAction) return;
    try {
      setIsSubmittingAction(true);
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication token not available");
      }
      await apiFetch(
        `/api/v1/files/${encodeURIComponent(pendingActionFile.id)}/firm-archive/remove?tenant_id=global`,
        {
          token,
          method: "POST",
          body: { mode },
        }
      );
      setPendingActionFile(null);
      await fetchGlobalFiles();
    } catch (error) {
      console.error("Failed to remove/delete firm document:", error);
      alert("Failed to process document action. Please try again.");
    } finally {
      setIsSubmittingAction(false);
    }
  };

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-12">
        <p className="text-sm text-zinc-500">Loading firm archive...</p>
      </div>
    );
  }

  return (
    <>
      {/* Search Bar */}
      <div className="px-6 py-4 border-b border-zinc-800">
        <div className="flex items-center gap-2 rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-3">
          <Search className="h-4 w-4 text-zinc-500" />
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search documents..."
            className="flex-1 bg-transparent text-zinc-100 placeholder:text-zinc-500 focus:outline-none"
          />
        </div>
      </div>

      {/* Files Table */}
      <div className="flex-1 overflow-y-auto">
        {filteredFiles.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 text-center">
            <p className="text-sm text-zinc-400">
              {searchQuery ? "No documents match your search." : "No documents in the firm archive yet."}
            </p>
          </div>
        ) : (
          <div className="p-6">
            <table className="w-full">
              <thead>
                <tr className="border-b border-zinc-800">
                  <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider">
                    File
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider">
                    Status
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-zinc-400 uppercase tracking-wider">
                    Origin
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-zinc-400 uppercase tracking-wider">
                    Action
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-zinc-800">
                {filteredFiles.map((file) => {
                  const { icon: FileIcon, color: iconColor } = getFileIcon(file.type);
                  return (
                    <tr
                      key={file.id}
                      className="hover:bg-zinc-900/50 transition-colors"
                    >
                      {/* Icon & Name */}
                      <td className="px-4 py-4">
                        <div className="flex items-center gap-3">
                          <FileIcon className={cn("h-5 w-5 flex-shrink-0", iconColor)} />
                          <span className="text-sm font-medium text-zinc-100 truncate max-w-md">
                            {file.name}
                          </span>
                        </div>
                      </td>

                      {/* Badges */}
                      <td className="px-4 py-4">
                        <div className="flex items-center gap-2">
                          {file.is_golden && (
                            <span className="inline-flex items-center gap-1 rounded-full bg-amber-500/10 border border-amber-500/30 px-2 py-1 text-xs font-semibold text-amber-400">
                              <Trophy className="h-3 w-3" />
                              Golden Standard
                            </span>
                          )}
                        </div>
                      </td>

                      {/* Origin Campaign */}
                      <td className="px-4 py-4">
                        <span className="text-sm text-zinc-400">
                          {file.origin_campaign_name || "Unknown Campaign"}
                        </span>
                      </td>

                      {/* Action */}
                      <td className="px-4 py-4 text-right">
                        <div className="inline-flex items-center gap-2">
                          <button
                            type="button"
                            onClick={() => handleView(file)}
                            className="inline-flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-800/50 px-3 py-1.5 text-sm font-medium text-zinc-300 hover:border-amber-400/50 hover:bg-amber-400/10 hover:text-amber-400 transition-colors"
                          >
                            <Eye className="h-4 w-4" />
                            View
                          </button>
                          <button
                            type="button"
                            onClick={() => setPendingActionFile(file)}
                            className="inline-flex items-center gap-2 rounded-lg border border-red-900/70 bg-red-950/30 px-3 py-1.5 text-sm font-medium text-red-300 hover:border-red-500/70 hover:bg-red-900/30 hover:text-red-200 transition-colors"
                          >
                            <Trash2 className="h-4 w-4" />
                            Remove
                          </button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Document Viewer */}
      {selectedFile && !onViewDocument && (
        <DocumentViewer
          file={selectedFile}
          onClose={() => setSelectedFile(null)}
          variant="modal"
        />
      )}

      {/* Remove/Delete Modal */}
      {pendingActionFile && (
        <div className="fixed inset-0 z-[70] flex items-center justify-center bg-black/70 p-4">
          <div className="w-full max-w-lg rounded-xl border border-zinc-800 bg-zinc-950 p-6 shadow-2xl">
            <div className="mb-4 flex items-start gap-3">
              <div className="rounded-full border border-red-500/30 bg-red-500/10 p-2">
                <AlertTriangle className="h-5 w-5 text-red-400" />
              </div>
              <div>
                <h3 className="text-lg font-semibold text-zinc-100">Remove or Delete Document</h3>
                <p className="mt-1 text-sm text-zinc-400">
                  <span className="font-medium text-zinc-200">{pendingActionFile.name}</span>
                </p>
              </div>
            </div>
            <p className="mb-4 text-sm text-zinc-300">
              Choose one action. &quot;Delete everywhere&quot; is destructive and cannot be undone.
            </p>
            <div className="space-y-3">
              <button
                type="button"
                disabled={isSubmittingAction}
                onClick={() => void handleFirmDocumentAction("archive_only")}
                className="w-full rounded-lg border border-amber-700/60 bg-amber-950/30 px-4 py-3 text-left text-sm text-amber-200 transition-colors hover:bg-amber-900/40 disabled:opacity-60"
              >
                <span className="block font-semibold">Remove from Firm Archive only</span>
                <span className="block text-xs text-amber-300/90">
                  Removes this document from Firm Documents, but keeps it in its original campaign.
                </span>
              </button>
              <button
                type="button"
                disabled={isSubmittingAction}
                onClick={() => void handleFirmDocumentAction("delete_everywhere")}
                className="w-full rounded-lg border border-red-700/70 bg-red-950/40 px-4 py-3 text-left text-sm text-red-200 transition-colors hover:bg-red-900/50 disabled:opacity-60"
              >
                <span className="block font-semibold">Delete everywhere (destructive)</span>
                <span className="block text-xs text-red-300/90">
                  Removes from Firm Archive and permanently deletes the campaign file and underlying records.
                </span>
              </button>
            </div>
            <div className="mt-4 flex justify-end">
              <button
                type="button"
                disabled={isSubmittingAction}
                onClick={() => setPendingActionFile(null)}
                className="rounded-lg border border-zinc-700 px-4 py-2 text-sm text-zinc-300 hover:bg-zinc-800 disabled:opacity-60"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
