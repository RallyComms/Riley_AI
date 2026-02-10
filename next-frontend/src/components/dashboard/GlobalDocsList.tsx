"use client";

import { useState, useEffect, useMemo } from "react";
import { useAuth } from "@clerk/nextjs";
import { FileText, FileType2, Table, Presentation, Image as ImageIcon, Search, Eye, Trophy } from "lucide-react";
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

// Helper to format campaign name from ID
function formatCampaignName(campaignId: string | undefined): string {
  if (!campaignId) return "Unknown Campaign";
  return campaignId
    .split("-")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
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

  // Fetch global files
  useEffect(() => {
    const fetchGlobalFiles = async () => {
      try {
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
        setFiles(data.files || []);
      } catch (error) {
        console.error("Error fetching global files:", error);
        setFiles([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchGlobalFiles();
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
                          {formatCampaignName(file.client_id)}
                        </span>
                      </td>

                      {/* Action */}
                      <td className="px-4 py-4 text-right">
                        <button
                          type="button"
                          onClick={() => handleView(file)}
                          className="inline-flex items-center gap-2 rounded-lg border border-zinc-700 bg-zinc-800/50 px-3 py-1.5 text-sm font-medium text-zinc-300 hover:border-amber-400/50 hover:bg-amber-400/10 hover:text-amber-400 transition-colors"
                        >
                          <Eye className="h-4 w-4" />
                          View
                        </button>
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
    </>
  );
}
