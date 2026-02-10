"use client";

import { useState, useRef, useEffect } from "react";
import { useParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Upload, Loader2 } from "lucide-react";
import { Asset, AssetTag } from "@app/lib/types";
import { AssetRow } from "@app/components/campaign/AssetRow";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { RenameAssetModal } from "@app/components/campaign/RenameAssetModal";
import { DeleteFileModal } from "@app/components/campaign/DeleteFileModal";
import { PromoteModal } from "@app/components/campaign/PromoteModal";
import { apiFetch } from "@app/lib/api";

// Helper function to determine if file type supports AI processing
function isAISupportedType(type: Asset["type"]): boolean {
  return type === "pdf" || type === "docx";
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

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
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
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Fetch files from backend on mount
  useEffect(() => {
    const fetchFiles = async () => {
      if (!campaignId) return; // Don't fetch if no campaign ID
      
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
            previewType,
            previewStatus: file.preview_status ?? undefined,
            tags: file.tags || [], // Use tags from backend
            uploadDate: new Date(file.date).toISOString().split("T")[0],
            uploader: "System",
            size: file.size,
            urgency: "medium",
            assignedTo: [],
            comments: 0,
            status: "ready",
            aiEnabled: isAISupportedType(effectiveType),
          };
        });
        
        setAssets(fetchedAssets);
      } catch (error) {
        console.error("Error fetching files:", error);
        // Return empty array if fetch fails (Qdrant is source of truth)
        setAssets([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchFiles();
  }, [campaignId]);

  const handleUpload = () => {
    fileInputRef.current?.click();
  };

  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setIsUploading(true);
    const fileType = getFileTypeFromExtension(file.name);
    const fileSize = formatFileSize(file.size);

    // Create FormData for multipart/form-data upload
    const formData = new FormData();
    formData.append("file", file);
    formData.append("tenant_id", campaignId); // Use actual campaign ID from URL
    formData.append("tags", ""); // Empty tags - user will add tags via UI

    const uploadFile = async (overwrite: boolean = false) => {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }
      
      const path = `/api/v1/upload${overwrite ? "?overwrite=true" : ""}`;
      try {
        const result = await apiFetch(path, {
          token,
          method: "POST",
          body: formData,
        });
        return { ok: true, json: async () => result, status: 200 };
      } catch (error) {
        // Convert apiFetch error to response-like object for compatibility
        const errorMessage = error instanceof Error ? error.message : "Upload failed";
        if (errorMessage.includes("409")) {
          return { ok: false, status: 409, json: async () => ({ detail: "File exists" }) };
        }
        const statusMatch = errorMessage.match(/HTTP (\d+)/);
        const status = statusMatch ? parseInt(statusMatch[1]) : 500;
        return { ok: false, status, json: async () => ({ detail: errorMessage }) };
      }
    };

    try {
      // Try upload
      let response = await uploadFile(false);

      // Handle 409 Conflict (file exists)
      if (response.status === 409) {
        const errorData = await response.json().catch(() => ({ detail: "File exists" }));
        const shouldOverwrite = window.confirm(
          `File "${file.name}" already exists. Overwrite?`
        );
        
        if (shouldOverwrite) {
          // Retry with overwrite=true
          response = await uploadFile(true);
        } else {
          setIsUploading(false);
          if (fileInputRef.current) {
            fileInputRef.current.value = "";
          }
          return;
        }
      }

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: "Upload failed" }));
        throw new Error(errorData.detail || `Upload failed with status ${response.status}`);
      }

      // Get the response data
      const result = await response.json();
      
      // Fetch the file from the list endpoint to get full metadata including tags
      // This ensures we have the correct tags that were saved to Qdrant
      const token = await getToken();
      if (token) {
        try {
          const listData = await apiFetch<{ files: any[] }>(
            `/api/v1/list?tenant_id=${encodeURIComponent(campaignId)}`,
            {
              token,
              method: "GET",
            }
          );
          const uploadedFile = listData.files.find((f: any) => f.id === result.id);
          
          if (uploadedFile) {
            const baseType = getFileTypeFromExtension(uploadedFile.name);
            const previewType = uploadedFile.preview_type as string | undefined;
            const effectiveType = previewType === "pdf" ? "pdf" : baseType;

            // Use the file data from Qdrant (source of truth)
            const newAsset: Asset = {
              id: uploadedFile.id,
              name: uploadedFile.name,
              type: effectiveType,
              url: uploadedFile.url, // original URL
              previewUrl: uploadedFile.preview_url ?? undefined,
              previewType,
              previewStatus: uploadedFile.preview_status ?? undefined,
              tags: uploadedFile.tags || [], // Use tags from Qdrant
              uploadDate: new Date(uploadedFile.date).toISOString().split("T")[0],
              uploader: "You",
              size: uploadedFile.size,
              urgency: "medium",
              assignedTo: [],
              comments: 0,
              status: "ready",
              aiEnabled: isAISupportedType(effectiveType),
            };
            
            // Add the new asset to the state
            setAssets([newAsset, ...assets]);
          }
        } catch (err) {
          console.error("Error fetching uploaded file metadata:", err);
        }
      }
      setIsUploading(false);
          return;
        }
      }
      
      // Fallback if we can't fetch from list (shouldn't happen, but handle gracefully)
      const newAsset: Asset = {
        id: result.id,
        name: result.filename,
        type: fileType,
        url: result.url,
        tags: [], // Empty tags - user will add via UI
        uploadDate: new Date().toISOString().split("T")[0],
        uploader: "You",
        size: fileSize,
        urgency: "medium",
        assignedTo: [],
        comments: 0,
        status: "ready",
        aiEnabled: isAISupportedType(fileType),
      };

      // Add the new asset to the state so it appears in the table instantly
      setAssets([newAsset, ...assets]);
      setIsUploading(false);
    } catch (error) {
      console.error("Upload error:", error);
      // Show error to user (you might want to add a toast notification here)
      alert(`Upload failed: ${error instanceof Error ? error.message : "Unknown error"}`);
      setIsUploading(false);
    } finally {
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
        
        await apiFetch(`/api/v1/files/${assetId}`, {
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

        if (!response.ok) {
          const errorData = await response.json().catch(() => ({
            detail: "Untag failed",
          }));
          throw new Error(
            errorData.detail || `Untag failed with status ${response.status}`
          );
        }

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

  const handleAIEnabledChange = (assetId: string, enabled: boolean) => {
    setAssets((prev) =>
      prev.map((asset) =>
        asset.id === assetId ? { ...asset, aiEnabled: enabled } : asset
      )
    );
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
      
      await apiFetch(`/api/v1/files/${promotingAsset.id}/archive`, {
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
    <div className="flex h-full relative">
      {/* Main Content */}
      <main className="flex-1 flex flex-col overflow-hidden bg-transparent">
        {/* Header */}
        <header className="flex items-center justify-between p-6 border-b border-white/5 bg-slate-900/50 backdrop-blur-md">
          <h1 className="text-2xl font-bold text-zinc-100">
            Campaign Assets
          </h1>
            <div className="flex items-center gap-3">
              <input
                ref={fileInputRef}
                type="file"
                className="hidden"
                accept=".pdf,.docx,.doc,.xlsx,.xls,.pptx,.ppt,.png,.jpg,.jpeg,.webp"
                onChange={handleFileSelect}
              />
              <button
                type="button"
                onClick={handleUpload}
                disabled={isUploading}
                className="inline-flex items-center gap-2 rounded-md border border-amber-500/20 bg-amber-500/10 px-4 py-2 text-sm font-medium text-amber-400 transition-all hover:bg-amber-500/20 hover:border-amber-500/30 focus:outline-none focus:ring-2 focus:ring-amber-500/50 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {isUploading ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Uploading...
                  </>
                ) : (
                  <>
                    <Upload className="h-4 w-4" />
                    Upload File
                  </>
                )}
              </button>
            </div>
        </header>

        {/* Scrollable Content Area */}
        <div className="flex-1 overflow-y-auto p-6">
          {/* The Vault (HUD-style Table) */}
          <div className="rounded-xl border border-slate-800 bg-slate-900/80 backdrop-blur-md overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-zinc-800/50">
                    <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-wider text-zinc-400">
                      Icon
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-wider text-zinc-400">
                      Name
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-wider text-zinc-400">
                      Spotlight Tags
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-wider text-zinc-400">
                      Meta
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-wider text-zinc-400">
                      Riley's Memory
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-wider text-zinc-400">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {isLoading ? (
                    <tr>
                      <td colSpan={6} className="px-6 py-12 text-center text-sm text-zinc-500">
                        Loading files...
                      </td>
                    </tr>
                  ) : assets.length === 0 ? (
                    <tr>
                      <td colSpan={6} className="px-6 py-12 text-center text-sm text-zinc-500">
                        No assets uploaded yet. Click "Upload File" to get started.
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
    </div>
  );
}
