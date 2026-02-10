"use client";

import { useEffect, useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { FileText, FileType2, Table, Presentation, Image as ImageIcon, File, Loader2 } from "lucide-react";
import { cn } from "@app/lib/utils";
import { apiFetch } from "@app/lib/api";

type FileItem = {
  id: string;
  filename: string;
  type: string;
  year?: number | null;
};

function basename(path?: string) {
  if (!path) return "Unknown file";
  const parts = path.split(/[\\/]/).filter(Boolean);
  return parts[parts.length - 1] ?? path;
}

function splitExtension(filename: string): { base: string; ext: string } {
  const lastDot = filename.lastIndexOf(".");
  if (lastDot <= 0 || lastDot === filename.length - 1) {
    return { base: filename, ext: "" };
  }
  return {
    base: filename.slice(0, lastDot),
    ext: filename.slice(lastDot + 1).toLowerCase(),
  };
}

function getFileIcon(ext: string) {
  switch (ext) {
    case "pdf":
      return FileText;
    case "docx":
    case "doc":
      return FileType2;
    case "xlsx":
    case "csv":
      return Table;
    case "pptx":
    case "ppt":
      return Presentation;
    case "png":
    case "jpg":
    case "jpeg":
    case "webp":
      return ImageIcon;
    default:
      return File;
  }
}

function getFileTypeColor(ext: string) {
  switch (ext) {
    case "pdf":
      return "text-rose-700 bg-rose-100";
    case "docx":
    case "doc":
      return "text-blue-700 bg-blue-100";
    case "xlsx":
    case "csv":
      return "text-emerald-700 bg-emerald-100";
    case "pptx":
    case "ppt":
      return "text-orange-700 bg-orange-100";
    case "png":
    case "jpg":
    case "jpeg":
    case "webp":
      return "text-purple-700 bg-purple-100";
    default:
      return "text-slate-700 bg-slate-100";
  }
}

interface KnowledgeBasketProps {
  tenantId: string;
}

export function KnowledgeBasket({ tenantId }: KnowledgeBasketProps) {
  const { getToken } = useAuth();
  const [files, setFiles] = useState<FileItem[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchFiles() {
      setIsLoading(true);
      setError(null);

      try {
        const token = await getToken();
        if (!token) {
          throw new Error("Authentication required");
        }

        const data = await apiFetch<{ files: FileItem[] }>(`/api/v1/files/${tenantId}`, {
          token,
          method: "GET",
        });
        setFiles(data.files || []);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Failed to load files");
        setFiles([]);
      } finally {
        setIsLoading(false);
      }
    }

    fetchFiles();
  }, [tenantId, getToken]);

  return (
    <div className="h-full overflow-y-auto p-6">
      {/* Header */}
      <div className="mb-6">
        <h2 className="text-lg font-semibold text-slate-900">Knowledge Basket</h2>
        <p className="mt-1 text-sm text-slate-500">Campaign documents and resources</p>
      </div>

      {/* Loading State */}
      {isLoading && (
        <div className="flex items-center justify-center py-8">
          <Loader2 className="h-5 w-5 animate-spin text-slate-400" aria-hidden="true" />
        </div>
      )}

      {/* Error State */}
      {error && !isLoading && (
        <div className="rounded-lg border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
          {error}
        </div>
      )}

      {/* File List */}
      {!isLoading && !error && (
        <div className="space-y-2">
          {files.length === 0 ? (
            <div className="rounded-lg border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-center text-sm text-slate-500">
              No files found for this tenant.
            </div>
          ) : (
            files.map((file) => {
              const { base, ext } = splitExtension(basename(file.filename));
              const Icon = getFileIcon(ext);
              const iconColor = getFileTypeColor(ext);

              return (
                <div
                  key={file.id}
                  className="group flex items-center gap-3 rounded-lg border border-slate-200 bg-slate-50/50 p-3 transition-all hover:border-amber-200 hover:bg-amber-50/30"
                >
                  <div className={cn("flex h-10 w-10 flex-shrink-0 items-center justify-center rounded-lg transition-colors", iconColor)}>
                    <Icon className="h-5 w-5" aria-hidden="true" />
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium text-slate-900 truncate">{base}</p>
                    <div className="flex items-center gap-2">
                      <p className="text-xs text-slate-500 uppercase">{ext || "file"}</p>
                      {file.year && (
                        <>
                          <span className="text-xs text-slate-400">•</span>
                          <p className="text-xs text-slate-500">{file.year}</p>
                        </>
                      )}
                    </div>
                  </div>
                </div>
              );
            })
          )}
        </div>
      )}
    </div>
  );
}

