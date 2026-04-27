"use client";

import { useState } from "react";
import { useAuth } from "@clerk/nextjs";
import * as Popover from "@radix-ui/react-popover";
import {
  FileText,
  FileType2,
  Table,
  Presentation,
  Image as ImageIcon,
  MoreHorizontal,
  Loader2,
  Check,
  Plus,
  Download,
  Trash2,
  Brain,
  Sparkles,
  Pencil,
  Globe,
} from "lucide-react";
import { cn } from "@app/lib/utils";
import { Asset, AssetTag } from "@app/lib/types";
import { apiFetch } from "@app/lib/api";

interface AssetRowProps {
  asset: Asset;
  onTagChange: (assetId: string, tags: AssetTag[]) => void;
  onDelete: (assetId: string) => void;
  onDownload: (assetId: string) => void;
  onAIEnabledChange: (assetId: string, enabled: boolean) => void;
  onClick?: (asset: Asset) => void;
  onRename?: (asset: Asset) => void;
  onPromote?: (asset: Asset) => void;
  // Current campaign / tenant context for this asset row
  campaignId: string;
}

// Helper function to determine if file type supports AI processing
function normalizeAssetType(type: string | undefined | null): Asset["type"] | "unknown" {
  const normalized = String(type || "").trim().toLowerCase().replace(/^\./, "");
  if (normalized === "pdf") return "pdf";
  if (normalized === "doc" || normalized === "docx") return "docx";
  if (normalized === "xls" || normalized === "xlsx" || normalized === "csv" || normalized === "tsv") return "xlsx";
  if (normalized === "ppt" || normalized === "pptx") return "pptx";
  if (["png", "jpg", "jpeg", "webp", "gif", "svg", "img", "image"].includes(normalized)) return "img";
  return "unknown";
}

function isAISupportedType(type: string | undefined | null): boolean {
  const normalized = normalizeAssetType(type);
  return normalized === "pdf" || normalized === "docx" || normalized === "xlsx" || normalized === "pptx" || normalized === "img";
}

const allTags: AssetTag[] = ["Messaging", "Research", "Strategy", "Media", "Pitch", "Other"];

const tagColors: Record<AssetTag, { bg: string; text: string; border: string }> = {
  Messaging: {
    bg: "bg-purple-500/12",
    text: "text-purple-700",
    border: "border-purple-500/25",
  },
  Research: {
    bg: "bg-cyan-500/12",
    text: "text-cyan-700",
    border: "border-cyan-500/25",
  },
  Strategy: {
    bg: "bg-amber-500/12",
    text: "text-amber-700",
    border: "border-amber-500/25",
  },
  Media: {
    bg: "bg-pink-500/12",
    text: "text-pink-700",
    border: "border-pink-500/25",
  },
  Pitch: {
    bg: "bg-orange-500/12",
    text: "text-orange-700",
    border: "border-orange-500/25",
  },
  Other: {
    bg: "bg-slate-500/12",
    text: "text-slate-700",
    border: "border-slate-500/25",
  },
};

const defaultTagColor = tagColors.Other;

function getTagColor(tag: string): { bg: string; text: string; border: string } {
  return tagColors[tag as AssetTag] || defaultTagColor;
}

function getFileIcon(type: string | undefined | null) {
  switch (normalizeAssetType(type)) {
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

function formatDate(dateString: string): string {
  const date = new Date(dateString);
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

// Helper function to check if a string looks like a UUID
function isUUID(str: string): boolean {
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
  return uuidRegex.test(str);
}

// Helper function to format filename for display
function formatFilename(name: string): string {
  // If it's a UUID, try to extract a meaningful part or show a generic name
  if (isUUID(name)) {
    // Extract file extension if present
    const extension = name.split(".").pop();
    if (extension && extension !== name) {
      return `File.${extension}`;
    }
    return "Uploaded File";
  }
  return name;
}

function ingestionBadgeClass(status?: Asset["ingestionStatus"]): string {
  switch (status) {
    case "queued":
      return "border-amber-500/30 bg-amber-500/12 text-amber-700";
    case "processing":
      return "border-blue-500/30 bg-blue-500/12 text-blue-700";
    case "indexed":
      return "border-emerald-500/30 bg-emerald-500/12 text-emerald-700";
    case "failed":
    case "low_text":
      return "border-rose-500/30 bg-rose-500/12 text-rose-700";
    case "ocr_needed":
      return "border-purple-500/30 bg-purple-500/12 text-purple-700";
    case "partial":
      return "border-orange-500/30 bg-orange-500/12 text-orange-700";
    default:
      return "border-[#d8d0bf] bg-[#f5f1e8] text-[#6f788a]";
  }
}

function multimodalBadgeClass(kind: "ocr_processed" | "vision_processed" | "partial"): string {
  switch (kind) {
    case "ocr_processed":
      return "border-cyan-500/30 bg-cyan-500/12 text-cyan-700";
    case "vision_processed":
      return "border-fuchsia-500/30 bg-fuchsia-500/12 text-fuchsia-700";
    case "partial":
      return "border-orange-500/30 bg-orange-500/12 text-orange-700";
    default:
      return "border-[#d8d0bf] bg-[#f5f1e8] text-[#6f788a]";
  }
}

function formatStatusLabel(value: string): string {
  return value.replaceAll("_", " ");
}

export function AssetRow({ asset, onTagChange, onDelete, onDownload, onAIEnabledChange, onClick, onRename, onPromote, campaignId }: AssetRowProps) {
  const { getToken } = useAuth();
  const normalizedType = normalizeAssetType(asset.type);
  const { icon: FileIcon, color: iconColor } = getFileIcon(asset.type);
  const [isUpdatingTags, setIsUpdatingTags] = useState(false);

  const handleTagToggle = async (tag: AssetTag) => {
    // Check if tag exists in current tags
    const currentTags = asset.tags || [];
    const tagExists = currentTags.includes(tag);
    
    // If tag exists: remove it, if not: add it
    const newTags = tagExists
      ? currentTags.filter((t) => t !== tag) // Remove tag
      : [...currentTags, tag]; // Add tag
    
    // Optimistic update: update local state immediately
    onTagChange(asset.id, newTags);
    
    // Persist to backend
    setIsUpdatingTags(true);
    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }
      
      await apiFetch(`/api/v1/files/${asset.id}/tags?tenant_id=${encodeURIComponent(campaignId)}`, {
        token,
        method: "PUT",
        body: { tags: newTags },
      });
    } catch (error) {
      // Revert on error
      onTagChange(asset.id, asset.tags);
      const errorMessage = error instanceof Error ? error.message : "Failed to update tags";
      console.error("Failed to update tags:", error);
      alert(`Failed to save tags: ${errorMessage}`);
    } finally {
      setIsUpdatingTags(false);
    }
  };

  return (
    <tr className="group border-b border-[#efe8da] last:border-0 transition-colors hover:bg-[#f5f1e8]">
      {/* Icon */}
      <td className="px-6 py-4">
        <div className="flex items-center justify-center">
          {asset.status === "processing" ? (
            <Loader2 className="h-5 w-5 animate-spin text-[#8a90a0]" />
          ) : (
            <FileIcon className={cn("h-5 w-5", iconColor)} />
          )}
        </div>
      </td>

      {/* Name */}
      <td className="px-6 py-4">
        <button
          type="button"
          onClick={() => onClick?.(asset)}
          className="block max-w-[180px] cursor-pointer truncate text-left font-medium text-[#1f2a44] transition-colors hover:text-[#6d560f] sm:max-w-[240px] lg:max-w-[320px]"
          title={asset.name} // Show full name on hover
        >
          {formatFilename(asset.name)}
        </button>
      </td>

      {/* Tags */}
      <td className="px-6 py-4">
        <div className="flex items-center gap-2">
          {/* Active Tags */}
          <div className="flex flex-wrap items-center gap-1.5">
            {asset.tags.map((tag) => (
              (() => {
                const colors = getTagColor(tag);
                return (
                  <span
                    key={tag}
                    className={cn(
                      "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium",
                      colors.bg,
                      colors.text,
                      colors.border
                    )}
                  >
                    {tag}
                  </span>
                );
              })()
            ))}
          </div>

          {/* Tag Popover with Portal */}
          <Popover.Root>
            <Popover.Trigger asChild>
              <button
                type="button"
                className={cn(
                  "inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs transition-opacity hover:text-zinc-100",
                  asset.tags.length === 0
                    ? "border border-[#d8d0bf] bg-white text-[#6f788a] hover:border-[#cfc5b1] hover:bg-[#f1ece2]"
                    : "text-[#8a90a0] opacity-0 group-hover:opacity-100"
                )}
              >
                <Plus className="h-3 w-3" />
                Tag
              </button>
            </Popover.Trigger>
            <Popover.Portal>
              <Popover.Content
                sideOffset={5}
                align="start"
                className="z-50 w-48 rounded-lg border border-[#d8d0bf] bg-[#fcfbf8] p-2 shadow-xl"
              >
                <div className="space-y-1">
                  {allTags.map((tag) => {
                    const isChecked = asset.tags.includes(tag);
                    return (
                      <label
                        key={tag}
                        className="flex cursor-pointer items-center gap-2 rounded-md px-3 py-2 text-sm text-[#1f2a44] transition-colors hover:bg-[#f1ece2]"
                      >
                        <div className="relative flex h-4 w-4 items-center justify-center">
                          <input
                            type="checkbox"
                            checked={isChecked}
                            onChange={() => handleTagToggle(tag)}
                            className="sr-only"
                          />
                          <div
                            className={cn(
                              "flex h-4 w-4 items-center justify-center rounded border-2 transition-colors",
                              isChecked
                                ? cn(
                                    getTagColor(tag).border,
                                    getTagColor(tag).bg,
                                    "border-transparent"
                                  )
                                : "border-[#cfc5b1] bg-transparent"
                            )}
                          >
                            {isChecked && (
                              <Check className={cn("h-3 w-3", getTagColor(tag).text)} />
                            )}
                          </div>
                        </div>
                        <span className="flex-1">{tag}</span>
                      </label>
                    );
                  })}
                </div>
                <Popover.Arrow className="fill-[#d8d0bf]" />
              </Popover.Content>
            </Popover.Portal>
          </Popover.Root>
        </div>
      </td>

      {/* Meta */}
      <td className="px-6 py-4">
          <div className="space-y-0.5">
          <div className="text-sm text-[#6f788a]" suppressHydrationWarning>{formatDate(asset.uploadDate)}</div>
          <div className="text-xs text-[#8a90a0]">{asset.size} • {asset.uploader}</div>
          {asset.ingestionStatus && (
            <span
              className={cn(
                "inline-flex rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-wide",
                ingestionBadgeClass(asset.ingestionStatus)
              )}
            >
              {formatStatusLabel(asset.ingestionStatus)}
            </span>
          )}
          {(asset.previewStatus === "failed" || asset.previewStatus === "skipped") && (
            <span className="ml-1 inline-flex rounded-full border border-amber-500/30 bg-amber-500/12 px-2 py-0.5 text-[10px] uppercase tracking-wide text-amber-700">
              {asset.ingestionStatus === "low_text"
                ? "preview unavailable / extraction degraded"
                : "preview unavailable"}
            </span>
          )}
          <div className="flex flex-wrap gap-1 pt-0.5">
            {asset.ocrStatus === "complete" && (
              <span
                className={cn(
                  "inline-flex rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-wide",
                  multimodalBadgeClass("ocr_processed")
                )}
              >
                ocr processed
              </span>
            )}
            {asset.visionStatus === "complete" && (
              <span
                className={cn(
                  "inline-flex rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-wide",
                  multimodalBadgeClass("vision_processed")
                )}
              >
                vision processed
              </span>
            )}
            {(asset.ingestionStatus === "partial" ||
              asset.multimodalStatus === "ocr_failed" ||
              asset.multimodalStatus === "ocr_unavailable" ||
              asset.ocrStatus === "failed" ||
              asset.visionStatus === "failed") && (
              <span
                className={cn(
                  "inline-flex rounded-full border px-2 py-0.5 text-[10px] uppercase tracking-wide",
                  multimodalBadgeClass("partial")
                )}
              >
                partial
              </span>
            )}
          </div>
        </div>
      </td>

      {/* Riley's Memory (AI Toggle) */}
      <td className="px-6 py-4">
        <div className="flex items-center justify-center">
          {isAISupportedType(normalizedType) ? (
            <button
              type="button"
              onClick={() => onAIEnabledChange(asset.id, !asset.aiEnabled)}
              className={cn(
                "relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-emerald-500/50 focus:ring-offset-2 focus:ring-offset-[#fcfbf8]",
                asset.aiEnabled
                  ? "bg-emerald-500/20 border border-emerald-500/30"
                  : "bg-[#ece6d9] border border-[#d8d0bf]"
              )}
              aria-label={asset.aiEnabled ? "Disable AI context" : "Enable AI context"}
            >
              <span
                className={cn(
                  "inline-block h-4 w-4 transform rounded-full bg-white transition-transform",
                  asset.aiEnabled ? "translate-x-6" : "translate-x-1"
                )}
              />
              <div className="absolute inset-0 flex items-center justify-center">
                {asset.aiEnabled ? (
                  <Brain className="h-3 w-3 text-emerald-400" />
                ) : (
                  <Brain className="h-3 w-3 text-[#8a90a0]" />
                )}
              </div>
            </button>
          ) : (
            <div className="relative group">
              <button
                type="button"
                disabled
                className={cn(
                  "relative inline-flex h-6 w-11 items-center rounded-full bg-[#ece6d9]/70 border border-[#d8d0bf] opacity-60 cursor-not-allowed"
                )}
                aria-label="Media processing disabled for speed"
              >
                <span className="inline-block h-4 w-4 transform rounded-full bg-[#b9ad95] translate-x-1" />
                <div className="absolute inset-0 flex items-center justify-center">
                  <Brain className="h-3 w-3 text-[#8a90a0]" />
                </div>
              </button>
              {/* Tooltip */}
              <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-2 opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10">
                <div className="whitespace-nowrap rounded-lg border border-[#d8d0bf] bg-[#fcfbf8] px-3 py-2 text-xs text-[#6f788a] shadow-xl">
                  Media processing disabled for speed
                  <div className="absolute left-1/2 top-full h-0 w-0 -translate-x-1/2 border-l-4 border-r-4 border-t-4 border-transparent border-t-[#d8d0bf]" />
                </div>
              </div>
            </div>
          )}
        </div>
      </td>

      {/* Actions */}
      <td className="px-6 py-4">
        <Popover.Root>
          <Popover.Trigger asChild>
            <button
              type="button"
              className="rounded-md p-1.5 text-[#8a90a0] transition-colors hover:bg-[#ece6d9] hover:text-[#1f2a44]"
              aria-label="More actions"
            >
              <MoreHorizontal className="h-4 w-4" />
            </button>
          </Popover.Trigger>
          <Popover.Portal>
            <Popover.Content
              sideOffset={5}
              align="end"
              className="z-50 min-w-[150px] rounded-lg border border-[#d8d0bf] bg-[#fcfbf8] p-1 shadow-xl"
            >
              <div className="space-y-0.5">
                {onRename && (
                  <button
                    type="button"
                    onClick={() => {
                      onRename(asset);
                    }}
                    className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-[#1f2a44] transition-colors hover:bg-[#f1ece2]"
                  >
                    <Pencil className="h-4 w-4" />
                    Rename
                  </button>
                )}
                {onPromote && (
                  <button
                    type="button"
                    onClick={() => {
                      onPromote(asset);
                    }}
                    className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-[#1f2a44] transition-colors hover:bg-[#f1ece2]"
                  >
                    <Globe className="h-4 w-4" />
                    Promote to Archive
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => {
                    onDownload(asset.id);
                  }}
                  className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-[#1f2a44] transition-colors hover:bg-[#f1ece2]"
                >
                  <Download className="h-4 w-4" />
                  Download
                </button>
                <button
                  type="button"
                  onClick={() => {
                    onDelete(asset.id);
                  }}
                  className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-red-700 transition-colors hover:bg-[#f1ece2]"
                >
                  <Trash2 className="h-4 w-4" />
                  Delete
                </button>
              </div>
              <Popover.Arrow className="fill-[#d8d0bf]" />
            </Popover.Content>
          </Popover.Portal>
        </Popover.Root>
      </td>
    </tr>
  );
}

