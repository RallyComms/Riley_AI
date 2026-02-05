"use client";

import { useState } from "react";
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

interface AssetRowProps {
  asset: Asset;
  onTagChange: (assetId: string, tags: AssetTag[]) => void;
  onDelete: (assetId: string) => void;
  onDownload: (assetId: string) => void;
  onAIEnabledChange: (assetId: string, enabled: boolean) => void;
  onClick?: (asset: Asset) => void;
  onRename?: (asset: Asset) => void;
  onPromote?: (asset: Asset) => void;
}

// Helper function to determine if file type supports AI processing
function isAISupportedType(type: Asset["type"]): boolean {
  return type === "pdf" || type === "docx";
}

const allTags: AssetTag[] = ["Messaging", "Research", "Strategy", "Media", "Pitch", "Other"];

const tagColors: Record<AssetTag, { bg: string; text: string; border: string }> = {
  Messaging: {
    bg: "bg-purple-500/10",
    text: "text-purple-400",
    border: "border-purple-500/20",
  },
  Research: {
    bg: "bg-cyan-500/10",
    text: "text-cyan-400",
    border: "border-cyan-500/20",
  },
  Strategy: {
    bg: "bg-amber-500/10",
    text: "text-amber-400",
    border: "border-amber-500/20",
  },
  Media: {
    bg: "bg-pink-500/10",
    text: "text-pink-400",
    border: "border-pink-500/20",
  },
  Pitch: {
    bg: "bg-orange-500/10",
    text: "text-orange-400",
    border: "border-orange-500/20",
  },
  Other: {
    bg: "bg-slate-500/10",
    text: "text-slate-400",
    border: "border-slate-500/20",
  },
};

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

export function AssetRow({ asset, onTagChange, onDelete, onDownload, onAIEnabledChange, onClick, onRename, onPromote }: AssetRowProps) {
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
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/v1/files/${asset.id}/tags`,
        {
          method: "PUT",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ tags: newTags }),
        }
      );

      if (!response.ok) {
        // Revert on error
        onTagChange(asset.id, asset.tags);
        const errorData = await response.json().catch(() => ({
          detail: "Failed to update tags",
        }));
        throw new Error(
          errorData.detail || `Failed to update tags: ${response.status}`
        );
      }
    } catch (error) {
      console.error("Failed to update tags:", error);
      // Revert optimistic update
      onTagChange(asset.id, asset.tags);
      alert("Failed to save tags. Please try again.");
    } finally {
      setIsUpdatingTags(false);
    }
  };

  return (
    <tr className="group border-b border-zinc-800/50 last:border-0 transition-colors hover:bg-zinc-800/50">
      {/* Icon */}
      <td className="px-6 py-4">
        <div className="flex items-center justify-center">
          {asset.status === "processing" ? (
            <Loader2 className="h-5 w-5 animate-spin text-zinc-400" />
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
          className="font-medium text-zinc-100 hover:text-amber-400 transition-colors cursor-pointer text-left max-w-[300px] truncate block"
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
              <span
                key={tag}
                className={cn(
                  "inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium",
                  tagColors[tag].bg,
                  tagColors[tag].text,
                  tagColors[tag].border
                )}
              >
                {tag}
              </span>
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
                    ? "border border-zinc-700/50 bg-zinc-800/30 text-zinc-400 hover:border-zinc-600 hover:bg-zinc-800/50"
                    : "text-zinc-500 opacity-0 group-hover:opacity-100"
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
                className="z-50 w-48 rounded-lg border border-zinc-700 bg-zinc-900/95 backdrop-blur-md p-2 shadow-xl"
              >
                <div className="space-y-1">
                  {allTags.map((tag) => {
                    const isChecked = asset.tags.includes(tag);
                    return (
                      <label
                        key={tag}
                        className="flex cursor-pointer items-center gap-2 rounded-md px-3 py-2 text-sm text-zinc-100 transition-colors hover:bg-zinc-800"
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
                                    tagColors[tag].border,
                                    tagColors[tag].bg,
                                    "border-transparent"
                                  )
                                : "border-zinc-600 bg-transparent"
                            )}
                          >
                            {isChecked && (
                              <Check className={cn("h-3 w-3", tagColors[tag].text)} />
                            )}
                          </div>
                        </div>
                        <span className="flex-1">{tag}</span>
                      </label>
                    );
                  })}
                </div>
                <Popover.Arrow className="fill-zinc-700" />
              </Popover.Content>
            </Popover.Portal>
          </Popover.Root>
        </div>
      </td>

      {/* Meta */}
      <td className="px-6 py-4">
        <div className="space-y-0.5">
          <div className="text-sm text-zinc-500" suppressHydrationWarning>{formatDate(asset.uploadDate)}</div>
          <div className="text-xs text-zinc-500">{asset.size} • {asset.uploader}</div>
        </div>
      </td>

      {/* Riley's Memory (AI Toggle) */}
      <td className="px-6 py-4">
        <div className="flex items-center justify-center">
          {isAISupportedType(asset.type) ? (
            <button
              type="button"
              onClick={() => onAIEnabledChange(asset.id, !asset.aiEnabled)}
              className={cn(
                "relative inline-flex h-6 w-11 items-center rounded-full transition-colors focus:outline-none focus:ring-2 focus:ring-emerald-500/50 focus:ring-offset-2 focus:ring-offset-zinc-900",
                asset.aiEnabled
                  ? "bg-emerald-500/20 border border-emerald-500/30"
                  : "bg-zinc-800 border border-zinc-700"
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
                  <Brain className="h-3 w-3 text-zinc-500" />
                )}
              </div>
            </button>
          ) : (
            <div className="relative group">
              <button
                type="button"
                disabled
                className={cn(
                  "relative inline-flex h-6 w-11 items-center rounded-full bg-zinc-800/30 border border-zinc-700/50 opacity-50 cursor-not-allowed"
                )}
                aria-label="Media processing disabled for speed"
              >
                <span className="inline-block h-4 w-4 transform rounded-full bg-zinc-600 translate-x-1" />
                <div className="absolute inset-0 flex items-center justify-center">
                  <Brain className="h-3 w-3 text-zinc-600" />
                </div>
              </button>
              {/* Tooltip */}
              <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-2 opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-10">
                <div className="bg-zinc-900 border border-zinc-800 rounded-lg px-3 py-2 text-xs text-zinc-400 whitespace-nowrap shadow-xl">
                  Media processing disabled for speed
                  <div className="absolute left-1/2 -translate-x-1/2 top-full w-0 h-0 border-l-4 border-r-4 border-t-4 border-transparent border-t-zinc-800" />
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
              className="rounded-md p-1.5 text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-100"
              aria-label="More actions"
            >
              <MoreHorizontal className="h-4 w-4" />
            </button>
          </Popover.Trigger>
          <Popover.Portal>
            <Popover.Content
              sideOffset={5}
              align="end"
              className="z-50 min-w-[150px] rounded-lg border border-zinc-800 bg-zinc-900 p-1 shadow-xl"
            >
              <div className="space-y-0.5">
                {onRename && (
                  <button
                    type="button"
                    onClick={() => {
                      onRename(asset);
                    }}
                    className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-zinc-100 transition-colors hover:bg-zinc-800"
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
                    className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-zinc-100 transition-colors hover:bg-zinc-800"
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
                  className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-zinc-100 transition-colors hover:bg-zinc-800"
                >
                  <Download className="h-4 w-4" />
                  Download
                </button>
                <button
                  type="button"
                  onClick={() => {
                    onDelete(asset.id);
                  }}
                  className="flex w-full items-center gap-2 rounded-md px-3 py-2 text-sm text-red-400 transition-colors hover:bg-zinc-800"
                >
                  <Trash2 className="h-4 w-4" />
                  Delete
                </button>
              </div>
              <Popover.Arrow className="fill-zinc-800" />
            </Popover.Content>
          </Popover.Portal>
        </Popover.Root>
      </td>
    </tr>
  );
}

