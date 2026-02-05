"use client";

import { useState } from "react";
import { X, Trash2, AlertTriangle, Loader2 } from "lucide-react";
import { Asset } from "@app/lib/types";
import { cn } from "@app/lib/utils";

interface DeleteFileModalProps {
  isOpen: boolean;
  onClose: () => void;
  file: Asset | null;
  currentContext: "assets" | "messaging" | "research" | "strategy" | "pitch";
  onDelete: (fileId: string, isGlobal: boolean) => Promise<void>;
}

type Step = "selection" | "confirmation";

export function DeleteFileModal({
  isOpen,
  onClose,
  file,
  currentContext,
  onDelete,
}: DeleteFileModalProps) {
  const [step, setStep] = useState<Step>("selection");
  const [isDeleting, setIsDeleting] = useState(false);

  if (!isOpen || !file) return null;

  const handleLocalDelete = async () => {
    if (!file) return;
    
    setIsDeleting(true);
    try {
      await onDelete(file.id, false);
      onClose();
      setStep("selection");
    } catch (error) {
      console.error("Failed to remove tag:", error);
      alert("Failed to remove file from this view. Please try again.");
    } finally {
      setIsDeleting(false);
    }
  };

  const handleGlobalDelete = async () => {
    if (!file) return;
    
    setIsDeleting(true);
    try {
      await onDelete(file.id, true);
      onClose();
      setStep("selection");
    } catch (error) {
      console.error("Failed to delete file:", error);
      alert("Failed to delete file. Please try again.");
    } finally {
      setIsDeleting(false);
    }
  };

  const handleBack = () => {
    setStep("selection");
  };

  // Map context to display name
  const contextName = {
    assets: "Assets Vault",
    messaging: "Messaging",
    research: "Research",
    strategy: "Strategy",
    pitch: "Pitch & Intake",
  }[currentContext];

  // In Assets context: show selection if file has tags, otherwise skip to confirmation
  const shouldSkipSelection = currentContext === "assets" && (!file.tags || file.tags.length === 0);
  const hasTags = file.tags && file.tags.length > 0;

  return (
    <div
      className={cn(
        "fixed inset-0 z-50 flex items-center justify-center",
        isOpen ? "opacity-100" : "opacity-0 pointer-events-none"
      )}
      onClick={(e) => {
        if (e.target === e.currentTarget) {
          onClose();
        }
      }}
    >
      {/* Backdrop */}
      <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />

      {/* Modal */}
      <div className="relative z-50 w-full max-w-md rounded-xl border border-zinc-800 bg-zinc-950 shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-zinc-800 p-6">
          <h2 className="text-xl font-semibold text-zinc-100">
            {step === "confirmation" ? (
              <>
                <AlertTriangle className="mr-2 inline-block h-5 w-5 text-red-500" />
                Irreversible Action
              </>
            ) : (
              `Delete ${file.name}?`
            )}
          </h2>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md p-1.5 text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-100"
            disabled={isDeleting}
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Content */}
        <div className="p-6">
          {step === "selection" && !shouldSkipSelection ? (
            <>
              {currentContext === "assets" && hasTags ? (
                <>
                  <p className="mb-2 text-sm text-zinc-300">
                    This file is currently active in:
                  </p>
                  <div className="mb-4 flex flex-wrap gap-2">
                    {file.tags.map((tag) => (
                      <span
                        key={tag}
                        className="rounded-full border border-zinc-700 bg-zinc-900/50 px-2 py-1 text-xs text-zinc-300"
                      >
                        {tag}
                      </span>
                    ))}
                  </div>
                  <p className="mb-6 text-sm text-zinc-400">
                    Choose how you want to manage this file:
                  </p>

                  {/* Option A: Remove from Workstreams */}
                  <button
                    type="button"
                    onClick={handleLocalDelete}
                    disabled={isDeleting}
                    className={cn(
                      "mb-3 w-full rounded-lg border border-zinc-700 bg-zinc-900/50 px-4 py-3 text-left transition-colors",
                      "hover:bg-zinc-900 hover:border-zinc-600",
                      "disabled:opacity-50 disabled:cursor-not-allowed"
                    )}
                  >
                    <div className="flex items-center justify-between">
                      <div>
                        <div className="font-medium text-zinc-100">
                          Remove from these Workstreams only
                        </div>
                        <div className="mt-1 text-xs text-zinc-500">
                          Removes all tags from this file. File remains in the
                          Assets Vault but disappears from all workstream tabs.
                        </div>
                      </div>
                      {isDeleting && (
                        <Loader2 className="h-4 w-4 animate-spin text-zinc-400" />
                      )}
                    </div>
                  </button>
                </>
              ) : (
                <p className="mb-6 text-sm text-zinc-400">
                  Choose how you want to remove this file:
                </p>
              )}

              {/* Option B: Local Delete (Remove from Context) - Only for workstream pages */}
              {currentContext !== "assets" && (
                <button
                  type="button"
                  onClick={handleLocalDelete}
                  disabled={isDeleting}
                  className={cn(
                    "mb-3 w-full rounded-lg border border-zinc-700 bg-zinc-900/50 px-4 py-3 text-left transition-colors",
                    "hover:bg-zinc-900 hover:border-zinc-600",
                    "disabled:opacity-50 disabled:cursor-not-allowed"
                  )}
                >
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="font-medium text-zinc-100">
                        Remove from {contextName}
                      </div>
                      <div className="mt-1 text-xs text-zinc-500">
                        Removes this file from the {contextName} view only. File
                        remains in the Vault and other workstreams.
                      </div>
                    </div>
                    {isDeleting && (
                      <Loader2 className="h-4 w-4 animate-spin text-zinc-400" />
                    )}
                  </div>
                </button>
              )}

              {/* Option C: Global Delete */}
              <button
                type="button"
                onClick={() => setStep("confirmation")}
                disabled={isDeleting}
                className={cn(
                  "w-full rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-left transition-colors",
                  "hover:bg-red-500/20 hover:border-red-500/50",
                  "disabled:opacity-50 disabled:cursor-not-allowed"
                )}
              >
                <div className="flex items-center justify-between">
                  <div>
                    <div className="font-medium text-red-400">
                      {currentContext === "assets" ? "Delete from System" : "Delete from Everywhere"}
                    </div>
                    <div className="mt-1 text-xs text-zinc-500">
                      Permanently deletes the file from disk, Qdrant, and all
                      workstreams. This cannot be undone.
                    </div>
                  </div>
                  <Trash2 className="h-4 w-4 text-red-400" />
                </div>
              </button>
            </>
          ) : (
            <>
              {/* Confirmation Step */}
              <div className="mb-6">
                <p className="mb-2 text-sm text-zinc-300">
                  This will permanently delete{" "}
                  <span className="font-semibold text-zinc-100">
                    {file.name}
                  </span>{" "}
                  from:
                </p>
                <ul className="ml-4 list-disc space-y-1 text-sm text-zinc-400">
                  <li>The Assets Vault</li>
                  <li>All active workstreams (Messaging, Research, Strategy, Pitch)</li>
                  <li>Qdrant vector database</li>
                  <li>Disk storage</li>
                </ul>
                <p className="mt-4 text-sm font-medium text-red-400">
                  ⚠️ This action cannot be undone.
                </p>
              </div>

              {/* Actions */}
              <div className="flex gap-3">
                <button
                  type="button"
                  onClick={shouldSkipSelection ? onClose : handleBack}
                  disabled={isDeleting}
                  className={cn(
                    "flex-1 rounded-lg border border-zinc-700 bg-zinc-900/50 px-4 py-2.5 font-medium text-zinc-300 transition-colors",
                    "hover:bg-zinc-900 hover:text-zinc-100",
                    "disabled:opacity-50 disabled:cursor-not-allowed"
                  )}
                >
                  Cancel
                </button>
                <button
                  type="button"
                  onClick={handleGlobalDelete}
                  disabled={isDeleting}
                  className={cn(
                    "flex-1 rounded-lg border border-red-500/50 bg-red-500/20 px-4 py-2.5 font-medium text-red-400 transition-colors",
                    "hover:bg-red-500/30 hover:border-red-500",
                    "disabled:opacity-50 disabled:cursor-not-allowed",
                    "flex items-center justify-center gap-2"
                  )}
                >
                  {isDeleting ? (
                    <>
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Deleting...
                    </>
                  ) : (
                    <>
                      <Trash2 className="h-4 w-4" />
                      Yes, Delete Everything
                    </>
                  )}
                </button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

