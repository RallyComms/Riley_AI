"use client";

import { useState, useEffect } from "react";
import { X, Pencil } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { cn } from "@app/lib/utils";

interface RenameAssetModalProps {
  isOpen: boolean;
  onClose: () => void;
  onRename: (newName: string) => Promise<void>;
  currentName: string;
}

export function RenameAssetModal({
  isOpen,
  onClose,
  onRename,
  currentName,
}: RenameAssetModalProps) {
  const [newName, setNewName] = useState("");
  const [isRenaming, setIsRenaming] = useState(false);

  // Extract filename without extension for the input
  const getBaseName = (filename: string): string => {
    const lastDot = filename.lastIndexOf(".");
    if (lastDot === -1) return filename;
    return filename.substring(0, lastDot);
  };

  // Get file extension
  const getExtension = (filename: string): string => {
    const lastDot = filename.lastIndexOf(".");
    if (lastDot === -1) return "";
    return filename.substring(lastDot);
  };

  // Initialize input with base name (without extension)
  useEffect(() => {
    if (isOpen) {
      setNewName(getBaseName(currentName));
    }
  }, [isOpen, currentName]);

  const handleSave = async () => {
    if (!newName.trim()) return;

    const extension = getExtension(currentName);
    const fullNewName = newName.trim() + extension;

    setIsRenaming(true);
    try {
      await onRename(fullNewName);
      onClose();
    } catch (error) {
      console.error("Rename error:", error);
      alert(`Rename failed: ${error instanceof Error ? error.message : "Unknown error"}`);
    } finally {
      setIsRenaming(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSave();
    }
    if (e.key === "Escape") {
      onClose();
    }
  };

  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
        {/* Backdrop */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          className="absolute inset-0 bg-black/60 backdrop-blur-sm"
          onClick={onClose}
        />

        {/* Modal */}
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.95 }}
          transition={{ duration: 0.2 }}
          className="relative w-full max-w-md rounded-xl border border-zinc-800 bg-zinc-900/95 backdrop-blur-xl shadow-2xl"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Header */}
          <div className="flex items-center justify-between border-b border-zinc-800 px-6 py-4">
            <div className="flex items-center gap-3">
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-amber-400/10 border border-amber-400/20">
                <Pencil className="h-5 w-5 text-amber-400" />
              </div>
              <div>
                <h2 className="text-lg font-semibold text-zinc-100">Rename File</h2>
                <p className="text-xs text-zinc-400">Enter a new name for the file</p>
              </div>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="rounded-lg p-1.5 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Content */}
          <div className="p-6 space-y-4">
            <div>
              <label className="block text-sm font-medium text-zinc-300 mb-2">
                File Name
              </label>
              <input
                type="text"
                value={newName}
                onChange={(e) => setNewName(e.target.value)}
                onKeyDown={handleKeyDown}
                placeholder="Enter file name"
                autoFocus
                className={cn(
                  "w-full rounded-lg border border-zinc-800 bg-zinc-950/50 px-4 py-3",
                  "text-zinc-100 placeholder:text-zinc-600",
                  "focus:outline-none focus:ring-2 focus:ring-amber-500/50 focus:border-amber-500/50",
                  "transition-colors"
                )}
              />
              <p className="mt-2 text-xs text-zinc-500">
                Extension <span className="text-zinc-400">{getExtension(currentName)}</span> will be preserved
              </p>
            </div>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end gap-3 border-t border-zinc-800 px-6 py-4">
            <button
              type="button"
              onClick={onClose}
              disabled={isRenaming}
              className="rounded-lg border border-zinc-700 bg-transparent px-4 py-2 text-sm font-medium text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-zinc-100 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSave}
              disabled={!newName.trim() || isRenaming}
              className={cn(
                "rounded-lg border border-amber-500/20 bg-amber-500/10 px-4 py-2 text-sm font-medium text-amber-400",
                "transition-colors hover:bg-amber-500/20 hover:border-amber-500/30",
                "disabled:opacity-50 disabled:cursor-not-allowed"
              )}
            >
              {isRenaming ? "Renaming..." : "Save"}
            </button>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
}

