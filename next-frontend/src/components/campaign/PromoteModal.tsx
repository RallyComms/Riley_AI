"use client";

import { useState } from "react";
import { X, Globe, Sparkles } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { cn } from "@app/lib/utils";

interface PromoteModalProps {
  isOpen: boolean;
  onClose: () => void;
  onPromote: (isGolden: boolean) => Promise<void>;
  fileName: string;
}

export function PromoteModal({
  isOpen,
  onClose,
  onPromote,
  fileName,
}: PromoteModalProps) {
  const [isGolden, setIsGolden] = useState(false);
  const [isPromoting, setIsPromoting] = useState(false);

  const handlePromote = async () => {
    setIsPromoting(true);
    try {
      await onPromote(isGolden);
      onClose();
      setIsGolden(false); // Reset for next time
    } catch (error) {
      console.error("Promote error:", error);
      alert(`Failed to promote file: ${error instanceof Error ? error.message : "Unknown error"}`);
    } finally {
      setIsPromoting(false);
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
              <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-blue-400/10 border border-blue-400/20">
                <Globe className="h-5 w-5 text-blue-400" />
              </div>
              <div>
                <h2 className="text-lg font-semibold text-zinc-100">Add to Rally Archive</h2>
                <p className="text-xs text-zinc-400">Promote to Global Firm Archive</p>
              </div>
            </div>
            <button
              type="button"
              onClick={onClose}
              disabled={isPromoting}
              className="rounded-lg p-1.5 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          {/* Content */}
          <div className="p-6 space-y-4">
            <p className="text-sm text-zinc-300">
              This makes the document visible to the entire firm via Global Riley.
            </p>

            {/* Golden Standard Toggle */}
            <label
              className={cn(
                "flex items-start gap-3 p-4 rounded-lg border cursor-pointer transition-colors",
                isGolden
                  ? "border-amber-500/50 bg-amber-500/10"
                  : "border-zinc-700 bg-zinc-800/30 hover:bg-zinc-800/50"
              )}
            >
              <div className="relative flex h-5 w-5 items-center justify-center flex-shrink-0 mt-0.5">
                <input
                  type="checkbox"
                  checked={isGolden}
                  onChange={(e) => setIsGolden(e.target.checked)}
                  className="sr-only"
                />
                <div
                  className={cn(
                    "flex h-5 w-5 items-center justify-center rounded border-2 transition-colors",
                    isGolden
                      ? "border-amber-500 bg-amber-500/20"
                      : "border-zinc-600 bg-transparent"
                  )}
                >
                  {isGolden && (
                    <Sparkles className="h-3 w-3 text-amber-400" />
                  )}
                </div>
              </div>
              <div className="flex-1">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium text-zinc-100">
                    🌟 Mark as Golden Standard
                  </span>
                </div>
                <p className="mt-1 text-xs text-zinc-400">
                  High Priority Learning - This document will be prioritized in Global Riley's training.
                </p>
              </div>
            </label>
          </div>

          {/* Footer */}
          <div className="flex items-center justify-end gap-3 border-t border-zinc-800 px-6 py-4">
            <button
              type="button"
              onClick={onClose}
              disabled={isPromoting}
              className="rounded-lg border border-zinc-700 bg-transparent px-4 py-2 text-sm font-medium text-zinc-300 transition-colors hover:bg-zinc-800 hover:text-zinc-100 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handlePromote}
              disabled={isPromoting}
              className={cn(
                "rounded-lg border border-blue-500/20 bg-blue-500/10 px-4 py-2 text-sm font-medium text-blue-400",
                "transition-colors hover:bg-blue-500/20 hover:border-blue-500/30",
                "disabled:opacity-50 disabled:cursor-not-allowed"
              )}
            >
              {isPromoting ? "Promoting..." : "Promote"}
            </button>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
}
