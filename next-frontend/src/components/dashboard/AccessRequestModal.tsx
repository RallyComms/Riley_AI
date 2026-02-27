"use client";

import { useState } from "react";
import { X } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { cn } from "@app/lib/utils";

interface AccessRequestModalProps {
  isOpen: boolean;
  campaignName: string;
  onClose: () => void;
  onSubmit: (message: string) => Promise<void>;
}

export function AccessRequestModal({ isOpen, campaignName, onClose, onSubmit }: AccessRequestModalProps) {
  const [message, setMessage] = useState("");
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleClose = () => {
    if (!isSubmitting) {
      setMessage("");
      setError(null);
      onClose();
    }
  };

  const handleSubmit = async () => {
    try {
      setIsSubmitting(true);
      setError(null);
      await onSubmit(message.trim());
      setMessage("");
      onClose();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to submit request");
    } finally {
      setIsSubmitting(false);
    }
  };

  if (!isOpen) return null;

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-[60] flex items-center justify-center p-4">
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          className="absolute inset-0 bg-black/60 backdrop-blur-sm"
          onClick={handleClose}
        />
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.95 }}
          transition={{ duration: 0.2 }}
          className="relative w-full max-w-lg rounded-xl border border-zinc-800 bg-zinc-900/95 p-5"
        >
          <div className="mb-4 flex items-center justify-between">
            <h3 className="text-base font-semibold text-zinc-100">Request Access</h3>
            <button
              type="button"
              onClick={handleClose}
              className="rounded p-1 text-zinc-400 hover:bg-zinc-800 hover:text-zinc-100"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
          <p className="mb-3 text-sm text-zinc-300">
            Request access to <span className="font-medium text-zinc-100">{campaignName}</span>.
          </p>
          <textarea
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            placeholder="Optional note for the campaign lead..."
            rows={4}
            className="w-full rounded-lg border border-zinc-800 bg-zinc-950/60 px-3 py-2 text-sm text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-amber-400/40"
          />
          {error && <p className="mt-2 text-xs text-red-400">{error}</p>}
          <div className="mt-4 flex justify-end gap-2">
            <button
              type="button"
              onClick={handleClose}
              className="rounded-lg border border-zinc-700 px-3 py-2 text-sm text-zinc-300 hover:bg-zinc-800"
              disabled={isSubmitting}
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleSubmit}
              disabled={isSubmitting}
              className={cn(
                "rounded-lg px-3 py-2 text-sm font-semibold text-zinc-900",
                isSubmitting ? "bg-amber-400/70" : "bg-amber-400 hover:bg-amber-500"
              )}
            >
              {isSubmitting ? "Submitting..." : "Submit Request"}
            </button>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
}
