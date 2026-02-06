"use client";

import { useState, useEffect } from "react";
import { X, User, Check } from "lucide-react";
import { cn } from "@app/lib/utils";

interface AssignmentModalProps {
  isOpen: boolean;
  fileName: string;
  onClose: () => void;
  onConfirm: (userIds: string[]) => void;
  currentAssignees?: string[]; // Array of user IDs like ["sarah", "john"]
}

// Mock users - in production, this would come from the backend
const mockUsers = [
  { id: "anova", name: "Anova", initials: "AK", isMe: true },
  { id: "sarah", name: "Sarah", initials: "SJ", isMe: false },
  { id: "john", name: "John", initials: "JD", isMe: false },
];

export function AssignmentModal({
  isOpen,
  fileName,
  onClose,
  onConfirm,
  currentAssignees = [],
}: AssignmentModalProps) {
  // Track selected users (initialize from currentAssignees)
  const [selectedUsers, setSelectedUsers] = useState<Set<string>>(new Set(currentAssignees));

  // Update selection when currentAssignees changes (when modal opens)
  useEffect(() => {
    if (isOpen) {
      setSelectedUsers(new Set(currentAssignees));
    }
  }, [isOpen, currentAssignees]);

  if (!isOpen) return null;

  // Toggle user selection
  const handleUserToggle = (userId: string) => {
    setSelectedUsers((prev) => {
      const next = new Set(prev);
      if (next.has(userId)) {
        next.delete(userId);
      } else {
        next.add(userId);
      }
      return next;
    });
  };

  // Handle confirm - pass array of selected user IDs
  const handleConfirm = () => {
    onConfirm(Array.from(selectedUsers));
    onClose();
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm">
      <div className="w-full max-w-md rounded-xl border border-zinc-800 bg-zinc-900 shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between border-b border-zinc-800 p-6">
          <h2 className="text-lg font-semibold text-zinc-100">Manage Team</h2>
          <button
            onClick={onClose}
            className="text-zinc-500 hover:text-zinc-100 transition-colors"
            aria-label="Close"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Body */}
        <div className="p-6">
          <p className="text-sm text-zinc-400 mb-6">
            Select team members for <span className="font-medium text-zinc-100">{fileName}</span>
          </p>

          {/* User List */}
          <div className="space-y-2">
            {mockUsers.map((user) => {
              const isSelected = selectedUsers.has(user.id);
              return (
                <button
                  key={user.id}
                  onClick={() => handleUserToggle(user.id)}
                  className={cn(
                    "w-full flex items-center gap-3 p-4 rounded-lg border transition-colors",
                    isSelected
                      ? user.isMe
                        ? "border-amber-500/70 bg-amber-500/15 hover:border-amber-500/90 hover:bg-amber-500/20"
                        : "border-amber-500/50 bg-amber-500/10 hover:border-amber-500/70 hover:bg-amber-500/15"
                      : user.isMe
                      ? "border-amber-500/30 bg-amber-500/5 hover:border-amber-500/50 hover:bg-amber-500/10"
                      : "border-zinc-800 bg-zinc-900/50 hover:border-amber-500/50 hover:bg-amber-500/10",
                    "text-left"
                  )}
                >
                  {/* Checkbox */}
                  <div
                    className={cn(
                      "flex-shrink-0 h-5 w-5 rounded border-2 flex items-center justify-center transition-colors",
                      isSelected
                        ? "border-amber-500 bg-amber-500"
                        : "border-zinc-600 bg-transparent"
                    )}
                  >
                    {isSelected && <Check className="h-3 w-3 text-zinc-900" />}
                  </div>

                  {/* Avatar */}
                  <div
                    className={cn(
                      "flex-shrink-0 h-10 w-10 rounded-full border-2 flex items-center justify-center text-sm font-medium",
                      isSelected
                        ? user.isMe
                          ? "border-amber-500/70 bg-amber-500/30 text-amber-200"
                          : "border-amber-500/50 bg-amber-500/20 text-amber-300"
                        : user.isMe
                        ? "border-amber-500/30 bg-amber-500/10 text-amber-400"
                        : "border-zinc-700 bg-zinc-800 text-zinc-300"
                    )}
                  >
                    {user.initials}
                  </div>

                  {/* Name */}
                  <div className="flex-1">
                    <p className="text-sm font-medium text-zinc-100">
                      {user.isMe ? "Me" : user.name}
                      {user.isMe && <span className="ml-2 text-xs text-amber-400">({user.name})</span>}
                    </p>
                    <p className="text-xs text-zinc-500">
                      {user.isMe ? "Assign to yourself" : "Team Member"}
                    </p>
                  </div>
                </button>
              );
            })}
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-end gap-3 border-t border-zinc-800 p-6">
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-zinc-400 hover:text-zinc-100 transition-colors"
          >
            Cancel
          </button>
          <button
            onClick={handleConfirm}
            className="px-4 py-2 text-sm font-medium bg-amber-500 hover:bg-amber-600 text-zinc-900 rounded-md transition-colors"
          >
            Update Team
          </button>
        </div>
      </div>
    </div>
  );
}

