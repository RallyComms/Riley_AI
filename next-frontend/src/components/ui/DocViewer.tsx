"use client";

import { useState, useRef, useEffect } from "react";
import { X, Send, FileText } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence } from "framer-motion";

interface Comment {
  id: string;
  author: string;
  content: string;
  timestamp: string;
  mentions?: string[];
}

interface KanbanCard {
  id: string;
  title: string;
  assignees: { name: string; initials: string }[];
  fileType: "pdf" | "docx";
}

interface DocViewerProps {
  card: KanbanCard;
  onClose: () => void;
}

const mockComments: Comment[] = [
  {
    id: "1",
    author: "Sarah",
    content: "@John check slide 4.",
    timestamp: "2h ago",
    mentions: ["John"],
  },
  {
    id: "2",
    author: "John",
    content: "Looks good! The messaging is clear and concise.",
    timestamp: "1h ago",
  },
  {
    id: "3",
    author: "Sarah",
    content: "Can we add a call-to-action section?",
    timestamp: "30m ago",
  },
];

export function DocViewer({ card, onClose }: DocViewerProps) {
  const [comments, setComments] = useState<Comment[]>(mockComments);
  const [commentInput, setCommentInput] = useState("");
  const commentsEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll comments to bottom
  useEffect(() => {
    commentsEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [comments]);

  const handleSendComment = () => {
    if (!commentInput.trim()) return;

    const newComment: Comment = {
      id: Date.now().toString(),
      author: "You",
      content: commentInput.trim(),
      timestamp: "just now",
    };

    setComments([...comments, newComment]);
    setCommentInput("");
  };

  const handleKeyPress = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSendComment();
    }
  };

  // Extract mentions from comment text
  const parseMentions = (text: string): string[] => {
    const mentionRegex = /@(\w+)/g;
    const matches = text.matchAll(mentionRegex);
    return Array.from(matches, (m) => m[1]);
  };

  return (
    <AnimatePresence>
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
        <motion.div
          initial={{ opacity: 0, scale: 0.95 }}
          animate={{ opacity: 1, scale: 1 }}
          exit={{ opacity: 0, scale: 0.95 }}
          transition={{ duration: 0.2 }}
          className="flex h-full max-h-[90vh] w-full max-w-6xl overflow-hidden rounded-xl bg-white shadow-2xl"
          onClick={(e) => e.stopPropagation()}
        >
          {/* Left Side - Document Preview */}
          <div className="flex flex-1 flex-col border-r border-slate-200">
            {/* Preview Header */}
            <div className="flex items-center justify-between border-b border-slate-200 bg-slate-50 px-6 py-4">
              <div className="flex items-center gap-3">
                <FileText
                  className={cn(
                    "h-5 w-5",
                    card.fileType === "pdf" ? "text-red-500" : "text-blue-500"
                  )}
                />
                <h2 className="text-lg font-semibold text-slate-900">{card.title}</h2>
              </div>
              <button
                type="button"
                onClick={onClose}
                className="rounded-lg p-1.5 text-slate-400 transition-colors hover:bg-slate-200 hover:text-slate-600"
                aria-label="Close"
              >
                <X className="h-5 w-5" />
              </button>
            </div>

            {/* Document Preview Area */}
            <div className="flex-1 overflow-auto bg-slate-100 p-8">
              <div className="mx-auto max-w-3xl">
                {/* Mock Document Preview */}
                <div className="rounded-lg border border-slate-300 bg-white shadow-lg">
                  <div className="flex h-[600px] items-center justify-center bg-gradient-to-br from-slate-50 to-slate-100">
                    <div className="text-center">
                      <FileText className="mx-auto h-16 w-16 text-slate-400" />
                      <p className="mt-4 text-lg font-medium text-slate-600">
                        Document Preview
                      </p>
                      <p className="mt-2 text-sm text-slate-500">
                        {card.fileType.toUpperCase()} content would appear here
                      </p>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Right Side - Comment Stream */}
          <div className="flex w-96 flex-col border-l border-slate-200 bg-white">
            {/* Comments Header */}
            <div className="border-b border-slate-200 bg-slate-50 px-6 py-4">
              <h3 className="text-sm font-semibold text-slate-900">Comments</h3>
              <p className="mt-1 text-xs text-slate-500">
                {comments.length} comment{comments.length !== 1 ? "s" : ""}
              </p>
            </div>

            {/* Comments List */}
            <div className="flex-1 overflow-y-auto px-6 py-4 [&::-webkit-scrollbar]:hidden [-ms-overflow-style:'none'] [scrollbar-width:'none']">
              <div className="space-y-4">
                {comments.map((comment) => {
                  const mentions = parseMentions(comment.content);
                  return (
                    <div key={comment.id} className="space-y-2">
                      <div className="flex items-center gap-2">
                        <div className="flex h-6 w-6 items-center justify-center rounded-full bg-slate-700 text-xs font-medium text-white">
                          {comment.author.charAt(0)}
                        </div>
                        <span className="text-xs font-medium text-slate-900">
                          {comment.author}
                        </span>
                        <span className="text-xs text-slate-400">{comment.timestamp}</span>
                      </div>
                      <p className="text-sm text-slate-700">
                        {comment.content.split(/(@\w+)/g).map((part, idx) => {
                          if (part.startsWith("@")) {
                            const mention = part.slice(1);
                            return (
                              <span
                                key={idx}
                                className="font-medium text-amber-600"
                              >
                                {part}
                              </span>
                            );
                          }
                          return <span key={idx}>{part}</span>;
                        })}
                      </p>
                    </div>
                  );
                })}
                <div ref={commentsEndRef} />
              </div>
            </div>

            {/* Comment Input */}
            <div className="border-t border-slate-200 bg-slate-50 p-4">
              <div className="flex gap-2">
                <textarea
                  value={commentInput}
                  onChange={(e) => setCommentInput(e.target.value)}
                  onKeyDown={handleKeyPress}
                  placeholder="Add a comment... (Use @ to mention)"
                  className="flex-1 resize-none rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 placeholder:text-slate-400 focus:border-amber-400 focus:outline-none focus:ring-2 focus:ring-amber-400 focus:ring-offset-1"
                  rows={2}
                />
                <button
                  type="button"
                  onClick={handleSendComment}
                  disabled={!commentInput.trim()}
                  className={cn(
                    "flex h-10 w-10 items-center justify-center rounded-lg transition-colors",
                    commentInput.trim()
                      ? "bg-amber-400 text-slate-900 hover:bg-amber-500"
                      : "bg-slate-200 text-slate-400 cursor-not-allowed"
                  )}
                  aria-label="Send comment"
                >
                  <Send className="h-4 w-4" />
                </button>
              </div>
              <p className="mt-2 text-xs text-slate-500">
                Press Enter to send, Shift+Enter for new line
              </p>
            </div>
          </div>
        </motion.div>
      </div>
    </AnimatePresence>
  );
}

