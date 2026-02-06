"use client";

import { useEffect, useState } from "react";
import ReactMarkdown from "react-markdown";

interface TypewriterMarkdownProps {
  content: string;
  onComplete?: () => void;
  shouldAnimate?: boolean;
}

export function TypewriterMarkdown({
  content,
  onComplete,
  shouldAnimate = true,
}: TypewriterMarkdownProps) {
  const [displayedText, setDisplayedText] = useState(shouldAnimate ? "" : content);
  const [isComplete, setIsComplete] = useState(!shouldAnimate);

  useEffect(() => {
    if (!shouldAnimate) {
      setDisplayedText(content);
      setIsComplete(true);
      return;
    }

    // Reset when content changes
    setDisplayedText("");
    setIsComplete(false);

    let currentIndex = 0;
    const charsPerChunk = 2; // Type 2-3 characters at a time for optimization
    const delayPerChunk = 12; // ~12ms per chunk = ~6ms per character

    const typeInterval = setInterval(() => {
      if (currentIndex >= content.length) {
        clearInterval(typeInterval);
        setIsComplete(true);
        onComplete?.();
        return;
      }

      const nextIndex = Math.min(currentIndex + charsPerChunk, content.length);
      setDisplayedText(content.slice(0, nextIndex));
      currentIndex = nextIndex;
    }, delayPerChunk);

    return () => {
      clearInterval(typeInterval);
    };
  }, [content, shouldAnimate, onComplete]);

  return (
    <div className="prose prose-sm max-w-none leading-loose prose-headings:mb-4 prose-headings:mt-6 prose-p:mb-6 prose-strong:text-slate-900 prose-ul:my-1 prose-li:mb-3 prose-li:leading-loose">
      <ReactMarkdown>{displayedText}</ReactMarkdown>
      {!isComplete && (
        <span className="inline-block w-0.5 h-4 bg-slate-400 ml-0.5 animate-pulse align-middle">
          |
        </span>
      )}
    </div>
  );
}

