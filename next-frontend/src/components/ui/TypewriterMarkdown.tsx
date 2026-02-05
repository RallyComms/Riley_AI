"use client";

import { useEffect, useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkBreaks from "remark-breaks";

interface TypewriterMarkdownProps {
  content: string;
  onComplete?: () => void;
}

export function TypewriterMarkdown({
  content,
  onComplete,
}: TypewriterMarkdownProps) {
  const [displayedContent, setDisplayedContent] = useState("");

  useEffect(() => {
    // Reset when content changes
    setDisplayedContent("");
    
    if (!content) {
      onComplete?.();
      return;
    }
    
    // Pre-process HTML: Replace <br> tags with newlines for proper markdown rendering
    const processedContent = content.replace(/<br\s*\/?>/gi, '\n\n');
    
    // Split processed content into words (preserve spaces)
    const words = processedContent.split(/(\s+)/);
    let tokenIndex = 0;
    const wordsPerBatch = 3; // Show 3 words at a time
    const delayPerBatch = 30; // 30ms per batch

    const typeInterval = setInterval(() => {
      // Calculate tokens to show (each word = 2 tokens: word + space)
      const tokensToShow = Math.min(tokenIndex + (wordsPerBatch * 2), words.length);
      
      if (tokensToShow >= words.length) {
        // Show all remaining content
        setDisplayedContent(processedContent);
        clearInterval(typeInterval);
        onComplete?.();
        return;
      }

      // Get batch of words
      const batch = words.slice(0, tokensToShow).join("");
      setDisplayedContent(batch);
      
      // Move forward by 3 words worth of tokens
      tokenIndex = tokensToShow;
    }, delayPerBatch);

    return () => {
      clearInterval(typeInterval);
    };
  }, [content, onComplete]);

  return (
    <div className="prose prose-invert max-w-none leading-loose prose-p:mb-8 prose-p:leading-7 prose-headings:mt-8 prose-headings:mb-4 prose-ul:my-4 prose-ul:list-disc prose-ul:pl-4 prose-li:mb-4 prose-strong:text-amber-400 prose-strong:font-semibold">
      <ReactMarkdown remarkPlugins={[remarkBreaks]}>{displayedContent}</ReactMarkdown>
      {displayedContent.length < content.length && (
        <span className="inline-block w-0.5 h-4 bg-amber-400 ml-0.5 animate-pulse align-middle">
          |
        </span>
      )}
    </div>
  );
}
