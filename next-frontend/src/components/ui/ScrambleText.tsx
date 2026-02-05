"use client";

import React, { useState, useEffect, useRef } from "react";
import { cn } from "@app/lib/utils";

interface ScrambleTextProps {
  text: string;
  className?: string;
  scrambleChars?: string[];
  duration?: number; // Duration of scramble animation in ms
  trigger?: "mount" | "hover" | "both";
}

const DEFAULT_SCRAMBLE_CHARS = ["X", "/", "#", "@", "&", "%", "$", "?"];

/**
 * ScrambleText - Easter Egg Text Component
 * 
 * Cycles through random characters before revealing the real text.
 * Perfect for headings and special UI elements.
 */
export function ScrambleText({
  text,
  className,
  scrambleChars = DEFAULT_SCRAMBLE_CHARS,
  duration = 1000,
  trigger = "mount",
}: ScrambleTextProps) {
  const [displayText, setDisplayText] = useState<string>(text);
  const [isScrambling, setIsScrambling] = useState(false);
  const intervalRef = useRef<NodeJS.Timeout | null>(null);
  const timeoutRef = useRef<NodeJS.Timeout | null>(null);

  const scramble = () => {
    if (isScrambling) return;
    
    setIsScrambling(true);
    const chars = text.split("");
    let iterations = 0;
    const maxIterations = Math.max(text.length, 10);

    intervalRef.current = setInterval(() => {
      setDisplayText(
        chars
          .map((char, index) => {
            // Skip spaces
            if (char === " ") return " ";
            
            // Reveal characters progressively
            if (index < iterations) {
              return char;
            }
            
            // Scramble remaining characters
            return scrambleChars[Math.floor(Math.random() * scrambleChars.length)];
          })
          .join("")
      );

      iterations += 1 / 3; // Smooth reveal

      if (iterations >= maxIterations) {
        if (intervalRef.current) {
          clearInterval(intervalRef.current);
          intervalRef.current = null;
        }
        setDisplayText(text);
        setIsScrambling(false);
      }
    }, duration / maxIterations);
  };

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
      }
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, []);

  // Trigger on mount
  useEffect(() => {
    if (trigger === "mount" || trigger === "both") {
      // Small delay for better UX
      timeoutRef.current = setTimeout(() => {
        scramble();
      }, 100);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [trigger]); // Only trigger on mount/trigger change, not text change

  const handleMouseEnter = () => {
    if (trigger === "hover" || trigger === "both") {
      scramble();
    }
  };

  return (
    <span
      className={cn("inline-block", className)}
      onMouseEnter={handleMouseEnter}
    >
      {displayText}
    </span>
  );
}
