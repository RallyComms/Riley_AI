"use client";

import { useEffect, useState } from "react";
import { useTheme } from "next-themes";

export function ShootingStars() {
  const { theme } = useTheme();
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  // Only show shooting stars in dark mode
  if (!mounted || theme === "light") {
    return null;
  }

  return (
    <div className="fixed inset-0 pointer-events-none z-0 overflow-hidden">
      <div className="shooting-star" style={{ top: "20%", left: "-100px", animationDelay: "0s", animationDuration: "3s" }} />
      <div className="shooting-star" style={{ top: "40%", left: "-100px", animationDelay: "1s", animationDuration: "4s" }} />
      <div className="shooting-star" style={{ top: "60%", left: "-100px", animationDelay: "2s", animationDuration: "3.5s" }} />
      <div className="shooting-star" style={{ top: "80%", left: "-100px", animationDelay: "0.5s", animationDuration: "4.5s" }} />
      <div className="shooting-star" style={{ top: "30%", left: "-100px", animationDelay: "1.5s", animationDuration: "3.8s" }} />
      <div className="shooting-star" style={{ top: "70%", left: "-100px", animationDelay: "2.5s", animationDuration: "4.2s" }} />
    </div>
  );
}
