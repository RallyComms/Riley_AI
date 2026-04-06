"use client";

import { ReactNode, useEffect, useState } from "react";
import { CampaignSidebar } from "@app/components/layout/CampaignSidebar";

interface CampaignLayoutProps {
  children: ReactNode;
}

export default function CampaignLayout({ children }: CampaignLayoutProps) {
  const [isCollapsed, setIsCollapsed] = useState(false);

  // Persist sidebar preference to avoid visual reset across refreshes.
  useEffect(() => {
    try {
      const stored = window.localStorage.getItem("campaign_sidebar_collapsed");
      if (stored === "1") setIsCollapsed(true);
    } catch {
      // no-op in restricted environments
    }
  }, []);

  useEffect(() => {
    try {
      window.localStorage.setItem("campaign_sidebar_collapsed", isCollapsed ? "1" : "0");
    } catch {
      // no-op in restricted environments
    }
  }, [isCollapsed]);

  return (
    <div className="flex h-screen overflow-hidden bg-[#f7f5ef] text-[#1f2a44]">
      <CampaignSidebar
        isCollapsed={isCollapsed}
        onToggle={() => setIsCollapsed(!isCollapsed)}
      />
      <main className="flex-1 min-w-0 overflow-hidden bg-[#f7f5ef]">
        {children}
      </main>
    </div>
  );
}
