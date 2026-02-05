"use client";

import { ReactNode, useState } from "react";
import { CampaignSidebar } from "@app/components/layout/CampaignSidebar";
import { cn } from "@app/lib/utils";

interface CampaignLayoutProps {
  children: ReactNode;
}

export default function CampaignLayout({ children }: CampaignLayoutProps) {
  const [isCollapsed, setIsCollapsed] = useState(false);

  return (
    <div className="flex h-screen overflow-hidden bg-transparent text-white">
      <CampaignSidebar
        isCollapsed={isCollapsed}
        onToggle={() => setIsCollapsed(!isCollapsed)}
      />
      <main 
        className={cn(
          "flex-1 flex flex-col min-w-0 overflow-hidden relative bg-transparent transition-all duration-300",
          isCollapsed ? "ml-20" : "ml-64"
        )}
      >
        {children}
      </main>
    </div>
  );
}
