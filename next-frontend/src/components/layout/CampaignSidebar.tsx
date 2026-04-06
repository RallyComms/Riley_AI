"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { usePathname, useParams } from "next/navigation";
import { useUser } from "@clerk/nextjs";
import {
  LayoutDashboard,
  MessageSquare,
  Search,
  FileText,
  Clapperboard,
  Inbox,
  Target,
  ChevronLeft,
  ChevronRight,
  MessageCircle,
} from "lucide-react";
import { cn } from "@app/lib/utils";
import { useCampaignName } from "@app/lib/useCampaignName";

type NavItem = {
  name: string;
  href: string;
  icon: React.ReactNode;
};

interface CampaignSidebarProps {
  isCollapsed: boolean;
  onToggle: () => void;
}

export function CampaignSidebar({ isCollapsed, onToggle }: CampaignSidebarProps) {
  const params = useParams();
  const campaignId = params.id as string;
  const pathname = usePathname();
  const { user } = useUser();
  const { name: campaignName } = useCampaignName(campaignId);
  const [isMounted, setIsMounted] = useState(false);

  // Defer user identity rendering until after mount for hydration safety.
  useEffect(() => {
    setIsMounted(true);
  }, []);

  const navItems: NavItem[] = [
    {
      name: "Overview",
      href: `/campaign/${campaignId}`,
      icon: <LayoutDashboard className="h-5 w-5" />,
    },
    {
      name: "Pitch & Intake",
      href: `/campaign/${campaignId}/pitch`,
      icon: <Inbox className="h-5 w-5" />,
    },
    {
      name: "Research",
      href: `/campaign/${campaignId}/research`,
      icon: <Search className="h-5 w-5" />,
    },
    {
      name: "Strategy",
      href: `/campaign/${campaignId}/strategy`,
      icon: <Target className="h-5 w-5" />,
    },
    {
      name: "Messaging",
      href: `/campaign/${campaignId}/messaging`,
      icon: <MessageSquare className="h-5 w-5" />,
    },
    {
      name: "Media",
      href: `/campaign/${campaignId}/media`,
      icon: <Clapperboard className="h-5 w-5" />,
    },
    {
      name: "Assets",
      href: `/campaign/${campaignId}/assets`,
      icon: <FileText className="h-5 w-5" />,
    },
    {
      name: "Riley AI",
      href: `/campaign/${campaignId}/riley`,
      icon: (
        <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-[#dcebe5] text-[11px] font-semibold text-[#1f2a44]">
          R
        </span>
      ),
    },
    {
      name: "Conversations",
      href: `/campaign/${campaignId}/conversations`,
      icon: <MessageCircle className="h-5 w-5" />,
    },
  ];

  return (
    <aside
      className={cn(
        "h-screen min-h-0 shrink-0 border-r border-[#cddfd8] bg-[#e6f2ee] text-[#1f2a44] transition-all duration-300 flex flex-col overflow-hidden",
        isCollapsed ? "w-20" : "w-64"
      )}
    >
      <div className={cn("px-5 pt-5 pb-4 border-b border-[#cddfd8]", isCollapsed && "px-3")}>
        <Link
          href="/"
          className={cn(
            "mb-5 inline-flex items-center gap-1.5 text-xs text-[#64748b] hover:text-[#1f2a44] transition-colors",
            isCollapsed && "mb-3 justify-center w-full"
          )}
        >
          <ChevronLeft className="h-3.5 w-3.5 flex-shrink-0" />
          {!isCollapsed && <span>All Campaigns</span>}
        </Link>
        {!isCollapsed ? (
          <>
            <h2 className="line-clamp-2 break-words text-lg font-semibold tracking-tight leading-tight text-[#1f2a44]" title={campaignName || "Campaign"}>
              {campaignName || "Campaign"}
            </h2>
            <p className="mt-1 text-[11px] text-[#64748b]">Active campaign</p>
          </>
        ) : (
          <div className="mx-auto h-9 w-9 rounded-full bg-[#dcebe5] text-[#1f2a44] text-sm font-semibold flex items-center justify-center">
            {(campaignName || "C").slice(0, 1).toUpperCase()}
          </div>
        )}
      </div>

      {/* Navigation */}
      <nav className="mt-3 min-h-0 flex-1 space-y-1 px-2 overflow-y-auto overflow-x-hidden">
        {navItems.map((item) => {
          const isActive =
            pathname === item.href || pathname.startsWith(item.href + "/");
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "mx-1 flex items-center gap-2.5 rounded-md px-3 py-2 text-sm transition-colors border-l-2",
                isCollapsed ? "justify-center px-3" : "",
                isActive
                  ? "border-[#d4ad47] bg-[#dcebe5] text-[#1f2a44] font-medium"
                  : "border-transparent text-[#64748b] hover:text-[#1f2a44] hover:bg-[#d7e8e0]"
              )}
              title={isCollapsed ? item.name : undefined}
            >
              <span className="flex-shrink-0">{item.icon}</span>
              {!isCollapsed && <span className="truncate">{item.name}</span>}
              {!isCollapsed && isActive && (
                <span className="ml-auto h-1.5 w-1.5 rounded-full bg-[#d4ad47]" />
              )}
            </Link>
          );
        })}
      </nav>

      {/* User mini-profile */}
      <div className={cn("mt-auto border-t border-[#cddfd8] p-4", isCollapsed && "px-3")}>
        {!isMounted ? (
          <div className="h-8 w-full rounded bg-[#dcebe5] animate-pulse" />
        ) : isCollapsed ? (
          <div className="mx-auto h-8 w-8 rounded-full bg-[#dcebe5] text-xs font-semibold flex items-center justify-center text-[#1f2a44]">
            {(user?.firstName?.[0] || user?.username?.[0] || user?.primaryEmailAddress?.emailAddress?.[0] || "U").toUpperCase()}
          </div>
        ) : (
          <div className="flex items-center gap-2.5">
            <div className="h-8 w-8 rounded-full bg-[#dcebe5] text-xs font-semibold flex items-center justify-center shrink-0 text-[#1f2a44]">
              {(user?.firstName?.[0] || user?.username?.[0] || user?.primaryEmailAddress?.emailAddress?.[0] || "U").toUpperCase()}
            </div>
            <div className="min-w-0">
              <p className="truncate text-xs font-medium text-[#1f2a44]">
                {user?.fullName || user?.username || "Campaign member"}
              </p>
              <p className="text-[10px] text-[#64748b]">Campaign member</p>
            </div>
          </div>
        )}
      </div>

      {/* Toggle Button - At Bottom */}
      <button
        type="button"
        onClick={onToggle}
        className="w-full p-3 border-t border-[#cddfd8] hover:bg-[#d7e8e0] hover:text-[#1f2a44] cursor-pointer flex justify-center items-center transition-all duration-200 text-[#64748b]"
        aria-label={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
        title={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
      >
        {isCollapsed ? (
          <ChevronRight className="h-5 w-5" />
        ) : (
          <ChevronLeft className="h-5 w-5" />
        )}
      </button>
    </aside>
  );
}
