"use client";

import { useState, useEffect } from "react";
import Link from "next/link";
import { usePathname, useParams } from "next/navigation";
import {
  LayoutDashboard,
  MessageSquare,
  Search,
  FileText,
  Home,
  Clapperboard,
  Inbox,
  Target,
  ChevronLeft,
  ChevronRight,
  Sparkles,
  MessageCircle,
} from "lucide-react";
import { UserButton } from "@clerk/nextjs";
import { cn } from "@app/lib/utils";

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
  const [isMounted, setIsMounted] = useState(false);

  // Defer UserButton rendering until after mount to prevent hydration mismatch
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
      icon: <Sparkles className="h-5 w-5" />,
    },
    {
      name: "Team Chat",
      href: `/campaign/${campaignId}/chat`,
      icon: <MessageCircle className="h-5 w-5" />,
    },
  ];

  return (
    <aside
      className={cn(
        "h-screen fixed left-0 top-0 z-40 border-r border-slate-800 bg-slate-900/60 backdrop-blur-xl transition-all duration-300 flex flex-col",
        isCollapsed ? "w-20" : "w-64"
      )}
    >
      {/* Logo */}
      <div className={cn("flex h-16 items-center border-b border-white/5 px-6", isCollapsed && "justify-center")}>
        <Link
          href="/"
          className={cn(
            "flex items-center gap-2 text-lg font-semibold text-slate-100 transition-colors hover:text-amber-400",
            isCollapsed && "justify-center"
          )}
        >
          <Home className="h-5 w-5 flex-shrink-0" />
          {!isCollapsed && <span>RILEY | PLATFORM</span>}
        </Link>
      </div>

      {/* Navigation */}
      <nav className="flex-1 mt-6 space-y-2 px-2 overflow-y-auto">
        {navItems.map((item) => {
          const isActive =
            pathname === item.href || pathname.startsWith(item.href + "/");
          return (
            <Link
              key={item.href}
              href={item.href}
              className={cn(
                "flex items-center gap-3 px-4 py-3 text-sm font-medium text-slate-400 transition-all duration-200 mx-2 rounded-lg",
                isCollapsed ? "justify-center px-3" : "",
                isActive
                  ? "text-amber-400 bg-amber-400/10 border border-amber-400/20 shadow-[0_0_15px_rgba(251,191,36,0.1)]"
                  : "hover:text-amber-400 hover:bg-amber-400/10 hover:shadow-[0_0_15px_rgba(251,191,36,0.1)]"
              )}
              title={isCollapsed ? item.name : undefined}
            >
              <span className="flex-shrink-0">{item.icon}</span>
              {!isCollapsed && <span>{item.name}</span>}
            </Link>
          );
        })}
      </nav>

      {/* User Profile - Pushed to bottom */}
      <div className={cn("mt-auto border-t border-white/5 p-4", isCollapsed && "flex justify-center")}>
        <div className={cn("flex items-center", isCollapsed ? "justify-center" : "gap-3")}>
          {!isMounted ? (
            // Skeleton placeholder to prevent hydration mismatch
            <>
              <div className="h-8 w-8 rounded-full bg-slate-700/50 animate-pulse flex-shrink-0" />
              {!isCollapsed && (
                <div className="h-4 w-24 rounded bg-slate-700/50 animate-pulse" />
              )}
            </>
          ) : (
            <UserButton
              showName={!isCollapsed}
              appearance={{
                elements: {
                  avatarBox: "h-8 w-8",
                  userButtonPopoverCard: "bg-slate-900/95 border border-white/5 backdrop-blur-xl",
                  userButtonPopoverActions: "bg-slate-900/95",
                  userButtonPopoverActionButton: "text-slate-100 hover:bg-amber-400/10 hover:text-amber-400",
                  userButtonPopoverFooter: "hidden",
                },
              }}
            />
          )}
        </div>
      </div>

      {/* Toggle Button - At Bottom */}
      <button
        type="button"
        onClick={onToggle}
        className="w-full p-4 border-t border-white/5 hover:bg-amber-400/10 hover:text-amber-400 cursor-pointer flex justify-center items-center transition-all duration-200 text-slate-400"
        aria-label={isCollapsed ? "Expand sidebar" : "Collapse sidebar"}
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
