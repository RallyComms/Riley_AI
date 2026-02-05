"use client";

import { ReactNode, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { LayoutDashboard, MessageSquare, Search, FileText, Home, Sparkles, X } from "lucide-react";
import { cn } from "@app/lib/utils";
import { NotificationBell } from "@app/components/ui/NotificationBell";
import { RileyOrb } from "@app/components/ui/RileyOrb";
import { ChatInterface } from "@app/components/ChatInterface";
import { motion, AnimatePresence } from "framer-motion";

type NavItem = {
  name: string;
  href: string;
  icon: React.ReactNode;
};

const navItems: NavItem[] = [
  { name: "Overview", href: "/", icon: <LayoutDashboard className="h-5 w-5" /> },
  { name: "Messaging", href: "/messaging", icon: <MessageSquare className="h-5 w-5" /> },
  { name: "Research", href: "/research", icon: <Search className="h-5 w-5" /> },
  { name: "Assets", href: "/assets", icon: <FileText className="h-5 w-5" /> },
];

export function Shell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const [isChatOpen, setIsChatOpen] = useState(false);

  // Extract breadcrumbs from pathname
  const getBreadcrumbs = () => {
    const segments = pathname.split("/").filter(Boolean);
    if (segments.length === 0) return "Dashboard";
    
    // Handle campaign pages
    if (segments[0] === "campaign" && segments[1]) {
      const campaignName = segments[1]
        .split("-")
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join(" ");
      
      // Add sub-page if present (messaging, assets, etc.)
      if (segments[2]) {
        const subPage = segments[2]
          .split("-")
          .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
          .join(" ");
        return `Campaign > ${campaignName} > ${subPage}`;
      }
      
      return `Campaign > ${campaignName}`;
    }
    
    // Handle other pages
    return segments
      .map((seg) => seg.charAt(0).toUpperCase() + seg.slice(1))
      .join(" > ");
  };

  return (
    <div className="flex h-screen overflow-hidden bg-slate-50">
      {/* Left Sidebar - Fixed */}
      <aside className="fixed left-0 top-0 z-40 h-screen w-64 border-r border-slate-800 bg-slate-900 text-white">
        {/* Logo */}
        <div className="flex h-16 items-center border-b border-slate-800 px-6">
          <Link
            href="/"
            className="flex items-center gap-2 text-lg font-semibold text-white transition-colors hover:text-amber-400"
          >
            <Home className="h-5 w-5" />
            <span>RILEY | PLATFORM</span>
          </Link>
        </div>

        {/* Navigation */}
        <nav className="mt-6 space-y-1 px-3">
          {navItems.map((item) => {
            const isActive = pathname === item.href || pathname.startsWith(item.href + "/");
            return (
              <Link
                key={item.href}
                href={item.href}
                className={cn(
                  "flex items-center gap-3 rounded-lg px-4 py-3 text-sm font-medium transition-colors",
                  isActive
                    ? "bg-slate-800 text-amber-400"
                    : "text-slate-300 hover:bg-slate-800 hover:text-white"
                )}
              >
                {item.icon}
                {item.name}
              </Link>
            );
          })}
        </nav>

        {/* User Profile (Bottom) */}
        <div className="absolute bottom-0 left-0 right-0 border-t border-slate-800 p-4">
          <div className="flex items-center gap-3">
            <div className="flex h-8 w-8 items-center justify-center rounded-full bg-slate-700 text-xs font-medium text-white">
              A
            </div>
            <div className="flex-1 min-w-0">
              <p className="truncate text-sm font-medium text-white">Alex</p>
              <p className="truncate text-xs text-slate-400">alex@rally.org</p>
            </div>
          </div>
        </div>
      </aside>

      {/* Main Content Area */}
      <div className="flex flex-1 flex-col pl-64">
        {/* Top Bar */}
        <header className="sticky top-0 z-30 flex h-16 items-center justify-between border-b border-slate-200 bg-white/80 backdrop-blur-sm px-6">
          {/* Breadcrumbs */}
          <div className="flex items-center gap-2 text-sm text-slate-600">
            <span>{getBreadcrumbs()}</span>
          </div>

          {/* Right Side Actions */}
          <div className="flex items-center gap-4">
            {/* Ask Riley Button */}
            <button
              type="button"
              onClick={() => setIsChatOpen(!isChatOpen)}
              className={cn(
                "inline-flex items-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-colors",
                isChatOpen
                  ? "bg-amber-400 text-slate-900 hover:bg-amber-500"
                  : "bg-slate-100 text-slate-700 hover:bg-slate-200"
              )}
            >
              <Sparkles className="h-4 w-4" />
              Ask Riley
            </button>
            <NotificationBell />
          </div>
        </header>

        {/* Scrollable Main Content */}
        <main className="flex-1 overflow-y-auto bg-slate-50">
          {children}
        </main>
      </div>

      {/* Riley Orb - Fixed Bottom Right */}
      <RileyOrb />

      {/* Slide-out Chat Panel */}
      <AnimatePresence>
        {isChatOpen && (
          <>
            {/* Backdrop */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}
              className="fixed inset-0 z-40 bg-black/20"
              onClick={() => setIsChatOpen(false)}
            />
            {/* Chat Panel */}
            <motion.div
              initial={{ x: "100%" }}
              animate={{ x: 0 }}
              exit={{ x: "100%" }}
              transition={{ type: "spring", damping: 25, stiffness: 200 }}
              className="fixed right-0 top-0 z-50 h-full w-full max-w-md border-l border-slate-200 bg-white shadow-2xl"
            >
              {/* Chat Header */}
              <div className="flex h-16 items-center justify-between border-b border-slate-200 bg-slate-50 px-6">
                <div className="flex items-center gap-2">
                  <Sparkles className="h-5 w-5 text-amber-500" />
                  <h2 className="text-lg font-semibold text-slate-900">Riley Assistant</h2>
                </div>
                <button
                  type="button"
                  onClick={() => setIsChatOpen(false)}
                  className="rounded-lg p-1.5 text-slate-400 transition-colors hover:bg-slate-200 hover:text-slate-600"
                  aria-label="Close chat"
                >
                  <X className="h-5 w-5" />
                </button>
              </div>

              {/* Chat Interface */}
              <div className="h-[calc(100%-4rem)]">
                <ChatInterface tenantId="rally" />
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  );
}

