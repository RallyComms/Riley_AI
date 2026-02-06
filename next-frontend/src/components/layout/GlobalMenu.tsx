"use client";

import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useClerk } from "@clerk/nextjs";
import { useTheme } from "next-themes";
import { Menu, Bell, User, Sparkles, LogOut, X, Sun, Moon } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence } from "framer-motion";

export function GlobalMenu() {
  const [isOpen, setIsOpen] = useState(false);
  const [mounted, setMounted] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);
  const { signOut } = useClerk();
  const router = useRouter();
  const { theme, setTheme } = useTheme();

  // Prevent hydration mismatch
  useEffect(() => {
    setMounted(true);
  }, []);

  // Close menu when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    }

    if (isOpen) {
      document.addEventListener("mousedown", handleClickOutside);
      return () => document.removeEventListener("mousedown", handleClickOutside);
    }
  }, [isOpen]);

  const menuItems = [
    {
      icon: Bell,
      label: "Notification Center",
      onClick: () => {
        console.log("Notification Center clicked");
        setIsOpen(false);
      },
    },
    {
      icon: User,
      label: "Account Settings",
      onClick: () => {
        console.log("Account Settings clicked");
        setIsOpen(false);
      },
    },
    {
      icon: mounted && theme === "light" ? Sun : Moon,
      label: "Themes",
      onClick: () => {
        setTheme(theme === "dark" ? "light" : "dark");
        setIsOpen(false);
      },
    },
    {
      icon: LogOut,
      label: "Log Out",
      onClick: async () => {
        setIsOpen(false);
        await signOut();
        router.push("/sign-in");
      },
      isDestructive: true,
    },
  ];

  return (
    <div className="absolute right-6 top-6 z-50" ref={menuRef}>
      {/* Menu Button */}
      <button
        type="button"
        onClick={() => setIsOpen(!isOpen)}
        className={cn(
          "flex h-10 w-10 items-center justify-center rounded-lg",
          "bg-zinc-900/60 backdrop-blur-sm border border-zinc-800",
          "text-zinc-300 transition-all",
          "hover:bg-zinc-800/80 hover:text-zinc-100 hover:border-zinc-700",
          "focus:outline-none focus:ring-2 focus:ring-amber-400/50 focus:ring-offset-2 focus:ring-offset-zinc-950"
        )}
        aria-label="Global Menu"
      >
        {isOpen ? (
          <X className="h-5 w-5" />
        ) : (
          <Menu className="h-5 w-5" />
        )}
      </button>

      {/* Dropdown Menu */}
      <AnimatePresence>
        {isOpen && (
          <>
            {/* Backdrop */}
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}
              className="fixed inset-0 -z-10"
              onClick={() => setIsOpen(false)}
            />
            {/* Menu Panel */}
            <motion.div
              initial={{ opacity: 0, scale: 0.95, y: -10 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95, y: -10 }}
              transition={{ duration: 0.2 }}
              className="absolute right-0 mt-2 w-56 rounded-lg border border-zinc-800 bg-zinc-900/90 backdrop-blur-xl shadow-2xl overflow-hidden"
            >
              <div className="py-1">
                {menuItems.map((item) => {
                  const Icon = item.icon;
                  return (
                    <button
                      key={item.label}
                      type="button"
                      onClick={item.isDisabled ? undefined : item.onClick}
                      disabled={item.isDisabled}
                      className={cn(
                        "flex w-full items-center gap-3 px-4 py-2.5 text-sm transition-colors",
                        item.isDisabled
                          ? "opacity-60 cursor-not-allowed text-zinc-400"
                          : item.isDestructive
                          ? "text-red-400 hover:bg-zinc-800/50 hover:text-red-300"
                          : "text-zinc-300 hover:bg-zinc-800/50 hover:text-zinc-100"
                      )}
                    >
                      <Icon className="h-4 w-4 flex-shrink-0" />
                      <span className="flex-1 text-left">{item.label}</span>
                      {item.suffix && (
                        <span className="text-xs text-zinc-500">{item.suffix}</span>
                      )}
                    </button>
                  );
                })}
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  );
}
