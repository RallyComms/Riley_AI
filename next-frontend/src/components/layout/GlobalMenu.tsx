"use client";

import { useState, useRef, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useClerk } from "@clerk/nextjs";
import { useAuth } from "@clerk/nextjs";
import { useUser } from "@clerk/nextjs";
import { useTheme } from "next-themes";
import { Menu, Bell, User, LogOut, X, Sun, Moon, LucideIcon } from "lucide-react";
import { cn } from "@app/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import { apiFetch } from "@app/lib/api";

interface MenuItem {
  icon: LucideIcon;
  label: string;
  onClick: () => void;
  isDestructive?: boolean;
  isDisabled?: boolean;
  suffix?: string;
}

export function GlobalMenu() {
  const [isOpen, setIsOpen] = useState(false);
  const [mounted, setMounted] = useState(false);
  const [status, setStatus] = useState<"active" | "away" | "in_meeting">("active");
  const [isStatusUpdating, setIsStatusUpdating] = useState(false);
  const [isAccountSettingsOpen, setIsAccountSettingsOpen] = useState(false);
  const [profileEmail, setProfileEmail] = useState("");
  const [profileDisplayName, setProfileDisplayName] = useState("");
  const [profileUpdatedAt, setProfileUpdatedAt] = useState<string | null>(null);
  const [isProfileLoading, setIsProfileLoading] = useState(false);
  const [isProfileSaving, setIsProfileSaving] = useState(false);
  const [profileError, setProfileError] = useState<string | null>(null);
  const menuRef = useRef<HTMLDivElement>(null);
  const { signOut } = useClerk();
  const { getToken, isLoaded } = useAuth();
  const { user } = useUser();
  const router = useRouter();
  const { theme, setTheme } = useTheme();

  // Prevent hydration mismatch
  useEffect(() => {
    setMounted(true);
  }, []);

  useEffect(() => {
    const loadStatus = async () => {
      if (!isLoaded) return;
      try {
        const token = await getToken();
        if (!token) return;
        const data = await apiFetch<{ status: "active" | "away" | "in_meeting" }>(
          "/api/v1/users/me/status",
          {
            token,
            method: "GET",
          }
        );
        if (data?.status) {
          setStatus(data.status);
        }
      } catch {
        // Keep default silently.
      }
    };
    loadStatus();
  }, [getToken, isLoaded]);

  const handleSetStatus = async (nextStatus: "active" | "away" | "in_meeting") => {
    try {
      setIsStatusUpdating(true);
      const token = await getToken();
      if (!token) return;
      await apiFetch("/api/v1/users/me/status", {
        token,
        method: "PATCH",
        body: { status: nextStatus },
      });
      setStatus(nextStatus);
    } finally {
      setIsStatusUpdating(false);
    }
  };

  const loadProfile = async () => {
    if (!isLoaded) return;
    try {
      setIsProfileLoading(true);
      setProfileError(null);
      const token = await getToken();
      if (!token) return;
      const data = await apiFetch<{ email: string; display_name?: string | null; updated_at?: string | null }>(
        "/api/v1/users/me/profile",
        {
          token,
          method: "GET",
        }
      );
      setProfileEmail(data.email || user?.primaryEmailAddress?.emailAddress || "");
      setProfileDisplayName(data.display_name || "");
      setProfileUpdatedAt(data.updated_at || null);
    } catch (err) {
      setProfileError(err instanceof Error ? err.message : "Failed to load profile.");
    } finally {
      setIsProfileLoading(false);
    }
  };

  const openAccountSettings = () => {
    setIsOpen(false);
    setIsAccountSettingsOpen(true);
    void loadProfile();
  };

  const saveProfile = async () => {
    try {
      setIsProfileSaving(true);
      setProfileError(null);
      const token = await getToken();
      if (!token) return;
      const updated = await apiFetch<{ email: string; display_name?: string | null; updated_at?: string | null }>(
        "/api/v1/users/me/profile",
        {
          token,
          method: "PATCH",
          body: { display_name: profileDisplayName },
        }
      );
      setProfileEmail(updated.email || profileEmail);
      setProfileDisplayName(updated.display_name || "");
      setProfileUpdatedAt(updated.updated_at || null);
      setIsAccountSettingsOpen(false);
    } catch (err) {
      setProfileError(err instanceof Error ? err.message : "Failed to save profile.");
    } finally {
      setIsProfileSaving(false);
    }
  };

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

  const menuItems: MenuItem[] = [
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
      onClick: openAccountSettings,
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
              <div className="border-b border-zinc-800 px-3 py-2">
                <p className="mb-1 text-[11px] uppercase tracking-wide text-zinc-500">Status</p>
                <div className="grid grid-cols-3 gap-1">
                  {([
                    ["active", "Active"],
                    ["away", "Away"],
                    ["in_meeting", "Meeting"],
                  ] as const).map(([value, label]) => (
                    <button
                      key={value}
                      type="button"
                      onClick={() => handleSetStatus(value)}
                      disabled={isStatusUpdating}
                      className={cn(
                        "rounded px-2 py-1 text-[11px] transition-colors",
                        status === value
                          ? "bg-amber-500 text-zinc-900"
                          : "bg-zinc-800 text-zinc-300 hover:bg-zinc-700"
                      )}
                    >
                      {label}
                    </button>
                  ))}
                </div>
              </div>
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

      <AnimatePresence>
        {isAccountSettingsOpen && (
          <>
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              transition={{ duration: 0.2 }}
              className="fixed inset-0 z-40 bg-zinc-950/70 backdrop-blur-sm"
              onClick={() => setIsAccountSettingsOpen(false)}
            />
            <motion.div
              initial={{ opacity: 0, scale: 0.98, y: -8 }}
              animate={{ opacity: 1, scale: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.98, y: -8 }}
              transition={{ duration: 0.2 }}
              className="fixed right-6 top-20 z-50 w-[420px] max-w-[calc(100vw-2rem)] rounded-xl border border-zinc-800 bg-zinc-900/95 p-5 shadow-2xl"
            >
              <div className="mb-4 flex items-center justify-between">
                <h2 className="text-base font-semibold text-zinc-100">Account Settings</h2>
                <button
                  type="button"
                  onClick={() => setIsAccountSettingsOpen(false)}
                  className="rounded p-1 text-zinc-400 transition-colors hover:bg-zinc-800 hover:text-zinc-200"
                  aria-label="Close account settings"
                >
                  <X className="h-4 w-4" />
                </button>
              </div>

              {isProfileLoading ? (
                <p className="text-sm text-zinc-400">Loading profile...</p>
              ) : (
                <div className="space-y-4">
                  <div>
                    <label className="mb-1 block text-xs uppercase tracking-wide text-zinc-500">Email</label>
                    <input
                      type="text"
                      readOnly
                      value={profileEmail || user?.primaryEmailAddress?.emailAddress || ""}
                      className="w-full rounded-md border border-zinc-800 bg-zinc-950 px-3 py-2 text-sm text-zinc-400"
                    />
                  </div>

                  <div>
                    <label htmlFor="display-name" className="mb-1 block text-xs uppercase tracking-wide text-zinc-500">
                      Username / Display Name
                    </label>
                    <input
                      id="display-name"
                      type="text"
                      value={profileDisplayName}
                      onChange={(event) => setProfileDisplayName(event.target.value)}
                      maxLength={120}
                      placeholder="How Riley should show your name"
                      className="w-full rounded-md border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 placeholder:text-zinc-500 focus:border-amber-500 focus:outline-none"
                    />
                  </div>

                  {profileUpdatedAt && (
                    <p className="text-xs text-zinc-500">Last updated: {new Date(profileUpdatedAt).toLocaleString()}</p>
                  )}

                  {profileError && <p className="text-sm text-red-400">{profileError}</p>}

                  <div className="flex justify-end gap-2 pt-1">
                    <button
                      type="button"
                      onClick={() => setIsAccountSettingsOpen(false)}
                      className="rounded-md border border-zinc-700 px-3 py-1.5 text-sm text-zinc-300 transition-colors hover:bg-zinc-800"
                    >
                      Cancel
                    </button>
                    <button
                      type="button"
                      onClick={saveProfile}
                      disabled={isProfileSaving}
                      className={cn(
                        "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                        isProfileSaving
                          ? "bg-zinc-700 text-zinc-300"
                          : "bg-amber-500 text-zinc-900 hover:bg-amber-400"
                      )}
                    >
                      {isProfileSaving ? "Saving..." : "Save"}
                    </button>
                  </div>
                </div>
              )}
            </motion.div>
          </>
        )}
      </AnimatePresence>
    </div>
  );
}
