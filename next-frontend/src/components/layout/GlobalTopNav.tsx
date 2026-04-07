"use client";

import { useEffect, useRef, useState } from "react";
import { UserButton, useAuth, useUser } from "@clerk/nextjs";
import { useRouter } from "next/navigation";
import { apiFetch } from "@app/lib/api";
import { GlobalNotificationBell } from "@app/components/layout/GlobalNotificationBell";

export function GlobalTopNav() {
  const { getToken, isLoaded } = useAuth();
  const { user } = useUser();
  const router = useRouter();
  const [canAccessMissionControl, setCanAccessMissionControl] = useState(false);
  const missionControlRetryCountRef = useRef(0);

  useEffect(() => {
    missionControlRetryCountRef.current = 0;
  }, [user?.id]);

  useEffect(() => {
    let cancelled = false;
    const checkMissionControlAccess = async () => {
      if (!isLoaded || !user?.id) return;
      const cacheKey = `mission-control-access:${user.id}`;
      if (typeof window !== "undefined") {
        const cachedValue = window.sessionStorage.getItem(cacheKey);
        if (cachedValue === "true") setCanAccessMissionControl(true);
        if (cachedValue === "false") setCanAccessMissionControl(false);
      }
      try {
        const token = await getToken();
        if (!token) return;
        const result = await apiFetch<{ allowed?: boolean }>("/api/v1/mission-control/access", {
          token,
          method: "GET",
        });
        if (cancelled) return;
        const allowed = Boolean(result?.allowed);
        setCanAccessMissionControl(allowed);
        if (typeof window !== "undefined") {
          window.sessionStorage.setItem(cacheKey, allowed ? "true" : "false");
        }
      } catch {
        if (!cancelled && missionControlRetryCountRef.current < 3) {
          missionControlRetryCountRef.current += 1;
          window.setTimeout(() => {
            if (!cancelled) void checkMissionControlAccess();
          }, 1200);
        }
      }
    };
    void checkMissionControlAccess();
    return () => {
      cancelled = true;
    };
  }, [getToken, isLoaded, user?.id]);

  if (!isLoaded || !user?.id) {
    return null;
  }

  return (
    <header className="border-b border-[#e6dece] bg-[#fbf8f2]">
      <div className="mx-auto flex w-full max-w-6xl items-center justify-end gap-3 px-8 py-3">
        <GlobalNotificationBell />
        {canAccessMissionControl ? (
          <button
            type="button"
            onClick={() => router.push("/mission-control")}
            className="rounded-md border border-[#1f2a44] px-3 py-1.5 text-sm font-medium text-[#1f2a44] hover:bg-[#f4efe5]"
          >
            Mission Control
          </button>
        ) : null}
        <div className="flex h-8 w-8 items-center justify-center overflow-hidden rounded-full border border-[#d7cfbf] bg-white">
          <UserButton appearance={{ elements: { userButtonAvatarBox: "h-8 w-8" } }} />
        </div>
      </div>
    </header>
  );
}

