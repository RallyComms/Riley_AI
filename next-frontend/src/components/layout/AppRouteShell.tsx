"use client";

import { usePathname } from "next/navigation";
import { GlobalShell } from "@app/components/layout/GlobalShell";

function isGlobalRoute(pathname: string): boolean {
  if (!pathname) return true;
  if (pathname.startsWith("/campaign")) return false;
  if (pathname.startsWith("/sign-in")) return false;
  if (pathname.startsWith("/sign-up")) return false;
  return true;
}

export function AppRouteShell({ children }: { children: React.ReactNode }) {
  const pathname = usePathname() || "";
  if (!isGlobalRoute(pathname)) return <>{children}</>;
  return <GlobalShell>{children}</GlobalShell>;
}
