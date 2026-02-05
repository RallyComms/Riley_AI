import os

# CONFIG: The root of the frontend application
BASE_DIR = "next-frontend/src"

# DEFINITIONS: Paths and Content
FILES = {
    # 1. The Utility Class Helper (Required for Tailwind classes)
    f"{BASE_DIR}/lib/utils.ts": """
import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
""",

    # 2. The Types Definition (Required for Security Status)
    f"{BASE_DIR}/lib/types.ts": """
export type SecurityStatus = "Top Secret" | "Restricted" | "Internal" | "Open";

export interface CampaignBucket {
  id: string;
  name: string;
  role: string;
  securityStatus: SecurityStatus;
  themeColor: string;
}
""",

    # 3. The Component (The UI Card itself)
    f"{BASE_DIR}/components/campaign/CampaignBucketCard.tsx": """
import type { CSSProperties } from "react";
import { ShieldCheck, ShieldAlert, Lock, Unlock } from "lucide-react";
import { cn } from "@app/lib/utils";
import type { SecurityStatus } from "@app/lib/types";

export interface CampaignBucketCardProps {
  name: string;
  role: string;
  securityStatus: SecurityStatus;
  themeColor: string;
  onClick?: () => void;
}

const securityLabelMap: Record<SecurityStatus, string> = {
  "Top Secret": "Top Secret · Zero-Trust",
  Restricted: "Restricted · Need-to-know",
  Internal: "Internal · Staff only",
  Open: "Open · Shared workspace",
};

function getSecurityIcon(status: SecurityStatus) {
  switch (status) {
    case "Top Secret": return ShieldCheck;
    case "Restricted": return ShieldAlert;
    case "Internal": return Lock;
    case "Open": default: return Unlock;
  }
}

export function CampaignBucketCard({
  name,
  role,
  securityStatus,
  themeColor,
  onClick,
}: CampaignBucketCardProps) {
  const Icon = getSecurityIcon(securityStatus);
  return (
    <button
      type="button"
      onClick={onClick}
      style={{ "--accent": themeColor } as CSSProperties}
      className={cn(
        "group relative flex flex-col items-stretch justify-between",
        "rounded-2xl border border-white/10 bg-zinc-900/60",
        "px-5 py-4 sm:px-6 sm:py-5",
        "backdrop-blur-xl shadow-[0_18px_60px_rgba(0,0,0,0.75)]",
        "transition-all duration-200 ease-out",
        "hover:border-white/20 hover:bg-zinc-900/80 hover:-translate-y-1 hover:shadow-[0_24px_80px_rgba(0,0,0,0.85)]",
        "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[color:var(--accent)] text-left"
      )}
    >
      <div
        aria-hidden="true"
        className="pointer-events-none absolute inset-px rounded-2xl opacity-0 blur-xl transition-opacity duration-300 group-hover:opacity-100"
        style={{
          background: "radial-gradient(circle at 0% 0%, color-mix(in srgb, var(--accent) 65%, transparent) 0, transparent 55%)",
        }}
      />
      <div className="relative flex items-start justify-between gap-4">
        <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-zinc-950/80 px-3 py-1 text-xs font-mono tracking-tight text-zinc-200">
          <span className="h-1.5 w-1.5 rounded-full" style={{ backgroundColor: "var(--accent)", boxShadow: "0 0 0 2px color-mix(in srgb, var(--accent) 20%, transparent)" }} />
          <span className="uppercase text-[0.65rem] text-zinc-400">{securityStatus}</span>
        </div>
        <div className="flex items-center gap-2 text-xs text-zinc-400">
          <Icon className="h-4 w-4 text-[color:var(--accent)]" />
        </div>
      </div>
      <div className="relative mt-4 space-y-1.5">
        <h2 className="text-base sm:text-lg font-semibold tracking-tight text-zinc-50">{name}</h2>
        <p className="text-xs sm:text-sm text-zinc-400">Operating as <span className="font-mono text-zinc-200">{role}</span></p>
      </div>
    </button>
  );
}
"""
}

def restore_files():
    print("🔧 Restoring Missing Component Files...")
    for path, content in FILES.items():
        # Ensure directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Write file
        with open(path, "w", encoding="utf-8") as f:
            f.write(content.strip())
        print(f"✅ Created: {path}")

    print("\n🚀 Files Restored. Running dependency check...")

if __name__ == "__main__":
    restore_files()
