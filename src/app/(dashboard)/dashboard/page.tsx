import type { Metadata } from "next";
import { CampaignBucketCard } from "@app/components/campaign/CampaignBucketCard";
import type { CampaignBucket } from "@app/lib/types";

export const metadata: Metadata = {
  title: "Campaign Selection · Project RILEY",
};

const demoBuckets: CampaignBucket[] = [
  {
    id: "youth-mission",
    name: "Youth Mission · 2026 Turnout",
    role: "Campaign Director",
    securityStatus: "Top Secret",
    themeColor: "hsl(222 84% 56%)", // Riley Blue
  },
  {
    id: "fire-recovery",
    name: "Fire Recovery · Mutual Aid Coalition",
    role: "Relief Coordinator",
    securityStatus: "Restricted",
    themeColor: "hsl(14 96% 56%)", // Signal Red
  },
  {
    id: "housing-justice",
    name: "Housing Justice · Citywide",
    role: "Policy Lead",
    securityStatus: "Internal",
    themeColor: "hsl(160 84% 45%)", // Civic Green
  },
];

export default function DashboardPage() {
  return (
    <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col px-4 pb-10 pt-8 sm:px-6 lg:px-8">
      <header className="mb-8 space-y-2">
        <div className="inline-flex items-center gap-2 rounded-full border border-zinc-800 bg-zinc-950/60 px-3 py-1 text-xs font-mono tracking-tight text-zinc-400 backdrop-blur">
          <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 shadow-[0_0_0_4px_rgba(52,211,153,0.35)]" />
          AIRLOCK · CAMPAIGN SELECTION
        </div>
        <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <h1 className="text-xl sm:text-2xl font-semibold tracking-tight text-zinc-50">
              Choose your active campaign
            </h1>
            <p className="mt-1 max-w-2xl text-sm text-zinc-400">
              Each campaign operates in its own secure bucket. Your role and
              clearances are applied the moment you step inside.
            </p>
          </div>
          <div className="flex items-center gap-3 text-xs text-zinc-500">
            <span className="inline-flex items-center gap-1 rounded-full border border-zinc-800 bg-zinc-950/70 px-3 py-1 font-mono">
              <span className="h-1.5 w-1.5 rounded-full bg-sky-400" />
              AI LINKED
            </span>
            <span className="hidden sm:inline font-mono">
              Visual sovereignty mode:{" "}
              <span className="text-zinc-300">ENGAGED</span>
            </span>
          </div>
        </div>
      </header>

      <section
        aria-label="Available campaign buckets"
        className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3"
      >
        {demoBuckets.map((bucket) => (
          <CampaignBucketCard
            key={bucket.id}
            name={bucket.name}
            role={bucket.role}
            securityStatus={bucket.securityStatus}
            themeColor={bucket.themeColor}
            onClick={() => {
              // TODO: Navigate to /[campaignId] once routing is wired
            }}
          />
        ))}
      </section>
    </main>
  );
}


