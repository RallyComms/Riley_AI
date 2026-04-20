"use client";

import { useState } from "react";
import { useAuth } from "@clerk/nextjs";
import { useRouter } from "next/navigation";
import { BookOpen } from "lucide-react";
import { CampaignDirectory } from "@app/components/dashboard/CampaignDirectory";
import { apiFetch } from "@app/lib/api";

export default function KnowledgebasePage() {
  const router = useRouter();
  const { getToken } = useAuth();

  const [campaignsVersion, setCampaignsVersion] = useState(0);
  const [terminatingCampaignId, setTerminatingCampaignId] = useState<string | null>(null);

  const handleRequestAccess = async (campaignId: string, message?: string) => {
    const token = await getToken();
    if (!token) throw new Error("No authentication token available");
    await apiFetch(`/api/v1/campaigns/${encodeURIComponent(campaignId)}/access-requests`, {
      token,
      method: "POST",
      body: { message: message || null },
    });
  };

  const handleArchive = (campaignId: string) => {
    const confirmed = window.confirm("Archive this campaign?");
    if (!confirmed) return;

    void (async () => {
      try {
        const token = await getToken();
        if (!token) throw new Error("No authentication token available");
        await apiFetch(`/api/v1/campaigns/${encodeURIComponent(campaignId)}/archive`, {
          token,
          method: "PATCH",
        });
        setCampaignsVersion((prev) => prev + 1);
      } catch (error) {
        alert(`Failed to archive campaign: ${error instanceof Error ? error.message : "Unknown error"}`);
      }
    })();
  };

  const handleTerminate = async (campaignId: string) => {
    setTerminatingCampaignId(campaignId);
    try {
      const token = await getToken();
      if (!token) throw new Error("No authentication token available");
      await apiFetch(`/api/v1/campaign/${campaignId}`, {
        token,
        method: "DELETE",
      });
      setCampaignsVersion((prev) => prev + 1);
    } catch (error) {
      alert(`Failed to delete campaign: ${error instanceof Error ? error.message : "Unknown error"}`);
    } finally {
      setTerminatingCampaignId(null);
    }
  };

  const handleRestore = (campaignId: string) => {
    const confirmed = window.confirm("Restore this archived campaign?");
    if (!confirmed) return;

    void (async () => {
      try {
        const token = await getToken();
        if (!token) throw new Error("No authentication token available");
        await apiFetch(`/api/v1/campaigns/${encodeURIComponent(campaignId)}/restore`, {
          token,
          method: "PATCH",
        });
        setCampaignsVersion((prev) => prev + 1);
      } catch (error) {
        alert(`Failed to restore campaign: ${error instanceof Error ? error.message : "Unknown error"}`);
      }
    })();
  };

  return (
    <div className="mx-auto w-full max-w-7xl">
      <section className="mb-3 px-1 py-1">
        <div className="flex items-center gap-3">
          <div className="inline-flex h-10 w-10 items-center justify-center rounded-xl border border-[#ddd1ba] bg-[#f1e9d8] text-[#2a3d64]">
            <BookOpen className="h-5 w-5" />
          </div>
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-[#1f2a44]">Firm Knowledgebase</h1>
            <p className="mt-1 text-sm text-[#6f788a]">
              Browse campaigns, archive history, and firm-level documents in one workspace.
            </p>
          </div>
        </div>
      </section>

      <CampaignDirectory
        isOpen
        onClose={() => {}}
        variant="embedded"
        onEnterCampaign={(campaignId) => router.push(`/campaign/${campaignId}`)}
        onRequestAccess={handleRequestAccess}
        onArchive={handleArchive}
        onRestore={handleRestore}
        onTerminate={handleTerminate}
        terminatingCampaignId={terminatingCampaignId}
        campaignsVersion={campaignsVersion}
      />
    </div>
  );
}
