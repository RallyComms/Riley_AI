"use client";

import { useState, useEffect } from "react";
import { useParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { X } from "lucide-react";
import { RileyContextChat } from "@app/components/campaign/RileyContextChat";
import { KanbanBoard } from "@app/components/campaign/KanbanBoard";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { AssignmentModal } from "@app/components/campaign/AssignmentModal";
import { Asset, KanbanCard, KanbanStatus } from "@app/lib/types";
import { apiFetch } from "@app/lib/api";
import { toAsset } from "@app/lib/files";

function isAISupportedType(type: Asset["type"]): boolean {
  return type === "pdf" || type === "docx";
}

// Convert Asset to KanbanCard
function assetToKanbanCard(asset: Asset): KanbanCard {
  const statusMap: Record<Asset["status"], KanbanStatus> = {
    "in_progress": "Draft",
    "needs_review": "Needs Review",
    "in_review": "In Review",
    "approved": "Completed",
    "ready": "Draft",
    "processing": "Draft",
    "error": "Draft",
  };

  const assignees = asset.assignedTo.map((name) => {
    return name
      .split(" ")
      .map((n) => n[0])
      .join("")
      .toUpperCase()
      .slice(0, 2);
  });

  return {
    id: asset.id,
    name: asset.name,
    type: asset.type,
    url: asset.url,
    status: statusMap[asset.status] || "Draft",
    assignees,
    tags: asset.tags,
  };
}

// Convert KanbanCard to DocumentViewer format
function kanbanCardToFile(card: KanbanCard): Asset {
  const statusMap: Record<KanbanStatus, Asset["status"]> = {
    "Draft": "in_progress",
    "Needs Review": "needs_review",
    "In Review": "in_review",
    "Completed": "approved",
  };

  return {
    id: card.id,
    name: card.name,
    type: card.type,
    url: card.url,
    tags: card.tags,
    uploadDate: new Date().toISOString().split("T")[0],
    uploader: "Team Member",
    size: "Unknown",
    urgency: "medium",
    assignedTo: [],
    comments: 0,
    status: statusMap[card.status] || "in_progress",
    aiEnabled: isAISupportedType(card.type),
  };
}

export default function StrategyPage() {
  const params = useParams();
  const { getToken, userId } = useAuth();
  const campaignId = params.id as string;
  const [assets, setAssets] = useState<Asset[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [selectedFile, setSelectedFile] = useState<Asset | null>(null);
  const [assignmentModalOpen, setAssignmentModalOpen] = useState(false);
  const [managingAssignees, setManagingAssignees] = useState<KanbanCard | null>(null);
  const [campaignMembers, setCampaignMembers] = useState<
    Array<{ user_id: string; display_name: string }>
  >([]);

  useEffect(() => {
    if (!isChatOpen) return;
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        setIsChatOpen(false);
      }
    };
    const shouldLockBody = window.innerWidth < 1024;
    const previousOverflow = document.body.style.overflow;
    if (shouldLockBody) {
      document.body.style.overflow = "hidden";
    }
    window.addEventListener("keydown", handleEscape);
    return () => {
      window.removeEventListener("keydown", handleEscape);
      if (shouldLockBody) {
        document.body.style.overflow = previousOverflow;
      }
    };
  }, [isChatOpen]);

  // Fetch files from backend and filter by Strategy tag
  useEffect(() => {
    const fetchFiles = async () => {
      if (!campaignId) return;

      try {
        const token = await getToken();
        if (!token) {
          throw new Error("Authentication required");
        }
        
        const data = await apiFetch<{ files: any[] }>(
          `/api/v1/list?tenant_id=${encodeURIComponent(campaignId)}`,
          {
            token,
            method: "GET",
          }
        );

        const fetchedAssets: Asset[] = data.files
          .filter((file: any) => {
            const tags = Array.isArray(file.tags) ? file.tags : [];
            return tags.includes("Strategy");
          })
          .map((file: any) => toAsset(file, { status: "in_progress" }));

        setAssets(fetchedAssets);
      } catch (error) {
        console.error("Error fetching files:", error);
        setAssets([]);
      } finally {
        setIsLoading(false);
      }
    };

    fetchFiles();
  }, [campaignId]);

  useEffect(() => {
    const fetchMembers = async () => {
      if (!campaignId) return;
      try {
        const token = await getToken();
        if (!token) return;
        const data = await apiFetch<{ members: Array<{ user_id: string; display_name: string }> }>(
          `/api/v1/campaigns/${encodeURIComponent(campaignId)}/members?mentions_only=true`,
          {
            token,
            method: "GET",
          }
        );
        setCampaignMembers(data.members || []);
      } catch (error) {
        console.error("Failed to load campaign members for assignment:", error);
        setCampaignMembers([]);
      }
    };
    void fetchMembers();
  }, [campaignId, getToken]);

  // Handle viewing an asset from citation badge
  const handleViewAsset = (filename: string) => {
    // Search for the asset in the local assets state
    const asset = assets.find((a) => a.name === filename);
    
    if (asset) {
      setSelectedFile(asset);
    } else {
      // Asset not found in current view (might be Global/Archived)
      alert(`Asset "${filename}" not found in current view (might be Global/Archived).`);
    }
  };

  // Convert assets to KanbanCards
  const kanbanCards: KanbanCard[] = assets.map(assetToKanbanCard);

  // Handle status change (when dragging cards)
  const handleStatusChange = async (cardId: string, newStatus: KanbanStatus) => {
    // Map KanbanStatus back to Asset status
    const statusMap: Record<KanbanStatus, Asset["status"]> = {
      "Draft": "in_progress",
      "Needs Review": "needs_review",
      "In Review": "in_review",
      "Completed": "approved",
    };

    const newAssetStatus = statusMap[newStatus] || "in_progress";
    const previousAssets = [...assets];

    // Optimistic update: update local state immediately
    setAssets((prev) =>
      prev.map((asset) =>
        asset.id === cardId
          ? { ...asset, status: newAssetStatus }
          : asset
      )
    );

    // API call to persist status change
    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }
      
      await apiFetch(`/api/v1/files/${cardId}/assign`, {
        token,
        method: "PATCH",
        body: {
          assignee: "", // Keep existing assignee
          status: newAssetStatus,
        },
      });
    } catch (error) {
      // Revert on error
      setAssets(previousAssets);
      const errorMessage = error instanceof Error ? error.message : "Failed to update status";
      console.error("Failed to update status:", error);
      alert(`Failed to save status change: ${errorMessage}`);
    }
  };

  // Helper: Map initials to user IDs
  const initialsToUserId = (initials: string): string | null => {
    const mapping: Record<string, string> = {
      "SJ": "sarah",
      "JD": "john",
      "AK": "anova",
    };
    return mapping[initials] || null;
  };

  // Helper: Map user IDs to full names
  const userIdToName = (userId: string): string => {
    const userMap: Record<string, string> = {
      sarah: "Sarah Johnson",
      john: "John Doe",
      anova: "Anova Kim",
    };
    return userMap[userId] || userId;
  };

  // Handle assignment confirmation (from manage assignees flow)
  const handleAssignmentConfirm = async (userIds: string[]) => {
    if (!managingAssignees) return;

    const cardId = managingAssignees.id;
    
    try {
      // Update assignees - convert user IDs to full names
      const assigneeNames = userIds.map(userIdToName);
      
      // Update local state
      setAssets((prev) =>
        prev.map((asset) => {
          if (asset.id === cardId) {
            return {
              ...asset,
              assignedTo: assigneeNames,
            };
          }
          return asset;
        })
      );

      // TODO: Call backend to update assignees
      // For now, we'll just update local state
    } catch (error) {
      console.error("Failed to update assignees:", error);
      alert("Failed to update assignees. Please try again.");
    } finally {
      setManagingAssignees(null);
      setAssignmentModalOpen(false);
    }
  };

  // Handle manage assignees (from + button)
  const handleManageAssignees = (card: KanbanCard) => {
    setManagingAssignees(card);
    setAssignmentModalOpen(true);
  };

  // Handle card click to open viewer
  const handleCardClick = (card: KanbanCard) => {
    const file = assets.find((asset) => asset.id === card.id) ?? kanbanCardToFile(card);
    setSelectedFile(file);
  };

  return (
    <div className="relative flex h-full min-h-0 bg-[#f7f5ef]">
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <header className="flex items-end justify-between border-b border-[#e5ddce] px-6 py-5 lg:px-8">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-[#1f2a44]">Strategy</h1>
            <p className="mt-1 text-sm text-[#6f788a]">
              Build and refine campaign strategy across the team.
            </p>
          </div>
          <button
            onClick={() => setIsChatOpen((prev) => !prev)}
            className={`inline-flex items-center gap-2 rounded-md border px-3 py-1.5 text-sm font-medium transition-colors ${
              isChatOpen
                ? "border-[#ccb67d] bg-[#f2ebd6] text-[#1f2a44]"
                : "border-[#d8d0bf] bg-white text-[#1f2a44] hover:bg-[#f1ece2]"
            }`}
            aria-expanded={isChatOpen}
            aria-controls="strategy-riley-panel"
          >
            <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-[#eadfb7] text-[11px] font-semibold text-[#6d560f]">R</span>
            Ask Riley
          </button>
        </header>

        <div className="flex-1 overflow-x-auto overflow-y-hidden px-6 py-5 lg:px-8">
          {isLoading ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-sm text-[#8a90a0]">Loading files...</p>
            </div>
          ) : (
            <KanbanBoard 
              cards={kanbanCards} 
              onStatusChange={handleStatusChange}
              onCardClick={handleCardClick}
              onManageAssignees={handleManageAssignees}
            />
          )}
        </div>
      </div>

      {isChatOpen && (
        <>
          <button
            type="button"
            className="fixed inset-0 z-30 bg-[#1f2a44]/20 backdrop-blur-[1px] lg:hidden"
            aria-label="Close Ask Riley panel backdrop"
            onClick={() => setIsChatOpen(false)}
          />
          <aside
            id="strategy-riley-panel"
            className="fixed inset-y-0 right-0 z-40 w-[94vw] max-w-[400px] border-l border-[#d8d0bf] bg-[#fcfbf8] shadow-xl shadow-[#1f2a44]/10 lg:static lg:z-auto lg:h-full lg:w-[360px] lg:max-w-none lg:shadow-none"
          >
          <div className="flex h-full flex-col">
            <div className="flex h-16 items-center justify-between border-b border-[#e5ddce] px-4">
              <span className="inline-flex items-center gap-2 text-sm font-semibold text-[#1f2a44]">
                <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-[#eadfb7] text-[11px] font-semibold text-[#6d560f]">R</span>
                Ask Riley
              </span>
              <button
                onClick={() => setIsChatOpen(false)}
                className="rounded-md p-1.5 text-[#6f788a] transition-colors hover:bg-[#f1ece2] hover:text-[#1f2a44]"
                aria-label="Close Ask Riley panel"
              >
                <X className="h-4 w-4" />
              </button>
            </div>
            <div className="border-b border-[#e5ddce] bg-white/80 px-4 py-3">
              <p className="text-sm leading-relaxed text-[#1f2a44]">
                Welcome to Riley Strategy. I can help you analyze strategic frameworks, review supporting documents, and think through campaign direction. I&apos;m getting much stronger in Riley V2.
              </p>
            </div>
            <div className="flex-1 overflow-hidden">
              <RileyContextChat
                mode="strategy"
                contextKey="strategy"
                campaignId={campaignId}
                onViewAsset={handleViewAsset}
              />
            </div>
          </div>
          </aside>
        </>
      )}

      {/* Document Viewer Modal */}
      {selectedFile && (
        <DocumentViewer
          file={selectedFile}
          variant="modal"
          onClose={() => setSelectedFile(null)}
        />
      )}

      {/* Assignment Modal */}
      <AssignmentModal
        isOpen={assignmentModalOpen}
        fileName={managingAssignees?.name || ""}
        currentAssignees={
          managingAssignees
            ? managingAssignees.assignees
                .map(initialsToUserId)
                .filter((id): id is string => id !== null)
            : []
        }
        onClose={() => {
          setAssignmentModalOpen(false);
          setManagingAssignees(null);
        }}
        users={campaignMembers.map((member) => ({
          id: member.user_id,
          displayName: member.display_name,
          isMe: member.user_id === userId,
        }))}
        onConfirm={handleAssignmentConfirm}
      />
    </div>
  );
}
