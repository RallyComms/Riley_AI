"use client";

import { useState, useEffect, useMemo } from "react";
import { useParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import { Sparkles, X } from "lucide-react";
import { RileyContextChat, getRileyTitle } from "@app/components/campaign/RileyContextChat";
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
function assetToKanbanCard(
  asset: Asset,
  resolveDisplayName: (userId: string) => string
): KanbanCard {
  // Map Asset status to KanbanStatus
  const statusMap: Record<Asset["status"], KanbanStatus> = {
    "in_progress": "Draft",
    "needs_review": "Needs Review",
    "in_review": "In Review",
    "approved": "Completed",
    "ready": "Draft",
    "processing": "Draft",
    "error": "Draft",
  };

  // Extract initials from assignedTo names
  const assignees = asset.assignedTo.map((assigneeId) => {
    const displayName = resolveDisplayName(assigneeId);
    return displayName
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
  // Map KanbanStatus back to Asset status
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

export default function MessagingPage() {
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

  const memberDisplayMap = useMemo(() => {
    const mapping: Record<string, string> = {};
    for (const member of campaignMembers) {
      mapping[String(member.user_id || "").toLowerCase()] = member.display_name;
    }
    return mapping;
  }, [campaignMembers]);

  const resolveDisplayName = (userId: string): string => {
    const normalized = String(userId || "").toLowerCase();
    return memberDisplayMap[normalized] || userId;
  };

  // Fetch Messaging-visible files from backend (user-scoped visibility).
  useEffect(() => {
    const fetchFiles = async () => {
      if (!campaignId) return;

      try {
        const token = await getToken();
        if (!token) {
          throw new Error("Authentication required");
        }
        
        const data = await apiFetch<{ files: any[] }>(
          `/api/v1/files/messaging/list?tenant_id=${encodeURIComponent(campaignId)}`,
          {
            token,
            method: "GET",
          }
        );

        // Convert backend file list to Asset format. Status/assignees are server-scoped.
        const fetchedAssets: Asset[] = data.files.map((file: any) => {
          const normalizedStatus = String(file.status || "").trim().toLowerCase();
          const status =
            normalizedStatus === "needs_review" ||
            normalizedStatus === "in_review" ||
            normalizedStatus === "approved" ||
            normalizedStatus === "in_progress" ||
            normalizedStatus === "ready" ||
            normalizedStatus === "processing" ||
            normalizedStatus === "error"
              ? (normalizedStatus as Asset["status"])
              : "in_progress";
          const assignedTo = Array.isArray(file.assigned_to)
            ? file.assigned_to.filter((value: unknown) => typeof value === "string")
            : [];
          const asset = toAsset(file, { status });
          return {
            ...asset,
            assignedTo,
          };
        });

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
  const kanbanCards: KanbanCard[] = assets.map((asset) =>
    assetToKanbanCard(asset, resolveDisplayName)
  );

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
      
      await apiFetch(`/api/v1/files/${cardId}/assign?tenant_id=${encodeURIComponent(campaignId)}`, {
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

  // Handle assignment confirmation (from manage assignees flow)
  const handleAssignmentConfirm = async (userIds: string[]) => {
    if (!managingAssignees) return;

    const cardId = managingAssignees.id;
    
    try {
      const token = await getToken();
      if (!token) {
        throw new Error("Authentication required");
      }

      const previousAsset = assets.find((asset) => asset.id === cardId);
      const currentStatus = previousAsset?.status || "in_progress";

      for (const userId of userIds) {
        await apiFetch(`/api/v1/files/${cardId}/assign?tenant_id=${encodeURIComponent(campaignId)}`, {
          token,
          method: "PATCH",
          body: {
            assignee: userId,
            status: currentStatus,
          },
        });
      }

      // Optimistic update with real user IDs.
      setAssets((prev) =>
        prev.map((asset) => {
          if (asset.id === cardId) {
            return {
              ...asset,
              assignedTo: userIds,
            };
          }
          return asset;
        })
      );
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
    <div className="flex h-full relative">
      {/* MIDDLE: Kanban Board */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="flex items-center justify-between p-6 border-b border-zinc-800">
          <h1 className="text-2xl font-bold text-zinc-100">Messaging Studio</h1>
          {/* THE "OPEN" BUTTON - Only visible if chat is closed */}
          {!isChatOpen && (
            <button
              onClick={() => setIsChatOpen(true)}
              className="flex items-center gap-2 text-amber-500 hover:bg-zinc-900 px-4 py-2 rounded-md transition-colors"
            >
              <Sparkles className="w-4 h-4" />
              <span>Ask Riley</span>
            </button>
          )}
        </header>

        {/* Scrollable Board Area */}
        <div className="flex-1 overflow-x-auto overflow-y-hidden p-6">
          {isLoading ? (
            <div className="flex items-center justify-center h-full">
              <p className="text-zinc-500">Loading files...</p>
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

      {/* RIGHT: Riley Chat - Conditionally Rendered */}
      {isChatOpen && (
        <div className="w-[400px] border-l border-zinc-800 bg-zinc-900/50 backdrop-blur-sm flex flex-col h-full shrink-0">
          {/* Header with CLOSE Button */}
          <div className="h-16 flex items-center justify-between px-4 border-b border-zinc-800">
            <span className="font-semibold text-amber-500 flex items-center gap-2">
              <Sparkles className="w-4 h-4" /> {getRileyTitle("messaging")}
            </span>
            <button
              onClick={() => setIsChatOpen(false)}
              className="text-zinc-500 hover:text-white p-2 transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
          </div>
          {/* The Chat Content */}
          <div className="flex-1 overflow-hidden">
            <RileyContextChat 
              mode="messaging" 
              contextKey="messaging" 
              campaignId={campaignId}
              onViewAsset={handleViewAsset}
            />
          </div>
        </div>
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
            ? assets.find((asset) => asset.id === managingAssignees.id)?.assignedTo || []
            : []
        }
        users={campaignMembers.map((member) => ({
          id: member.user_id,
          displayName: member.display_name,
          isMe: member.user_id === userId,
        }))}
        onClose={() => {
          setAssignmentModalOpen(false);
          setManagingAssignees(null);
        }}
        onConfirm={handleAssignmentConfirm}
      />
    </div>
  );
}
