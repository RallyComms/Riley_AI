"use client";

import { useState, useEffect } from "react";
import { useParams } from "next/navigation";
import { Sparkles, X } from "lucide-react";
import { RileyContextChat, getRileyTitle } from "@app/components/campaign/RileyContextChat";
import { KanbanBoard } from "@app/components/campaign/KanbanBoard";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { AssignmentModal } from "@app/components/campaign/AssignmentModal";
import { Asset, KanbanCard, KanbanStatus } from "@app/lib/types";

// Helper functions
function getFileTypeFromExtension(filename: string): Asset["type"] {
  const extension = filename.split(".").pop()?.toLowerCase() || "";
  if (extension === "pdf") return "pdf";
  if (extension === "docx" || extension === "doc") return "docx";
  if (extension === "xlsx" || extension === "xls" || extension === "csv") return "xlsx";
  if (extension === "pptx" || extension === "ppt") return "pptx";
  if (["png", "jpg", "jpeg", "webp", "gif", "svg"].includes(extension)) return "img";
  return "pdf"; // Default
}

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
  const campaignId = params.id as string;
  const [assets, setAssets] = useState<Asset[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [selectedFile, setSelectedFile] = useState<Asset | null>(null);
  const [assignmentModalOpen, setAssignmentModalOpen] = useState(false);
  const [managingAssignees, setManagingAssignees] = useState<KanbanCard | null>(null);

  // Fetch files from backend and filter by Strategy tag
  useEffect(() => {
    const fetchFiles = async () => {
      if (!campaignId) return;

      try {
        const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/v1/list?tenant_id=${encodeURIComponent(campaignId)}`);
        if (!response.ok) {
          throw new Error("Failed to fetch files");
        }
        const data = await response.json();

        const fetchedAssets: Asset[] = data.files
          .filter((file: any) => {
            const tags = file.tags || [];
            return tags.includes("Strategy");
          })
          .map((file: any) => {
            const fileType = getFileTypeFromExtension(file.name);
            return {
              id: file.id || file.name,
              name: file.name,
              type: fileType,
              url: file.url,
              tags: file.tags || [],
              uploadDate: new Date(file.date).toISOString().split("T")[0],
              uploader: "System",
              size: file.size,
              urgency: "medium",
              assignedTo: [],
              comments: 0,
              status: "in_progress" as Asset["status"],
              aiEnabled: isAISupportedType(fileType),
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
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/v1/files/${cardId}/assign`,
        {
          method: "PATCH",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            assignee: "", // Keep existing assignee
            status: newAssetStatus,
          }),
        }
      );

      if (!response.ok) {
        // Revert on error
        setAssets(previousAssets);
        const errorData = await response.json().catch(() => ({
          detail: "Failed to update status",
        }));
        throw new Error(
          errorData.detail || `Failed to update status: ${response.status}`
        );
      }
    } catch (error) {
      console.error("Failed to update status:", error);
      // Revert optimistic update
      setAssets(previousAssets);
      alert("Failed to save status change. Please try again.");
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
    const file = kanbanCardToFile(card);
    setSelectedFile(file);
  };

  return (
    <div className="flex h-full relative">
      {/* MIDDLE: Kanban Board */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="flex items-center justify-between p-6 border-b border-zinc-800">
          <h1 className="text-2xl font-bold text-zinc-100">Strategy Workspace</h1>
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

      {/* RIGHT: Riley Chat */}
      {isChatOpen && (
        <div className="w-[400px] border-l border-zinc-800 bg-zinc-900/50 backdrop-blur-sm flex flex-col h-full shrink-0">
          <div className="h-16 flex items-center justify-between px-4 border-b border-zinc-800">
            <span className="font-semibold text-amber-500 flex items-center gap-2">
              <Sparkles className="w-4 h-4" /> {getRileyTitle("strategy")}
            </span>
            <button
              onClick={() => setIsChatOpen(false)}
              className="text-zinc-500 hover:text-white p-2 transition-colors"
            >
              <X className="w-5 h-5" />
            </button>
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
        onConfirm={handleAssignmentConfirm}
      />
    </div>
  );
}
