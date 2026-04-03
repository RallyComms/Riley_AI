"use client";

import { useState } from "react";
import {
  DndContext,
  DragOverlay,
  closestCorners,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragStartEvent,
  DragEndEvent,
  useDroppable,
} from "@dnd-kit/core";
import {
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
  useSortable,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import { FileText, FileType2, Table, Presentation, Image as ImageIcon, Plus } from "lucide-react";
import { cn } from "@app/lib/utils";
import { KanbanCard, KanbanStatus, Asset } from "@app/lib/types";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";

interface KanbanBoardProps {
  cards: KanbanCard[];
  onStatusChange: (cardId: string, newStatus: KanbanStatus) => void;
  onCardClick?: (card: KanbanCard) => void;
  onManageAssignees?: (card: KanbanCard) => void;
}

type ColumnType = "Draft" | "Needs Review" | "In Review" | "Completed";

interface Column {
  id: ColumnType;
  label: string;
  accentDot: string;
}

const columns: Column[] = [
  {
    id: "Draft",
    label: "Draft",
    accentDot: "bg-[#9aa3b5]",
  },
  {
    id: "Needs Review",
    label: "Needs Review",
    accentDot: "bg-[#d4ad47]",
  },
  {
    id: "In Review",
    label: "In Review",
    accentDot: "bg-[#6b89b0]",
  },
  {
    id: "Completed",
    label: "Completed",
    accentDot: "bg-[#5f987a]",
  },
];

function getFileIcon(type: KanbanCard["type"]) {
  switch (type) {
    case "pdf":
      return { icon: FileText, color: "text-red-500" };
    case "docx":
      return { icon: FileType2, color: "text-blue-500" };
    case "xlsx":
      return { icon: Table, color: "text-emerald-500" };
    case "pptx":
      return { icon: Presentation, color: "text-orange-500" };
    case "img":
      return { icon: ImageIcon, color: "text-purple-500" };
    default:
      return { icon: FileText, color: "text-zinc-500" };
  }
}

interface KanbanCardComponentProps {
  card: KanbanCard;
  onClick: () => void;
  onManageAssignees?: (card: KanbanCard) => void;
}

function KanbanCardComponent({ card, onClick, onManageAssignees }: KanbanCardComponentProps) {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id: card.id });

  const { icon: FileIcon, color: iconColor } = getFileIcon(card.type);

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    scale: isDragging ? 1.05 : 1,
  };

  // Separate click handler that doesn't interfere with drag
  const handleClick = (e: React.MouseEvent) => {
    // Only fire onClick if we're not dragging
    if (!isDragging) {
      onClick();
    }
  };

  return (
    <div
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...listeners}
      onClick={handleClick}
      className={cn(
        "cursor-grab active:cursor-grabbing rounded-xl border border-[#e5ddce] bg-white p-4 shadow-sm transition-all hover:border-[#d8cfbb] hover:shadow-md",
        isDragging && "scale-105 ring-2 ring-[#d4ad47]/35 shadow-lg"
      )}
    >
      {/* Card Header */}
      <div className="mb-3 flex items-start gap-2">
        <FileIcon className={cn("h-4 w-4 flex-shrink-0 mt-0.5", iconColor)} aria-hidden="true" />
        <h4 className="flex-1 text-sm font-medium text-[#1f2a44] break-words leading-snug">{card.name}</h4>
      </div>

      {/* Team Avatars */}
      <div className="flex items-center gap-2">
        {card.assignees.length > 0 ? (
          <div className="flex -space-x-2">
            {card.assignees.map((initials, idx) => (
              <div
                key={idx}
                className="flex h-7 w-7 items-center justify-center rounded-full border-2 border-white bg-[#ece6d9] text-xs font-medium text-[#1f2a44]"
                title={initials}
              >
                {initials}
              </div>
            ))}
          </div>
        ) : (
          <span className="rounded-full border border-[#e5ddce] bg-[#f9f7f2] px-2 py-0.5 text-[11px] text-[#8a90a0]">
            Unassigned
          </span>
        )}
        {onManageAssignees && (
          <button
            onClick={(e) => {
              e.stopPropagation();
              onManageAssignees(card);
            }}
            className="flex h-7 w-7 items-center justify-center rounded-full border border-[#d8d0bf] bg-white text-[#6f788a] hover:bg-[#f1ece2] hover:text-[#1f2a44] transition-colors flex-shrink-0"
            aria-label="Manage assignees"
            title="Manage team"
          >
            <Plus className="h-3.5 w-3.5" />
          </button>
        )}
      </div>
    </div>
  );
}

interface ColumnComponentProps {
  column: Column;
  cards: KanbanCard[];
  onCardClick: (card: KanbanCard) => void;
  onManageAssignees?: (card: KanbanCard) => void;
}

function ColumnComponent({ column, cards, onCardClick, onManageAssignees }: ColumnComponentProps) {
  const { setNodeRef } = useDroppable({
    id: column.id,
  });

  const cardIds = cards.map((card) => card.id);

  return (
    <div
      ref={setNodeRef}
      className="flex h-full max-h-full min-w-[300px] flex-shrink-0 flex-col rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4 shadow-sm"
    >
      {/* Column Header */}
      <div className="mb-4 flex items-center gap-2 flex-shrink-0">
        <span className={cn("h-2 w-2 rounded-full", column.accentDot)} />
        <h3 className="text-sm font-semibold text-[#1f2a44]">{column.label}</h3>
        <span className="ml-auto rounded-full bg-[#ece6d9] px-2 py-0.5 text-xs font-medium text-[#6f788a]">
          {cards.length}
        </span>
      </div>

      {/* Cards */}
      <SortableContext items={cardIds} strategy={verticalListSortingStrategy}>
        <div className="flex-1 space-y-3 overflow-y-auto overflow-x-hidden min-h-0 pr-1" style={{ WebkitOverflowScrolling: "touch" }}>
          {cards.length === 0 ? (
            <div className="rounded-lg border border-dashed border-[#e3dac8] bg-white/60 px-3 py-4 text-xs text-[#8a90a0]">
              No items in this column.
            </div>
          ) : (
            cards.map((card) => (
              <KanbanCardComponent
                key={card.id}
                card={card}
                onClick={() => onCardClick(card)}
                onManageAssignees={onManageAssignees}
              />
            ))
          )}
        </div>
      </SortableContext>
    </div>
  );
}

// Convert KanbanCard to DocumentViewer format
function convertToDocumentViewerFile(card: KanbanCard) {
  // Map KanbanStatus to Asset status
  const statusMap: Record<KanbanCard["status"], Asset["status"]> = {
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
    status: statusMap[card.status] || "in_progress",
    uploader: "Team Member",
    size: "Unknown",
    uploadDate: new Date().toISOString().split("T")[0],
  };
}

export function KanbanBoard({ cards, onStatusChange, onCardClick, onManageAssignees }: KanbanBoardProps) {
  const [activeCard, setActiveCard] = useState<KanbanCard | null>(null);
  const [selectedCard, setSelectedCard] = useState<KanbanCard | null>(null);
  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 5, // Require 5px movement before drag starts (allows clicks to work)
      },
    }),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  // Group cards by status
  const cardsByStatus = columns.reduce(
    (acc, column) => {
      acc[column.id] = cards.filter((card) => card.status === column.id);
      return acc;
    },
    {} as Record<ColumnType, KanbanCard[]>
  );

  const handleDragStart = (event: DragStartEvent) => {
    const { active } = event;
    const card = cards.find((c) => c.id === active.id);
    setActiveCard(card || null);
  };

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;
    setActiveCard(null);

    if (!over) return;

    const activeId = active.id as string;
    const overId = over.id as string;

    // Find target column
    const targetColumn = columns.find((col) => col.id === overId) || 
      columns.find((col) => cardsByStatus[col.id].some((card) => card.id === overId));

    if (!targetColumn) return;

    // Find source card
    const sourceCard = cards.find((c) => c.id === activeId);
    if (!sourceCard || sourceCard.status === targetColumn.id) return;

    // Direct status update - always update immediately on drag
    onStatusChange(activeId, targetColumn.id);
  };

  const handleCardClick = (card: KanbanCard) => {
    if (onCardClick) {
      // If parent provides onCardClick, use it (parent handles viewer)
      onCardClick(card);
    } else {
      // Otherwise, use internal viewer
      setSelectedCard(card);
    }
  };

  const handleCloseViewer = () => {
    setSelectedCard(null);
  };

  return (
    <>
      <DndContext
        sensors={sensors}
        collisionDetection={closestCorners}
        onDragStart={handleDragStart}
        onDragEnd={handleDragEnd}
      >
        <div className="relative h-full">
          <div className="flex h-full min-w-0 gap-5 overflow-x-auto overflow-y-hidden pb-2 pr-2" style={{ WebkitOverflowScrolling: "touch" }}>
            {columns.map((column) => (
              <ColumnComponent
                key={column.id}
                column={column}
                cards={cardsByStatus[column.id] || []}
                onCardClick={handleCardClick}
                onManageAssignees={onManageAssignees}
              />
            ))}
          </div>

          {cards.length === 0 ? (
            <div className="pointer-events-none absolute inset-0 flex items-center justify-center px-6">
              <div className="rounded-lg border border-dashed border-[#dfd5c3] bg-[#fcfbf8]/90 px-4 py-3 text-sm text-[#8a90a0]">
                No cards yet. Items will appear here as documents and workflows progress.
              </div>
            </div>
          ) : null}
        </div>

        <DragOverlay>
          {activeCard ? (
            <div className="rounded-xl border border-[#d8cfbb] bg-white/95 backdrop-blur-sm p-4 shadow-lg">
              <div className="mb-3 flex items-start gap-2">
                {(() => {
                  const { icon: FileIcon, color } = getFileIcon(activeCard.type);
                  return <FileIcon className={cn("h-4 w-4 flex-shrink-0 mt-0.5", color)} />;
                })()}
                <h4 className="flex-1 text-sm font-medium text-[#1f2a44]">{activeCard.name}</h4>
              </div>
            </div>
          ) : null}
        </DragOverlay>
      </DndContext>

      {/* Document Viewer Modal - Only show if parent doesn't handle clicks */}
      {!onCardClick && selectedCard && (
        <DocumentViewer
          file={convertToDocumentViewerFile(selectedCard)}
          onClose={handleCloseViewer}
          variant="modal"
        />
      )}
    </>
  );
}
