export type SecurityStatus = "Top Secret" | "Restricted" | "Open" | "Internal";

export interface CampaignBucket {
  id: string;
  name: string;
  role: string;
  securityStatus: SecurityStatus;
  /**
   * Arbitrary CSS color value used to tint the bucket card
   * (e.g., "#22c55e", "rgb(59,130,246)", "hsl(210 100% 56%)").
   */
  themeColor: string;
}

export type AssetTag = "Messaging" | "Research" | "Strategy" | "Media" | "Pitch" | "Other";

export interface Asset {
  id: string;
  name: string;
  type: "pdf" | "docx" | "xlsx" | "img" | "pptx";
  url: string;
  // Optional server-generated preview metadata (e.g., Office/HTML -> PDF)
  previewUrl?: string | null;
  previewType?: string | null;
  previewStatus?: "complete" | "failed" | "processing" | "queued" | "skipped" | "not_requested" | null;
  previewError?: string | null;
  ingestionStatus?:
    | "uploaded"
    | "queued"
    | "processing"
    | "indexed"
    | "failed"
    | "low_text"
    | "ocr_needed"
    | "partial"
    | null;
  ocrStatus?: "queued" | "processing" | "complete" | "failed" | "not_requested" | null;
  visionStatus?: "queued" | "processing" | "complete" | "failed" | "not_requested" | null;
  multimodalStatus?:
    | "pending"
    | "native_only"
    | "ocr_attempted"
    | "ocr_enriched"
    | "ocr_unavailable"
    | "ocr_failed"
    | "vision_enriched"
    | "partial"
    | null;
  ocrProcessed?: boolean;
  visionProcessed?: boolean;
  tags: AssetTag[]; // Array for multi-tagging
  uploadDate: string;
  uploader: string;
  size: string; // File size (e.g., "2.4 MB", "150 KB")
  urgency: "low" | "medium" | "high" | "critical";
  assignedTo: string[]; // List of user IDs or Names
  comments: number; // Count of comments
  status: "processing" | "ready" | "error" | "in_progress" | "needs_review" | "in_review" | "approved";
  aiEnabled: boolean; // Whether this file is included in AI context (default true for PDF/Docx, false for Images/Video)
}

export type KanbanStatus = "Draft" | "Needs Review" | "In Review" | "Completed";

export interface KanbanCard {
  id: string;
  name: string;
  type: Asset["type"];
  url: string;
  status: KanbanStatus;
  assignees: string[]; // Array of initials like "SJ", "JD"
  tags: AssetTag[];
}


