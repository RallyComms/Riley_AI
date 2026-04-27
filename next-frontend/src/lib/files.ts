import { Asset, AssetTag } from "@app/lib/types";

const VALID_TAGS: Set<AssetTag> = new Set(["Messaging", "Research", "Strategy", "Media", "Pitch", "Other"]);

export interface BackendFileRecord {
  id?: string;
  name?: string;
  filename?: string;
  url?: string;
  date?: string;
  size?: string;
  tags?: string[] | string | null;
  preview_url?: string | null;
  previewUrl?: string | null;
  preview?: string | null;
  preview_type?: string | null;
  previewType?: string | null;
  preview_status?: "complete" | "failed" | "processing" | "queued" | "skipped" | "not_requested" | null;
  previewStatus?: "complete" | "failed" | "processing" | "queued" | "skipped" | "not_requested" | null;
  preview_error?: string | null;
  previewError?: string | null;
  ingestion_status?: "uploaded" | "queued" | "processing" | "indexed" | "failed" | "low_text" | "ocr_needed" | "partial" | null;
  ingestionStatus?: "uploaded" | "queued" | "processing" | "indexed" | "failed" | "low_text" | "ocr_needed" | "partial" | null;
  ocr_status?: "queued" | "processing" | "complete" | "failed" | "not_requested" | null;
  ocrStatus?: "queued" | "processing" | "complete" | "failed" | "not_requested" | null;
  vision_status?: "queued" | "processing" | "complete" | "failed" | "not_requested" | null;
  visionStatus?: "queued" | "processing" | "complete" | "failed" | "not_requested" | null;
  multimodal_status?: "pending" | "native_only" | "ocr_attempted" | "ocr_enriched" | "ocr_unavailable" | "ocr_failed" | "vision_enriched" | "partial" | null;
  multimodalStatus?: "pending" | "native_only" | "ocr_attempted" | "ocr_enriched" | "ocr_unavailable" | "ocr_failed" | "vision_enriched" | "partial" | null;
  ocr_processed?: boolean;
  ocrProcessed?: boolean;
  vision_processed?: boolean;
  visionProcessed?: boolean;
}

export function getFileTypeFromExtension(filename: string): Asset["type"] {
  const extension = filename.split(".").pop()?.toLowerCase() || "";
  if (extension === "pdf") return "pdf";
  if (extension === "docx" || extension === "doc") return "docx";
  if (extension === "xlsx" || extension === "xls" || extension === "csv") return "xlsx";
  if (extension === "pptx" || extension === "ppt") return "pptx";
  if (["png", "jpg", "jpeg", "webp", "gif", "svg"].includes(extension)) return "img";
  return "pdf";
}

export function normalizeTags(rawTags: BackendFileRecord["tags"]): AssetTag[] {
  const list = Array.isArray(rawTags)
    ? rawTags
    : typeof rawTags === "string" && rawTags.trim()
      ? rawTags.split(",")
      : [];

  return list
    .map((tag) => tag.trim())
    .filter((tag): tag is AssetTag => VALID_TAGS.has(tag as AssetTag));
}

export function toAsset(
  file: BackendFileRecord,
  options?: { status?: Asset["status"]; uploader?: string; urgency?: Asset["urgency"] }
): Asset {
  const name = file.name ?? file.filename ?? "Untitled";
  const previewUrl = file.preview_url ?? file.previewUrl ?? file.preview ?? null;
  const previewType = file.preview_type ?? file.previewType ?? (previewUrl?.toLowerCase().endsWith(".pdf") ? "pdf" : null);
  const baseType = getFileTypeFromExtension(name);
  const effectiveType = previewType === "pdf" ? "pdf" : baseType;

  return {
    id: String(file.id ?? name),
    name,
    type: effectiveType,
    url: file.url ?? "",
    previewUrl,
    previewType,
    previewStatus: file.preview_status ?? file.previewStatus ?? null,
    previewError: file.preview_error ?? file.previewError ?? null,
    ingestionStatus: file.ingestion_status ?? file.ingestionStatus ?? null,
    ocrStatus: file.ocr_status ?? file.ocrStatus ?? null,
    visionStatus: file.vision_status ?? file.visionStatus ?? null,
    multimodalStatus: file.multimodal_status ?? file.multimodalStatus ?? null,
    ocrProcessed: file.ocr_processed ?? file.ocrProcessed ?? false,
    visionProcessed: file.vision_processed ?? file.visionProcessed ?? false,
    tags: normalizeTags(file.tags),
    uploadDate: new Date(file.date ?? Date.now()).toISOString().split("T")[0],
    uploader: options?.uploader ?? "System",
    size: file.size ?? "Unknown",
    urgency: options?.urgency ?? "medium",
    assignedTo: [],
    comments: 0,
    status: options?.status ?? "ready",
    aiEnabled: effectiveType === "pdf" || effectiveType === "docx",
  };
}
