"use client";

import { useMemo, useState } from "react";
import {
  File,
  FileText,
  FileType2,
  Image as ImageIcon,
  Loader2,
  Presentation,
  Search,
  Table,
} from "lucide-react";
import { cn } from "@app/lib/utils";

type SearchResult = {
  id: string;
  score: number;
  payload?: {
    filename?: string;
    text_preview?: string;
    content?: string;
    type?: string;
    year?: number;
    [key: string]: unknown;
  };
};

type SearchResponse = {
  results: SearchResult[];
};

function basename(path?: string) {
  if (!path) return "Unknown file";
  const parts = path.split(/[\\/]/).filter(Boolean);
  return parts[parts.length - 1] ?? path;
}

function splitExtension(filename: string): { base: string; ext: string } {
  const lastDot = filename.lastIndexOf(".");
  if (lastDot <= 0 || lastDot === filename.length - 1) {
    return { base: filename, ext: "" };
  }
  return {
    base: filename.slice(0, lastDot),
    ext: filename.slice(lastDot + 1).toLowerCase(),
  };
}

function truncate(text: string, max = 200) {
  const t = text.trim();
  if (t.length <= max) return t;
  return `${t.slice(0, max).trimEnd()}…`;
}

function fileTypePresentation(ext: string) {
  switch (ext) {
    case "pdf":
      return {
        kindLabel: "PDF",
        icon: FileText,
        iconClass: "text-rose-700",
        chipClass: "bg-rose-50 text-rose-700 border-rose-200",
      };
    case "docx":
    case "doc":
      return {
        kindLabel: "Document",
        icon: FileType2,
        iconClass: "text-blue-700",
        chipClass: "bg-blue-50 text-blue-700 border-blue-200",
      };
    case "xlsx":
    case "csv":
      return {
        kindLabel: "Dataset",
        icon: Table,
        iconClass: "text-emerald-700",
        chipClass: "bg-emerald-50 text-emerald-700 border-emerald-200",
      };
    case "pptx":
    case "ppt":
      return {
        kindLabel: "Presentation",
        icon: Presentation,
        iconClass: "text-orange-700",
        chipClass: "bg-orange-50 text-orange-700 border-orange-200",
      };
    case "png":
    case "jpg":
    case "jpeg":
    case "webp":
      return {
        kindLabel: "Visual Asset",
        icon: ImageIcon,
        iconClass: "text-purple-700",
        chipClass: "bg-purple-50 text-purple-700 border-purple-200",
      };
    default:
      return {
        kindLabel: ext ? ext.toUpperCase() : "File",
        icon: File,
        iconClass: "text-slate-600",
        chipClass: "bg-slate-50 text-slate-700 border-slate-200",
      };
  }
}

function relevanceBadgeClass(pct: number) {
  if (pct > 70) return "bg-emerald-50 text-emerald-800 border-emerald-200";
  if (pct < 50) return "bg-yellow-50 text-yellow-800 border-yellow-200";
  return "bg-slate-100 text-slate-800 border-slate-200";
}

export function SearchInterface() {
  const [queryText, setQueryText] = useState("");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);

  const canSearch = useMemo(() => queryText.trim().length > 0, [queryText]);

  async function onSearch() {
    if (!canSearch || isLoading) return;

    setIsLoading(true);
    setError(null);

    try {
      const res = await fetch(`${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/v1/search`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          query_text: queryText,
          tenant_id: "bypass-mode",
        }),
      });

      if (!res.ok) {
        const text = await res.text().catch(() => "");
        throw new Error(
          `Search failed (${res.status}). ${text ? `Details: ${text}` : ""}`.trim()
        );
      }

      const data = (await res.json()) as SearchResponse;
      setResults(Array.isArray(data?.results) ? data.results : []);
    } catch (e) {
      setResults([]);
      setError(e instanceof Error ? e.message : "Unknown error");
    } finally {
      setIsLoading(false);
    }
  }

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm sm:p-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
        <div className="space-y-1">
          <div className="inline-flex items-center gap-2 text-sm font-semibold text-slate-900">
            <Search className="h-4 w-4 text-slate-700" aria-hidden="true" />
            Live Search
          </div>
          <p className="text-sm text-slate-500">
            Query the Intelligence Core (tenant:{" "}
            <span className="font-mono text-slate-700">bypass-mode</span>)
          </p>
        </div>
      </div>

      <div className="mt-4 flex flex-col gap-3 sm:flex-row">
        <div className="relative flex-1">
          <Search
            className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400"
            aria-hidden="true"
          />
          <input
            value={queryText}
            onChange={(e) => setQueryText(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") onSearch();
            }}
            placeholder="Ask: What happened in the 2024 community outreach report?"
            className={cn(
              "w-full rounded-xl border border-slate-200 bg-white px-10 py-3 text-sm text-slate-900",
              "placeholder:text-slate-400 shadow-sm",
              "focus:outline-none focus:ring-2 focus:ring-slate-300 focus:border-slate-300"
            )}
          />
        </div>

        <button
          type="button"
          onClick={onSearch}
          disabled={!canSearch || isLoading}
          className={cn(
            "inline-flex items-center justify-center gap-2 rounded-xl px-4 py-3 text-sm font-semibold",
            "border border-slate-200 bg-slate-900 text-white shadow-sm",
            "hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60",
            "focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-300 focus-visible:ring-offset-2 focus-visible:ring-offset-slate-50"
          )}
        >
          {isLoading ? (
            <>
              <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
              Analyzing…
            </>
          ) : (
            <>
              <Search className="h-4 w-4" aria-hidden="true" />
              Analyze
            </>
          )}
        </button>
      </div>

      {error ? (
        <div className="mt-4 rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
          {error}
        </div>
      ) : null}

      <div className="mt-5 space-y-3">
        {results.length > 0 ? (
          <div className="grid gap-3">
            {results.map((r) => {
              const rawFilename = basename(r.payload?.filename);
              const { base: displayTitle, ext } = splitExtension(rawFilename);
              const fileMeta = fileTypePresentation(ext);

              const rawPreview =
                r.payload?.text_preview ??
                r.payload?.content ??
                "No preview available.";
              const isPreviewMissing = rawPreview === "No preview available.";
              const preview = isPreviewMissing ? "" : truncate(rawPreview, 200);

              const year = r.payload?.year;
              const pct = Number.isFinite(r.score)
                ? Math.round(r.score * 100)
                : 0;
              const Icon = fileMeta.icon;

              return (
                <div
                  key={r.id}
                  className={cn(
                    "rounded-xl border border-slate-200 bg-white p-4 shadow-sm",
                    isPreviewMissing && "bg-slate-50"
                  )}
                >
                  <div className="flex items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="flex items-start gap-3">
                        <div className="mt-0.5 flex h-9 w-9 items-center justify-center rounded-lg border border-slate-200 bg-white">
                          <Icon
                            className={cn("h-4 w-4", fileMeta.iconClass)}
                            aria-hidden="true"
                          />
                        </div>

                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-center gap-2">
                            <div className="truncate text-sm font-semibold text-slate-900">
                              {displayTitle || rawFilename}
                            </div>
                            <div
                              className={cn(
                                "inline-flex items-center rounded-full border px-2 py-0.5 text-[0.7rem] font-semibold",
                                fileMeta.chipClass
                              )}
                            >
                              {fileMeta.kindLabel}
                            </div>
                          </div>

                          {isPreviewMissing ? (
                            <div className="mt-1 text-xs text-slate-500">
                              Visual/metadata-only result
                            </div>
                          ) : (
                            <div className="mt-1 text-sm text-slate-600">
                              {preview}
                            </div>
                          )}
                        </div>
                      </div>
                    </div>

                    <div className="flex flex-none flex-col items-end gap-2">
                      <div
                        className={cn(
                          "rounded-full border px-3 py-1 text-xs font-semibold",
                          relevanceBadgeClass(pct)
                        )}
                      >
                        Relevance: {pct}%
                      </div>
                      {typeof year === "number" ? (
                        <div className="text-xs text-slate-500">
                          Year:{" "}
                          <span className="font-mono text-slate-700">
                            {year}
                          </span>
                        </div>
                      ) : null}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 px-4 py-6 text-sm text-slate-500">
            {isLoading
              ? "Searching…"
              : "Run a query to see matching documents, snippets, and scores."}
          </div>
        )}
      </div>
    </section>
  );
}


