"use client";

import { useEffect, useMemo, useState } from "react";
import { useParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import {
  ExternalLink,
  FileText,
  FileType2,
  Image as ImageIcon,
  Link2,
  Presentation,
  Search,
  Table,
  X,
} from "lucide-react";
import { RileyContextChat } from "@app/components/campaign/RileyContextChat";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { Asset } from "@app/lib/types";
import { apiFetch } from "@app/lib/api";
import { cn } from "@app/lib/utils";
import { toAsset } from "@app/lib/files";

type SourceCategory =
  | "Media"
  | "Legislative"
  | "Polling"
  | "Stakeholder"
  | "Academic"
  | "Internal"
  | "Other";

type SourceLink = {
  id: string;
  title: string;
  url: string;
  category: SourceCategory;
  notes?: string;
  addedAt: string;
};

const SOURCE_CATEGORIES: SourceCategory[] = [
  "Media",
  "Legislative",
  "Polling",
  "Stakeholder",
  "Academic",
  "Internal",
  "Other",
];

function getFileIcon(type: Asset["type"]) {
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

function categoryClass(category: SourceCategory): string {
  if (category === "Media") return "border-[#c9d6f0] bg-[#eff4ff] text-[#2f4f88]";
  if (category === "Legislative") return "border-[#d5c4ec] bg-[#f4effc] text-[#5d3b87]";
  if (category === "Polling") return "border-[#c6dfcf] bg-[#edf7f1] text-[#2f7047]";
  if (category === "Stakeholder") return "border-[#f2d5a7] bg-[#fff7eb] text-[#8a5a16]";
  if (category === "Academic") return "border-[#d6dce9] bg-[#f2f5fb] text-[#496186]";
  if (category === "Internal") return "border-[#d9d4c7] bg-[#f7f3ea] text-[#5f6470]";
  return "border-[#e5d9c3] bg-[#fbf5e9] text-[#7a6440]";
}

function formatAddedAt(iso?: string): string {
  if (!iso) return "Unknown date";
  const parsed = new Date(iso);
  if (Number.isNaN(parsed.getTime())) return "Unknown date";
  return parsed.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" });
}

export default function ResearchPage() {
  const params = useParams();
  const { getToken } = useAuth();
  const campaignId = params.id as string;

  const [isChatOpen, setIsChatOpen] = useState(false);
  const [assets, setAssets] = useState<Asset[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [selectedFile, setSelectedFile] = useState<Asset | null>(null);

  const [searchQuery, setSearchQuery] = useState("");
  const [sourceLinks, setSourceLinks] = useState<SourceLink[]>([]);
  const [isAddSourceOpen, setIsAddSourceOpen] = useState(false);
  const [newSourceTitle, setNewSourceTitle] = useState("");
  const [newSourceUrl, setNewSourceUrl] = useState("");
  const [newSourceCategory, setNewSourceCategory] = useState<SourceCategory>("Media");
  const [newSourceNotes, setNewSourceNotes] = useState("");
  const [sourceError, setSourceError] = useState<string | null>(null);

  useEffect(() => {
    if (!isChatOpen) return;
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") setIsChatOpen(false);
    };

    const shouldLockBody = window.innerWidth < 1024;
    const previousOverflow = document.body.style.overflow;
    if (shouldLockBody) document.body.style.overflow = "hidden";

    window.addEventListener("keydown", handleEscape);
    return () => {
      window.removeEventListener("keydown", handleEscape);
      if (shouldLockBody) document.body.style.overflow = previousOverflow;
    };
  }, [isChatOpen]);

  useEffect(() => {
    const fetchFiles = async () => {
      if (!campaignId) return;
      try {
        const token = await getToken();
        if (!token) throw new Error("Authentication required");

        const data = await apiFetch<{ files: any[] }>(
          `/api/v1/list?tenant_id=${encodeURIComponent(campaignId)}`,
          { token, method: "GET" },
        );

        const fetchedAssets: Asset[] = (data.files || [])
          .filter((file: any) => {
            const tags = Array.isArray(file.tags) ? file.tags : [];
            return tags.includes("Research");
          })
          .map((file: any) => toAsset(file, { status: "ready" }));

        setAssets(fetchedAssets);
      } catch (error) {
        console.error("Error fetching research files:", error);
        setAssets([]);
      } finally {
        setIsLoading(false);
      }
    };

    void fetchFiles();
  }, [campaignId, getToken]);

  useEffect(() => {
    if (!campaignId) return;
    try {
      const raw = window.localStorage.getItem(`research_source_links_${campaignId}`);
      if (!raw) return;
      const parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return;

      const safeLinks = parsed
        .map((item: any) => ({
          id: String(item?.id || ""),
          title: String(item?.title || "").trim(),
          url: String(item?.url || "").trim(),
          category: SOURCE_CATEGORIES.includes(item?.category) ? item.category : "Other",
          notes: String(item?.notes || "").trim(),
          addedAt: String(item?.addedAt || ""),
        }))
        .filter((item: SourceLink) => item.id && item.title && item.url);

      setSourceLinks(safeLinks);
    } catch {
      setSourceLinks([]);
    }
  }, [campaignId]);

  useEffect(() => {
    if (!campaignId) return;
    try {
      window.localStorage.setItem(`research_source_links_${campaignId}`, JSON.stringify(sourceLinks));
    } catch {
      // no-op for unsupported storage contexts
    }
  }, [campaignId, sourceLinks]);

  const handleViewAsset = (filename: string) => {
    const asset = assets.find((item) => item.name === filename);
    if (!asset) return;
    setSelectedFile(asset);
  };

  const normalizedQuery = searchQuery.trim().toLowerCase();

  const filteredAssets = useMemo(() => {
    const sorted = [...assets].sort((a, b) => {
      const aDate = new Date(a.uploadDate).getTime();
      const bDate = new Date(b.uploadDate).getTime();
      return bDate - aDate;
    });
    if (!normalizedQuery) return sorted;
    return sorted.filter((asset) => {
      const tagText = Array.isArray(asset.tags) ? asset.tags.join(" ").toLowerCase() : "";
      return (
        asset.name.toLowerCase().includes(normalizedQuery) ||
        asset.uploader.toLowerCase().includes(normalizedQuery) ||
        tagText.includes(normalizedQuery)
      );
    });
  }, [assets, normalizedQuery]);

  const filteredSourceLinks = useMemo(() => {
    if (!normalizedQuery) return sourceLinks;
    return sourceLinks.filter((link) => {
      return (
        link.title.toLowerCase().includes(normalizedQuery) ||
        link.url.toLowerCase().includes(normalizedQuery) ||
        link.category.toLowerCase().includes(normalizedQuery) ||
        (link.notes || "").toLowerCase().includes(normalizedQuery)
      );
    });
  }, [normalizedQuery, sourceLinks]);

  const recentUploadCount = useMemo(() => {
    const threshold = Date.now() - 7 * 24 * 60 * 60 * 1000;
    return assets.filter((asset) => {
      const stamp = new Date(asset.uploadDate).getTime();
      return !Number.isNaN(stamp) && stamp >= threshold;
    }).length;
  }, [assets]);

  const handleAddSourceLink = () => {
    const title = newSourceTitle.trim();
    const url = newSourceUrl.trim();
    const notes = newSourceNotes.trim();

    if (!title || !url) {
      setSourceError("Title and URL are required.");
      return;
    }

    try {
      const parsed = new URL(url);
      if (!["http:", "https:"].includes(parsed.protocol)) {
        setSourceError("Please enter a valid http/https URL.");
        return;
      }

      setSourceLinks((prev) => [
        {
          id: `${Date.now()}`,
          title,
          url: parsed.toString(),
          category: newSourceCategory,
          notes,
          addedAt: new Date().toISOString(),
        },
        ...prev,
      ]);

      setNewSourceTitle("");
      setNewSourceUrl("");
      setNewSourceCategory("Media");
      setNewSourceNotes("");
      setSourceError(null);
      setIsAddSourceOpen(false);
    } catch {
      setSourceError("Please enter a valid URL.");
    }
  };

  const handleRemoveSource = (id: string) => {
    setSourceLinks((prev) => prev.filter((item) => item.id !== id));
  };

  return (
    <div className="relative flex h-full min-h-0 bg-[#f7f5ef]">
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <header className="flex flex-wrap items-end justify-between gap-4 border-b border-[#e5ddce] px-6 py-5 lg:px-8">
          <div>
            <h1 className="text-2xl font-semibold tracking-tight text-[#1f2a44]">Campaign Research</h1>
            <p className="mt-1 max-w-3xl text-sm text-[#6f788a]">
              Working library for this campaign&apos;s research documents and tracked source links.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[#8a90a0]" />
              <input
                type="text"
                placeholder="Search documents and sources..."
                value={searchQuery}
                onChange={(event) => setSearchQuery(event.target.value)}
                className="w-72 rounded-lg border border-[#d8d0bf] bg-white py-2 pl-10 pr-4 text-sm text-[#1f2a44] placeholder:text-[#8a90a0] focus:outline-none focus:ring-2 focus:ring-[#d4ad47]/35"
              />
            </div>
            <button
              onClick={() => setIsChatOpen((prev) => !prev)}
              className={`inline-flex items-center gap-2 rounded-md border px-3 py-1.5 text-sm font-medium transition-colors ${
                isChatOpen
                  ? "border-[#ccb67d] bg-[#f2ebd6] text-[#1f2a44]"
                  : "border-[#d8d0bf] bg-white text-[#1f2a44] hover:bg-[#f1ece2]"
              }`}
              aria-expanded={isChatOpen}
              aria-controls="research-riley-panel"
            >
              <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-[#eadfb7] text-[11px] font-semibold text-[#6d560f]">
                R
              </span>
              Ask Riley
            </button>
          </div>
        </header>

        <div className="flex-1 overflow-y-auto px-6 py-5 lg:px-8">
          <section className="mb-5 grid grid-cols-1 gap-3 sm:grid-cols-3">
            <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4">
              <p className="text-xs font-semibold uppercase tracking-wide text-[#6f788a]">Research Docs</p>
              <p className="mt-1 text-2xl font-semibold text-[#1f2a44]">{assets.length}</p>
              <p className="mt-1 text-xs text-[#8a90a0]">Files tagged as Research in this campaign</p>
            </div>
            <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4">
              <p className="text-xs font-semibold uppercase tracking-wide text-[#6f788a]">Tracked Sources</p>
              <p className="mt-1 text-2xl font-semibold text-[#1f2a44]">{sourceLinks.length}</p>
              <p className="mt-1 text-xs text-[#8a90a0]">URL references saved for this campaign</p>
            </div>
            <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4">
              <p className="text-xs font-semibold uppercase tracking-wide text-[#6f788a]">Recent Uploads</p>
              <p className="mt-1 text-2xl font-semibold text-[#1f2a44]">{recentUploadCount}</p>
              <p className="mt-1 text-xs text-[#8a90a0]">Research docs uploaded in the last 7 days</p>
            </div>
          </section>

          <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
            <section className="xl:col-span-2">
              <div className="mb-3 flex items-center gap-2">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-[#6f788a]">
                  Research Documents
                </h2>
              </div>
              <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-3">
                {isLoading ? (
                  <div className="py-12 text-center text-sm text-[#8a90a0]">Loading research files...</div>
                ) : filteredAssets.length === 0 ? (
                  <div className="py-12 text-center text-sm text-[#8a90a0]">
                    {normalizedQuery
                      ? "No research files match your search."
                      : "No files tagged with Research yet."}
                  </div>
                ) : (
                  <div className="space-y-2">
                    {filteredAssets.map((asset) => {
                      const { icon: FileIcon, color: iconColor } = getFileIcon(asset.type);
                      return (
                        <button
                          key={asset.id}
                          type="button"
                          onClick={() => setSelectedFile(asset)}
                          className="flex w-full items-center gap-3 rounded-lg border border-[#ece6d9] bg-white px-3 py-2.5 text-left transition-colors hover:bg-[#f5f1e8]"
                        >
                          <FileIcon className={cn("h-4 w-4 shrink-0", iconColor)} />
                          <div className="min-w-0 flex-1">
                            <p className="truncate text-sm font-medium text-[#1f2a44]">{asset.name}</p>
                            <p className="mt-0.5 truncate text-[11px] text-[#8a90a0]">
                              {asset.size} · {asset.uploader} · {formatAddedAt(asset.uploadDate)}
                            </p>
                          </div>
                          {Array.isArray(asset.tags) && asset.tags.length > 0 ? (
                            <div className="hidden shrink-0 gap-1 md:flex">
                              {asset.tags.slice(0, 2).map((tag) => (
                                <span
                                  key={`${asset.id}-${tag}`}
                                  className="rounded-full border border-[#ddd3bf] bg-[#f8f3e9] px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-[#6f788a]"
                                >
                                  {tag}
                                </span>
                              ))}
                            </div>
                          ) : null}
                        </button>
                      );
                    })}
                  </div>
                )}
              </div>
            </section>

            <section>
              <div className="mb-3 flex items-center gap-2">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-[#6f788a]">Source Links</h2>
                <button
                  type="button"
                  onClick={() => {
                    setIsAddSourceOpen((prev) => !prev);
                    setSourceError(null);
                  }}
                  className="ml-auto inline-flex items-center gap-1 rounded-md border border-[#d8d0bf] bg-white px-2 py-1 text-xs font-medium text-[#1f2a44] hover:bg-[#f1ece2]"
                >
                  <Link2 className="h-3 w-3" />
                  Add Source
                </button>
              </div>
              {isAddSourceOpen ? (
                <div className="mb-3 rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-3">
                  <input
                    type="text"
                    value={newSourceTitle}
                    onChange={(event) => setNewSourceTitle(event.target.value)}
                    placeholder="Source title"
                    className="mb-2 w-full rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44] placeholder:text-[#8a90a0]"
                  />
                  <input
                    type="url"
                    value={newSourceUrl}
                    onChange={(event) => setNewSourceUrl(event.target.value)}
                    placeholder="https://..."
                    className="mb-2 w-full rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44] placeholder:text-[#8a90a0]"
                  />
                  <select
                    value={newSourceCategory}
                    onChange={(event) => setNewSourceCategory(event.target.value as SourceCategory)}
                    className="mb-2 w-full rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44]"
                  >
                    {SOURCE_CATEGORIES.map((category) => (
                      <option key={category} value={category}>
                        {category}
                      </option>
                    ))}
                  </select>
                  <textarea
                    value={newSourceNotes}
                    onChange={(event) => setNewSourceNotes(event.target.value)}
                    placeholder="Optional notes..."
                    rows={2}
                    className="w-full rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44] placeholder:text-[#8a90a0]"
                  />
                  {sourceError ? <p className="mt-2 text-xs text-red-600">{sourceError}</p> : null}
                  <div className="mt-2 flex justify-end">
                    <button
                      type="button"
                      onClick={handleAddSourceLink}
                      className="rounded-md border border-[#d8d0bf] bg-white px-2.5 py-1 text-xs font-medium text-[#1f2a44] hover:bg-[#f1ece2]"
                    >
                      Save Source
                    </button>
                  </div>
                  <p className="mt-2 text-[11px] text-[#8a90a0]">
                    Source links are stored locally in your browser for this campaign.
                  </p>
                </div>
              ) : null}

              <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-3">
                {filteredSourceLinks.length === 0 ? (
                  <div className="py-10 text-center text-sm text-[#8a90a0]">
                    {normalizedQuery ? "No source links match your search." : "No source links added yet."}
                  </div>
                ) : (
                  <div className="space-y-2.5">
                    {filteredSourceLinks.map((link) => (
                      <div key={link.id} className="rounded-md border border-[#ece6d9] bg-white p-2.5">
                        <div className="mb-1 flex items-center justify-between gap-2">
                          <span
                            className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${categoryClass(link.category)}`}
                          >
                            {link.category}
                          </span>
                          <button
                            type="button"
                            onClick={() => handleRemoveSource(link.id)}
                            className="rounded-md p-1 text-[#8a90a0] transition-colors hover:bg-[#f4eee0] hover:text-[#5f6470]"
                            aria-label={`Remove source ${link.title}`}
                          >
                            <X className="h-3.5 w-3.5" />
                          </button>
                        </div>
                        <a
                          href={link.url}
                          target="_blank"
                          rel="noreferrer"
                          className="flex items-start justify-between gap-2"
                        >
                          <div className="min-w-0">
                            <p className="truncate text-sm font-medium text-[#1f2a44]">{link.title}</p>
                            <p className="truncate text-[11px] text-[#8a90a0]">{link.url}</p>
                          </div>
                          <ExternalLink className="mt-0.5 h-3.5 w-3.5 shrink-0 text-[#6f788a]" />
                        </a>
                        {link.notes ? <p className="mt-1.5 text-xs text-[#6f788a]">{link.notes}</p> : null}
                        <p className="mt-1 text-[11px] text-[#8a90a0]">Added {formatAddedAt(link.addedAt)}</p>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </section>
          </div>
        </div>
      </div>

      {isChatOpen ? (
        <>
          <button
            type="button"
            className="fixed inset-0 z-30 bg-[#1f2a44]/20 backdrop-blur-[1px] lg:hidden"
            aria-label="Close Ask Riley panel backdrop"
            onClick={() => setIsChatOpen(false)}
          />
          <aside
            id="research-riley-panel"
            className="fixed inset-y-0 right-0 z-40 w-[94vw] max-w-[400px] border-l border-[#d8d0bf] bg-[#fcfbf8] shadow-xl shadow-[#1f2a44]/10 lg:static lg:z-auto lg:h-full lg:w-[360px] lg:max-w-none lg:shadow-none"
          >
            <div className="flex h-full flex-col">
              <div className="flex h-16 items-center justify-between border-b border-[#e5ddce] px-4">
                <span className="inline-flex items-center gap-2 text-sm font-semibold text-[#1f2a44]">
                  <span className="inline-flex h-5 w-5 items-center justify-center rounded-full bg-[#eadfb7] text-[11px] font-semibold text-[#6d560f]">
                    R
                  </span>
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
                  I can help analyze this campaign&apos;s research documents and source links, summarize findings,
                  and draft strategy-ready takeaways grounded in the materials above.
                </p>
              </div>
              <div className="flex-1 overflow-hidden">
                <RileyContextChat
                  mode="research"
                  contextKey="research"
                  campaignId={campaignId}
                  onViewAsset={handleViewAsset}
                />
              </div>
            </div>
          </aside>
        </>
      ) : null}

      {selectedFile ? (
        <DocumentViewer file={selectedFile} variant="modal" onClose={() => setSelectedFile(null)} />
      ) : null}
    </div>
  );
}
