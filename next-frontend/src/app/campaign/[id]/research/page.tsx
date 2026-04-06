"use client";

import { useState, useEffect } from "react";
import { useParams } from "next/navigation";
import { useAuth } from "@clerk/nextjs";
import {
  TrendingUp,
  FileText,
  X,
  Search,
  FileType2,
  Table,
  Presentation,
  Image as ImageIcon,
  Radar,
  Radio,
  Scale,
  Sparkles,
  Link2,
  ExternalLink,
} from "lucide-react";
import { RileyContextChat } from "@app/components/campaign/RileyContextChat";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { Asset } from "@app/lib/types";
import { cn } from "@app/lib/utils";
import { apiFetch } from "@app/lib/api";
import { toAsset } from "@app/lib/files";

const previewKpis = [
  {
    label: "Narrative Momentum",
    value: "+11.8%",
    note: "Projected positive movement from monitored discourse segments.",
    icon: TrendingUp,
  },
  {
    label: "Monitored Feed Groups",
    value: "24",
    note: "Media, social, policy, and stakeholder watchlists in preview scope.",
    icon: Radio,
  },
  {
    label: "Legislative Priority Flags",
    value: "7",
    note: "Hearing windows and filing events requiring rapid campaign response.",
    icon: Scale,
  },
  {
    label: "Listening Agents",
    value: "5",
    note: "Campaign-specific monitor agents planned for continuous signal capture.",
    icon: Radar,
  },
];

const mockSignals = [
  {
    id: "sig-1",
    sourceType: "Media",
    title: "Regional energy-cost framing shifts from skepticism to pragmatism",
    insight: "Language in local press now favors affordability-and-reliability framing over policy abstractions.",
    confidence: "high",
    source: "Regional business press + TV recap transcripts",
    action: "Prepare localized proof points for spokespeople before next interview cycle.",
  },
  {
    id: "sig-2",
    sourceType: "Social",
    title: "Volunteer channels surface sustained transportation cost anxiety",
    insight: "Community conversations repeatedly connect mobility costs to broader household pressure narratives.",
    confidence: "medium",
    source: "Closed volunteer community channels",
    action: "Prioritize message variants emphasizing immediate household impact.",
  },
  {
    id: "sig-3",
    sourceType: "Legislative",
    title: "State committee hearing moved up one week",
    insight: "Compressed timeline reduces campaign response window for briefing prep and coalition alignment.",
    confidence: "high",
    source: "State committee calendar updates",
    action: "Trigger accelerated briefing package and surrogate outreach sequence.",
  },
  {
    id: "sig-4",
    sourceType: "Polling",
    title: "Parent audience confidence softens on reliability-focused messaging",
    insight: "Audience response strengthens when language shifts to concrete household outcomes and near-term wins.",
    confidence: "medium",
    source: "Panel polling + message-fragment testing",
    action: "Shift near-term copy testing toward concrete savings and service stability examples.",
  },
];

const intelligenceBrief = [
  "Narrative momentum is positive, but confidence depends on preserving practical, household-first language.",
  "Policy calendar compression creates a near-term execution risk across briefing, media prep, and coalition sync.",
  "Audience response is strongest when recommendations translate into immediate, concrete campaign moves.",
];

const liveSignalsPreview = [
  {
    id: "live-1",
    sourceName: "State Energy Docket Watch",
    description: "Agenda amendment added reliability testimony window for Tuesday hearing.",
    timestamp: "11:42 AM",
  },
  {
    id: "live-2",
    sourceName: "Regional Media Pulse",
    description: "Morning segment package reframed affordability narrative around service stability.",
    timestamp: "10:18 AM",
  },
  {
    id: "live-3",
    sourceName: "Community Listening Agent",
    description: "Parent-thread volume increased on commuting cost concerns in two target counties.",
    timestamp: "9:07 AM",
  },
];

type SourceLink = {
  id: string;
  title: string;
  url: string;
};

const roadmapItems = [
  "Automated source credibility scoring across feed clusters",
  "Event-driven briefing generation tied to campaign priorities",
  "Cross-campaign pattern detection and anomaly alerts",
  "Analyst-ready daily intelligence digest with explainable signal traces",
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
  const [sourceError, setSourceError] = useState<string | null>(null);

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
          { token, method: "GET" }
        );
        const fetchedAssets: Asset[] = data.files
          .filter((file: any) => {
            const tags = Array.isArray(file.tags) ? file.tags : [];
            return tags.includes("Research");
          })
          .map((file: any) => toAsset(file, { status: "ready" }));
        setAssets(fetchedAssets);
      } catch (error) {
        console.error("Error fetching files:", error);
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
      window.localStorage.setItem(
        `research_source_links_${campaignId}`,
        JSON.stringify(sourceLinks)
      );
    } catch {
      // no-op
    }
  }, [campaignId, sourceLinks]);

  const handleViewAsset = (filename: string) => {
    const asset = assets.find((a) => a.name === filename);
    if (asset) {
      setSelectedFile(asset);
    } else {
      alert(
        `Asset "${filename}" not found in current view (might be Global/Archived).`
      );
    }
  };

  const filteredAssets = assets.filter((asset) =>
    asset.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const confidenceClass = (confidence: string) => {
    if (confidence === "high") return "border-emerald-500/30 bg-emerald-500/10 text-emerald-700";
    if (confidence === "medium") return "border-amber-500/30 bg-amber-500/10 text-amber-700";
    return "border-rose-500/30 bg-rose-500/10 text-rose-700";
  };

  const handleAddSourceLink = () => {
    const title = newSourceTitle.trim();
    const url = newSourceUrl.trim();
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
        },
        ...prev,
      ]);
      setNewSourceTitle("");
      setNewSourceUrl("");
      setSourceError(null);
      setIsAddSourceOpen(false);
    } catch {
      setSourceError("Please enter a valid URL.");
    }
  };

  return (
    <div className="relative flex h-full min-h-0 bg-[#f7f5ef]">
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <header className="flex flex-wrap items-end justify-between gap-4 border-b border-[#e5ddce] px-6 py-5 lg:px-8">
          <div>
            <div className="mb-2 inline-flex items-center gap-2 rounded-full border border-[#d4ad47]/40 bg-[#f4ecd7] px-2.5 py-1 text-[11px] font-semibold uppercase tracking-wide text-[#6d560f]">
              <Sparkles className="h-3.5 w-3.5" />
              Coming Soon
            </div>
            <h1 className="text-2xl font-semibold tracking-tight text-[#1f2a44]">
              Research - Riley V2 Intelligence Center
            </h1>
            <p className="mt-1 max-w-3xl text-sm text-[#6f788a]">
              Strategic preview of the intelligence layer Riley V2 is bringing to campaign decision-making.
            </p>
            <p className="mt-1 text-xs text-[#9a8a5b]">Illustrative preview only - feeds and agent outputs shown here are not live production integrations yet.</p>
          </div>
          <div className="flex items-center gap-2">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-[#8a90a0]" />
              <input
                type="text"
                placeholder="Search research files..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-64 rounded-lg border border-[#d8d0bf] bg-white py-2 pl-10 pr-4 text-sm text-[#1f2a44] placeholder:text-[#8a90a0] focus:outline-none focus:ring-2 focus:ring-[#d4ad47]/40"
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
          <section className="mb-5 rounded-xl border border-[#dfd6c5] bg-[#fcfbf8] p-4">
            <div className="mb-2 flex items-center gap-2">
              <h2 className="text-sm font-semibold uppercase tracking-wider text-[#6f788a]">
                Riley Intelligence Brief
              </h2>
              <span className="rounded-full border border-[#d4ad47]/40 bg-[#f4ecd7] px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-[#6d560f]">
                V2 Preview
              </span>
            </div>
            <ul className="space-y-1.5 text-sm text-[#2a3650]">
              {intelligenceBrief.map((item) => (
                <li key={item} className="flex items-start gap-2">
                  <span className="mt-1.5 inline-block h-1.5 w-1.5 rounded-full bg-[#b7ad98]" />
                  <span>{item}</span>
                </li>
              ))}
            </ul>
          </section>

          <div className="mb-5 rounded-xl border border-[#dfd6c5] bg-[#f9f6ee] p-3">
            <p className="text-xs text-[#6f788a]">
              <span className="font-semibold text-[#1f2a44]">Riley V2 Preview:</span> This page demonstrates where research intelligence is heading - continuous monitoring, explainable signals, and briefing-ready campaign context in one place.
            </p>
          </div>

          <div className="mb-6 grid grid-cols-1 gap-4 sm:grid-cols-2 xl:grid-cols-4">
            {previewKpis.map((kpi) => (
              <div key={kpi.label} className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4">
                <div className="mb-2 flex items-center gap-2 text-[#6f788a]">
                  <kpi.icon className="h-4 w-4" />
                  <span className="text-xs font-semibold uppercase tracking-wide">{kpi.label}</span>
                </div>
                <p className="text-xl font-semibold text-[#1f2a44]">{kpi.value}</p>
                <p className="mt-1 text-xs text-[#8a90a0]">{kpi.note}</p>
              </div>
            ))}
          </div>

          <div className="grid grid-cols-1 gap-6 xl:grid-cols-3">
            <section className="xl:col-span-2">
              <div className="mb-3 flex items-center gap-2">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-[#6f788a]">
                  Intelligence Streams (Preview)
                </h2>
              </div>
              <div className="space-y-3">
                {mockSignals.map((signal) => (
                  <div
                    key={signal.id}
                    className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-4"
                  >
                    <div className="mb-2 flex items-center justify-between gap-2">
                      <span className="text-xs text-[#8a90a0]">
                        {signal.sourceType}
                      </span>
                      <span className={cn("inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide", confidenceClass(signal.confidence))}>
                        {signal.confidence}
                      </span>
                    </div>
                    <p className="text-sm font-semibold text-[#1f2a44]">
                      {signal.title}
                    </p>
                    <p className="mt-1 text-sm text-[#6f788a]">{signal.insight}</p>
                    <p className="mt-1 text-[11px] text-[#8a90a0]">Source: {signal.source}</p>
                    <div className="mt-3 rounded-md border border-[#e9dfcb] bg-[#f9f6ee] px-2.5 py-2">
                      <p className="text-[11px] font-semibold uppercase tracking-wide text-[#6f788a]">
                        Recommended campaign move
                      </p>
                      <p className="mt-0.5 text-xs text-[#6f788a]">{signal.action}</p>
                    </div>
                  </div>
                ))}
              </div>
            </section>

            <section>
              <div className="mb-3 flex items-center gap-2">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-[#6f788a]">
                  Live Signals (Preview)
                </h2>
              </div>
              <div className="mb-4 rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-3">
                <div className="space-y-2.5">
                  {liveSignalsPreview.map((signal) => (
                    <div key={signal.id} className="border-b border-[#ece6d9] pb-2.5 last:border-0 last:pb-0">
                      <div className="flex items-center justify-between gap-2">
                        <p className="text-sm font-medium text-[#1f2a44]">{signal.sourceName}</p>
                        <span className="text-[11px] text-[#8a90a0]">{signal.timestamp}</span>
                      </div>
                      <p className="mt-0.5 text-xs text-[#6f788a]">{signal.description}</p>
                    </div>
                  ))}
                </div>
              </div>

              <div className="mb-3 flex items-center gap-2">
                <h2 className="text-sm font-semibold uppercase tracking-wider text-[#6f788a]">
                  Source Material
                </h2>
                <button
                  type="button"
                  onClick={() => {
                    setIsAddSourceOpen((prev) => !prev);
                    setSourceError(null);
                  }}
                  className="ml-auto inline-flex items-center gap-1 rounded-md border border-[#d8d0bf] bg-white px-2 py-1 text-xs font-medium text-[#1f2a44] hover:bg-[#f1ece2]"
                >
                  <Link2 className="h-3 w-3" />
                  Add Source Link
                </button>
              </div>
              <div className="mb-3 rounded-lg border border-[#d8d0bf] bg-[#f9f6ee] px-3 py-2">
                <p className="text-[11px] text-[#6f788a]">
                  <span className="font-semibold text-[#1f2a44]">Preview note:</span> Source links added here are stored locally in this browser for this campaign preview only. They are not shared with teammates and are not persisted to backend systems.
                </p>
              </div>
              {isAddSourceOpen ? (
                <div className="mb-3 rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-3">
                  <input
                    type="text"
                    value={newSourceTitle}
                    onChange={(e) => setNewSourceTitle(e.target.value)}
                    placeholder="Source title"
                    className="mb-2 w-full rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44] placeholder:text-[#8a90a0]"
                  />
                  <input
                    type="url"
                    value={newSourceUrl}
                    onChange={(e) => setNewSourceUrl(e.target.value)}
                    placeholder="https://..."
                    className="w-full rounded-md border border-[#d8d0bf] bg-white px-2 py-1.5 text-sm text-[#1f2a44] placeholder:text-[#8a90a0]"
                  />
                  {sourceError ? <p className="mt-2 text-xs text-red-600">{sourceError}</p> : null}
                  <div className="mt-2 flex justify-end">
                    <button
                      type="button"
                      onClick={handleAddSourceLink}
                      className="rounded-md border border-[#d8d0bf] bg-white px-2.5 py-1 text-xs font-medium text-[#1f2a44] hover:bg-[#f1ece2]"
                    >
                      Save Link
                    </button>
                  </div>
                  <p className="mt-2 text-[11px] text-[#8a90a0]">Local preview-only link storage (campaign browser session).</p>
                </div>
              ) : null}
              <div className="rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-3">
                {isLoading ? (
                  <div className="py-10 text-center text-sm text-[#8a90a0]">
                    Loading files...
                  </div>
                ) : filteredAssets.length === 0 ? (
                  <div className="py-10 text-center text-sm text-[#8a90a0]">
                    {searchQuery
                      ? "No files match your search."
                      : "No files tagged with Research yet."}
                  </div>
                ) : (
                  <div className="space-y-2">
                    {filteredAssets.map((asset) => {
                      const { icon: FileIcon, color: iconColor } = getFileIcon(
                        asset.type
                      );
                      return (
                        <button
                          key={asset.id}
                          onClick={() => setSelectedFile(asset)}
                          className="flex w-full items-center gap-3 rounded-lg border border-[#ece6d9] bg-white px-3 py-2 text-left transition-colors hover:bg-[#f5f1e8]"
                        >
                          <FileIcon
                            className={cn("h-4 w-4 flex-shrink-0", iconColor)}
                          />
                          <div className="min-w-0 flex-1">
                            <p className="truncate text-sm font-medium text-[#1f2a44]">
                              {asset.name}
                            </p>
                            <p className="mt-0.5 truncate text-[11px] text-[#8a90a0]">
                              {asset.size} • {asset.uploader}
                            </p>
                          </div>
                        </button>
                      );
                    })}
                  </div>
                )}
              </div>
              {sourceLinks.length > 0 ? (
                <div className="mt-3 rounded-xl border border-[#e5ddce] bg-[#fcfbf8] p-3">
                  <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-[#6f788a]">Added Source Links</p>
                  <div className="space-y-2">
                    {sourceLinks.map((link) => (
                      <a
                        key={link.id}
                        href={link.url}
                        target="_blank"
                        rel="noreferrer"
                        className="flex items-center justify-between gap-2 rounded-md border border-[#ece6d9] bg-white px-2.5 py-2 text-left hover:bg-[#f5f1e8]"
                      >
                        <div className="min-w-0">
                          <p className="truncate text-sm font-medium text-[#1f2a44]">{link.title}</p>
                          <p className="truncate text-[11px] text-[#8a90a0]">{link.url}</p>
                        </div>
                        <ExternalLink className="h-3.5 w-3.5 shrink-0 text-[#6f788a]" />
                      </a>
                    ))}
                  </div>
                  <p className="mt-2 text-[11px] text-[#8a90a0]">
                    Local preview list only - not shared and not backend-persistent.
                  </p>
                </div>
              ) : null}
              <div className="mt-4 rounded-xl border border-dashed border-[#d8d0bf] bg-[#f9f7f2] p-3">
                <p className="text-xs font-semibold uppercase tracking-wide text-[#6f788a]">
                  Next in Riley V2
                </p>
                <ul className="mt-2 space-y-1.5 text-xs text-[#8a90a0]">
                  {roadmapItems.map((item) => (
                    <li key={item}>- {item}</li>
                  ))}
                </ul>
              </div>
            </section>
          </div>
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
                  Welcome to Riley Research preview. I can help you evaluate these
                  prototype intelligence streams, pressure-test assumptions, and
                  draft strategic questions for Riley V2 rollout planning.
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
      )}

      {selectedFile && (
        <DocumentViewer
          file={selectedFile}
          variant="modal"
          onClose={() => setSelectedFile(null)}
        />
      )}
    </div>
  );
}
