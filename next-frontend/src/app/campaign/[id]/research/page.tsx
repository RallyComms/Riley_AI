"use client";

import { useState, useEffect } from "react";
import { useParams } from "next/navigation";
import { TrendingUp, BarChart3, FileText, Lightbulb, Sparkles, X, Search, FileType2, Table, Presentation, Image as ImageIcon } from "lucide-react";
import { RileyContextChat, getRileyTitle } from "@app/components/campaign/RileyContextChat";
import { DocumentViewer } from "@app/components/ui/DocumentViewer";
import { Asset } from "@app/lib/types";
import { cn } from "@app/lib/utils";

// Mock research insights
const mockInsights = [
  {
    id: "1",
    title: "Voter Sentiment Shift",
    description: "Support for environmental policy increased 12% in Q3 polling data.",
    type: "trend",
    icon: TrendingUp,
  },
  {
    id: "2",
    title: "Demographic Analysis",
    description: "18-34 age group shows strongest engagement with messaging campaigns.",
    type: "analysis",
    icon: BarChart3,
  },
  {
    id: "3",
    title: "Key Messaging Themes",
    description: "Top performing themes: 'Community First', 'Sustainable Future', 'Local Impact'.",
    type: "insight",
    icon: Lightbulb,
  },
  {
    id: "4",
    title: "Competitive Landscape",
    description: "Analysis of opponent messaging strategies and positioning gaps.",
    type: "analysis",
    icon: FileText,
  },
  {
    id: "5",
    title: "Media Coverage Trends",
    description: "Positive coverage increased 23% following Q3 campaign launch.",
    type: "trend",
    icon: TrendingUp,
  },
  {
    id: "6",
    title: "Policy Impact Metrics",
    description: "Survey data shows 68% support for proposed policy changes.",
    type: "insight",
    icon: BarChart3,
  },
];

function getInsightColor(type: string) {
  switch (type) {
    case "trend":
      return "border-emerald-500/30 bg-emerald-500/10";
    case "analysis":
      return "border-blue-500/30 bg-blue-500/10";
    case "insight":
      return "border-amber-500/30 bg-amber-500/10";
    default:
      return "border-zinc-800 bg-zinc-900/50";
  }
}

// Helper to get file type from extension
function getFileTypeFromExtension(filename: string): Asset["type"] {
  const ext = filename.split(".").pop()?.toLowerCase();
  switch (ext) {
    case "pdf":
      return "pdf";
    case "docx":
    case "doc":
      return "docx";
    case "xlsx":
    case "xls":
      return "xlsx";
    case "pptx":
    case "ppt":
      return "pptx";
    case "jpg":
    case "jpeg":
    case "png":
    case "gif":
    case "webp":
      return "img";
    default:
      return "pdf";
  }
}

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

function isAISupportedType(type: Asset["type"]): boolean {
  return ["pdf", "docx", "xlsx", "pptx"].includes(type);
}

export default function ResearchPage() {
  const params = useParams();
  const campaignId = params.id as string;
  const [isChatOpen, setIsChatOpen] = useState(false);
  const [assets, setAssets] = useState<Asset[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [selectedFile, setSelectedFile] = useState<Asset | null>(null);
  const [searchQuery, setSearchQuery] = useState("");

  // Fetch files from backend and filter by Research tag
  useEffect(() => {
    const fetchFiles = async () => {
      if (!campaignId) return;

      try {
        const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000"}/api/v1/list?tenant_id=${encodeURIComponent(campaignId)}`);
        if (!response.ok) {
          throw new Error("Failed to fetch files");
        }
        const data = await response.json();

        // Convert backend file list to Asset format and filter by Research tag
        const fetchedAssets: Asset[] = data.files
          .filter((file: any) => {
            const tags = file.tags || [];
            return tags.includes("Research");
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
              status: "ready" as Asset["status"],
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

  // Filter assets by search query
  const filteredAssets = assets.filter((asset) =>
    asset.name.toLowerCase().includes(searchQuery.toLowerCase())
  );

  return (
    <div className="flex h-full relative">
      {/* MIDDLE: Research Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="flex items-center justify-between p-6 border-b border-zinc-800">
          <h1 className="text-2xl font-bold text-zinc-100">Research Lab</h1>
          <div className="flex items-center gap-3">
            {/* Search Bar */}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-zinc-500" />
              <input
                type="text"
                placeholder="Search research files..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="pl-10 pr-4 py-2 bg-zinc-900/50 border border-zinc-800 rounded-lg text-zinc-100 placeholder:text-zinc-500 focus:outline-none focus:ring-2 focus:ring-amber-500/50 w-64"
              />
            </div>
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
            {/* THE "HIDE" BUTTON - Only visible if chat is open */}
            {isChatOpen && (
              <button
                onClick={() => setIsChatOpen(false)}
                className="flex items-center gap-2 bg-amber-500/10 text-amber-500 border border-amber-500/50 hover:bg-amber-500/20 px-4 py-2 rounded-md transition-colors"
              >
                <Sparkles className="w-4 h-4" />
                <span>Hide Riley</span>
              </button>
            )}
          </div>
        </header>

        {/* Scrollable Content Area */}
        <div className="flex-1 overflow-y-auto p-6">
          {/* Key Insights Section */}
          <div className="mb-8">
            <h2 className="text-lg font-semibold text-zinc-100 mb-4">Key Insights</h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {mockInsights.map((insight) => {
                const Icon = insight.icon;
                return (
                  <div
                    key={insight.id}
                    className={cn(
                      "rounded-xl border backdrop-blur-md p-6 hover:border-opacity-50 transition-colors",
                      getInsightColor(insight.type)
                    )}
                  >
                    <div className="flex items-start gap-3 mb-3">
                      <div className="flex-shrink-0 p-2 rounded-lg bg-zinc-900/50">
                        <Icon className="h-5 w-5 text-zinc-300" />
                      </div>
                      <div className="flex-1">
                        <h3 className="text-base font-semibold text-zinc-100 mb-1">
                          {insight.title}
                        </h3>
                        <p className="text-sm text-zinc-400 leading-relaxed">
                          {insight.description}
                        </p>
                      </div>
                    </div>
                    <div className="mt-4 pt-4 border-t border-zinc-800/50">
                      <span className="inline-flex items-center rounded-full bg-zinc-800/50 px-2.5 py-1 text-xs font-medium text-zinc-400 capitalize">
                        {insight.type}
                      </span>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Source Material Section */}
          <div>
            <h2 className="text-lg font-semibold text-zinc-100 mb-4">Source Material</h2>
            {isLoading ? (
              <div className="flex items-center justify-center py-12">
                <p className="text-zinc-500">Loading files...</p>
              </div>
            ) : filteredAssets.length === 0 ? (
              <div className="flex items-center justify-center py-12">
                <p className="text-zinc-500">
                  {searchQuery ? "No files match your search." : "No files tagged with Research yet."}
                </p>
              </div>
            ) : (
              <div className="space-y-2">
                {filteredAssets.map((asset) => {
                  const { icon: FileIcon, color: iconColor } = getFileIcon(asset.type);
                  return (
                    <button
                      key={asset.id}
                      onClick={() => setSelectedFile(asset)}
                      className="w-full flex items-center gap-4 p-4 rounded-lg border border-zinc-800 bg-zinc-900/30 hover:bg-zinc-900/50 hover:border-zinc-700 transition-colors text-left"
                    >
                      <FileIcon className={cn("h-5 w-5 flex-shrink-0", iconColor)} />
                      <div className="flex-1 min-w-0">
                        <p className="text-sm font-medium text-zinc-100 truncate">{asset.name}</p>
                        <p className="text-xs text-zinc-500 mt-1">
                          {asset.size} • {asset.uploader} • {new Date(asset.uploadDate).toLocaleDateString()}
                        </p>
                      </div>
                    </button>
                  );
                })}
              </div>
            )}
          </div>
        </div>
      </div>

      {/* RIGHT: Riley Chat - Conditionally Rendered */}
      {isChatOpen && (
        <div className="w-[400px] border-l border-zinc-800 bg-zinc-900/50 backdrop-blur-sm flex flex-col h-full shrink-0">
          {/* Header with CLOSE Button */}
          <div className="h-16 flex items-center justify-between px-4 border-b border-zinc-800">
            <span className="font-semibold text-amber-500 flex items-center gap-2">
              <Sparkles className="w-4 h-4" /> {getRileyTitle("research")}
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
              mode="research" 
              contextKey="research" 
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
    </div>
  );
}
