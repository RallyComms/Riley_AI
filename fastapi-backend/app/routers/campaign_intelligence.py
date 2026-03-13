import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from app.dependencies.auth import check_tenant_membership, verify_clerk_token
from app.dependencies.graph_dep import get_graph
from app.services.campaign_intelligence import enqueue_campaign_intelligence_job
from app.services.graph import GraphService

router = APIRouter()


class CampaignThemeCluster(BaseModel):
    theme: str
    doc_count: int
    share_of_docs: float
    sample_documents: List[str] = Field(default_factory=list)


class RileyCampaignIntelligenceResponse(BaseModel):
    snapshot_id: str
    tenant_id: str
    version: int
    created_at: str
    campaign_theme_clusters: List[CampaignThemeCluster] = Field(default_factory=list)
    dominant_narratives: List[str] = Field(default_factory=list)
    key_actors_entities: List[str] = Field(default_factory=list)
    sentiment_distribution: Dict[str, int] = Field(default_factory=dict)
    tone_distribution: Dict[str, int] = Field(default_factory=dict)
    framing_distribution: Dict[str, int] = Field(default_factory=dict)
    campaign_contradictions: List[str] = Field(default_factory=list)
    contradiction_tensions: List[Dict[str, Any]] = Field(default_factory=list)
    strategic_opportunities: List[str] = Field(default_factory=list)
    strategic_risks: List[str] = Field(default_factory=list)
    evidence_snippets: List[str] = Field(default_factory=list)
    docs_total: int
    docs_analyzed: int
    docs_failed: int
    partial_recompute: bool
    doc_intel_coverage_ratio: float = 0.0
    input_completeness_status: str = "unknown"
    input_completeness_note: str = ""
    doc_intel_full_fidelity_docs: int = 0
    doc_intel_degraded_docs: int = 0
    doc_intel_degraded_ratio: float = 0.0
    input_quality_status: str = "unknown"
    input_quality_note: str = ""


class RileyCampaignIntelligenceRefreshResponse(BaseModel):
    job_id: str
    status: str
    trigger_source: str
    created_at: Optional[str] = None


def _safe_json_loads(raw: Any, default: Any) -> Any:
    if not isinstance(raw, str) or not raw.strip():
        return default
    try:
        return json.loads(raw)
    except Exception:
        return default


@router.post(
    "/riley/campaign-intelligence/refresh",
    response_model=RileyCampaignIntelligenceRefreshResponse,
)
async def refresh_campaign_intelligence(
    http_request: Request,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    graph: GraphService = Depends(get_graph),
    current_user: Dict = Depends(verify_clerk_token),
) -> RileyCampaignIntelligenceRefreshResponse:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)
    job = await enqueue_campaign_intelligence_job(
        graph=graph,
        tenant_id=tenant_id,
        requested_by_user_id=user_id,
        trigger_source="manual_refresh",
    )
    return RileyCampaignIntelligenceRefreshResponse(
        job_id=str(job.get("job_id")),
        status=str(job.get("status") or "queued"),
        trigger_source=str(job.get("trigger_source") or "manual_refresh"),
        created_at=job.get("created_at"),
    )


@router.get(
    "/riley/campaign-intelligence",
    response_model=RileyCampaignIntelligenceResponse,
)
async def get_campaign_intelligence(
    http_request: Request,
    tenant_id: str = Query(..., description="Tenant/client identifier for scope isolation"),
    graph: GraphService = Depends(get_graph),
    current_user: Dict = Depends(verify_clerk_token),
) -> RileyCampaignIntelligenceResponse:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, tenant_id, http_request)
    snapshot = await graph.get_latest_riley_campaign_intelligence_snapshot(tenant_id=tenant_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Campaign intelligence snapshot not found")

    return RileyCampaignIntelligenceResponse(
        snapshot_id=str(snapshot.get("snapshot_id")),
        tenant_id=str(snapshot.get("tenant_id")),
        version=int(snapshot.get("version") or 1),
        created_at=str(snapshot.get("created_at") or ""),
        campaign_theme_clusters=_safe_json_loads(snapshot.get("campaign_theme_clusters_json"), []),
        dominant_narratives=list(snapshot.get("dominant_narratives") or []),
        key_actors_entities=list(snapshot.get("key_actors_entities") or []),
        sentiment_distribution=_safe_json_loads(snapshot.get("sentiment_distribution_json"), {}),
        tone_distribution=_safe_json_loads(snapshot.get("tone_distribution_json"), {}),
        framing_distribution=_safe_json_loads(snapshot.get("framing_distribution_json"), {}),
        campaign_contradictions=list(snapshot.get("campaign_contradictions") or []),
        contradiction_tensions=_safe_json_loads(snapshot.get("contradiction_tensions_json"), []),
        strategic_opportunities=list(snapshot.get("strategic_opportunities") or []),
        strategic_risks=list(snapshot.get("strategic_risks") or []),
        evidence_snippets=list(snapshot.get("evidence_snippets") or []),
        docs_total=int(snapshot.get("docs_total") or 0),
        docs_analyzed=int(snapshot.get("docs_analyzed") or 0),
        docs_failed=int(snapshot.get("docs_failed") or 0),
        partial_recompute=bool(snapshot.get("partial_recompute")),
        doc_intel_coverage_ratio=float(snapshot.get("doc_intel_coverage_ratio") or 0.0),
        input_completeness_status=str(snapshot.get("input_completeness_status") or "unknown"),
        input_completeness_note=str(snapshot.get("input_completeness_note") or ""),
        doc_intel_full_fidelity_docs=int(snapshot.get("doc_intel_full_fidelity_docs") or 0),
        doc_intel_degraded_docs=int(snapshot.get("doc_intel_degraded_docs") or 0),
        doc_intel_degraded_ratio=float(snapshot.get("doc_intel_degraded_ratio") or 0.0),
        input_quality_status=str(snapshot.get("input_quality_status") or "unknown"),
        input_quality_note=str(snapshot.get("input_quality_note") or ""),
    )
