from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.dependencies.auth import check_tenant_membership, verify_clerk_token
from app.dependencies.graph_dep import get_graph
from app.services.comparisons import generate_comparison_table
from app.services.graph import GraphService

router = APIRouter()


class ComparisonGroupFilter(BaseModel):
    themes_any: List[str] = Field(default_factory=list)
    tone_labels_any: List[str] = Field(default_factory=list)
    framing_labels_any: List[str] = Field(default_factory=list)
    sentiment_any: List[str] = Field(default_factory=list)
    filename_contains: Optional[str] = None


class ComparisonGroupRequest(BaseModel):
    key: Optional[str] = None
    label: str
    document_ids: List[str] = Field(default_factory=list)
    document_filenames: List[str] = Field(default_factory=list)
    filters: Optional[ComparisonGroupFilter] = None


class ComparisonTableRequest(BaseModel):
    tenant_id: str = Field(..., max_length=50)
    mode: Literal["two_documents", "selected_sets", "campaign_filtered_sets"] = "selected_sets"
    groups: List[ComparisonGroupRequest] = Field(default_factory=list)


class ComparisonColumnResponse(BaseModel):
    key: str
    label: str
    document_ids: List[str] = Field(default_factory=list)
    document_names: List[str] = Field(default_factory=list)
    doc_count: int
    filters: Dict[str, Any] = Field(default_factory=dict)


class ComparisonCellResponse(BaseModel):
    column_key: str
    value: str
    supporting_document_ids: List[str] = Field(default_factory=list)
    supporting_evidence_refs: List[str] = Field(default_factory=list)


class ComparisonRowResponse(BaseModel):
    row_key: str
    label: str
    cells: List[ComparisonCellResponse] = Field(default_factory=list)


class ComparisonGroundingResponse(BaseModel):
    documents_considered: int
    campaign_snapshot_version: int
    campaign_snapshot_id: Optional[str] = None


class ComparisonTableResponse(BaseModel):
    comparison_id: str
    tenant_id: str
    generated_at: str
    mode: str
    columns: List[ComparisonColumnResponse] = Field(default_factory=list)
    rows: List[ComparisonRowResponse] = Field(default_factory=list)
    grounding: ComparisonGroundingResponse


@router.post("/riley/comparisons", response_model=ComparisonTableResponse)
async def create_comparison_table(
    request: ComparisonTableRequest,
    http_request: Request,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> ComparisonTableResponse:
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id, http_request)
    try:
        result = await generate_comparison_table(
            tenant_id=request.tenant_id,
            mode=request.mode,
            groups=[group.model_dump() for group in request.groups],
            graph=graph,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to generate comparison table: {type(exc).__name__}: {exc}",
        ) from exc
    return ComparisonTableResponse(**result)
