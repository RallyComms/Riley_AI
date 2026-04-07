import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, UploadFile, Form, Query, Depends, Request, Header
from pydantic import BaseModel
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

from app.core.config import get_settings
from app.dependencies.auth import verify_clerk_token, verify_tenant_access, check_tenant_membership
from app.dependencies.graph_dep import get_graph
from app.services.ocr import run_ocr, is_image_ext
from app.services.graph import GraphService
from app.services.pricing_registry import estimate_single_unit_cost, estimate_storage_cost
from app.services.qdrant import vector_service
from app.services.ingestion import (
    process_upload,
    delete_file,
    untag_file,
    _generate_embedding,
    reindex_existing_file,
)
from app.services.storage import StorageService
from qdrant_client.http.models import PointStruct

logger = logging.getLogger(__name__)


router = APIRouter()


class FileItem(BaseModel):
    id: str
    filename: str
    type: str
    year: int | None = None


class FilesResponse(BaseModel):
    files: List[FileItem]


class UploadResponse(BaseModel):
    id: str
    url: str
    filename: str
    type: str


class FileListItem(BaseModel):
    name: str
    url: str
    type: str
    date: str
    size: str
    tags: List[str] = []
    id: str  # Qdrant point ID (required)
    ai_enabled: Optional[bool] = None
    ocr_enabled: Optional[bool] = None
    ocr_status: Optional[str] = None
    vision_enabled: Optional[bool] = None
    vision_status: Optional[str] = None
    multimodal_status: Optional[str] = None
    ocr_processed: Optional[bool] = None
    vision_processed: Optional[bool] = None
    ocr_confidence: Optional[float] = None
    ocr_extracted_at: Optional[str] = None
    preview_url: Optional[str] = None
    preview_type: Optional[str] = None
    preview_status: Optional[str] = None
    preview_error: Optional[str] = None
    status: Optional[str] = None
    assignee: Optional[str] = None
    assigned_to: Optional[List[str]] = None
    ingestion_status: Optional[str] = None
    extracted_char_count: Optional[int] = None
    chunk_count: Optional[int] = None
    is_golden: Optional[bool] = None
    client_id: Optional[str] = None
    source_campaign_id: Optional[str] = None
    origin_campaign_name: Optional[str] = None


class FileListResponse(BaseModel):
    files: List[FileListItem]


class FirmDocumentsAuditResponse(BaseModel):
    total_scanned: int
    valid_records: int
    blocked_records: int
    blocked_by_reason: Dict[str, int]
    safe_delete_candidate_count: int
    safe_delete_candidate_ids: List[str]
    blocked_examples: List[Dict[str, Any]]


class RenameRequest(BaseModel):
    old_name: str
    new_name: str


class RenameResponse(BaseModel):
    status: str
    new_url: str
    new_name: str


class UntagRequest(BaseModel):
    tag: str


class UpdateTagsRequest(BaseModel):
    tags: List[str]


class AssignRequest(BaseModel):
    assignee: str
    status: str  # "needs_review" or "in_review"


class FileCommentItem(BaseModel):
    id: str
    author_user_id: str
    author_display_name: str
    content: str
    mentions: List[str] = []
    mention_display_names: List[str] = []
    created_at: str


class FileCommentsResponse(BaseModel):
    comments: List[FileCommentItem]


class CreateFileCommentRequest(BaseModel):
    content: str
    mentions: List[str] = []


class PromoteRequest(BaseModel):
    is_golden: bool


class AIEnabledRequest(BaseModel):
    ai_enabled: bool


class DeleteResponse(BaseModel):
    status: str
    message: str


class FirmArchiveRemoveRequest(BaseModel):
    mode: Literal["archive_only", "delete_everywhere"]


async def _get_file_point_for_tenant(
    *,
    file_id: str,
    tenant_id: str,
    collection_name: Optional[str] = None,
) -> tuple[Any, Dict[str, Any], str]:
    """Load a file point and enforce tenant/file ownership before action."""
    settings = get_settings()
    resolved_collection = collection_name or (
        settings.QDRANT_COLLECTION_TIER_1 if tenant_id == "global" else settings.QDRANT_COLLECTION_TIER_2
    )
    points = await vector_service.client.retrieve(
        collection_name=resolved_collection,
        ids=[file_id],
        with_payload=True,
        with_vectors=False,
    )
    if not points:
        raise HTTPException(status_code=404, detail=f"File {file_id} not found")

    point = points[0]
    payload = point.payload or {}

    if tenant_id == "global":
        if not bool(payload.get("is_global")):
            raise HTTPException(
                status_code=403,
                detail="File is not in the global archive for this tenant scope.",
            )
    else:
        payload_tenant_id = str(payload.get("client_id") or "").strip()
        if payload_tenant_id != tenant_id:
            raise HTTPException(
                status_code=403,
                detail="File does not belong to the provided campaign.",
            )

    return point, payload, resolved_collection


def _normalize_user_id(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_optional_string(value: Any) -> str:
    return str(value or "").strip()


def _looks_invalid_filename(filename: str) -> bool:
    lowered = filename.strip().lower()
    if not lowered:
        return True
    return lowered in {"unknown", "untitled", "none", "null", "n/a", "na"}


def _resolve_origin_campaign_id(file_data: Dict[str, Any]) -> str:
    for key in ("source_campaign_id", "client_id", "tenant_id"):
        candidate = _normalize_optional_string(file_data.get(key))
        lowered = candidate.lower()
        if candidate and lowered not in {"unknown", "none", "null", "global"}:
            return candidate
    return ""


def _has_usable_file_reference(file_data: Dict[str, Any]) -> bool:
    url = _normalize_optional_string(file_data.get("url"))
    preview_url = _normalize_optional_string(file_data.get("preview_url"))
    return bool(url or preview_url)


def _classify_global_record(file_data: Dict[str, Any]) -> List[str]:
    reasons: List[str] = []
    filename = _normalize_optional_string(file_data.get("filename"))
    origin_campaign_id = _resolve_origin_campaign_id(file_data)
    if _looks_invalid_filename(filename):
        reasons.append("missing_filename")
    if not origin_campaign_id:
        reasons.append("missing_origin_campaign")
    if not _has_usable_file_reference(file_data):
        reasons.append("missing_file_reference")
    return reasons


def _is_safe_orphan_delete_candidate(reasons: List[str]) -> bool:
    # Conservative delete candidate: no filename, no origin, and no usable URL/preview.
    return {
        "missing_filename",
        "missing_origin_campaign",
        "missing_file_reference",
    }.issubset(set(reasons))


def _normalize_user_id_list(values: Any) -> List[str]:
    if not isinstance(values, list):
        return []
    unique_ids: List[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = _normalize_user_id(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique_ids.append(normalized)
    return unique_ids


def _is_messaging_visible_to_user(payload: Dict[str, Any], user_id: str) -> bool:
    tags = payload.get("tags") if isinstance(payload.get("tags"), list) else []
    if "Messaging" not in tags:
        return False

    normalized_user_id = _normalize_user_id(user_id)
    if not normalized_user_id:
        return False

    creator_id = _normalize_user_id(payload.get("messaging_created_by_user_id"))
    if creator_id and creator_id == normalized_user_id:
        return True

    visible_user_ids = _normalize_user_id_list(payload.get("messaging_visible_user_ids"))
    if normalized_user_id in set(visible_user_ids):
        return True

    assignee = _normalize_user_id(payload.get("assignee"))
    if assignee and assignee == normalized_user_id:
        return True

    assigned_to = _normalize_user_id_list(payload.get("assigned_to"))
    if normalized_user_id in set(assigned_to):
        return True

    return False


def _resolve_display_name_from_member(member: Dict[str, Any]) -> str:
    display_name = str(member.get("display_name") or "").strip()
    if display_name:
        return display_name
    email = str(member.get("email") or "").strip()
    if email and "@" in email:
        return email.split("@")[0]
    if email:
        return email
    return str(member.get("user_id") or member.get("id") or "Unknown User").strip()


async def _get_campaign_member_display_map(
    *,
    graph: GraphService,
    campaign_id: str,
) -> Dict[str, str]:
    members = await graph.list_campaign_members_for_mentions(campaign_id=campaign_id)
    mapping: Dict[str, str] = {}
    for member in members:
        member_id = _normalize_user_id(member.get("user_id") or member.get("id"))
        if not member_id:
            continue
        mapping[member_id] = _resolve_display_name_from_member(member)
    return mapping


def _normalize_comments_payload(comments_raw: Any) -> List[Dict[str, Any]]:
    if not isinstance(comments_raw, list):
        return []
    normalized: List[Dict[str, Any]] = []
    for item in comments_raw:
        if not isinstance(item, dict):
            continue
        comment_id = str(item.get("id") or "").strip()
        author_user_id = _normalize_user_id(item.get("author_user_id"))
        content = str(item.get("content") or "").strip()
        created_at = str(item.get("created_at") or "").strip() or datetime.now().isoformat()
        if not comment_id or not author_user_id or not content:
            continue
        mentions = _normalize_user_id_list(item.get("mentions"))
        normalized.append(
            {
                "id": comment_id,
                "author_user_id": author_user_id,
                "author_display_name": str(item.get("author_display_name") or "").strip(),
                "content": content,
                "mentions": mentions,
                "created_at": created_at,
            }
        )
    return normalized


class PreviewUrlResponse(BaseModel):
    file_id: str
    preview_url: str
    preview_type: str = "pdf"
    ttl_seconds: Optional[int] = None


@router.get("/files/{tenant_id}", response_model=FilesResponse)
async def get_tenant_files(
    tenant_id: str,
    current_user: Dict = Depends(verify_tenant_access)
) -> FilesResponse:
    """Get list of files for a specific tenant from the vector database.
    
    SECURITY: Tenant membership is enforced via verify_tenant_access dependency.
    """
    settings = get_settings()

    try:
        files = await vector_service.list_tenant_files(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            tenant_id=tenant_id,
            limit=20,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Failed to retrieve files: {exc}"
        ) from exc

    # Convert to response model
    file_items = [
        FileItem(
            id=file.get("id", ""),
            filename=file.get("filename", "Unknown"),
            type=file.get("type", ""),
            year=file.get("year"),
        )
        for file in files
    ]

    return FilesResponse(files=file_items)


@router.get("/list", response_model=FileListResponse)
async def list_files(
    tenant_id: str = Query(..., description="Tenant/client ID to filter files, or 'global' for firm archive"),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> FileListResponse:
    """
    List files from Qdrant filtered by tenant_id (source of truth).
    
    Special case: If tenant_id == "global", returns files from the Firm Archive (Tier 1)
    that have been explicitly promoted with is_global=True flag.
    
    Returns a list of file objects with metadata including name, URL, type, date, size, and tags.
    Files are retrieved from Qdrant with strict tenant isolation.
    File URLs point to GCS public URLs.
    
    SECURITY: This endpoint enforces tenant isolation. Only files belonging to the specified tenant_id are returned.
    For global requests, only files with is_global=True are returned (ignoring client_id filters).
    """
    # Log user_id + tenant_id for every request
    user_id = current_user.get("id", "unknown")
    print(f"List files request: user_id={user_id}, tenant_id={tenant_id}")
    
    settings = get_settings()
    
    # Enforce tenant_id requirement
    if not tenant_id or not tenant_id.strip():
        raise HTTPException(
            status_code=400,
            detail="tenant_id is required for file listing. Cannot perform unfiltered query."
        )
    
    files = []
    
    try:
        # Special handling for global archive
        if tenant_id == "global":
            # Query Tier 1 collection for global files
            global_files = await vector_service.list_global_files(
                collection_name=settings.QDRANT_COLLECTION_TIER_1,
                limit=1000,
            )
            origin_campaign_ids = sorted(
                {
                    _resolve_origin_campaign_id(file_data)
                    for file_data in global_files
                    if _resolve_origin_campaign_id(file_data)
                }
            )
            campaign_name_map = await graph.get_campaign_names_by_ids(origin_campaign_ids)
            
            # Convert to FileListItem format
            for file_data in global_files:
                reasons = _classify_global_record(file_data)
                if reasons:
                    continue
                filename = _normalize_optional_string(file_data.get("filename"))
                if not filename:
                    continue
                origin_campaign_id = _resolve_origin_campaign_id(file_data)
                
                # Determine file type from extension or payload
                extension = Path(filename).suffix.lower().lstrip(".")
                file_type = file_data.get("type") or extension or "unknown"
                
                # Ensure date is always a string
                promoted_at = file_data.get("promoted_at")
                date = promoted_at if isinstance(promoted_at, str) and promoted_at else datetime.now().isoformat()
                
                files.append(FileListItem(
                    id=file_data.get("id", ""),
                    name=filename,
                    url=file_data.get("url", ""),
                    type=file_type,
                    date=date,
                    size="Unknown",  # Global files may not have size
                    tags=[],  # Global files don't have tags
                    is_golden=bool(file_data.get("is_golden", False)),
                    client_id=origin_campaign_id,
                    source_campaign_id=origin_campaign_id,
                    origin_campaign_name=campaign_name_map.get(origin_campaign_id) or origin_campaign_id,
                    ai_enabled=file_data.get("ai_enabled"),
                    ocr_enabled=file_data.get("ocr_enabled"),
                    ocr_status=file_data.get("ocr_status"),
                    vision_enabled=file_data.get("vision_enabled"),
                    vision_status=file_data.get("vision_status"),
                    multimodal_status=file_data.get("multimodal_status"),
                    ocr_processed=file_data.get("ocr_processed"),
                    vision_processed=file_data.get("vision_processed"),
                    ocr_confidence=file_data.get("ocr_confidence"),
                    ocr_extracted_at=file_data.get("ocr_extracted_at"),
                    preview_url=file_data.get("preview_url"),
                    preview_type=file_data.get("preview_type"),
                    preview_status=file_data.get("preview_status"),
                    preview_error=file_data.get("preview_error"),
                    ingestion_status=file_data.get("ingestion_status"),
                    extracted_char_count=file_data.get("extracted_char_count"),
                    chunk_count=file_data.get("chunk_count"),
                ))
        else:
            tenant_files = await vector_service.list_tenant_files(
                collection_name=settings.QDRANT_COLLECTION_TIER_2,
                tenant_id=tenant_id,
                limit=1000,
            )
            for file_data in tenant_files:
                filename = file_data.get("filename")
                if not filename:
                    continue
                extension = Path(filename).suffix.lower().lstrip(".")
                file_type = file_data.get("type") or extension or "unknown"
                files.append(FileListItem(
                    id=file_data.get("id", ""),
                    name=filename,
                    url=file_data.get("url", ""),
                    type=file_type,
                    date=file_data.get("upload_date", datetime.now().isoformat()),
                    size=file_data.get("size", "Unknown"),
                    tags=file_data.get("tags", []),
                    ai_enabled=file_data.get("ai_enabled"),
                    ocr_enabled=file_data.get("ocr_enabled"),
                    ocr_status=file_data.get("ocr_status"),
                    vision_enabled=file_data.get("vision_enabled"),
                    vision_status=file_data.get("vision_status"),
                    multimodal_status=file_data.get("multimodal_status"),
                    ocr_processed=file_data.get("ocr_processed"),
                    vision_processed=file_data.get("vision_processed"),
                    ocr_confidence=file_data.get("ocr_confidence"),
                    ocr_extracted_at=file_data.get("ocr_extracted_at"),
                    preview_url=file_data.get("preview_url"),
                    preview_type=file_data.get("preview_type"),
                    preview_status=file_data.get("preview_status"),
                    preview_error=file_data.get("preview_error"),
                    ingestion_status=file_data.get("ingestion_status"),
                    extracted_char_count=file_data.get("extracted_char_count"),
                    chunk_count=file_data.get("chunk_count"),
                ))
        
        # Sort by date (newest first)
        files.sort(key=lambda x: x.date, reverse=True)
        
    except Exception as exc:
        # If Qdrant query fails, raise HTTPException instead of returning empty list
        logger.error(f"Error fetching files from Qdrant: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching files from Qdrant: {exc}"
        ) from exc
    
    return FileListResponse(files=files)


@router.post("/files/{file_id}/firm-archive/remove", response_model=DeleteResponse)
async def remove_or_delete_firm_archive_file(
    file_id: str,
    request: FirmArchiveRemoveRequest,
    tenant_id: str = Query("global", description="Must be global for Firm Archive actions."),
    current_user: Dict = Depends(verify_tenant_access),
) -> DeleteResponse:
    """Remove a file from the Firm Archive only, or delete it everywhere."""
    if tenant_id != "global":
        raise HTTPException(status_code=400, detail="tenant_id must be 'global' for firm archive removal actions.")

    settings = get_settings()
    await _get_file_point_for_tenant(
        file_id=file_id,
        tenant_id="global",
        collection_name=settings.QDRANT_COLLECTION_TIER_1,
    )

    removed_from_global = await vector_service.remove_from_global_archive(file_id)
    if not removed_from_global:
        raise HTTPException(status_code=404, detail=f"File {file_id} is not currently promoted in Firm Archive.")

    if request.mode == "archive_only":
        return DeleteResponse(
            status="success",
            message=f"File {file_id} removed from Firm Archive only. Source campaign document was kept.",
        )

    # delete_everywhere mode: remove source campaign file and underlying storage.
    try:
        await delete_file(
            file_id=file_id,
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
        )
        source_deletion_note = "Source campaign document and storage were deleted."
    except HTTPException as exc:
        if exc.status_code != 404:
            raise
        source_deletion_note = "Global archive copy removed. Source campaign copy was already missing."

    return DeleteResponse(
        status="success",
        message=(
            f"File {file_id} deleted everywhere. Removed from Firm Archive. {source_deletion_note}"
        ),
    )


@router.get("/files/global/audit", response_model=FirmDocumentsAuditResponse)
async def audit_firm_documents(
    tenant_id: str = Query("global", description="Must be global for Firm Documents audit."),
    current_user: Dict = Depends(verify_tenant_access),
) -> FirmDocumentsAuditResponse:
    """Audit global Firm Documents records and classify invalid/orphaned rows."""
    if tenant_id != "global":
        raise HTTPException(status_code=400, detail="tenant_id must be 'global' for this audit endpoint.")

    settings = get_settings()
    global_files = await vector_service.list_global_files(
        collection_name=settings.QDRANT_COLLECTION_TIER_1,
        limit=5000,
    )

    blocked_by_reason: Dict[str, int] = {}
    blocked_examples: List[Dict[str, Any]] = []
    valid_records = 0
    safe_delete_candidate_ids: List[str] = []

    for file_data in global_files:
        reasons = _classify_global_record(file_data)
        if not reasons:
            valid_records += 1
            continue

        for reason in reasons:
            blocked_by_reason[reason] = blocked_by_reason.get(reason, 0) + 1

        if _is_safe_orphan_delete_candidate(reasons):
            safe_delete_candidate_ids.append(str(file_data.get("id") or ""))

        if len(blocked_examples) < 50:
            blocked_examples.append(
                {
                    "id": str(file_data.get("id") or ""),
                    "filename": _normalize_optional_string(file_data.get("filename")) or None,
                    "origin_campaign_id": _resolve_origin_campaign_id(file_data) or None,
                    "url_present": bool(_normalize_optional_string(file_data.get("url"))),
                    "preview_url_present": bool(_normalize_optional_string(file_data.get("preview_url"))),
                    "reasons": reasons,
                }
            )

    total_scanned = len(global_files)
    blocked_records = total_scanned - valid_records
    safe_delete_candidate_ids = [item for item in safe_delete_candidate_ids if item]

    return FirmDocumentsAuditResponse(
        total_scanned=total_scanned,
        valid_records=valid_records,
        blocked_records=blocked_records,
        blocked_by_reason=blocked_by_reason,
        safe_delete_candidate_count=len(safe_delete_candidate_ids),
        safe_delete_candidate_ids=safe_delete_candidate_ids,
        blocked_examples=blocked_examples,
    )


@router.get("/files/messaging/list", response_model=FileListResponse)
async def list_messaging_files(
    tenant_id: str = Query(..., description="Tenant/client ID for campaign Messaging Studio"),
    current_user: Dict = Depends(verify_tenant_access),
) -> FileListResponse:
    """List Messaging Studio files visible to the current user only."""
    settings = get_settings()
    if tenant_id == "global":
        raise HTTPException(status_code=400, detail="Messaging Studio is campaign-scoped.")
    user_id = _normalize_user_id(current_user.get("id"))
    if not user_id:
        raise HTTPException(status_code=401, detail="Authenticated user id is required.")

    try:
        tenant_files = await vector_service.list_tenant_files(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            tenant_id=tenant_id,
            limit=1000,
        )
        files: List[FileListItem] = []
        for file_data in tenant_files:
            if not _is_messaging_visible_to_user(file_data, user_id):
                continue
            filename = file_data.get("filename")
            if not filename:
                continue
            extension = Path(filename).suffix.lower().lstrip(".")
            file_type = file_data.get("type") or extension or "unknown"
            files.append(
                FileListItem(
                    id=file_data.get("id", ""),
                    name=filename,
                    url=file_data.get("url", ""),
                    type=file_type,
                    date=file_data.get("upload_date", datetime.now().isoformat()),
                    size=file_data.get("size", "Unknown"),
                    tags=file_data.get("tags", []),
                    status=file_data.get("status"),
                    assignee=file_data.get("assignee"),
                    assigned_to=file_data.get("assigned_to", []),
                    ai_enabled=file_data.get("ai_enabled"),
                    ocr_enabled=file_data.get("ocr_enabled"),
                    ocr_status=file_data.get("ocr_status"),
                    vision_enabled=file_data.get("vision_enabled"),
                    vision_status=file_data.get("vision_status"),
                    multimodal_status=file_data.get("multimodal_status"),
                    ocr_processed=file_data.get("ocr_processed"),
                    vision_processed=file_data.get("vision_processed"),
                    ocr_confidence=file_data.get("ocr_confidence"),
                    ocr_extracted_at=file_data.get("ocr_extracted_at"),
                    preview_url=file_data.get("preview_url"),
                    preview_type=file_data.get("preview_type"),
                    preview_status=file_data.get("preview_status"),
                    preview_error=file_data.get("preview_error"),
                    ingestion_status=file_data.get("ingestion_status"),
                    extracted_char_count=file_data.get("extracted_char_count"),
                    chunk_count=file_data.get("chunk_count"),
                )
            )
        files.sort(key=lambda x: x.date, reverse=True)
        return FileListResponse(files=files)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load Messaging Studio files: {exc}",
        ) from exc


@router.get("/files/{file_id}/comments", response_model=FileCommentsResponse)
async def list_file_comments(
    file_id: str,
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> FileCommentsResponse:
    """Return document comments with Riley-native display names and resolved mentions."""
    try:
        _, payload, _ = await _get_file_point_for_tenant(file_id=file_id, tenant_id=tenant_id)
        comments = _normalize_comments_payload(payload.get("comments"))
        display_map = await _get_campaign_member_display_map(graph=graph, campaign_id=tenant_id)
        items: List[FileCommentItem] = []
        for item in comments:
            author_id = _normalize_user_id(item.get("author_user_id"))
            mention_ids = _normalize_user_id_list(item.get("mentions"))
            author_display_name = (
                display_map.get(author_id)
                or str(item.get("author_display_name") or "").strip()
                or author_id
            )
            mention_display_names = [display_map.get(mention_id, mention_id) for mention_id in mention_ids]
            items.append(
                FileCommentItem(
                    id=str(item.get("id")),
                    author_user_id=author_id,
                    author_display_name=author_display_name,
                    content=str(item.get("content") or ""),
                    mentions=mention_ids,
                    mention_display_names=mention_display_names,
                    created_at=str(item.get("created_at") or datetime.now().isoformat()),
                )
            )
        items.sort(key=lambda c: c.created_at)
        return FileCommentsResponse(comments=items)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to list file comments: {exc}") from exc


@router.post("/files/{file_id}/comments", response_model=FileCommentsResponse)
async def create_file_comment(
    file_id: str,
    request: CreateFileCommentRequest,
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> FileCommentsResponse:
    """Create a document comment, persist mentions, and emit activity events for mentioned users."""
    content = str(request.content or "").strip()
    if not content:
        raise HTTPException(status_code=400, detail="Comment content is required.")

    author_user_id = _normalize_user_id(current_user.get("id"))
    if not author_user_id:
        raise HTTPException(status_code=401, detail="Authenticated user id is required.")

    try:
        _, payload, _ = await _get_file_point_for_tenant(file_id=file_id, tenant_id=tenant_id)
        display_map = await _get_campaign_member_display_map(graph=graph, campaign_id=tenant_id)
        member_ids = set(display_map.keys())
        requested_mentions = _normalize_user_id_list(request.mentions)
        mention_ids = [mentioned_id for mentioned_id in requested_mentions if mentioned_id in member_ids and mentioned_id != author_user_id]

        comments = _normalize_comments_payload(payload.get("comments"))
        new_comment = {
            "id": str(datetime.now().timestamp()).replace(".", ""),
            "author_user_id": author_user_id,
            "author_display_name": display_map.get(author_user_id, author_user_id),
            "content": content,
            "mentions": mention_ids,
            "created_at": datetime.now().isoformat(),
        }
        comments.append(new_comment)
        await vector_service.client.set_payload(
            collection_name=get_settings().QDRANT_COLLECTION_TIER_2,
            payload={"comments": comments},
            points=[file_id],
        )

        filename = str(payload.get("filename") or file_id)
        actor_display = display_map.get(author_user_id, author_user_id)
        for mentioned_user_id in mention_ids:
            mentioned_display = display_map.get(mentioned_user_id, mentioned_user_id)
            await graph.create_campaign_event(
                campaign_id=tenant_id,
                event_type="document_mentioned_user",
                message=f"{actor_display} mentioned {mentioned_display} in {filename}",
                user_id=mentioned_user_id,
                actor_user_id=author_user_id,
                object_id=f"{file_id}:{new_comment.get('id')}",
                metadata={"file_id": file_id, "comment_id": new_comment.get("id")},
            )

        items: List[FileCommentItem] = []
        for item in comments:
            author_id = _normalize_user_id(item.get("author_user_id"))
            item_mentions = _normalize_user_id_list(item.get("mentions"))
            items.append(
                FileCommentItem(
                    id=str(item.get("id")),
                    author_user_id=author_id,
                    author_display_name=display_map.get(author_id, str(item.get("author_display_name") or author_id)),
                    content=str(item.get("content") or ""),
                    mentions=item_mentions,
                    mention_display_names=[display_map.get(mention_id, mention_id) for mention_id in item_mentions],
                    created_at=str(item.get("created_at") or datetime.now().isoformat()),
                )
            )
        items.sort(key=lambda c: c.created_at)
        return FileCommentsResponse(comments=items)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create file comment: {exc}") from exc


@router.get("/files/{file_id}/preview-url", response_model=PreviewUrlResponse)
async def get_file_preview_url(
    file_id: str,
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file, or 'global'"),
    current_user: Dict = Depends(verify_tenant_access),
) -> PreviewUrlResponse:
    """
    Mint a fresh preview URL on demand for Office/HTML-generated PDF previews.

    This avoids relying on persisted expiring signed URLs across sessions.
    """
    settings = get_settings()
    collection_name = (
        settings.QDRANT_COLLECTION_TIER_1 if tenant_id == "global" else settings.QDRANT_COLLECTION_TIER_2
    )

    try:
        _, payload, _ = await _get_file_point_for_tenant(
            file_id=file_id,
            tenant_id=tenant_id,
            collection_name=collection_name,
        )
        preview_type = str(payload.get("preview_type") or "").strip().lower()
        preview_status = str(payload.get("preview_status") or "").strip().lower()
        if preview_type != "pdf" or preview_status != "complete":
            raise HTTPException(
                status_code=404,
                detail="Preview is not available for this file.",
            )

        preview_object_name = (
            str(payload.get("preview_object_name") or "").strip()
            or f"{settings.PREVIEW_BUCKET_PATH_PREFIX}/{file_id}.pdf"
        )

        ttl_seconds: Optional[int] = None
        if settings.SIGN_PREVIEW_URLS:
            # Keep signed previews usable for a normal review session.
            ttl_seconds = max(int(settings.PREVIEW_URL_TTL_SECONDS), 7200)
            preview_url = await StorageService.generate_signed_url(
                preview_object_name,
                ttl_seconds,
            )
        else:
            preview_url = str(payload.get("preview_url") or "").strip()
            if not preview_url:
                preview_url = (
                    f"https://storage.googleapis.com/{settings.GCS_BUCKET_NAME}/{preview_object_name}"
                )

        return PreviewUrlResponse(
            file_id=file_id,
            preview_url=preview_url,
            preview_type="pdf",
            ttl_seconds=ttl_seconds,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to mint preview URL: {exc}",
        ) from exc


@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile,
    request: Request,
    tenant_id: str = Form(...),
    tags: str = Form(""),  # Comma-separated tags
    overwrite: bool = Query(False, description="Overwrite existing file if it exists"),
    batch_size: Optional[int] = Query(None, description="Optional client-declared upload batch size"),
    upload_batch_size: Optional[int] = Header(None, alias="X-Upload-Batch-Size"),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> UploadResponse:
    """
    Upload a file, process it, and index it in Qdrant.
    
    The file is saved to the static directory, text is extracted (for PDFs),
    an embedding is generated, and the file is indexed in Qdrant.
    
    If a file with the same name exists and overwrite=False, returns 409 Conflict.
    """
    # Log user_id + tenant_id for every request
    user_id = current_user.get("id", "unknown")
    print(f"Upload file request: user_id={user_id}, tenant_id={tenant_id}")

    # SECURITY: verify_tenant_access cannot read multipart FormData tenant_id automatically.
    # Enforce membership explicitly using the parsed form tenant_id.
    await check_tenant_membership(user_id, tenant_id, request)
    
    settings = get_settings()

    # Server-side backpressure guard for current/future multi-file clients.
    declared_batch_size = 1
    if isinstance(batch_size, int):
        declared_batch_size = max(declared_batch_size, batch_size)
    if isinstance(upload_batch_size, int):
        declared_batch_size = max(declared_batch_size, upload_batch_size)

    if declared_batch_size > settings.MAX_UPLOAD_BATCH_SIZE:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Upload batch too large ({declared_batch_size}). "
                f"Maximum allowed is {settings.MAX_UPLOAD_BATCH_SIZE} files per batch."
            ),
        )

    # Hard server-side file size limit (prevents memory/OCR/ingestion failures)
    max_bytes = int(settings.MAX_UPLOAD_MB) * 1024 * 1024

    # If client provided size metadata, enforce immediately
    file_size = getattr(file, "size", None)
    if isinstance(file_size, int) and file_size > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=f"File exceeds maximum allowed size ({settings.MAX_UPLOAD_MB}MB). Please compress or split.",
        )

    # Always enforce by reading in chunks (covers missing/incorrect size metadata)
    total = 0
    try:
        while True:
            chunk = await file.read(1024 * 1024)  # 1MB chunks
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise HTTPException(
                    status_code=413,
                    detail=f"File exceeds maximum allowed size ({settings.MAX_UPLOAD_MB}MB). Please compress or split.",
                )
    finally:
        # Reset stream for downstream ingestion
        await file.seek(0)

    # Parse comma-separated tags
    tag_list = [tag.strip() for tag in tags.split(",") if tag.strip()] if tags else []
    
    try:
        result = await process_upload(
            file=file,
            tenant_id=tenant_id,
            tags=tag_list,
            overwrite=overwrite,
        )
        try:
            target_collection = (
                settings.QDRANT_COLLECTION_TIER_1
                if tenant_id == "global"
                else settings.QDRANT_COLLECTION_TIER_2
            )
            uploaded_id = str(result.get("id") or "")
            uploaded_records = await vector_service.client.retrieve(
                collection_name=target_collection,
                ids=[uploaded_id],
                with_payload=True,
                with_vectors=False,
            )
            uploaded_payload = (uploaded_records[0].payload or {}) if uploaded_records else {}
            bytes_stored = int(uploaded_payload.get("size_bytes") or 0)
            storage_cost = estimate_storage_cost(bytes_stored=bytes_stored, retention_days=30)
            await graph.append_analytics_event(
                event_id=f"storage_artifact_written:file_upload:{uploaded_id}:{datetime.now().isoformat()}",
                source_event_type_raw="storage_artifact_written",
                source_entity="FileUpload",
                campaign_id=tenant_id,
                user_id=user_id,
                actor_user_id=user_id,
                object_id=uploaded_id or None,
                status="succeeded",
                provider="gcs",
                model="standard_storage",
                cost_estimate_usd=(
                    float(storage_cost["cost_estimate_usd"])
                    if storage_cost.get("cost_estimate_usd") is not None
                    else None
                ),
                pricing_version=str(storage_cost.get("pricing_version") or "cost-accounting-v1"),
                cost_confidence=str(storage_cost.get("cost_confidence") or "estimated_units"),
                metadata={
                    "service": "storage_artifact",
                    "bytes_stored": bytes_stored,
                    "gb_month_assumed": round(bytes_stored / float(1024 ** 3), 9),
                    "retention_days_assumed": 30,
                    "requests_count": 1,
                },
            )
        except Exception:
            pass
        if str(result.get("preview_status") or "").strip().lower() == "failed":
            try:
                await graph.append_analytics_event(
                    event_id=f"preview_generation_failed:{result.get('id')}:{datetime.now().isoformat()}",
                    source_event_type_raw="preview_generation_failed",
                    source_entity="FileUpload",
                    campaign_id=tenant_id,
                    user_id=user_id,
                    actor_user_id=user_id,
                    object_id=str(result.get("id") or "").strip() or None,
                    status="failed",
                    metadata={
                        "filename": str(result.get("filename") or ""),
                        "preview_error": str(result.get("preview_error") or "")[:500] or None,
                    },
                )
            except Exception:
                pass
        return UploadResponse(**result)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {exc}"
        ) from exc


@router.post("/files/rename", response_model=RenameResponse)
async def rename_file(
    request: RenameRequest,
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access)
) -> RenameResponse:
    """
    Rename a file in GCS by updating the filename in Qdrant metadata.
    
    Note: This updates the filename in Qdrant but does not rename the actual file in GCS.
    For Beta phase, we update metadata only. Full rename would require copying blob in GCS.
    
    Validates that the extension is preserved to prevent file corruption.
    """
    settings = get_settings()
    
    # Validate extension preservation
    old_ext = Path(request.old_name).suffix.lower()
    new_ext = Path(request.new_name).suffix.lower()
    
    if old_ext != new_ext:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot change file extension. Original: {old_ext}, New: {new_ext}"
        )
    
    try:
        # Find file by old name in Qdrant
        tenant_filter = Filter(
            must=[
                FieldCondition(
                    key="record_type",
                    match=MatchValue(value="file")
                ),
                FieldCondition(
                    key="filename",
                    match=MatchValue(value=request.old_name)
                ),
                FieldCondition(
                    key="client_id",
                    match=MatchValue(value=tenant_id)
                )
            ]
        )
        
        scroll_result = await vector_service.client.scroll(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            scroll_filter=tenant_filter,
            limit=1,
            with_payload=True,
            with_vectors=False,
        )
        
        if not scroll_result[0]:
            raise HTTPException(
                status_code=404,
                detail=f"File '{request.old_name}' not found in Qdrant"
            )
        
        point = scroll_result[0][0]
        file_id = str(point.id)
        current_payload = point.payload or {}
        current_url = current_payload.get("url", "")
        
        # Update filename in Qdrant payload
        # Note: The GCS URL remains the same, only the display name changes
        await vector_service.client.set_payload(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            payload={"filename": request.new_name},
            points=[file_id]
        )
        
        return RenameResponse(
            status="success",
            new_url=current_url,  # GCS URL doesn't change
            new_name=request.new_name
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to rename file: {exc}"
        ) from exc


@router.delete("/files/{file_id}", response_model=DeleteResponse)
async def delete_file_endpoint(
    file_id: str,
    request: Request,
    current_user: Dict = Depends(verify_clerk_token)
) -> DeleteResponse:
    """
    Permanently delete a file from disk, Qdrant, and all workstreams.
    
    This is an irreversible action that removes the file completely.
    
    SECURITY: Tenant membership is enforced by retrieving the file's client_id
    from Tier 2 and verifying the user has access to that tenant.
    """
    settings = get_settings()
    user_id = current_user.get("id", "unknown")
    
    try:
        # Step 1: Retrieve the file point from Tier 2 to get tenant_id from payload
        points = await vector_service.client.retrieve(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            ids=[file_id],
            with_payload=True,
        )
        
        # Step 2: Check if file exists
        if not points:
            raise HTTPException(
                status_code=404,
                detail=f"File {file_id} not found"
            )
        
        # Step 3: Extract tenant_id from payload.client_id
        point = points[0]
        payload = point.payload or {}
        tenant_id = payload.get("client_id")
        
        if not tenant_id:
            raise HTTPException(
                status_code=400,
                detail=f"File {file_id} does not have a client_id in payload"
            )
        
        # Step 4: Verify tenant membership before deleting
        await check_tenant_membership(user_id, tenant_id, request)
        
        # Step 5: Delete the file
        await delete_file(
            file_id=file_id,
            collection_name=settings.QDRANT_COLLECTION_TIER_2
        )
        return DeleteResponse(
            status="success",
            message=f"File {file_id} deleted successfully"
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete file: {exc}"
        ) from exc


@router.patch("/files/{file_id}/untag", response_model=DeleteResponse)
async def untag_file_endpoint(
    file_id: str,
    request: UntagRequest,
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access)
) -> DeleteResponse:
    """
    Remove a specific tag from a file (local delete from a workstream).
    
    This removes the file from the current view (e.g., Messaging) by removing
    the tag, but keeps the file in the vault and other workstreams.
    
    If tag is "*" or "all", removes all tags from the file.
    """
    settings = get_settings()
    
    try:
        await _get_file_point_for_tenant(file_id=file_id, tenant_id=tenant_id)
        if request.tag == "*" or request.tag.lower() == "all":
            # Clear all tags
            await untag_file(
                file_id=file_id,
                tag=None,  # None means clear all
                collection_name=settings.QDRANT_COLLECTION_TIER_2
            )
            return DeleteResponse(
                status="success",
                message=f"All tags removed from file {file_id}"
            )
        else:
            await untag_file(
                file_id=file_id,
                tag=request.tag,
                collection_name=settings.QDRANT_COLLECTION_TIER_2
            )
            return DeleteResponse(
                status="success",
                message=f"Tag '{request.tag}' removed from file {file_id}"
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to untag file: {exc}"
        ) from exc


@router.put("/files/{file_id}/tags", response_model=DeleteResponse)
async def update_tags_endpoint(
    file_id: str,
    request: UpdateTagsRequest,
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access)
) -> DeleteResponse:
    """
    Update the tags for a file.
    
    This replaces the entire tags list with the provided tags array.
    """
    settings = get_settings()
    
    try:
        _, current_payload, _ = await _get_file_point_for_tenant(file_id=file_id, tenant_id=tenant_id)
        current_tags = current_payload.get("tags") if isinstance(current_payload.get("tags"), list) else []
        user_id = _normalize_user_id((current_user or {}).get("id"))
        messaging_visible_ids = _normalize_user_id_list(current_payload.get("messaging_visible_user_ids"))

        payload_update: Dict[str, Any] = {"tags": request.tags}
        if "Messaging" in request.tags:
            if "Messaging" not in current_tags and user_id:
                messaging_visible_ids.append(user_id)
            messaging_visible_ids = _normalize_user_id_list(messaging_visible_ids)
            payload_update["messaging_visible_user_ids"] = messaging_visible_ids
            if user_id and not _normalize_user_id(current_payload.get("messaging_created_by_user_id")):
                payload_update["messaging_created_by_user_id"] = user_id

        # Update tags in Qdrant
        await vector_service.client.set_payload(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            payload=payload_update,
            points=[file_id]
        )
        
        return DeleteResponse(
            status="success",
            message=f"Tags updated for file {file_id}"
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update tags: {exc}"
        ) from exc


@router.patch("/files/{file_id}/assign", response_model=DeleteResponse)
async def assign_file_endpoint(
    file_id: str,
    request: AssignRequest,
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> DeleteResponse:
    """
    Assign a file to a reviewer and update its status.
    
    This updates the file's status and adds the assignee to the file's metadata.
    """
    settings = get_settings()
    
    try:
        # Get current payload
        _, current_payload, _ = await _get_file_point_for_tenant(
            file_id=file_id,
            tenant_id=tenant_id,
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
        )
        actor_user_id = _normalize_user_id(current_user.get("id"))
        previous_assignee = _normalize_user_id(current_payload.get("assignee"))
        previous_status = str(current_payload.get("status") or "").strip().lower()
        assignee_value = _normalize_user_id(request.assignee)
        assigned_to = _normalize_user_id_list(current_payload.get("assigned_to"))
        if assignee_value:
            assigned_to = _normalize_user_id_list([*assigned_to, assignee_value])

        tags = current_payload.get("tags") if isinstance(current_payload.get("tags"), list) else []
        messaging_visible_ids = _normalize_user_id_list(current_payload.get("messaging_visible_user_ids"))
        if "Messaging" in tags and assignee_value:
            messaging_visible_ids = _normalize_user_id_list([*messaging_visible_ids, assignee_value])
        
        # Update payload with assignee and status
        updated_payload = {
            **current_payload,
            "status": request.status,
            "assignee": assignee_value or "",
            "assigned_to": assigned_to,
            "messaging_visible_user_ids": messaging_visible_ids,
        }
        
        # Update Qdrant payload
        await vector_service.client.set_payload(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            payload=updated_payload,
            points=[file_id]
        )

        # Best-effort campaign event for dashboard/feed realism.
        try:
            filename = str(current_payload.get("filename") or file_id)
            status_value = str(request.status).strip().lower()
            member_display_map = await _get_campaign_member_display_map(
                graph=graph,
                campaign_id=tenant_id,
            )
            actor_display = member_display_map.get(actor_user_id, actor_user_id or "A teammate")
            assignee_display = member_display_map.get(assignee_value, assignee_value)

            # Assignment/tagging notifications are targeted recent-activity events (not Riley Bot feed).
            if assignee_value and assignee_value != previous_assignee:
                targeted_event_type = (
                    "document_tagged_for_review"
                    if status_value == "needs_review"
                    else "document_assigned_to_user"
                )
                targeted_message = (
                    f"{actor_display} tagged {assignee_display} for review on {filename}"
                    if targeted_event_type == "document_tagged_for_review"
                    else f"{actor_display} assigned {filename} to {assignee_display}"
                )
                await graph.create_campaign_event(
                    campaign_id=tenant_id,
                    event_type=targeted_event_type,
                    message=targeted_message,
                    user_id=assignee_value,
                    actor_user_id=actor_user_id,
                    object_id=f"{file_id}:{targeted_event_type}:{assignee_value}",
                    metadata={"file_id": file_id, "status": status_value},
                )
            elif not assignee_value and status_value != previous_status:
                event_type = "document_assigned"
                event_message = f"Document assigned for review: {filename}"
                if status_value == "needs_review":
                    event_type = "document_moved_needs_review"
                    event_message = f"Document moved to Needs Review: {filename}"
                elif status_value == "in_review":
                    event_type = "document_moved_in_review"
                    event_message = f"Document moved to In Review: {filename}"
                await graph.create_campaign_event(
                    campaign_id=tenant_id,
                    event_type=event_type,
                    message=event_message,
                    actor_user_id=actor_user_id or current_user.get("id", "unknown"),
                )
        except Exception as event_exc:
            logger.warning(
                "campaign_event_create_failed file_id=%s tenant_id=%s error=%s",
                file_id,
                tenant_id,
                str(event_exc),
            )
        
        return DeleteResponse(
            status="success",
            message=f"File {file_id} assigned to {request.assignee} with status {request.status}"
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to assign file: {exc}"
        ) from exc


@router.post("/files/{file_id}/archive", response_model=DeleteResponse)
async def promote_to_archive(
    file_id: str,
    request: PromoteRequest,
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access)
) -> DeleteResponse:
    """
    Promote a file from the Private Client Silo (Tier 2) to the Global Firm Archive (Tier 1).
    
    This makes the document visible to the entire firm via Global Riley without violating
    tenant security for non-shared files. The file is copied to Tier 1 with optional
    "Golden Standard" weighting for high-priority learning.
    """
    # Log user_id for every request
    user_id = current_user.get("id", "unknown")
    print(f"Promote to archive request: user_id={user_id}, file_id={file_id}")
    settings = get_settings()
    
    try:
        await _get_file_point_for_tenant(
            file_id=file_id,
            tenant_id=tenant_id,
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
        )
        await vector_service.promote_to_global(
            file_id=file_id,
            is_golden=request.is_golden,
            source_campaign_id=tenant_id,
        )
        
        golden_text = " (marked as Golden Standard)" if request.is_golden else ""
        return DeleteResponse(
            status="success",
            message=f"File {file_id} promoted to Global Archive{golden_text}"
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=404,
            detail=str(exc)
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to promote file: {exc}"
        ) from exc


@router.patch("/files/{file_id}/ai_enabled", response_model=DeleteResponse)
async def toggle_ai_enabled(
    file_id: str,
    request: AIEnabledRequest,
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access)
) -> DeleteResponse:
    """
    Toggle the AI Memory (ai_enabled) flag for a file.
    
    This controls whether the file is included in RAG searches for the campaign.
    Files with ai_enabled=false are excluded from vector search results.
    
    Args:
        file_id: The Qdrant point ID (UUID) of the file
        request: AIEnabledRequest containing the new ai_enabled boolean value
    
    Returns:
        DeleteResponse with status and message
    """
    try:
        # Verify file exists in tenant/global scope and load current payload.
        _, payload, resolved_collection = await _get_file_point_for_tenant(
            file_id=file_id,
            tenant_id=tenant_id,
        )

        current_status = str(payload.get("ingestion_status") or "uploaded").strip().lower()
        payload_update: Dict[str, Any] = {"ai_enabled": request.ai_enabled}
        if not request.ai_enabled and current_status in {"queued", "processing", "uploaded"}:
            payload_update.update(
                {
                    "ingestion_status": "uploaded",
                    "ingestion_job_status": "cancelled",
                    "ingestion_job_completed_at": datetime.now().isoformat(),
                    "ingestion_job_error_message": "Riley Memory disabled before ingestion completed.",
                }
            )

        # Update ai_enabled flag in Qdrant payload
        await vector_service.client.set_payload(
            collection_name=resolved_collection,
            payload=payload_update,
            points=[file_id]
        )

        # Keep chunk records aligned with parent AI toggle.
        chunk_filter = Filter(
            must=[
                FieldCondition(
                    key="parent_file_id",
                    match=MatchValue(value=file_id),
                ),
                FieldCondition(
                    key="record_type",
                    match=MatchValue(value="chunk"),
                ),
            ]
        )
        try:
            chunk_points, _ = await vector_service.client.scroll(
                collection_name=resolved_collection,
                scroll_filter=chunk_filter,
                limit=1000,
                with_payload=False,
                with_vectors=False,
            )
        except Exception as exc:
            message = str(exc).lower()
            if "index required but not found" not in message:
                raise
            fallback_filter = Filter(
                must=[
                    FieldCondition(
                        key="parent_file_id",
                        match=MatchValue(value=file_id),
                    )
                ]
            )
            points_with_payload, _ = await vector_service.client.scroll(
                collection_name=resolved_collection,
                scroll_filter=fallback_filter,
                limit=1000,
                with_payload=True,
                with_vectors=False,
            )
            chunk_points = [
                point for point in points_with_payload
                if (point.payload or {}).get("record_type") == "chunk"
            ]
        if chunk_points:
            await vector_service.client.set_payload(
                collection_name=resolved_collection,
                payload={"ai_enabled": request.ai_enabled},
                points=[str(point.id) for point in chunk_points],
            )

        # Turning Riley Memory ON is the explicit ingestion trigger.
        if request.ai_enabled:
            status_now = str(payload.get("ingestion_status") or "uploaded").strip().lower()
            should_start_ingestion = status_now not in {"queued", "processing"}
            if should_start_ingestion:
                await reindex_existing_file(file_id=file_id, collection_name=resolved_collection)

        status_text = "enabled" if request.ai_enabled else "disabled"
        return DeleteResponse(
            status="success",
            message=f"AI Memory {status_text} for file {file_id}"
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update ai_enabled: {exc}"
        ) from exc


class OCRResponse(BaseModel):
    status: str
    message: str
    ocr_text: Optional[str] = None
    ocr_confidence: Optional[float] = None
    ocr_language: Optional[str] = None
    ocr_status: str


# SINGLE SOURCE OF TRUTH: Only ONE OCR endpoint exists in the codebase
# This endpoint handles: cache check, queued status, GCS download, OCR execution,
# payload merge, preserves ai_enabled toggle state, vector embedding update, and upsert.
@router.post("/files/{file_id}/ocr", response_model=OCRResponse)
async def request_ocr(
    file_id: str,
    collection: str = Query(..., description="Collection name: 'tier_1' or 'tier_2'"),
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> OCRResponse:
    """
    Request OCR processing for an image file.
    
    SINGLE SOURCE OF TRUTH: This is the ONLY OCR endpoint in the codebase.
    
    Behavior:
    1. Validates system-wide OCR is enabled
    2. Checks if OCR is already complete (returns cached result immediately)
    3. Sets status to "queued" and persists
    4. Downloads file bytes from GCS
    5. Runs OCR extraction
    6. On success:
       - Merges OCR fields into existing payload (preserves all existing fields)
       - Preserves existing ai_enabled value (does not auto-enable Riley Memory)
       - Generates new embedding from OCR text (truncated to 9000 chars)
       - Upserts PointStruct with NEW vector and MERGED payload
    7. On failure: Sets ocr_status="failed" and stores error
    
    OCR only runs if:
    1. System-wide OCR is enabled (OCR_ENABLED_SYSTEMWIDE=True)
    2. File is an image type
    3. OCR hasn't already been completed (uses cached result if available)
    
    Args:
        file_id: The Qdrant point ID (UUID) of the file
        collection: Collection name - 'tier_1' for global archive, 'tier_2' for campaign assets
    
    Returns:
        OCRResponse with OCR status and extracted text (if successful)
    """
    settings = get_settings()
    
    # Validate system-wide OCR is enabled
    if not settings.OCR_ENABLED_SYSTEMWIDE:
        raise HTTPException(
            status_code=403,
            detail="OCR is not enabled system-wide. Contact administrator to enable OCR."
        )
    
    # Strict validation: collection must be exactly "tier_1" or "tier_2"
    if collection not in ("tier_1", "tier_2"):
        raise HTTPException(
            status_code=400,
            detail=f"collection must be 'tier_1' or 'tier_2', got: {collection}"
        )
    
    # Determine collection name
    collection_name = (
        settings.QDRANT_COLLECTION_TIER_1 if collection == "tier_1"
        else settings.QDRANT_COLLECTION_TIER_2
    )
    
    try:
        # Retrieve the file point
        _, payload, _ = await _get_file_point_for_tenant(
            file_id=file_id,
            tenant_id=tenant_id,
            collection_name=collection_name,
            # collection is chosen by request param, still enforced against tenant ownership
        )
        filename = payload.get("filename", "unknown")
        
        # Check if file is an image
        if not is_image_ext(filename):
            raise HTTPException(
                status_code=400,
                detail=f"OCR is only available for image files. File '{filename}' is not an image."
            )
        
        # Check if OCR is already complete (return cached result)
        ocr_status = payload.get("ocr_status", "not_requested")
        if ocr_status == "complete" and payload.get("ocr_text"):
            return OCRResponse(
                status="success",
                message="OCR text retrieved from cache",
                ocr_text=payload.get("ocr_text"),
                ocr_confidence=payload.get("ocr_confidence"),
                ocr_language=payload.get("ocr_language"),
                ocr_status="complete"
            )
        
        # Set status to queued
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={
                "ocr_enabled": True,
                "ocr_status": "queued",
                "ocr_error": None,
            },
            points=[file_id]
        )
        
        # Get file content from GCS
        file_url = payload.get("url")
        if not file_url:
            raise HTTPException(
                status_code=404,
                detail=f"File URL not found for ID {file_id}"
            )
        
        # Download file from GCS
        file_content = await StorageService.download_file(file_url)
        
        # Perform OCR
        try:
            ocr_result = await run_ocr(file_content, max_chars=settings.OCR_MAX_CHARS)
            
            # Update payload with OCR results
            ocr_extracted_at = datetime.now().isoformat()
            
            # Merge OCR fields into existing payload (preserve all existing fields)
            merged_payload = payload.copy()
            merged_payload.update({
                "ocr_enabled": True,
                "ocr_status": "complete",
                "ocr_text": ocr_result["text"],
                "ocr_confidence": ocr_result["confidence"],
                "ocr_language": ocr_result["language"],
                "ocr_extracted_at": ocr_extracted_at,
                "ocr_error": None,
                # Preserve Riley Memory toggle state; OCR should not auto-enable retrieval.
                "ai_enabled": bool(payload.get("ai_enabled", False)),
                # Update content_preview to show OCR text preview
                "content_preview": ocr_result["text"][:1000],
            })
            
            # Generate new embedding from OCR text for improved retrieval
            # Truncate to 9000 chars (same as ingestion) for embedding
            ocr_text_for_embedding = ocr_result["text"][:9000]
            new_vector = await _generate_embedding(ocr_text_for_embedding)
            
            # Update the point with new vector and merged payload
            updated_point = PointStruct(
                id=file_id,
                vector=new_vector,
                payload=merged_payload
            )
            
            await vector_service.client.upsert(
                collection_name=collection_name,
                points=[updated_point]
            )
            try:
                pages_processed = 1
                requests_count = 1
                ocr_cost = estimate_single_unit_cost(
                    service="ocr",
                    provider="google_cloud_vision",
                    model="document_text_detection",
                    unit_type="request",
                    quantity=requests_count,
                )
                user_id = current_user.get("id", "unknown")
                await graph.append_analytics_event(
                    event_id=f"ocr_completed:{file_id}:{ocr_extracted_at}",
                    source_event_type_raw="ocr_completed",
                    source_entity="FileOCR",
                    campaign_id=tenant_id,
                    user_id=user_id,
                    actor_user_id=user_id,
                    object_id=file_id,
                    status="succeeded",
                    provider="google_cloud_vision",
                    model="document_text_detection",
                    cost_estimate_usd=(
                        float(ocr_cost["cost_estimate_usd"])
                        if ocr_cost.get("cost_estimate_usd") is not None
                        else None
                    ),
                    pricing_version=str(ocr_cost.get("pricing_version") or "cost-accounting-v1"),
                    cost_confidence="estimated_units",
                    metadata={
                        "service": "ocr",
                        "images_processed": 1,
                        "pages_processed": pages_processed,
                        "requests_count": requests_count,
                        "ocr_confidence": ocr_result.get("confidence"),
                    },
                )
            except Exception:
                pass
            
            return OCRResponse(
                status="success",
                message="OCR completed successfully and vector embedding updated",
                ocr_text=ocr_result["text"],
                ocr_confidence=ocr_result["confidence"],
                ocr_language=ocr_result["language"],
                ocr_status="complete"
            )
            
        except ImportError as e:
            error_msg = f"OCR dependencies not installed: {str(e)}"
            await vector_service.client.set_payload(
                collection_name=collection_name,
                payload={
                    "ocr_status": "failed",
                    "ocr_error": error_msg,
                },
                points=[file_id]
            )
            raise HTTPException(
                status_code=500,
                detail=error_msg
            ) from e
        except Exception as e:
            error_msg = f"OCR processing failed: {str(e)}"
            logger.error(f"OCR failed for file {file_id}: {e}")
            await vector_service.client.set_payload(
                collection_name=collection_name,
                payload={
                    "ocr_status": "failed",
                    "ocr_error": error_msg,
                },
                points=[file_id]
            )
            raise HTTPException(
                status_code=500,
                detail=error_msg
            ) from e
            
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process OCR request: {exc}"
        ) from exc

