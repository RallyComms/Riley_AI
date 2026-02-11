import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, UploadFile, Form, Query, Depends, Request
from pydantic import BaseModel
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

from app.core.config import get_settings
from app.dependencies.auth import verify_clerk_token, verify_tenant_access, check_tenant_membership
from app.services.ocr import run_ocr, is_image_ext
from app.services.qdrant import vector_service
from app.services.ingestion import process_upload, delete_file, untag_file, _generate_embedding
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
    ocr_confidence: Optional[float] = None
    ocr_extracted_at: Optional[str] = None
    preview_url: Optional[str] = None
    preview_type: Optional[str] = None
    preview_status: Optional[str] = None


class FileListResponse(BaseModel):
    files: List[FileListItem]


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


class PromoteRequest(BaseModel):
    is_golden: bool


class AIEnabledRequest(BaseModel):
    ai_enabled: bool


class DeleteResponse(BaseModel):
    status: str
    message: str


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
    current_user: Dict = Depends(verify_tenant_access)
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
            
            # Convert to FileListItem format
            for file_data in global_files:
                filename = file_data.get("filename")
                if not filename:
                    continue
                
                # Determine file type from extension or payload
                extension = Path(filename).suffix.lower().lstrip(".")
                file_type = file_data.get("type") or extension or "unknown"
                
                files.append(FileListItem(
                    id=file_data.get("id", ""),
                    name=filename,
                    url=file_data.get("url", ""),
                    type=file_type,
                    date=file_data.get("promoted_at", datetime.now().isoformat()),
                    size="Unknown",  # Global files may not have size
                    tags=[],  # Global files don't have tags
                    ai_enabled=file_data.get("ai_enabled"),
                    ocr_enabled=file_data.get("ocr_enabled"),
                    ocr_status=file_data.get("ocr_status"),
                    ocr_confidence=file_data.get("ocr_confidence"),
                    ocr_extracted_at=file_data.get("ocr_extracted_at"),
                ))
        else:
            # Standard tenant-scoped query (Tier 2)
            # Create tenant filter for strict isolation
            tenant_filter = Filter(
                must=[
                    FieldCondition(
                        key="client_id",
                        match=MatchValue(value=tenant_id)
                    )
                ]
            )
            
            # Scroll points from Qdrant filtered by tenant_id (source of truth)
            scroll_result = await vector_service.client.scroll(
                collection_name=settings.QDRANT_COLLECTION_TIER_2,
                scroll_filter=tenant_filter,  # SECURITY: Always use tenant filter
                limit=1000,  # Get all files for this tenant
                with_payload=True,
                with_vectors=False,
            )
            
            # Process each Qdrant point
            for point in scroll_result[0]:
                payload = point.payload or {}
                filename = payload.get("filename")
                
                if not filename:
                    continue  # Skip points without filename
                
                # Get upload date from payload, fallback to current time for old files
                file_date = payload.get("upload_date", datetime.now().isoformat())
                
                # Get file size from payload, fallback to "Unknown" for old files
                size_str = payload.get("size", "Unknown")
                
                # Determine file type from extension or payload
                extension = Path(filename).suffix.lower().lstrip(".")
                file_type = payload.get("type") or extension or "unknown"
                
                files.append(FileListItem(
                    id=str(point.id),  # UUID from Qdrant
                    name=filename,
                    url=payload.get("url", ""),  # GCS public URL from payload
                    type=file_type,
                    date=file_date,
                    size=size_str,
                    tags=payload.get("tags", []),
                    ai_enabled=payload.get("ai_enabled"),
                    ocr_enabled=payload.get("ocr_enabled"),
                    ocr_status=payload.get("ocr_status"),
                    ocr_confidence=payload.get("ocr_confidence"),
                    ocr_extracted_at=payload.get("ocr_extracted_at"),
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


@router.post("/upload", response_model=UploadResponse)
async def upload_file(
    file: UploadFile,
    tenant_id: str = Form(...),
    tags: str = Form(""),  # Comma-separated tags
    overwrite: bool = Query(False, description="Overwrite existing file if it exists"),
    current_user: Dict = Depends(verify_tenant_access)
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
    
    settings = get_settings()

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
            overwrite=overwrite
        )
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
                    key="filename",
                    match=MatchValue(value=request.old_name)
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
        # Update tags in Qdrant
        await vector_service.client.set_payload(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            payload={"tags": request.tags},
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
    current_user: Dict = Depends(verify_tenant_access)
) -> DeleteResponse:
    """
    Assign a file to a reviewer and update its status.
    
    This updates the file's status and adds the assignee to the file's metadata.
    """
    settings = get_settings()
    
    try:
        # Get current payload
        points = await vector_service.client.retrieve(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            ids=[file_id]
        )
        
        if not points:
            raise HTTPException(status_code=404, detail=f"File {file_id} not found")
        
        current_payload = points[0].payload or {}
        
        # Update payload with assignee and status
        updated_payload = {
            **current_payload,
            "status": request.status,
            "assignee": request.assignee,
            "assigned_to": current_payload.get("assigned_to", []) + [request.assignee] if request.assignee not in current_payload.get("assigned_to", []) else current_payload.get("assigned_to", [])
        }
        
        # Update Qdrant payload
        await vector_service.client.set_payload(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            payload=updated_payload,
            points=[file_id]
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
    
    try:
        await vector_service.promote_to_global(
            file_id=file_id,
            is_golden=request.is_golden
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
    settings = get_settings()
    
    try:
        # Verify file exists in Tier 2
        points = await vector_service.client.retrieve(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            ids=[file_id],
            with_payload=True,
            with_vectors=False,
        )
        
        if not points:
            raise HTTPException(
                status_code=404,
                detail=f"File with ID {file_id} not found in Qdrant"
            )
        
        # Update ai_enabled flag in Qdrant payload
        await vector_service.client.set_payload(
            collection_name=settings.QDRANT_COLLECTION_TIER_2,
            payload={"ai_enabled": request.ai_enabled},
            points=[file_id]
        )
        
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
# payload merge, ai_enabled=True auto-flip, vector embedding update, and upsert.
@router.post("/files/{file_id}/ocr", response_model=OCRResponse)
async def request_ocr(
    file_id: str,
    collection: str = Query(..., description="Collection name: 'tier_1' or 'tier_2'"),
    tenant_id: str = Query(..., description="Tenant/client ID that owns this file"),
    current_user: Dict = Depends(verify_tenant_access)
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
       - Sets ai_enabled=True automatically
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
        points = await vector_service.client.retrieve(
            collection_name=collection_name,
            ids=[file_id],
            with_payload=True,
            with_vectors=False,
        )
        
        if not points:
            raise HTTPException(
                status_code=404,
                detail=f"File with ID {file_id} not found in collection {collection_name}"
            )
        
        point = points[0]
        payload = point.payload or {}
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
                # Automatically enable AI Memory after successful OCR
                "ai_enabled": True,
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

