"""Direct-to-GCS upload session endpoints.

These endpoints let the browser PUT large files directly to a private GCS
bucket via short-lived V4 signed URLs, bypassing the Cloud Run request body
limit that affects the legacy proxy upload path. The backend still owns
auth, the object path, file ID, size limits, and the Qdrant parent-file
metadata.

Flow:

1. POST `/api/v1/uploads/direct/init`
   - Auth + tenant membership
   - Validate filename / content_type / size / surface
   - Mint a server-owned object name and a V4 signed PUT URL
2. Browser uploads bytes directly to GCS using the signed URL.
3. POST `/api/v1/uploads/direct/complete`
   - Auth + tenant membership
   - Re-validate the object path (must belong to this tenant/surface/file_id)
   - Verify the GCS object exists and matches expected size
   - Create the same parent-file Qdrant payload the legacy upload route creates
   - Returns the same response shape as `/api/v1/upload`
   - Does NOT enqueue ingestion (Riley Memory toggle still owns that)

Known orphan risk:
- If the browser PUT to signed GCS succeeds but `/complete` is never called,
  the object remains in GCS under the direct-upload prefix without a Qdrant
  parent-file record.
- There is intentionally no pending-upload DB table yet.
- A future cleanup job should periodically scan `campaigns/*/*/*/*` objects
  older than a grace period and delete ones that have no matching Qdrant
  parent file (by `url`/`file_id`).
"""

import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from app.core.config import get_settings
from app.dependencies.auth import (
    check_tenant_membership,
    verify_clerk_token,
    verify_tenant_access,
)
from app.services.ingestion import (
    IMAGE_EXTENSIONS,
    TEXT_NATIVE_EXTENSIONS,
    finalize_direct_upload_record,
)
from app.services.qdrant import vector_service
from app.services.storage import StorageService

logger = logging.getLogger(__name__)
router = APIRouter()


# Surfaces are server-owned; only these are allowed in the object path.
SUPPORTED_SURFACES: set[str] = {"assets", "media"}

# Media-only extensions on top of the document/image set used by ingestion.
MEDIA_EXTRA_EXTENSIONS: set[str] = {
    "mp4",
    "webm",
    "mov",
    "m4v",
    "mp3",
    "wav",
    "ogg",
    "m4a",
    "gif",
    "svg",
}

ALLOWED_UPLOAD_EXTENSIONS: set[str] = (
    set(TEXT_NATIVE_EXTENSIONS) | set(IMAGE_EXTENSIONS) | MEDIA_EXTRA_EXTENSIONS
)

# TODO(riley-direct-upload-cleanup):
# Implement a periodic orphan cleanup worker that:
# 1) lists objects under `campaigns/{tenant}/{surface}/{file_id}/...`
# 2) skips objects newer than a grace period (for slow/retried clients)
# 3) checks Qdrant for a parent `record_type=file` with matching `file_id`/`url`
# 4) deletes unreferenced objects and emits `direct_upload_orphan_deleted` logs/events
# This router currently logs orphan-risk windows but does not auto-clean.

# Conservative tenant id pattern. We only accept ids that are safe to embed in
# a GCS object path: alphanumerics, dashes, underscores, dots. "global" is
# also allowed for the firm-archive surface.
_TENANT_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,128}$")
_FILE_ID_PATTERN = re.compile(r"^[A-Za-z0-9-]{8,64}$")


class DirectUploadInitRequest(BaseModel):
    tenant_id: str
    filename: str
    content_type: Optional[str] = None
    size_bytes: int = Field(..., ge=0)
    surface: Literal["assets", "media"]
    tags: Optional[List[str]] = None


class DirectUploadInitResponse(BaseModel):
    file_id: str
    object_name: str
    gcs_url: str
    signed_upload_url: str
    required_headers: Dict[str, str]
    expires_at: str
    max_size_bytes: int


class DirectUploadCompleteRequest(BaseModel):
    tenant_id: str
    file_id: str
    object_name: str
    filename: str
    content_type: Optional[str] = None
    size_bytes: int = Field(..., ge=0)
    surface: Literal["assets", "media"]
    tags: Optional[List[str]] = None


class DirectUploadCompleteResponse(BaseModel):
    id: str
    url: str
    filename: str
    type: str
    preview_status: Optional[str] = None
    preview_error: Optional[str] = None


def _safe_filename(name: str) -> str:
    """Return a path-safe filename, preserving extension where possible."""
    raw = (name or "").strip()
    if not raw:
        return "file"
    # Drop any directory parts a client may have included.
    raw = raw.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
    # Replace any character outside the safe set.
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", raw)
    # Avoid leading dot which would make the file hidden / weird in CLI tools.
    cleaned = cleaned.lstrip(".") or "file"
    if len(cleaned) > 200:
        ext = ""
        if "." in cleaned:
            ext = "." + cleaned.rsplit(".", 1)[-1]
            ext = ext[:20]
        cleaned = cleaned[: 200 - len(ext)] + ext
    return cleaned or "file"


def _file_extension(filename: str) -> str:
    return Path(filename).suffix.lower().lstrip(".")


def _validate_tenant_id(tenant_id: str) -> str:
    cleaned = (tenant_id or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="tenant_id is required.")
    if cleaned == "global":
        return cleaned
    if not _TENANT_ID_PATTERN.match(cleaned):
        raise HTTPException(status_code=400, detail="Invalid tenant_id.")
    return cleaned


def _validate_surface(surface: str) -> str:
    cleaned = (surface or "").strip().lower()
    if cleaned not in SUPPORTED_SURFACES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported surface '{surface}'. Allowed: {sorted(SUPPORTED_SURFACES)}.",
        )
    return cleaned


def _validate_file_id(file_id: str) -> str:
    cleaned = (file_id or "").strip()
    if not _FILE_ID_PATTERN.match(cleaned):
        raise HTTPException(status_code=400, detail="Invalid file_id.")
    return cleaned


def _validate_extension(filename: str) -> str:
    ext = _file_extension(filename)
    if not ext:
        raise HTTPException(status_code=400, detail="Filename must include an extension.")
    if ext not in ALLOWED_UPLOAD_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file extension '.{ext}'.",
        )
    return ext


def _build_object_name(*, tenant_id: str, surface: str, file_id: str, filename: str) -> str:
    safe = _safe_filename(filename)
    return f"campaigns/{tenant_id}/{surface}/{file_id}/{safe}"


def _expected_object_prefix(*, tenant_id: str, surface: str, file_id: str) -> str:
    return f"campaigns/{tenant_id}/{surface}/{file_id}/"


def _complete_response_from_payload(
    *,
    file_id: str,
    payload: Dict,
    fallback_filename: str,
    fallback_url: str,
) -> DirectUploadCompleteResponse:
    file_type = str(payload.get("file_type") or payload.get("type") or _file_extension(fallback_filename) or "unknown")
    return DirectUploadCompleteResponse(
        id=file_id,
        url=str(payload.get("url") or fallback_url),
        filename=str(payload.get("filename") or fallback_filename),
        type=file_type,
        preview_status=payload.get("preview_status"),
        preview_error=payload.get("preview_error"),
    )


@router.post(
    "/uploads/direct/init",
    response_model=DirectUploadInitResponse,
)
async def direct_upload_init(
    payload: DirectUploadInitRequest,
    request: Request,
    current_user: Dict = Depends(verify_clerk_token),
) -> DirectUploadInitResponse:
    """Issue a server-owned signed URL the browser can PUT bytes to."""
    settings = get_settings()
    user_id = current_user.get("id", "unknown")

    tenant_id = _validate_tenant_id(payload.tenant_id)
    surface = _validate_surface(payload.surface)
    await check_tenant_membership(user_id, tenant_id, request)

    extension = _validate_extension(payload.filename)

    max_bytes = int(settings.DIRECT_UPLOAD_MAX_MB) * 1024 * 1024
    if int(payload.size_bytes) <= 0:
        raise HTTPException(status_code=400, detail="size_bytes must be greater than zero.")
    if int(payload.size_bytes) > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=(
                f"This file is too large. Current limit is {settings.DIRECT_UPLOAD_MAX_MB} MB."
            ),
        )

    content_type = (payload.content_type or "application/octet-stream").strip() or "application/octet-stream"
    file_id = uuid.uuid4().hex
    object_name = _build_object_name(
        tenant_id=tenant_id,
        surface=surface,
        file_id=file_id,
        filename=payload.filename,
    )
    ttl_seconds = int(settings.DIRECT_UPLOAD_URL_TTL_SECONDS)
    signed_url = await StorageService.generate_signed_upload_url(
        object_name=object_name,
        ttl_seconds=ttl_seconds,
        content_type=content_type,
    )
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
    gcs_url = f"https://storage.googleapis.com/{settings.GCS_BUCKET_NAME}/{object_name}"

    logger.info(
        "direct_upload_init issued tenant=%s surface=%s file_id=%s extension=%s size_bytes=%s",
        tenant_id,
        surface,
        file_id,
        extension,
        int(payload.size_bytes),
    )
    logger.info(
        "direct_upload_orphan_risk_window tenant=%s surface=%s file_id=%s object_name=%s expires_at=%s",
        tenant_id,
        surface,
        file_id,
        object_name,
        expires_at,
    )

    return DirectUploadInitResponse(
        file_id=file_id,
        object_name=object_name,
        gcs_url=gcs_url,
        signed_upload_url=signed_url,
        required_headers={"Content-Type": content_type},
        expires_at=expires_at,
        max_size_bytes=max_bytes,
    )


@router.post(
    "/uploads/direct/complete",
    response_model=DirectUploadCompleteResponse,
)
async def direct_upload_complete(
    payload: DirectUploadCompleteRequest,
    request: Request,
    current_user: Dict = Depends(verify_clerk_token),
) -> DirectUploadCompleteResponse:
    """Verify the GCS object and persist the same parent-file metadata as `/upload`."""
    settings = get_settings()
    user_id = current_user.get("id", "unknown")

    tenant_id = _validate_tenant_id(payload.tenant_id)
    surface = _validate_surface(payload.surface)
    await check_tenant_membership(user_id, tenant_id, request)

    file_id = _validate_file_id(payload.file_id)
    _validate_extension(payload.filename)

    expected_prefix = _expected_object_prefix(
        tenant_id=tenant_id, surface=surface, file_id=file_id
    )
    cleaned_object_name = (payload.object_name or "").strip().lstrip("/")
    if not cleaned_object_name.startswith(expected_prefix):
        logger.warning(
            "direct_upload_complete rejected: object_name does not match expected prefix "
            "tenant=%s surface=%s file_id=%s object_name=%s expected_prefix=%s",
            tenant_id,
            surface,
            file_id,
            cleaned_object_name,
            expected_prefix,
        )
        raise HTTPException(
            status_code=400,
            detail="object_name does not belong to this tenant/surface/file_id.",
        )

    stat = await StorageService.stat_object(cleaned_object_name)
    if stat is None:
        raise HTTPException(
            status_code=404,
            detail="GCS object was not found. The browser upload may have failed.",
        )

    actual_size = int(stat.get("size") or 0)
    max_bytes = int(settings.DIRECT_UPLOAD_MAX_MB) * 1024 * 1024
    if actual_size > max_bytes:
        raise HTTPException(
            status_code=413,
            detail=(
                f"Uploaded file exceeds the maximum allowed size "
                f"({settings.DIRECT_UPLOAD_MAX_MB} MB)."
            ),
        )
    declared_size = int(payload.size_bytes or 0)
    if declared_size > 0 and abs(declared_size - actual_size) > 0:
        # Trust GCS's actual size as the source of truth, but log mismatches.
        logger.info(
            "direct_upload_complete size_mismatch tenant=%s file_id=%s declared=%s actual=%s",
            tenant_id,
            file_id,
            declared_size,
            actual_size,
        )

    file_url = stat.get("public_url") or (
        f"https://storage.googleapis.com/{settings.GCS_BUCKET_NAME}/{cleaned_object_name}"
    )
    tags = list(payload.tags or [])
    # Media surface uploads are tagged "Media" in the legacy path; mirror that
    # behaviour so existing UI filters keep working.
    if surface == "media" and "Media" not in tags:
        tags = [*tags, "Media"]

    # Preview is only useful in the assets surface; media uploads (mp4/wav/etc.)
    # have no LibreOffice preview path. We always *attempt* preview for office
    # documents; the helper itself enforces the size cap.
    attempt_preview = surface == "assets"

    target_collection = (
        settings.QDRANT_COLLECTION_TIER_1
        if tenant_id == "global"
        else settings.QDRANT_COLLECTION_TIER_2
    )
    existing_points = await vector_service.client.retrieve(
        collection_name=target_collection,
        ids=[file_id],
        with_payload=True,
        with_vectors=False,
    )
    if existing_points:
        existing_payload = existing_points[0].payload or {}
        if str(existing_payload.get("record_type") or "").strip().lower() == "chunk":
            raise HTTPException(
                status_code=409,
                detail="file_id already exists as a chunk record.",
            )
        existing_url = str(existing_payload.get("url") or "").strip()
        expected_url = f"https://storage.googleapis.com/{settings.GCS_BUCKET_NAME}/{cleaned_object_name}"
        if existing_url and not (
            existing_url == expected_url
            or existing_url.endswith(f"/{cleaned_object_name}")
        ):
            raise HTTPException(
                status_code=409,
                detail="file_id already exists with a different object path.",
            )

        existing_has_core_fields = bool(existing_url) and bool(
            str(existing_payload.get("filename") or "").strip()
        )
        if existing_has_core_fields:
            merged_tags = list(existing_payload.get("tags") or [])
            if not isinstance(merged_tags, list):
                merged_tags = []
            seen_tags = {str(tag) for tag in merged_tags}
            for tag in tags:
                if tag not in seen_tags:
                    merged_tags.append(tag)
                    seen_tags.add(tag)
            if merged_tags != list(existing_payload.get("tags") or []):
                await vector_service.client.set_payload(
                    collection_name=target_collection,
                    payload={"tags": merged_tags},
                    points=[file_id],
                )
                existing_payload["tags"] = merged_tags

            logger.info(
                "direct_upload_complete_idempotent_replay tenant=%s surface=%s file_id=%s object_name=%s",
                tenant_id,
                surface,
                file_id,
                cleaned_object_name,
            )
            return _complete_response_from_payload(
                file_id=file_id,
                payload=existing_payload,
                fallback_filename=payload.filename,
                fallback_url=file_url,
            )

    record = await finalize_direct_upload_record(
        tenant_id=tenant_id,
        file_id=file_id,
        filename=payload.filename,
        file_url=file_url,
        file_size=actual_size,
        tags=tags,
        attempt_preview=attempt_preview,
    )

    logger.info(
        "direct_upload_complete tenant=%s surface=%s file_id=%s size_bytes=%s preview_status=%s",
        tenant_id,
        surface,
        file_id,
        actual_size,
        record.get("preview_status"),
    )
    return DirectUploadCompleteResponse(**record)
