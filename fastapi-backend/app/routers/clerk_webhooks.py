import base64
import hashlib
import hmac
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request, status, Depends

from app.core.config import get_settings
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService

router = APIRouter()
logger = logging.getLogger(__name__)

_SVIX_TOLERANCE_SECONDS = 300


def _extract_primary_email(data: Dict[str, Any]) -> Optional[str]:
    primary = data.get("primary_email_address")
    if isinstance(primary, dict):
        email = str(primary.get("email_address") or "").strip()
        if email:
            return email

    email_addresses = data.get("email_addresses")
    if isinstance(email_addresses, list):
        for item in email_addresses:
            if not isinstance(item, dict):
                continue
            email = str(item.get("email_address") or "").strip()
            if email:
                return email
    return None


def _resolve_display_name(*, first_name: Optional[str], last_name: Optional[str], username: Optional[str], email: Optional[str], clerk_id: str) -> str:
    full_name = f"{str(first_name or '').strip()} {str(last_name or '').strip()}".strip()
    if full_name:
        return full_name
    normalized_username = str(username or "").strip()
    if normalized_username:
        return normalized_username
    normalized_email = str(email or "").strip()
    if normalized_email:
        email_prefix = normalized_email.split("@")[0].strip()
        if email_prefix:
            return email_prefix
    return "Unknown user"


def _parse_svix_signatures(signature_header: str) -> List[str]:
    signatures: List[str] = []
    for part in signature_header.split():
        segment = part.strip()
        if not segment:
            continue
        if segment.startswith("v1,"):
            signatures.append(segment.split(",", 1)[1].strip())
    return signatures


def _verify_clerk_webhook_signature(*, body: str, headers: Dict[str, str], secret: str) -> None:
    msg_id = headers.get("svix-id")
    msg_timestamp = headers.get("svix-timestamp")
    msg_signature = headers.get("svix-signature")
    if not msg_id or not msg_timestamp or not msg_signature:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Missing Svix signature headers",
        )

    try:
        timestamp_int = int(msg_timestamp)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid Svix timestamp header",
        ) from exc

    now = int(time.time())
    if abs(now - timestamp_int) > _SVIX_TOLERANCE_SECONDS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Svix timestamp outside allowed tolerance",
        )

    secret_value = secret.strip()
    if secret_value.startswith("whsec_"):
        secret_value = secret_value.split("_", 1)[1]

    try:
        key = base64.b64decode(secret_value)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Invalid Clerk webhook secret format",
        ) from exc

    signed_content = f"{msg_id}.{msg_timestamp}.{body}".encode("utf-8")
    expected = base64.b64encode(hmac.new(key, signed_content, hashlib.sha256).digest()).decode("utf-8")
    candidates = _parse_svix_signatures(msg_signature)
    if not candidates or not any(hmac.compare_digest(expected, candidate) for candidate in candidates):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid webhook signature",
        )


@router.post("/webhooks/clerk")
async def handle_clerk_webhook(
    request: Request,
    graph: GraphService = Depends(get_graph),
) -> Dict[str, Any]:
    settings = get_settings()
    secret = getattr(settings, "CLERK_WEBHOOK_SECRET", None)
    if not secret:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="CLERK_WEBHOOK_SECRET not configured",
        )

    raw_body = await request.body()
    body = raw_body.decode("utf-8")
    headers = {
        "svix-id": request.headers.get("svix-id", ""),
        "svix-timestamp": request.headers.get("svix-timestamp", ""),
        "svix-signature": request.headers.get("svix-signature", ""),
    }
    _verify_clerk_webhook_signature(body=body, headers=headers, secret=secret)

    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook payload must be valid JSON",
        ) from exc

    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Webhook payload must be an object",
        )

    event_type = str(payload.get("type") or "").strip()
    data = payload.get("data")
    if not isinstance(data, dict):
        data = {}

    clerk_user_id = str(data.get("id") or "").strip()
    if not event_type:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Webhook event type missing")

    if event_type in {"user.created", "user.updated"}:
        if not clerk_user_id:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Clerk user id missing")
        email = _extract_primary_email(data)
        username = str(data.get("username") or "").strip() or None
        avatar_url = str(data.get("image_url") or "").strip() or None
        first_name = str(data.get("first_name") or "").strip() or None
        last_name = str(data.get("last_name") or "").strip() or None
        display_name = _resolve_display_name(
            first_name=first_name,
            last_name=last_name,
            username=username,
            email=email,
            clerk_id=clerk_user_id,
        )
        await graph.sync_user_from_clerk(
            user_id=clerk_user_id,
            email=email,
            username=username,
            display_name=display_name,
            avatar_url=avatar_url,
        )
        logger.info("clerk_webhook_user_synced event=%s user_id=%s", event_type, clerk_user_id)
        return {"ok": True, "event_type": event_type, "user_id": clerk_user_id}

    if event_type == "user.deleted":
        if clerk_user_id:
            await graph.mark_user_deleted_from_clerk(user_id=clerk_user_id)
            logger.info("clerk_webhook_user_marked_deleted user_id=%s", clerk_user_id)
        return {"ok": True, "event_type": event_type, "user_id": clerk_user_id}

    logger.info("clerk_webhook_event_ignored type=%s", event_type)
    return {"ok": True, "event_type": event_type, "ignored": True}
