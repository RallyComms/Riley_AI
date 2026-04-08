from typing import Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from app.dependencies.auth import verify_clerk_token
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService

router = APIRouter()


class NotificationItem(BaseModel):
    id: str
    user_id: str
    actor_user_id: Optional[str] = None
    target_user_id: Optional[str] = None
    actor_display_name: Optional[str] = None
    target_display_name: Optional[str] = None
    type: str
    entity_id: Optional[str] = None
    campaign_id: Optional[str] = None
    message: str
    status: Literal["unread", "read", "completed"]
    created_at: Optional[str] = None
    acted_at: Optional[str] = None


class NotificationsResponse(BaseModel):
    notifications: List[NotificationItem]


class NotificationsUnreadCountResponse(BaseModel):
    unread_count: int


class NotificationActionResponse(BaseModel):
    ok: bool
    notification: NotificationItem


@router.get("/notifications", response_model=NotificationsResponse)
async def list_notifications(
    limit: int = Query(50, ge=1, le=200),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> NotificationsResponse:
    user_id = current_user.get("id", "unknown")
    rows = await graph.list_notifications(user_id=user_id, limit=limit)
    return NotificationsResponse(notifications=[NotificationItem(**row) for row in rows])


@router.get(
    "/notifications/unread-count",
    response_model=NotificationsUnreadCountResponse,
)
async def get_unread_notifications_count(
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> NotificationsUnreadCountResponse:
    user_id = current_user.get("id", "unknown")
    unread_count = await graph.count_unread_notifications(user_id=user_id)
    return NotificationsUnreadCountResponse(unread_count=unread_count)


@router.post("/notifications/{notification_id}/mark-read", response_model=NotificationActionResponse)
async def mark_notification_read(
    notification_id: str,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> NotificationActionResponse:
    user_id = current_user.get("id", "unknown")
    try:
        updated = await graph.update_notification_status(
            user_id=user_id,
            notification_id=notification_id,
            status="read",
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return NotificationActionResponse(ok=True, notification=NotificationItem(**updated))


@router.post("/notifications/{notification_id}/complete", response_model=NotificationActionResponse)
async def complete_notification(
    notification_id: str,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> NotificationActionResponse:
    user_id = current_user.get("id", "unknown")
    try:
        updated = await graph.update_notification_status(
            user_id=user_id,
            notification_id=notification_id,
            status="completed",
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return NotificationActionResponse(ok=True, notification=NotificationItem(**updated))

