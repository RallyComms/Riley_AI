import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Union
from fastapi import APIRouter, HTTPException, Depends, Query, status
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

from app.dependencies.auth import verify_tenant_access, verify_clerk_token
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService
from app.services.qdrant import vector_service
from app.services.storage import StorageService
from app.services.clerk_directory import find_user_by_email, find_user_by_id, search_users


router = APIRouter()


class CreateCampaignRequest(BaseModel):
    name: str = Field(..., max_length=200, description="Campaign name")
    description: Optional[str] = Field(None, max_length=1000, description="Campaign description")


class CampaignResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    role: Optional[str] = None
    access: str = "member"
    created_at: Optional[str] = None
    status: Literal["active", "archived"] = "active"
    archived_at: Optional[str] = None
    owner_id: Optional[str] = None
    owner_name: Optional[str] = None


class CampaignsListResponse(BaseModel):
    campaigns: List[CampaignResponse]


class ArchiveCampaignResponse(BaseModel):
    ok: bool
    id: str
    status: Literal["archived"]
    archived_at: Optional[str] = None


class CreateAccessRequest(BaseModel):
    message: Optional[str] = Field(None, max_length=500, description="Optional access request message")


class AccessRequestResponse(BaseModel):
    ok: bool
    request_id: Optional[str] = None
    status: Optional[str] = None
    created_at: Optional[str] = None


class CampaignAccessRequestItem(BaseModel):
    id: str
    campaign_id: str
    user_id: str
    user_email: str
    user_name: str
    message: Optional[str] = None
    status: Literal["pending", "approved", "denied"]
    created_at: Optional[str] = None
    decided_at: Optional[str] = None
    decided_by: Optional[str] = None


class CampaignAccessRequestsResponse(BaseModel):
    requests: List[CampaignAccessRequestItem]


class AccessRequestDecisionRequest(BaseModel):
    action: Literal["approve", "deny"]


class CampaignEventItem(BaseModel):
    id: str
    type: str
    message: str
    user_id: Optional[str] = None
    actor_user_id: Optional[str] = None
    request_id: Optional[str] = None
    requester_display_name: Optional[str] = None
    created_at: Optional[str] = None


class CampaignEventsResponse(BaseModel):
    events: List[CampaignEventItem]


class UserCampaignFeedEventItem(BaseModel):
    id: str
    type: str
    message: str
    campaign_id: str
    campaign_name: Optional[str] = None
    user_id: Optional[str] = None
    actor_user_id: Optional[str] = None
    request_id: Optional[str] = None
    requester_display_name: Optional[str] = None
    member_role: Optional[str] = None
    created_at: Optional[str] = None


class UserCampaignFeedResponse(BaseModel):
    events: List[UserCampaignFeedEventItem]


class DismissFeedEventResponse(BaseModel):
    ok: bool
    event_id: str
    dismissed_at: Optional[str] = None


class DirectoryUserResult(BaseModel):
    id: str
    email: str
    display_name: str


class DirectoryUserSearchResponse(BaseModel):
    users: List[DirectoryUserResult]


class CreateCampaignDeadlineRequest(BaseModel):
    title: str = Field(..., min_length=1, max_length=200)
    description: Optional[str] = Field(None, max_length=2000)
    due_at: str = Field(..., description="ISO datetime")
    visibility: Literal["team", "personal"] = "team"
    assigned_user_id: Optional[str] = None


class CampaignDeadlineItem(BaseModel):
    id: str
    campaign_id: str
    created_by: str
    title: str
    description: Optional[str] = None
    due_at: str
    visibility: Literal["team", "personal"]
    assigned_user_id: Optional[str] = None
    created_at: Optional[str] = None
    completed_at: Optional[str] = None


class CampaignDeadlinesResponse(BaseModel):
    deadlines: List[CampaignDeadlineItem]


class TerminationResponse(BaseModel):
    status: str
    message: str
    files_deleted: int


class PostMessageRequest(BaseModel):
    content: str = Field(..., max_length=2000, description="Message content (max 2000 characters)")
    mention_user_ids: List[str] = Field(
        default_factory=list,
        description="Optional mentioned user IDs for campaign activity notifications",
    )


class UpdateMessageRequest(BaseModel):
    content: str = Field(..., max_length=2000, description="Updated message content (max 2000 characters)")


class TeamMessageResponse(BaseModel):
    id: str
    content: str
    timestamp: str
    author_id: str
    author_display_name: str
    author_avatar_url: Optional[str] = None
    edited_at: Optional[str] = None
    deleted_at: Optional[str] = None


class TeamMessagesResponse(BaseModel):
    messages: List[TeamMessageResponse]


class AddMemberRequest(BaseModel):
    email: Optional[str] = Field(None, description="Email address of the user to add")
    user_id: Optional[str] = Field(None, description="Optional Clerk/Riley user ID to add")
    role: str = Field(..., description="Role to assign: 'Member' or 'Lead'")


class MemberResponse(BaseModel):
    id: str
    email: str
    role: str


class MembersListResponse(BaseModel):
    members: List[MemberResponse]


class MentionMemberResponse(BaseModel):
    user_id: str
    display_name: str
    username: Optional[str] = None
    email: Optional[str] = None
    avatar_url: Optional[str] = None
    role: Optional[str] = None
    status: Optional[Literal["active", "away", "in_meeting"]] = None
    status_updated_at: Optional[str] = None


class MentionMembersListResponse(BaseModel):
    members: List[MentionMemberResponse]


class ChatThreadResponse(BaseModel):
    thread_id: str
    tenant_id: str
    name: Optional[str] = None
    is_private: bool
    created_by_user_id: str
    created_at: str
    has_unread: bool = False
    unread_count: int = 0


class ChatThreadsResponse(BaseModel):
    threads: List[ChatThreadResponse]
    team_comms_has_unread: bool = False
    team_comms_unread_count: int = 0


class CreateChatThreadRequest(BaseModel):
    member_user_ids: List[str] = Field(default_factory=list, description="Campaign member IDs to include in private thread")
    name: Optional[str] = Field(None, max_length=120, description="Optional thread name")


class UserStatusResponse(BaseModel):
    status: Literal["active", "away", "in_meeting"]
    updated_at: Optional[str] = None


class UpdateUserStatusRequest(BaseModel):
    status: Literal["active", "away", "in_meeting"]


class UserProfileResponse(BaseModel):
    email: str
    display_name: Optional[str] = None
    updated_at: Optional[str] = None


class UpdateUserProfileRequest(BaseModel):
    display_name: Optional[str] = Field(None, max_length=120)


async def _require_lead_for_campaign(campaign_id: str, current_user: Dict, graph: GraphService) -> str:
    """Server-side Lead authorization helper for campaign-scoped management routes."""
    requester_id = current_user.get("id", "unknown")
    requester_role = await graph.get_member_role(requester_id, campaign_id)
    if requester_role != "Lead":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only Lead members can perform this action",
        )
    return requester_id


def _extract_riley_identity_fields(current_user: Dict) -> Dict[str, Optional[str]]:
    """Extract Riley-native identity fields from verified JWT payload."""
    raw_claims = current_user.get("raw") if isinstance(current_user, dict) else None
    raw = raw_claims if isinstance(raw_claims, dict) else {}

    username = str(raw.get("username") or "").strip() or None
    display_name = (
        str(raw.get("display_name") or "").strip()
        or str(raw.get("name") or "").strip()
        or str(raw.get("full_name") or "").strip()
        or None
    )
    if not display_name:
        first_name = str(raw.get("given_name") or raw.get("first_name") or "").strip()
        last_name = str(raw.get("family_name") or raw.get("last_name") or "").strip()
        combined = f"{first_name} {last_name}".strip()
        display_name = combined or None

    return {"username": username, "display_name": display_name}


@router.get("/users/me/status", response_model=UserStatusResponse)
async def get_current_user_status(
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> UserStatusResponse:
    """Get current user's global manual status."""
    user_id = current_user.get("id", "unknown")
    status_payload = await graph.get_user_status(user_id)
    return UserStatusResponse(**status_payload)


@router.patch("/users/me/status", response_model=UserStatusResponse)
async def update_current_user_status(
    request: UpdateUserStatusRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> UserStatusResponse:
    """Update current user's global manual status."""
    user_id = current_user.get("id", "unknown")
    email = current_user.get("email")
    try:
        updated = await graph.update_user_status(
            user_id=user_id,
            status_value=request.status,
            email=email,
        )
        return UserStatusResponse(**updated)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get("/users/me/profile", response_model=UserProfileResponse)
async def get_current_user_profile(
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> UserProfileResponse:
    """Get current user's Riley profile."""
    user_id = current_user.get("id", "unknown")
    profile = await graph.get_user_profile(
        user_id=user_id,
        email_fallback=current_user.get("email"),
    )
    return UserProfileResponse(**profile)


@router.patch("/users/me/profile", response_model=UserProfileResponse)
async def update_current_user_profile(
    request: UpdateUserProfileRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> UserProfileResponse:
    """Update current user's Riley profile."""
    user_id = current_user.get("id", "unknown")
    updated = await graph.update_user_profile(
        user_id=user_id,
        email=current_user.get("email"),
        display_name=request.display_name,
    )
    return UserProfileResponse(**updated)


@router.post("/campaigns", response_model=CampaignResponse)
async def create_campaign(
    request: CreateCampaignRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph)
) -> CampaignResponse:
    """Create a new campaign and add the current user as Lead member.
    
    The creator is automatically added as a member with role "Lead".
    Returns the created campaign with the user's role.
    """
    user_id = current_user.get("id", "unknown")
    
    try:
        campaign = await graph.create_campaign(
            name=request.name,
            description=request.description,
            user_id=user_id
        )
        return CampaignResponse(**campaign)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create campaign: {exc}"
        ) from exc


@router.get("/campaigns", response_model=CampaignsListResponse)
async def get_campaigns(
    scope: str = Query("my", pattern="^(my|all)$", description="Visibility scope: my|all"),
    status_filter: Literal["active", "archived"] = Query("active", alias="status"),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph)
) -> CampaignsListResponse:
    """Get all campaigns that the current user is a member of.
    
    Returns campaigns for either:
    - scope=my: campaigns where user is a member (existing behavior)
    - scope=all: metadata-only list of all campaigns in org with access indicator
    """
    user_id = current_user.get("id", "unknown")
    
    try:
        if scope == "all":
            campaigns = await graph.get_all_campaigns_with_access(user_id, status_filter=status_filter)
        else:
            campaigns = await graph.get_user_campaigns(user_id, status_filter=status_filter)
        return CampaignsListResponse(
            campaigns=[CampaignResponse(**camp) for camp in campaigns]
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve campaigns: {exc}"
        ) from exc


@router.patch("/campaigns/{id}/archive", response_model=ArchiveCampaignResponse)
async def archive_campaign(
    id: str,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> ArchiveCampaignResponse:
    """Archive a campaign for all members (soft state change)."""
    try:
        await _require_lead_for_campaign(id, current_user, graph)
        archived = await graph.archive_campaign(campaign_id=id)
        return ArchiveCampaignResponse(
            ok=True,
            id=archived.get("id") or id,
            status="archived",
            archived_at=archived.get("archived_at"),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to archive campaign: {exc}",
        ) from exc


@router.post("/campaigns/{tenant_id}/access-requests", response_model=AccessRequestResponse)
async def request_campaign_access(
    tenant_id: str,
    request: CreateAccessRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> AccessRequestResponse:
    """Create a pending access request for a campaign.

    SECURITY:
    - Requires authentication
    - Validates campaign exists
    - Does not grant access to campaign contents
    """
    user_id = current_user.get("id", "unknown")
    try:
        identity_fields = _extract_riley_identity_fields(current_user)
        await graph.upsert_user_identity(
            user_id=user_id,
            email=current_user.get("email"),
            username=identity_fields.get("username"),
            display_name=identity_fields.get("display_name"),
        )
        created = await graph.create_access_request(
            tenant_id=tenant_id,
            requester_user_id=user_id,
            message=request.message,
        )
        return AccessRequestResponse(
            ok=True,
            request_id=created.get("id"),
            status=created.get("status"),
            created_at=created.get("created_at"),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create access request: {exc}",
        ) from exc


@router.get(
    "/campaigns/{tenant_id}/access-requests",
    response_model=CampaignAccessRequestsResponse,
)
async def list_campaign_access_requests(
    tenant_id: str,
    status_filter: Literal["pending", "approved", "denied", "all"] = Query("pending", alias="status"),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> CampaignAccessRequestsResponse:
    """List campaign access requests for leads."""
    await _require_lead_for_campaign(tenant_id, current_user, graph)
    status_value = "" if status_filter == "all" else status_filter
    items = await graph.list_campaign_access_requests(
        campaign_id=tenant_id,
        status=status_value,
        limit=100,
    )
    return CampaignAccessRequestsResponse(
        requests=[CampaignAccessRequestItem(**item) for item in items]
    )


@router.post(
    "/campaigns/{tenant_id}/access-requests/{request_id}/decision",
    response_model=CampaignAccessRequestItem,
)
async def decide_campaign_access_request(
    tenant_id: str,
    request_id: str,
    request: AccessRequestDecisionRequest,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> CampaignAccessRequestItem:
    """Approve/deny campaign access requests for leads."""
    requester_id = await _require_lead_for_campaign(tenant_id, current_user, graph)
    decision = "approved" if request.action == "approve" else "denied"
    try:
        decided = await graph.decide_campaign_access_request(
            campaign_id=tenant_id,
            request_id=request_id,
            actor_user_id=requester_id,
            decision=decision,
        )
        latest_items = await graph.list_campaign_access_requests(
            campaign_id=tenant_id,
            status="",
            limit=100,
        )
        by_id = {item.get("id"): item for item in latest_items}
        enriched = by_id.get(request_id) or {
            "id": decided.get("id"),
            "campaign_id": tenant_id,
            "user_id": decided.get("user_id"),
            "user_email": "",
            "user_name": decided.get("user_id") or "",
            "message": None,
            "status": decided.get("status") or decision,
            "created_at": decided.get("created_at"),
            "decided_at": decided.get("decided_at"),
            "decided_by": decided.get("decided_by"),
        }
        return CampaignAccessRequestItem(**enriched)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc


@router.get("/campaigns/{tenant_id}/events", response_model=CampaignEventsResponse)
async def list_campaign_events(
    tenant_id: str,
    limit: int = Query(50, ge=1, le=200),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> CampaignEventsResponse:
    """List recent campaign events for activity/feed surfaces."""
    viewer_user_id = current_user.get("id", "unknown")
    events = await graph.list_campaign_events(
        campaign_id=tenant_id,
        limit=limit,
        viewer_user_id=viewer_user_id,
    )
    logger.info(
        "notification_delivered surface=campaign_events campaign_id=%s user_id=%s count=%d",
        tenant_id, viewer_user_id, len(events),
    )
    try:
        delivered_ids = [e.get("id") for e in events if e.get("id")]
        missed = await graph.mark_events_seen_and_detect_missed(
            user_id=viewer_user_id,
            delivered_event_ids=delivered_ids,
        )
        for m in missed:
            logger.warning(
                "notification_missed event_id=%s user_id=%s event_type=%s campaign_id=%s created_at=%s",
                m.get("event_id"), viewer_user_id, m.get("event_type"),
                m.get("campaign_id"), m.get("created_at"),
            )
    except Exception:
        logger.debug("mark_events_seen failed for user_id=%s", viewer_user_id, exc_info=True)
    return CampaignEventsResponse(events=[CampaignEventItem(**item) for item in events])


@router.get("/campaigns/events/feed", response_model=UserCampaignFeedResponse)
async def list_user_campaign_feed(
    limit: int = Query(50, ge=1, le=200),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> UserCampaignFeedResponse:
    """List cross-campaign Riley Bot feed events relevant to current user."""
    user_id = current_user.get("id", "unknown")
    events = await graph.list_user_campaign_feed_events(user_id=user_id, limit=limit)
    logger.info(
        "notification_delivered surface=intelligence_feed user_id=%s count=%d",
        user_id, len(events),
    )
    try:
        delivered_ids = [e.get("id") for e in events if e.get("id")]
        missed = await graph.mark_events_seen_and_detect_missed(
            user_id=user_id,
            delivered_event_ids=delivered_ids,
        )
        for m in missed:
            logger.warning(
                "notification_missed event_id=%s user_id=%s event_type=%s campaign_id=%s created_at=%s",
                m.get("event_id"), user_id, m.get("event_type"),
                m.get("campaign_id"), m.get("created_at"),
            )
    except Exception:
        logger.debug("mark_events_seen failed for user_id=%s", user_id, exc_info=True)
    return UserCampaignFeedResponse(
        events=[UserCampaignFeedEventItem(**item) for item in events]
    )


@router.post("/campaigns/{tenant_id}/events/{event_id}/dismiss", response_model=DismissFeedEventResponse)
async def dismiss_campaign_event_for_user(
    tenant_id: str,
    event_id: str,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> DismissFeedEventResponse:
    """Dismiss a campaign activity event for current user only."""
    user_id = current_user.get("id", "unknown")
    try:
        result = await graph.dismiss_user_feed_event(
            user_id=user_id,
            event_id=event_id,
            campaign_id=tenant_id,
        )
        return DismissFeedEventResponse(
            ok=True,
            event_id=result.get("event_id") or event_id,
            dismissed_at=result.get("dismissed_at"),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post("/campaigns/events/feed/{event_id}/dismiss", response_model=DismissFeedEventResponse)
async def dismiss_user_campaign_feed_event(
    event_id: str,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> DismissFeedEventResponse:
    """Dismiss a feed event for current user only."""
    user_id = current_user.get("id", "unknown")
    try:
        result = await graph.dismiss_user_feed_event(user_id=user_id, event_id=event_id)
        return DismissFeedEventResponse(
            ok=True,
            event_id=result.get("event_id") or event_id,
            dismissed_at=result.get("dismissed_at"),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.post("/campaigns/{tenant_id}/deadlines", response_model=CampaignDeadlineItem)
async def create_campaign_deadline(
    tenant_id: str,
    request: CreateCampaignDeadlineRequest,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> CampaignDeadlineItem:
    """Create a manual campaign deadline (team or personal)."""
    user_id = current_user.get("id", "unknown")
    try:
        due_at_value = request.due_at.strip()
        parsed = datetime.fromisoformat(due_at_value.replace("Z", "+00:00"))
        due_at_iso = parsed.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid due_at datetime: {exc}",
        ) from exc

    try:
        created = await graph.create_campaign_deadline(
            campaign_id=tenant_id,
            created_by=user_id,
            title=request.title,
            description=request.description,
            due_at=due_at_iso,
            visibility=request.visibility,
            assigned_user_id=request.assigned_user_id,
        )
        campaign_name = tenant_id
        try:
            campaigns = await graph.get_user_campaigns(user_id)
            for camp in campaigns:
                if camp.get("id") == tenant_id:
                    campaign_name = camp.get("name") or tenant_id
                    break
        except Exception:
            pass
        await graph.create_campaign_event(
            campaign_id=tenant_id,
            event_type="deadline_created",
            message=f"Deadline created: {created.get('title')} ({campaign_name})",
            user_id=created.get("assigned_user_id") if created.get("visibility") == "personal" else None,
            actor_user_id=user_id,
        )
        return CampaignDeadlineItem(**created)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get("/campaigns/{tenant_id}/deadlines", response_model=CampaignDeadlinesResponse)
async def list_campaign_deadlines(
    tenant_id: str,
    include_past: bool = Query(False, description="Include past deadlines"),
    limit: int = Query(100, ge=1, le=300),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> CampaignDeadlinesResponse:
    """List visible deadlines for a campaign, nearest due first."""
    user_id = current_user.get("id", "unknown")
    deadlines = await graph.list_campaign_deadlines(
        campaign_id=tenant_id,
        viewer_user_id=user_id,
        limit=limit,
        include_past=include_past,
    )
    return CampaignDeadlinesResponse(
        deadlines=[CampaignDeadlineItem(**item) for item in deadlines]
    )


@router.post(
    "/campaigns/{tenant_id}/deadlines/{deadline_id}/complete",
    response_model=CampaignDeadlineItem,
)
async def complete_campaign_deadline(
    tenant_id: str,
    deadline_id: str,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> CampaignDeadlineItem:
    """Mark a visible deadline complete (idempotent)."""
    user_id = current_user.get("id", "unknown")
    try:
        completed = await graph.complete_campaign_deadline(
            campaign_id=tenant_id,
            deadline_id=deadline_id,
            viewer_user_id=user_id,
        )
        return CampaignDeadlineItem(**completed)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get("/campaigns/{tenant_id}/users/search", response_model=DirectoryUserSearchResponse)
async def search_campaign_users(
    tenant_id: str,
    query: str = Query(..., min_length=2, max_length=120),
    limit: int = Query(8, ge=1, le=20),
    current_user: Dict = Depends(verify_tenant_access),
) -> DirectoryUserSearchResponse:
    """Search Clerk users globally for direct campaign member add."""
    clerk_users = search_users(query=query, limit=limit)
    users = [DirectoryUserResult(**u) for u in clerk_users[: max(1, min(int(limit), 20))]]
    logger.info("campaign_user_search_completed query=%s count=%d", query.strip().lower(), len(users))
    return DirectoryUserSearchResponse(users=users)


@router.delete("/campaign/{tenant_id}", response_model=TerminationResponse)
async def terminate_campaign(
    tenant_id: str,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> TerminationResponse:
    """
    Permanently delete a campaign and ALL its associated data.
    
    The Ghostbuster Protocol:
    1. Security: Prevents deleting "global" tenant
    2. Vector Wipe: Deletes all Qdrant vectors for the tenant
    3. Storage Wipe: Deletes all GCS files for the tenant
    
    This is an irreversible action. Use archive for soft delete.
    """
    # Log user_id + tenant_id for every request
    user_id = current_user.get("id", "unknown")
    print(f"Terminate campaign request: user_id={user_id}, tenant_id={tenant_id}")
    
    # Security: Prevent deleting global tenant
    if tenant_id == "global":
        raise HTTPException(
            status_code=403,
            detail="Cannot terminate 'global' tenant. This is a protected system tenant."
        )
    await _require_lead_for_campaign(tenant_id, current_user, graph)
    
    try:
        # Step 1: Get all file URLs from Qdrant before deletion
        file_urls = await vector_service.delete_tenant_data(tenant_id)
        
        # Step 2: Delete all files from GCS
        if file_urls:
            await StorageService.delete_batch(file_urls)
        
        return TerminationResponse(
            status="success",
            message=f"Campaign {tenant_id} terminated successfully. All data has been permanently deleted.",
            files_deleted=len(file_urls)
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc)
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to terminate campaign: {exc}"
        ) from exc


@router.get("/campaigns/{id}/chat", response_model=TeamMessagesResponse)
async def get_team_messages(
    id: str,
    since: Optional[str] = Query(None, description="ISO timestamp - only return messages after this time"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of messages to return"),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph)
) -> TeamMessagesResponse:
    """Get team messages for a campaign.
    
    SECURITY: Requires authentication and campaign membership (enforced by verify_tenant_access).
    The tenant_id is extracted from the path parameter {id}.
    
    Args:
        id: Campaign ID (extracted from path)
        since: Optional ISO timestamp - only return messages after this time
        limit: Maximum number of messages to return (1-100, default: 50)
        current_user: Authenticated user (from verify_tenant_access)
        
    Returns:
        List of team messages ordered chronologically (oldest -> newest)
    """
    try:
        user_id = current_user.get("id", "unknown")
        messages = await graph.get_team_messages(
            campaign_id=id,
            limit=limit,
            since=since,
            user_id=user_id,
        )
        
        return TeamMessagesResponse(
            messages=[TeamMessageResponse(**msg) for msg in messages]
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve team messages: {exc}"
        ) from exc


@router.post("/campaigns/{id}/chat", response_model=TeamMessageResponse)
async def post_team_message(
    id: str,
    request: PostMessageRequest,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph)
) -> TeamMessageResponse:
    """Post a team message to a campaign.
    
    SECURITY: Requires authentication and campaign membership (enforced by verify_tenant_access).
    The tenant_id is extracted from the path parameter {id}.
    Author identity is derived from the verified current_user, not from request body.
    
    Args:
        id: Campaign ID (extracted from path)
        request: Message content
        current_user: Authenticated user (from verify_tenant_access)
        
    Returns:
        Created message object
    """
    try:
        identity_fields = _extract_riley_identity_fields(current_user)
        await graph.upsert_user_identity(
            user_id=current_user.get("id"),
            email=current_user.get("email"),
            username=identity_fields.get("username"),
            display_name=identity_fields.get("display_name"),
        )
        message = await graph.post_team_message(
            campaign_id=id,
            user=current_user,
            content=request.content,
            mention_user_ids=request.mention_user_ids,
        )
        
        return TeamMessageResponse(**message)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc)
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to post team message: {exc}"
        ) from exc


@router.patch("/campaigns/{tenant_id}/messages/{message_id}", response_model=TeamMessageResponse)
async def update_team_message(
    tenant_id: str,
    message_id: str,
    request: UpdateMessageRequest,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph)
) -> TeamMessageResponse:
    """Edit a team message (author-only)."""
    user_id = current_user.get("id", "unknown")

    try:
        message = await graph.update_team_message(
            campaign_id=tenant_id,
            message_id=message_id,
            user_id=user_id,
            content=request.content,
        )
        return TeamMessageResponse(**message)
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        ) from exc
    except ValueError as exc:
        detail = str(exc)
        if "not found" in detail.lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=detail,
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update team message: {exc}",
        ) from exc


@router.delete("/campaigns/{tenant_id}/messages/{message_id}", response_model=TeamMessageResponse)
async def delete_team_message(
    tenant_id: str,
    message_id: str,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph)
) -> TeamMessageResponse:
    """Soft-delete a team message (author-only)."""
    user_id = current_user.get("id", "unknown")

    try:
        message = await graph.soft_delete_team_message(
            campaign_id=tenant_id,
            message_id=message_id,
            user_id=user_id,
        )
        return TeamMessageResponse(**message)
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        ) from exc
    except ValueError as exc:
        detail = str(exc)
        if "not found" in detail.lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=detail,
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=detail,
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete team message: {exc}",
        ) from exc


@router.get("/campaigns/{tenant_id}/chat-threads", response_model=ChatThreadsResponse)
async def list_chat_threads(
    tenant_id: str,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> ChatThreadsResponse:
    """List private campaign chat threads that include current user."""
    user_id = current_user.get("id", "unknown")
    try:
        threads = await graph.list_private_chat_threads(
            campaign_id=tenant_id,
            user_id=user_id,
        )
        team_comms = await graph.get_team_comms_unread_status(
            campaign_id=tenant_id,
            user_id=user_id,
        )
        return ChatThreadsResponse(
            threads=[ChatThreadResponse(**thread) for thread in threads],
            team_comms_has_unread=bool(team_comms.get("has_unread")),
            team_comms_unread_count=int(team_comms.get("unread_count") or 0),
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list chat threads: {exc}",
        ) from exc


@router.post("/campaigns/{tenant_id}/chat-threads", response_model=ChatThreadResponse)
async def create_chat_thread(
    tenant_id: str,
    request: CreateChatThreadRequest,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> ChatThreadResponse:
    """Create a private campaign-scoped chat thread."""
    user_id = current_user.get("id", "unknown")
    try:
        thread = await graph.create_private_chat_thread(
            campaign_id=tenant_id,
            created_by_user_id=user_id,
            member_user_ids=request.member_user_ids,
            name=request.name,
        )
        return ChatThreadResponse(**thread)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create chat thread: {exc}",
        ) from exc


@router.get("/campaigns/{tenant_id}/chat-threads/{thread_id}/messages", response_model=TeamMessagesResponse)
async def get_chat_thread_messages(
    tenant_id: str,
    thread_id: str,
    since: Optional[str] = Query(None, description="ISO timestamp - only return messages after this time"),
    limit: int = Query(50, ge=1, le=100, description="Maximum number of messages to return"),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> TeamMessagesResponse:
    """Get messages for a private campaign thread (thread members only)."""
    user_id = current_user.get("id", "unknown")
    try:
        messages = await graph.get_private_thread_messages(
            campaign_id=tenant_id,
            thread_id=thread_id,
            user_id=user_id,
            limit=limit,
            since=since,
        )
        return TeamMessagesResponse(
            messages=[TeamMessageResponse(**msg) for msg in messages]
        )
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get thread messages: {exc}",
        ) from exc


@router.post("/campaigns/{tenant_id}/chat-threads/{thread_id}/messages", response_model=TeamMessageResponse)
async def post_chat_thread_message(
    tenant_id: str,
    thread_id: str,
    request: PostMessageRequest,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph),
) -> TeamMessageResponse:
    """Post message to a private campaign thread (thread members only)."""
    user_id = current_user.get("id", "unknown")
    try:
        identity_fields = _extract_riley_identity_fields(current_user)
        await graph.upsert_user_identity(
            user_id=user_id,
            email=current_user.get("email"),
            username=identity_fields.get("username"),
            display_name=identity_fields.get("display_name"),
        )
        message = await graph.post_private_thread_message(
            campaign_id=tenant_id,
            thread_id=thread_id,
            user_id=user_id,
            content=request.content,
            mention_user_ids=request.mention_user_ids,
        )
        return TeamMessageResponse(**message)
    except PermissionError as exc:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=str(exc),
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to post thread message: {exc}",
        ) from exc


@router.get(
    "/campaigns/{id}/members",
    response_model=Union[MembersListResponse, MentionMembersListResponse],
)
async def get_campaign_members(
    id: str,
    mentions_only: bool = Query(
        False,
        description="When true, returns mention-safe member fields only",
    ),
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph)
) -> Union[MembersListResponse, MentionMembersListResponse]:
    """Get all members of a campaign.
    
    SECURITY: Requires authentication and campaign membership (enforced by verify_tenant_access).
    The tenant_id is extracted from the path parameter {id}.
    
    Args:
        id: Campaign ID (extracted from path)
        current_user: Authenticated user (from verify_tenant_access)
        
    Returns:
        List of campaign members with their roles
    """
    try:
        if mentions_only:
            members = await graph.list_campaign_members_for_mentions(campaign_id=id)
            return MentionMembersListResponse(
                members=[MentionMemberResponse(**member) for member in members]
            )

        members = await graph.list_campaign_members(campaign_id=id)
        return MembersListResponse(
            members=[MemberResponse(**member) for member in members]
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve campaign members: {exc}"
        ) from exc


@router.post("/campaigns/{id}/members", response_model=MembersListResponse)
async def add_campaign_member(
    id: str,
    request: AddMemberRequest,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph)
) -> MembersListResponse:
    """Add a member to a campaign.
    
    SECURITY: 
    - Requires authentication and campaign membership (enforced by verify_tenant_access)
    - Additional authorization: requester must be Lead for this campaign (403 otherwise)
    - Resolves target user by email using Clerk directory
    - Returns 404 if user not found in Clerk
    
    Args:
        id: Campaign ID (extracted from path)
        request: Member addition request with email and role
        current_user: Authenticated user (from verify_tenant_access)
        
    Returns:
        Updated list of campaign members
    """
    user_id = current_user.get("id", "unknown")
    
    # Validate role
    if request.role not in ["Member", "Lead"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role must be 'Member' or 'Lead'"
        )
    
    # Check if requester is Lead
    try:
        user_id = await _require_lead_for_campaign(id, current_user, graph)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check requester role: {exc}"
        ) from exc
    
    normalized_email = (request.email or "").strip()
    normalized_user_id = (request.user_id or "").strip()
    if not normalized_email and not normalized_user_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Either email or user_id is required",
        )

    # Resolve user by user_id/email using Clerk
    try:
        clerk_user = find_user_by_id(normalized_user_id) if normalized_user_id else None
        if clerk_user is None:
            clerk_user = find_user_by_email(normalized_email)
        if not clerk_user:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found, ask them to sign up first"
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to resolve user by email: {exc}"
        ) from exc
    
    # Add member to campaign
    try:
        added = await graph.add_campaign_member(
            campaign_id=id,
            target_user_id=clerk_user["id"],
            target_email=clerk_user["email"],
            role=request.role,
            target_first_name=clerk_user.get("first_name"),
            target_last_name=clerk_user.get("last_name"),
        )
        await graph.upsert_user_identity(
            user_id=clerk_user.get("id"),
            email=clerk_user.get("email"),
            username=clerk_user.get("username"),
            display_name=clerk_user.get("display_name"),
        )
        if not bool(added.get("already_member")):
            campaign_name = (added.get("campaign_name") or "").strip() or id
            await graph.create_campaign_event(
                campaign_id=id,
                event_type="campaign_member_added",
                message=f"You were added to Campaign {campaign_name}",
                user_id=clerk_user["id"],
                actor_user_id=user_id,
            )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc)
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add member: {exc}"
        ) from exc
    
    # Return updated member list
    try:
        members = await graph.list_campaign_members(campaign_id=id)
        return MembersListResponse(
            members=[MemberResponse(**member) for member in members]
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve updated members: {exc}"
        ) from exc


@router.delete("/campaigns/{id}/members/{user_id}", response_model=MembersListResponse)
async def remove_campaign_member(
    id: str,
    user_id: str,
    current_user: Dict = Depends(verify_tenant_access),
    graph: GraphService = Depends(get_graph)
) -> MembersListResponse:
    """Remove a member from a campaign.
    
    SECURITY:
    - Requires authentication and campaign membership (enforced by verify_tenant_access)
    - Additional authorization: requester must be Lead for this campaign (403 otherwise)
    
    Args:
        id: Campaign ID (extracted from path)
        user_id: ID of the user to remove
        current_user: Authenticated user (from verify_tenant_access)
        
    Returns:
        Updated list of campaign members
    """
    requester_id = current_user.get("id", "unknown")
    
    # Check if requester is Lead
    try:
        requester_role = await graph.get_member_role(requester_id, id)
        if requester_role != "Lead":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only Lead members can remove members from a campaign"
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check requester role: {exc}"
        ) from exc
    
    # Remove member from campaign
    try:
        await graph.remove_campaign_member(
            campaign_id=id,
            target_user_id=user_id
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc)
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to remove member: {exc}"
        ) from exc
    
    # Return updated member list
    try:
        members = await graph.list_campaign_members(campaign_id=id)
        return MembersListResponse(
            members=[MemberResponse(**member) for member in members]
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve updated members: {exc}"
        ) from exc
