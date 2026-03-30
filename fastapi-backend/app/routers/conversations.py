from typing import Dict, List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from app.dependencies.auth import verify_clerk_token
from app.dependencies.graph_dep import get_graph
from app.services.graph import GraphService

router = APIRouter()


class ConversationParticipantItem(BaseModel):
    user_id: str
    display_name: str


class ConversationItem(BaseModel):
    id: str
    type: Literal["public", "private"]
    campaign_id: str
    created_by: str
    created_at: str
    name: Optional[str] = None
    joined_at: Optional[str] = None
    history_access: Optional[Literal["full", "from_join"]] = None
    unread_count: int = 0
    participants: List[ConversationParticipantItem] = Field(default_factory=list)


class ConversationListResponse(BaseModel):
    conversations: List[ConversationItem]


class CreateConversationRequest(BaseModel):
    type: Literal["private"] = "private"
    campaign_id: str
    participants: List[str] = Field(default_factory=list)
    name: Optional[str] = None


class AddConversationUserRequest(BaseModel):
    user_id: str
    history_access: Literal["full", "from_join"] = "from_join"


class AddConversationUserResponse(BaseModel):
    ok: bool
    conversation_id: str
    campaign_id: str
    user_id: str
    joined_at: Optional[str] = None
    history_access: Literal["full", "from_join"]


class LeaveConversationResponse(BaseModel):
    ok: bool
    conversation_id: str
    campaign_id: str


class ConversationMessageItem(BaseModel):
    id: str
    conversation_id: str
    content: str
    created_at: str
    author_id: str
    author_display_name: str
    author_avatar_url: Optional[str] = None


class ConversationMessagesResponse(BaseModel):
    messages: List[ConversationMessageItem]


class PostConversationMessageRequest(BaseModel):
    campaign_id: str
    content: str = Field(..., max_length=2000)
    mention_user_ids: List[str] = Field(default_factory=list)


@router.get("/conversations", response_model=ConversationListResponse)
async def list_conversations(
    campaign_id: str = Query(..., min_length=1),
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> ConversationListResponse:
    user_id = current_user.get("id", "unknown")
    is_member = await graph.check_membership(user_id, campaign_id)
    if not is_member:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied to campaign {campaign_id}. You are not a member of this campaign.",
        )
    conversations = await graph.list_conversations(campaign_id=campaign_id, user_id=user_id)
    return ConversationListResponse(
        conversations=[ConversationItem(**row) for row in conversations]
    )


@router.post("/conversations", response_model=ConversationItem)
async def create_conversation(
    request: CreateConversationRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> ConversationItem:
    user_id = current_user.get("id", "unknown")
    is_member = await graph.check_membership(user_id, request.campaign_id)
    if not is_member:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied to campaign {request.campaign_id}. You are not a member of this campaign.",
        )
    try:
        conversation = await graph.create_private_conversation(
            campaign_id=request.campaign_id,
            created_by=user_id,
            participant_user_ids=request.participants,
            name=request.name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return ConversationItem(**conversation)


@router.post("/conversations/{conversation_id}/add-user", response_model=AddConversationUserResponse)
async def add_conversation_user(
    conversation_id: str,
    request: AddConversationUserRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> AddConversationUserResponse:
    actor_user_id = current_user.get("id", "unknown")
    try:
        result = await graph.add_user_to_conversation(
            conversation_id=conversation_id,
            actor_user_id=actor_user_id,
            target_user_id=request.user_id,
            history_access=request.history_access,
        )
    except ValueError as exc:
        message = str(exc)
        code = status.HTTP_403_FORBIDDEN if "permission" in message.lower() else status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=code, detail=message) from exc
    return AddConversationUserResponse(
        ok=True,
        conversation_id=result.get("conversation_id"),
        campaign_id=result.get("campaign_id"),
        user_id=result.get("user_id"),
        joined_at=result.get("joined_at"),
        history_access=result.get("history_access") or "from_join",
    )


@router.post("/conversations/{conversation_id}/leave", response_model=LeaveConversationResponse)
async def leave_conversation(
    conversation_id: str,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> LeaveConversationResponse:
    user_id = current_user.get("id", "unknown")
    try:
        result = await graph.leave_conversation(conversation_id=conversation_id, user_id=user_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return LeaveConversationResponse(
        ok=True,
        conversation_id=result.get("conversation_id"),
        campaign_id=result.get("campaign_id"),
    )


@router.get("/conversations/{conversation_id}/messages", response_model=ConversationMessagesResponse)
async def get_conversation_messages(
    conversation_id: str,
    campaign_id: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=200),
    since: Optional[str] = None,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> ConversationMessagesResponse:
    user_id = current_user.get("id", "unknown")
    messages = await graph.get_conversation_messages(
        conversation_id=conversation_id,
        campaign_id=campaign_id,
        user_id=user_id,
        limit=limit,
        since=since,
    )
    return ConversationMessagesResponse(
        messages=[ConversationMessageItem(**row) for row in messages]
    )


@router.post("/conversations/{conversation_id}/messages", response_model=ConversationMessageItem)
async def post_conversation_message(
    conversation_id: str,
    request: PostConversationMessageRequest,
    current_user: Dict = Depends(verify_clerk_token),
    graph: GraphService = Depends(get_graph),
) -> ConversationMessageItem:
    try:
        message = await graph.post_conversation_message(
            conversation_id=conversation_id,
            campaign_id=request.campaign_id,
            user=current_user,
            content=request.content,
            mention_user_ids=request.mention_user_ids,
        )
    except ValueError as exc:
        text = str(exc)
        code = status.HTTP_403_FORBIDDEN if "permission" in text.lower() else status.HTTP_400_BAD_REQUEST
        raise HTTPException(status_code=code, detail=text) from exc
    return ConversationMessageItem(**message)
