from typing import Dict, List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, status
from pydantic import BaseModel, Field

from app.dependencies.auth import verify_tenant_access, verify_clerk_token
from app.services.graph import graph_service
from app.services.qdrant import vector_service
from app.services.storage import StorageService
from app.services.clerk_directory import find_user_by_email


router = APIRouter()


class CreateCampaignRequest(BaseModel):
    name: str = Field(..., max_length=200, description="Campaign name")
    description: Optional[str] = Field(None, max_length=1000, description="Campaign description")


class CampaignResponse(BaseModel):
    id: str
    name: str
    description: Optional[str] = None
    role: str


class CampaignsListResponse(BaseModel):
    campaigns: List[CampaignResponse]


class TerminationResponse(BaseModel):
    status: str
    message: str
    files_deleted: int


class PostMessageRequest(BaseModel):
    content: str = Field(..., max_length=2000, description="Message content (max 2000 characters)")


class TeamMessageResponse(BaseModel):
    id: str
    content: str
    timestamp: str
    author_id: str


class TeamMessagesResponse(BaseModel):
    messages: List[TeamMessageResponse]


class AddMemberRequest(BaseModel):
    email: str = Field(..., description="Email address of the user to add")
    role: str = Field(..., description="Role to assign: 'Member' or 'Lead'")


class MemberResponse(BaseModel):
    id: str
    email: str
    role: str


class MembersListResponse(BaseModel):
    members: List[MemberResponse]


@router.post("/campaigns", response_model=CampaignResponse)
async def create_campaign(
    request: CreateCampaignRequest,
    current_user: Dict = Depends(verify_clerk_token)
) -> CampaignResponse:
    """Create a new campaign and add the current user as Lead member.
    
    The creator is automatically added as a member with role "Lead".
    Returns the created campaign with the user's role.
    """
    if graph_service is None:
        raise HTTPException(
            status_code=503,
            detail="Graph service not initialized"
        )
    
    user_id = current_user.get("id", "unknown")
    
    try:
        campaign = await graph_service.create_campaign(
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
    current_user: Dict = Depends(verify_clerk_token)
) -> CampaignsListResponse:
    """Get all campaigns that the current user is a member of.
    
    Returns a list of campaigns with the user's role in each campaign.
    """
    if graph_service is None:
        raise HTTPException(
            status_code=503,
            detail="Graph service not initialized"
        )
    
    user_id = current_user.get("id", "unknown")
    
    try:
        campaigns = await graph_service.get_user_campaigns(user_id)
        return CampaignsListResponse(
            campaigns=[CampaignResponse(**camp) for camp in campaigns]
        )
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve campaigns: {exc}"
        ) from exc


@router.delete("/campaign/{tenant_id}", response_model=TerminationResponse)
async def terminate_campaign(
    tenant_id: str,
    current_user: Dict = Depends(verify_tenant_access)
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
    current_user: Dict = Depends(verify_tenant_access)
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
    if graph_service is None:
        raise HTTPException(
            status_code=503,
            detail="Graph service not initialized"
        )
    
    try:
        messages = await graph_service.get_team_messages(
            campaign_id=id,
            limit=limit,
            since=since
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
    current_user: Dict = Depends(verify_tenant_access)
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
    if graph_service is None:
        raise HTTPException(
            status_code=503,
            detail="Graph service not initialized"
        )
    
    try:
        message = await graph_service.post_team_message(
            campaign_id=id,
            user=current_user,
            content=request.content
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


@router.get("/campaigns/{id}/members", response_model=MembersListResponse)
async def get_campaign_members(
    id: str,
    current_user: Dict = Depends(verify_tenant_access)
) -> MembersListResponse:
    """Get all members of a campaign.
    
    SECURITY: Requires authentication and campaign membership (enforced by verify_tenant_access).
    The tenant_id is extracted from the path parameter {id}.
    
    Args:
        id: Campaign ID (extracted from path)
        current_user: Authenticated user (from verify_tenant_access)
        
    Returns:
        List of campaign members with their roles
    """
    if graph_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Graph service not initialized"
        )
    
    try:
        members = await graph_service.list_campaign_members(campaign_id=id)
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
    current_user: Dict = Depends(verify_tenant_access)
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
    if graph_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Graph service not initialized"
        )
    
    user_id = current_user.get("id", "unknown")
    
    # Validate role
    if request.role not in ["Member", "Lead"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role must be 'Member' or 'Lead'"
        )
    
    # Check if requester is Lead
    try:
        requester_role = await graph_service.get_member_role(user_id, id)
        if requester_role != "Lead":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only Lead members can add members to a campaign"
            )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check requester role: {exc}"
        ) from exc
    
    # Resolve user by email using Clerk
    try:
        clerk_user = find_user_by_email(request.email)
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
        await graph_service.add_campaign_member(
            campaign_id=id,
            target_user_id=clerk_user["id"],
            target_email=clerk_user["email"],
            role=request.role
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
        members = await graph_service.list_campaign_members(campaign_id=id)
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
    current_user: Dict = Depends(verify_tenant_access)
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
    if graph_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Graph service not initialized"
        )
    
    requester_id = current_user.get("id", "unknown")
    
    # Check if requester is Lead
    try:
        requester_role = await graph_service.get_member_role(requester_id, id)
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
        await graph_service.remove_campaign_member(
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
        members = await graph_service.list_campaign_members(campaign_id=id)
        return MembersListResponse(
            members=[MemberResponse(**member) for member in members]
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve updated members: {exc}"
        ) from exc
