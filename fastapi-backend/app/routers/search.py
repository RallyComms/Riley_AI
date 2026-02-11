from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Depends, Request
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

from app.core.config import get_settings
from app.dependencies.auth import verify_clerk_token, check_tenant_membership
from app.services.genai_client import get_genai_client
from app.services.qdrant import vector_service


router = APIRouter()


class SearchRequest(BaseModel):
    query_text: str = Field(..., max_length=2000, description="Search query text (max 2000 characters)")
    tenant_id: str = Field(..., max_length=50, description="Tenant/client identifier (max 50 characters)")


class SearchResponse(BaseModel):
    results: List[Dict[str, Any]]


class GlobalSearchRequest(BaseModel):
    """Request model for global (firm-wide) search.

    Note: This endpoint is NOT tenant-scoped and does not require tenant membership.
    """

    query_text: str = Field(
        ...,
        max_length=2000,
        description="Search query text (max 2000 characters)",
    )
    limit: int = Field(
        5,
        ge=1,
        le=20,
        description="Maximum number of results to return (1-20, default 5)",
    )


def _embed_query_text_sync(text: str) -> List[float]:
    """Embed the query text using Gemini text embeddings.
    
    Uses the new google-genai SDK via the shared client.
    This function is synchronous and should be run in a threadpool.
    """
    settings = get_settings()
    model_name = settings.EMBEDDING_MODEL
    
    try:
        # Get client (lazy initialization)
        client = get_genai_client()
        # Use the shared client to generate embedding
        response = client.models.embed_content(
            model=model_name,
            contents=text
        )
        
        # Extract embedding vector from response
        if not response.embeddings or len(response.embeddings) == 0:
            raise RuntimeError(
                f"Embedding response did not contain any embeddings for model '{model_name}'."
            )
        
        embedding = response.embeddings[0].values
        if not isinstance(embedding, list):
            raise RuntimeError(
                f"Embedding response did not contain a valid vector for model '{model_name}'."
            )
        
        return embedding
    except RuntimeError:
        # Re-raise RuntimeError as-is
        raise
    except Exception as exc:
        error_msg = (
            f"Query embedding failed using model '{model_name}': {exc}"
        )
        raise RuntimeError(error_msg) from exc


async def _embed_query_text(content: str) -> List[float]:
    """Embed the query text using Gemini text embeddings.

    Wraps _embed_query_text_sync in a threadpool for async execution.
    """
    try:
        # Run blocking GenAI call in threadpool to avoid blocking event loop
        return await run_in_threadpool(_embed_query_text_sync, content)
    except RuntimeError:
        # Re-raise RuntimeError as-is
        raise
    except Exception as exc:
        settings = get_settings()
        model_name = settings.EMBEDDING_MODEL
        error_msg = (
            f"Query embedding failed using model '{model_name}': {exc}"
        )
        raise RuntimeError(error_msg) from exc


@router.post("/search", response_model=SearchResponse)
async def search(
    request: SearchRequest,
    http_request: Request,
    current_user: Dict = Depends(verify_clerk_token)
) -> SearchResponse:
    """Search tenant-scoped vectors using a natural language query.
    
    SECURITY: Tenant membership is enforced after parsing the request body.
    """
    # Verify tenant membership (tenant_id comes from request body)
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id, http_request)
    
    try:
        query_vector = await _embed_query_text(request.query_text)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Embedding generation failed: {exc}"
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error during embedding: {exc}"
        ) from exc

    settings = get_settings()

    results = await vector_service.search_silo(
        collection_name=settings.QDRANT_COLLECTION_TIER_2,
        query_vector=query_vector,
        tenant_id=request.tenant_id,
    )

    return SearchResponse(results=results)


@router.post("/search/global", response_model=SearchResponse)
async def search_global(
    request: GlobalSearchRequest,
    current_user: Dict = Depends(verify_clerk_token),
) -> SearchResponse:
    """Search global Tier 1 vectors using a natural language query.

    SECURITY: This endpoint is authenticated via Clerk but is NOT tenant-scoped.
    Only documents explicitly marked as global (is_global == true) are searched.
    """
    try:
        query_vector = await _embed_query_text(request.query_text)
    except RuntimeError as exc:
        # Explicit 500 when embedding fails
        raise HTTPException(
            status_code=500,
            detail=f"Embedding generation failed: {exc}",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error during embedding: {exc}",
        ) from exc

    settings = get_settings()

    # Validate embedding dimension matches expected dimension
    expected_dim = settings.EMBEDDING_DIM
    if len(query_vector) != expected_dim:
        raise HTTPException(
            status_code=500,
            detail=(
                f"Embedding dimension mismatch: got {len(query_vector)} expected {expected_dim}. "
                f"Check EMBEDDING_MODEL/EMBEDDING_DIM vs Qdrant collection dim."
            ),
        )

    # SECURITY: Always require is_global == true filter for global search
    global_filter = Filter(
        must=[
            FieldCondition(
                key="is_global",
                match=MatchValue(value=True),
            )
        ]
    )

    results = await vector_service.search_global(
        collection_name=settings.QDRANT_COLLECTION_TIER_1,
        query_vector=query_vector,
        limit=request.limit,
        filter=global_filter,
    )

    return SearchResponse(results=results)



