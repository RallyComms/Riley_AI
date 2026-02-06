from typing import Any, Dict, List

from fastapi import APIRouter, HTTPException, Depends
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

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
    current_user: Dict = Depends(verify_clerk_token)
) -> SearchResponse:
    """Search tenant-scoped vectors using a natural language query.
    
    SECURITY: Tenant membership is enforced after parsing the request body.
    """
    # Verify tenant membership (tenant_id comes from request body)
    user_id = current_user.get("id", "unknown")
    await check_tenant_membership(user_id, request.tenant_id)
    
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



