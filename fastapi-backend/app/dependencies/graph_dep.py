from typing import Optional
from fastapi import Request, HTTPException, status
from app.services.graph import GraphService


async def get_graph(request: Request) -> GraphService:
    """Dependency injection for GraphService.
    
    Retrieves the GraphService instance from app.state, which is initialized
    during application startup. This ensures all workers use the same initialized
    service instance.
    
    Args:
        request: FastAPI request object containing app.state
        
    Returns:
        GraphService instance
        
    Raises:
        HTTPException(503): If GraphService is not initialized
    """
    gs = getattr(request.app.state, "graph", None)
    if gs is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Graph service not initialized"
        )
    return gs


async def get_graph_optional(request: Request) -> Optional[GraphService]:
    """Optional dependency injection for GraphService.
    
    Returns None if GraphService is not initialized, instead of raising an exception.
    Use this for endpoints where graph functionality is optional.
    
    Args:
        request: FastAPI request object containing app.state
        
    Returns:
        GraphService instance or None if not initialized
    """
    return getattr(request.app.state, "graph", None)
