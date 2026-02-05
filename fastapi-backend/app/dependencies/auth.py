"""Clerk JWT authentication dependency for FastAPI.

This module provides JWT verification for Clerk-authenticated requests.
It supports both CLERK_JWKS_URL and CLERK_ISSUER environment variables.
"""

import base64
import json
from typing import Dict, Optional

import jwt
import requests
from cachetools import TTLCache
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.core.config import get_settings

# JWKS cache with 10-minute TTL
_jwks_cache: TTLCache = TTLCache(maxsize=10, ttl=600)  # 10 minutes = 600 seconds

# HTTP Bearer token security scheme
http_bearer = HTTPBearer(auto_error=True)


def _get_jwks_url() -> str:
    """Determine JWKS URL from environment variables.
    
    Priority:
    1. CLERK_JWKS_URL (if set, use directly)
    2. CLERK_ISSUER (if set, derive: {issuer}/.well-known/jwks.json)
    3. Raise RuntimeError if neither is set
    
    Returns:
        JWKS URL string
        
    Raises:
        RuntimeError: If neither CLERK_JWKS_URL nor CLERK_ISSUER is set
    """
    settings = get_settings()
    
    # Check for direct JWKS URL
    jwks_url = getattr(settings, "CLERK_JWKS_URL", None)
    if jwks_url:
        return jwks_url
    
    # Check for issuer and derive JWKS URL
    issuer = getattr(settings, "CLERK_ISSUER", None)
    if issuer:
        # Remove trailing slash if present
        issuer = issuer.rstrip("/")
        return f"{issuer}/.well-known/jwks.json"
    
    # Neither is set - fail closed
    raise RuntimeError(
        "Clerk authentication not configured: "
        "Set either CLERK_JWKS_URL or CLERK_ISSUER environment variable"
    )


def _fetch_jwks(jwks_url: str) -> Dict:
    """Fetch JWKS from the given URL with caching.
    
    Args:
        jwks_url: URL to fetch JWKS from
        
    Returns:
        JWKS dictionary
        
    Raises:
        HTTPException: If JWKS fetch fails
    """
    # Check cache first
    cache_key = jwks_url
    if cache_key in _jwks_cache:
        return _jwks_cache[cache_key]
    
    # Fetch from URL
    try:
        response = requests.get(jwks_url, timeout=5)
        response.raise_for_status()
        jwks = response.json()
        
        # Cache the result
        _jwks_cache[cache_key] = jwks
        return jwks
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to fetch JWKS: {exc}"
        ) from exc


def _get_signing_key(jwks: Dict, kid: str) -> Optional[Dict]:
    """Get the signing key from JWKS by key ID.
    
    Args:
        jwks: JWKS dictionary
        kid: Key ID from JWT header
        
    Returns:
        Key dictionary or None if not found
    """
    for key in jwks.get("keys", []):
        if key.get("kid") == kid:
            return key
    return None


async def verify_clerk_token(
    credentials: HTTPAuthorizationCredentials = Depends(http_bearer)
) -> Dict:
    """Verify Clerk JWT token and return user information.
    
    This dependency:
    1. Extracts the Bearer token from Authorization header
    2. Determines JWKS URL from environment variables
    3. Fetches and caches JWKS
    4. Decodes JWT header to get key ID (kid)
    5. Selects the correct key from JWKS
    6. Verifies JWT signature, expiration, and issuer
    7. Returns user information dictionary
    
    Args:
        credentials: HTTP Bearer token credentials from Authorization header
        
    Returns:
        Dictionary with:
        - "id": User ID (sub claim)
        - "email": User email (email claim, if present)
        - "raw": Full decoded JWT claims
        
    Raises:
        HTTPException(401): If token is invalid, expired, or missing
        HTTPException(503): If JWKS cannot be fetched
        RuntimeError: If Clerk configuration is missing
    """
    token = credentials.credentials
    
    try:
        # Step 1: Decode JWT header to get kid (without verification)
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")
        if not kid:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="JWT missing key ID (kid) in header"
            )
        
        # Step 2: Get JWKS URL
        jwks_url = _get_jwks_url()
        
        # Step 3: Fetch JWKS (with caching)
        jwks = _fetch_jwks(jwks_url)
        
        # Step 4: Get signing key
        signing_key = _get_signing_key(jwks, kid)
        if not signing_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Signing key not found for kid: {kid}"
            )
        
        # Step 5: Decode and verify JWT
        # Convert JWK to RSA public key for PyJWT
        try:
            # Extract RSA components from JWK
            n = base64.urlsafe_b64decode(signing_key["n"] + "==")
            e = base64.urlsafe_b64decode(signing_key["e"] + "==")
            
            # Convert to integers
            n_int = int.from_bytes(n, "big")
            e_int = int.from_bytes(e, "big")
            
            # Build RSA public key
            public_key = rsa.RSAPublicNumbers(e_int, n_int).public_key(default_backend())
        except (KeyError, ValueError) as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid JWK format: {exc}"
            ) from exc
        
        # Get issuer for verification
        settings = get_settings()
        issuer = getattr(settings, "CLERK_ISSUER", None)
        if not issuer:
            # Try to derive from JWKS URL
            if jwks_url.endswith("/.well-known/jwks.json"):
                issuer = jwks_url[:-23]  # Remove "/.well-known/jwks.json"
        
        # Verify JWT
        try:
            decoded = jwt.decode(
                token,
                public_key,
                algorithms=["RS256"],
                issuer=issuer,
                options={
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_iss": bool(issuer),
                }
            )
        except jwt.ExpiredSignatureError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="JWT token has expired"
            )
        except jwt.InvalidIssuerError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="JWT token issuer is invalid"
            )
        except jwt.InvalidSignatureError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="JWT token signature is invalid"
            )
        except jwt.DecodeError as exc:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"JWT token decode failed: {exc}"
            )
        
        # Step 6: Return user information
        return {
            "id": decoded.get("sub", ""),
            "email": decoded.get("email", ""),
            "raw": decoded
        }
        
    except HTTPException:
        # Re-raise HTTP exceptions
        raise
    except RuntimeError:
        # Re-raise configuration errors
        raise
    except Exception as exc:
        # Catch-all for any other errors
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Token verification failed: {exc}"
        ) from exc


def extract_tenant_id(request: Request) -> Optional[str]:
    """Extract tenant_id from request using standardized priority.
    
    Priority order:
    1. Query parameter: request.query_params.get("tenant_id")
    2. Path parameter: request.path_params.get("tenant_id")
    3. JSON body: Attempt to parse request body (only if not already consumed)
    
    Note: For JSON body extraction, this function tries to read the body stream.
    If the body has already been consumed by FastAPI's automatic parsing, it may not
    be available. In such cases, endpoints should pass tenant_id explicitly or
    extract it from the parsed Pydantic model.
    
    Args:
        request: FastAPI Request object
        
    Returns:
        tenant_id string if found, None otherwise
    """
    # Priority 1: Query parameter
    tenant_id = request.query_params.get("tenant_id")
    if tenant_id:
        return tenant_id
    
    # Priority 2: Path parameter
    tenant_id = request.path_params.get("tenant_id")
    if tenant_id:
        return tenant_id
    
    # Priority 3: JSON body (if content type is JSON and body is available)
    content_type = request.headers.get("content-type", "")
    if "application/json" in content_type:
        try:
            # Check if body is in request state (FastAPI may cache it)
            if hasattr(request.state, "body") and isinstance(request.state.body, dict):
                tenant_id = request.state.body.get("tenant_id")
                if tenant_id:
                    return tenant_id
        except Exception:
            # If body parsing fails, continue
            pass
    
    return None


async def verify_tenant_access(
    request: Request,
    user: Dict = Depends(verify_clerk_token),
    tenant_id: Optional[str] = None
) -> Dict:
    """Verify user has access to the tenant_id in the request.
    
    This dependency:
    1. Uses provided tenant_id if given, otherwise extracts from request (query or path)
    2. If tenant_id is None, returns user (endpoint not tenant-scoped)
    3. If tenant_id is "global", allows access (launch behavior)
    4. Otherwise, checks membership in Neo4j
    5. Raises 403 if user is not a member
    
    Note: For POST requests with JSON bodies, the endpoint should extract tenant_id
    from the parsed body and pass it explicitly, since FastAPI parses the body
    after dependencies are resolved.
    
    Args:
        request: FastAPI Request object
        user: User dictionary from verify_clerk_token
        tenant_id: Optional explicit tenant_id (for body params)
        
    Returns:
        User dictionary (same as verify_clerk_token output)
        
    Raises:
        HTTPException(403): If user is not a member of the tenant
    """
    # Use provided tenant_id or extract from request
    if tenant_id is None:
        tenant_id = extract_tenant_id(request)
    
    # If no tenant_id, endpoint is not tenant-scoped - allow access
    if tenant_id is None:
        return user
    
    # Import here to avoid circular imports
    from app.services.graph import graph_service
    
    if graph_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Graph service not initialized"
        )
    
    # Check membership
    is_member = await graph_service.check_membership(user["id"], tenant_id)
    
    if not is_member:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied to campaign {tenant_id}. You are not a member of this campaign."
        )
    
    return user


async def check_tenant_membership(user_id: str, tenant_id: str) -> None:
    """Helper function to check tenant membership after body is parsed.
    
    Use this in endpoints where tenant_id comes from the request body
    and you need to verify membership after parsing.
    
    Args:
        user_id: User ID to check
        tenant_id: Tenant ID to check membership for
        
    Raises:
        HTTPException(403): If user is not a member of the tenant
    """
    from app.services.graph import graph_service
    
    if graph_service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Graph service not initialized"
        )
    
    is_member = await graph_service.check_membership(user_id, tenant_id)
    
    if not is_member:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied to campaign {tenant_id}. You are not a member of this campaign."
        )
