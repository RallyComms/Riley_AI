"""Clerk Directory Service for user resolution.

This module provides functionality to look up users in Clerk by email
using the Clerk Backend API.
"""

from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import requests
from fastapi import HTTPException, status

from app.core.config import get_settings


def _to_object(value: Any) -> Any:
    """Recursively convert dict/list payloads to attribute-style objects."""
    if isinstance(value, dict):
        return SimpleNamespace(**{k: _to_object(v) for k, v in value.items()})
    if isinstance(value, list):
        return [_to_object(item) for item in value]
    return value


def _extract_users_payload(payload: Any) -> List[Any]:
    """Normalize Clerk users API payload to a list of Clerk user objects."""
    if isinstance(payload, list):
        return [_to_object(item) for item in payload]
    if isinstance(payload, dict):
        raw = payload.get("data")
        if isinstance(raw, list):
            return [_to_object(item) for item in raw]
    payload_obj = _to_object(payload)
    raw_attr = getattr(payload_obj, "data", None)
    if isinstance(raw_attr, list):
        return raw_attr
    return []


def _extract_primary_email(user: Any) -> str:
    """Extract primary email from Clerk user object safely."""
    primary = getattr(user, "primary_email_address", None)
    if primary is not None:
        email = str(getattr(primary, "email_address", "") or "").strip()
        if email:
            return email

    email_addresses = getattr(user, "email_addresses", []) or []
    if isinstance(email_addresses, list):
        for item in email_addresses:
            email = str(getattr(item, "email_address", "") or "").strip()
            if email:
                return email

    return ""


def find_user_by_email(email: str) -> Optional[Dict[str, str]]:
    """Find a user in Clerk by email address.
    
    Uses the Clerk Backend API to search for users by email.
    Returns minimal user information: id, email, first_name, last_name.
    
    Args:
        email: Email address to search for
        
    Returns:
        Dictionary with user information: {id, email, first_name, last_name}
        Returns None if user is not found
        
    Raises:
        HTTPException: If Clerk API request fails
    """
    settings = get_settings()
    secret_key = getattr(settings, "CLERK_SECRET_KEY", None)
    
    if not secret_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Clerk secret key not configured. Set CLERK_SECRET_KEY environment variable."
        )
    
    # Clerk Backend API endpoint for listing users
    api_url = "https://api.clerk.com/v1/users"
    
    headers = {
        "Authorization": f"Bearer {secret_key}",
        "Content-Type": "application/json"
    }
    
    # Search for users - Clerk API supports query parameter for email search
    # We'll use the query parameter if available, otherwise search through results
    params = {
        "query": email
    }
    
    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        users = _extract_users_payload(data)
        
        # Find exact email match (case-insensitive)
        email_lower = email.lower()
        for user in users:
            primary_email = _extract_primary_email(user)
            if primary_email.lower() == email_lower:
                first_name = str(getattr(user, "first_name", "") or "").strip()
                last_name = str(getattr(user, "last_name", "") or "").strip()
                username = str(getattr(user, "username", "") or "").strip()
                user_id = str(getattr(user, "id", "") or "").strip()
                full_name = f"{first_name} {last_name}".strip()
                return {
                    "id": user_id,
                    "email": primary_email,
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "display_name": (
                        full_name
                        or username
                        or primary_email
                        or "Unknown user"
                    ),
                }
        
        # No exact match found
        return None
        
    except requests.exceptions.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to query Clerk API: {exc}"
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error querying Clerk: {exc}"
        ) from exc


def search_users(query: str, limit: int = 8) -> List[Dict[str, str]]:
    """Search users in Clerk by name/email query."""
    settings = get_settings()
    secret_key = getattr(settings, "CLERK_SECRET_KEY", None)
    if not secret_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Clerk secret key not configured. Set CLERK_SECRET_KEY environment variable."
        )

    normalized_query = (query or "").strip()
    if not normalized_query:
        return []

    api_url = "https://api.clerk.com/v1/users"
    headers = {
        "Authorization": f"Bearer {secret_key}",
        "Content-Type": "application/json"
    }
    params = {
        "query": normalized_query,
        "limit": max(1, min(int(limit), 20)),
    }

    try:
        response = requests.get(api_url, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        users = _extract_users_payload(data)
        results: List[Dict[str, str]] = []
        skipped_invalid = 0
        for user in users:
            try:
                user_id = str(getattr(user, "id", "") or "").strip()
                primary_email = _extract_primary_email(user)
                if not user_id or not primary_email:
                    skipped_invalid += 1
                    continue

                username = str(getattr(user, "username", "") or "").strip()
                first_name = str(getattr(user, "first_name", "") or "").strip()
                last_name = str(getattr(user, "last_name", "") or "").strip()
                full_name = f"{first_name} {last_name}".strip()
                email_prefix = primary_email.split("@")[0].strip() if "@" in primary_email else primary_email
                display_name = full_name or username or email_prefix or primary_email

                results.append(
                    {
                        "id": user_id,
                        "email": primary_email,
                        "username": username,
                        "display_name": display_name,
                    }
                )
            except Exception:
                skipped_invalid += 1
                continue
        if skipped_invalid > 0:
            print("clerk_search_skipped_invalid_users", {"count": skipped_invalid})
        return results
    except requests.exceptions.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to query Clerk API: {exc}"
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error querying Clerk: {exc}"
        ) from exc


def find_user_by_id(user_id: str) -> Optional[Dict[str, str]]:
    """Find a user in Clerk by user ID."""
    settings = get_settings()
    secret_key = getattr(settings, "CLERK_SECRET_KEY", None)
    if not secret_key:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Clerk secret key not configured. Set CLERK_SECRET_KEY environment variable."
        )

    normalized_user_id = (user_id or "").strip()
    if not normalized_user_id:
        return None

    api_url = f"https://api.clerk.com/v1/users/{normalized_user_id}"
    headers = {
        "Authorization": f"Bearer {secret_key}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(api_url, headers=headers, timeout=10)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        user = _to_object(response.json())
        primary_email = _extract_primary_email(user)
        first_name = str(getattr(user, "first_name", "") or "").strip()
        last_name = str(getattr(user, "last_name", "") or "").strip()
        username = str(getattr(user, "username", "") or "").strip()
        user_id_value = str(getattr(user, "id", normalized_user_id) or normalized_user_id).strip()
        display_name = (
            str(getattr(user, "display_name", "") or "").strip()
            or username
            or f"{first_name} {last_name}".strip()
            or primary_email
            or "Unknown user"
        )
        image_url = str(getattr(user, "image_url", "") or "")
        return {
            "id": user_id_value,
            "email": primary_email,
            "username": username,
            "first_name": first_name,
            "last_name": last_name,
            "display_name": display_name,
            "avatar_url": image_url,
        }
    except requests.exceptions.RequestException as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to query Clerk API: {exc}"
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Unexpected error querying Clerk: {exc}"
        ) from exc
