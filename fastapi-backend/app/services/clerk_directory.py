"""Clerk Directory Service for user resolution.

This module provides functionality to look up users in Clerk by email
using the Clerk Backend API.
"""

from typing import Dict, List, Optional

import requests
from fastapi import HTTPException, status

from app.core.config import get_settings


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
        users = data.get("data", [])
        
        # Find exact email match (case-insensitive)
        email_lower = email.lower()
        for user in users:
            email_addresses = user.get("email_addresses", [])
            for email_addr in email_addresses:
                user_email = email_addr.get("email_address", "").lower()
                if user_email == email_lower:
                    # Extract user information
                    return {
                        "id": user.get("id", ""),
                        "email": email_addr.get("email_address", ""),
                        "first_name": user.get("first_name", ""),
                        "last_name": user.get("last_name", "")
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
        users = data.get("data", [])
        results: List[Dict[str, str]] = []
        for user in users:
            email_addresses = user.get("email_addresses", [])
            primary_email = ""
            if isinstance(email_addresses, list) and email_addresses:
                first_email = email_addresses[0] or {}
                primary_email = str(first_email.get("email_address") or "")
            first_name = str(user.get("first_name") or "").strip()
            last_name = str(user.get("last_name") or "").strip()
            display_name = f"{first_name} {last_name}".strip() or primary_email or str(user.get("id") or "Unknown User")
            results.append(
                {
                    "id": str(user.get("id") or ""),
                    "email": primary_email,
                    "display_name": display_name,
                }
            )
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
        user = response.json()
        email_addresses = user.get("email_addresses", [])
        primary_email = ""
        if isinstance(email_addresses, list) and email_addresses:
            first_email = email_addresses[0] or {}
            primary_email = str(first_email.get("email_address") or "")
        first_name = str(user.get("first_name") or "").strip()
        last_name = str(user.get("last_name") or "").strip()
        display_name = f"{first_name} {last_name}".strip() or primary_email or normalized_user_id
        image_url = str(user.get("image_url") or "")
        return {
            "id": str(user.get("id") or normalized_user_id),
            "email": primary_email,
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
