"""Shared Google Gemini client for embedding generation.

This module provides a lazy-initialized singleton client instance for all embedding operations
across the backend. Uses the new google-genai SDK (not the deprecated google.generativeai).

The client is initialized on first use, not at import time, to prevent Cloud Run boot loops
if GOOGLE_API_KEY is missing or there are transient API issues.
"""

from google import genai
from app.core.config import get_settings

# Lazy singleton pattern - client initialized on first use
_client = None


def get_genai_client():
    """Get or create the shared Google GenAI client instance.
    
    Uses lazy initialization to prevent import-time failures. The client is created
    on first use and cached for subsequent calls.
    
    Returns:
        genai.Client: The shared client instance
        
    Raises:
        RuntimeError: If GOOGLE_API_KEY is missing or invalid
    """
    global _client
    
    if _client is None:
        settings = get_settings()
        if not settings.GOOGLE_API_KEY:
            raise RuntimeError(
                "GOOGLE_API_KEY is missing. Set the GOOGLE_API_KEY environment variable."
            )
        
        try:
            _client = genai.Client(api_key=settings.GOOGLE_API_KEY)
        except Exception as exc:
            raise RuntimeError(
                f"Failed to initialize Google GenAI client: {exc}"
            ) from exc
    
    return _client
