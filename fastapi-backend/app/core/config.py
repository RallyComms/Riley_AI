from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict


# Load environment variables from a local .env file if present.
load_dotenv()


class Settings(BaseSettings):
    """Application configuration loaded from environment variables.

    This configuration is designed for a multi-tenant, high-security environment.
    """

    # Qdrant configuration
    # For Qdrant Cloud: set QDRANT_URL (e.g., "https://xxx-xxx-xxx.qdrant.io") and QDRANT_API_KEY
    # For local dev: leave QDRANT_URL=None and use QDRANT_HOST/QDRANT_PORT
    QDRANT_URL: Optional[str] = None
    QDRANT_API_KEY: Optional[str] = None
    QDRANT_HOST: str = "localhost"  # Fallback for local dev
    QDRANT_PORT: int = 6333  # Fallback for local dev

    # Global firm knowledge collection (Tier 1 - firm-wide, is_global=true)
    QDRANT_COLLECTION_TIER_1: str = "riley_campaigns_768"
    
    # Private client data collection (Tier 2 - tenant-filtered)
    QDRANT_COLLECTION_TIER_2: str = "riley_production_v1"

    # Neo4j Graph Database configuration
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USER: str = "neo4j"
    NEO4J_PASSWORD: str = "password"

    # Google Gemini / Generative AI
    GOOGLE_API_KEY: Optional[str] = None
    EMBEDDING_MODEL: str = "models/gemini-embedding-001"  # Supported Gemini embedding model
    EMBEDDING_DIM: int = 3072  # gemini-embedding-001 outputs 3072-d vectors

    # Qdrant vector configuration
    # NOTE: Keep as a string for env override; code maps to qdrant_client Distance enum.
    QDRANT_DISTANCE: str = "Cosine"

    # Google Cloud Storage configuration
    GCS_BUCKET_NAME: str = "riley-assets-riley-ai-479422"
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = None  # Path to GCP service account JSON

    # OCR configuration
    OCR_ENABLED_SYSTEMWIDE: bool = False  # Master switch for OCR functionality
    OCR_MIN_CONFIDENCE: float = 0.50  # Minimum confidence threshold (0-1)
    OCR_MAX_CHARS: int = 8000  # Maximum characters to extract from OCR

    # Upload limits (server-side hard cap)
    MAX_UPLOAD_MB: int = 25

    # Preview generation configuration (Office/HTML -> PDF)
    ENABLE_PREVIEW_GENERATION: bool = True
    PREVIEW_MAX_MB: int = 25
    PREVIEW_BUCKET_PATH_PREFIX: str = "previews"
    SIGN_PREVIEW_URLS: bool = True
    PREVIEW_URL_TTL_SECONDS: int = 3600

    # Clerk JWT authentication configuration
    # Either CLERK_JWKS_URL (direct JWKS endpoint) or CLERK_ISSUER (to derive JWKS URL)
    CLERK_JWKS_URL: Optional[str] = None
    CLERK_ISSUER: Optional[str] = None
    
    # Clerk Backend API secret key (for user directory lookups)
    CLERK_SECRET_KEY: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance so configuration is read once."""
    return Settings()



