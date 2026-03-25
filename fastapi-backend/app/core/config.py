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
    BM25_ENABLED: bool = True
    HYBRID_SEARCH_ENABLED: bool = True
    RERANK_ENABLED: bool = True
    RERANK_PROVIDER: str = "gemini"
    RERANK_MODEL: str = "gemini-2.5-flash"
    RERANK_CANDIDATES: int = 60
    RERANK_TOP_K: int = 15
    RERANK_MAX_SNIPPET_TOKENS: int = 180
    RERANK_MAX_TOTAL_INPUT_TOKENS: int = 6000
    OPENAI_API_KEY: Optional[str] = None
    RILEY_PROVIDER: str = "openai"
    RILEY_MODEL: str = "gemini-2.5-flash"
    RILEY_DEEP_MODEL: str = "gemini-2.5-pro"
    RILEY_GEMINI_MODEL: str = "gemini-2.5-pro"
    RILEY_OPENAI_FALLBACK_MODEL: str = "gpt-4.1"
    RILEY_TIMEOUT_SECONDS: int = 45
    RILEY_REPORTS_USE_CLOUD_TASKS: bool = True
    RILEY_REPORTS_TASKS_QUEUE: str = "riley-report-jobs"
    RILEY_REPORTS_TASKS_LOCATION: str = "us-west1"
    RILEY_REPORT_WORKER_URL: Optional[str] = None
    RILEY_REPORT_WORKER_TOKEN: Optional[str] = None
    RILEY_REPORTS_TASKS_SERVICE_ACCOUNT_EMAIL: Optional[str] = None
    RILEY_REPORT_MODEL: str = "gemini-2.5-pro"
    RILEY_REPORT_DEEP_MODEL: str = "gemini-2.5-pro"
    RILEY_REPORT_TIMEOUT_SECONDS: int = 120
    RILEY_REPORT_MAX_TIMEOUT_SECONDS: int = 420
    RILEY_REPORT_RETRY_ATTEMPTS: int = 4
    RILEY_REPORT_RETRY_BACKOFF_SECONDS: float = 2.0
    RILEY_REPORT_MAX_CONTEXT_CHARS: int = 180000
    RILEY_VISION_ENABLED: bool = True
    RILEY_VISION_MODEL: str = "gpt-4.1-mini"
    RILEY_VISION_TIMEOUT_SECONDS: int = 30
    RILEY_VISION_MAX_SEGMENTS: int = 30
    RILEY_DOC_INTEL_ENABLED: bool = True
    RILEY_DOC_INTEL_USE_CLOUD_TASKS: bool = True
    RILEY_DOC_INTEL_TASKS_QUEUE: str = "riley-doc-intel-jobs"
    RILEY_DOC_INTEL_TASKS_LOCATION: str = "us-west1"
    RILEY_DOC_INTEL_WORKER_URL: Optional[str] = None
    RILEY_DOC_INTEL_WORKER_TOKEN: Optional[str] = None
    RILEY_DOC_INTEL_TASKS_SERVICE_ACCOUNT_EMAIL: Optional[str] = None
    RILEY_DOC_INTEL_MODEL: str = "gemini-2.5-flash"
    RILEY_DOC_INTEL_SYNTHESIS_MODEL: str = "gemini-2.5-pro"
    RILEY_DOC_INTEL_TIMEOUT_SECONDS: int = 90
    RILEY_DOC_INTEL_MAX_CHUNKS: int = 180
    RILEY_DOC_INTEL_MAX_CONTEXT_CHARS: int = 120000
    RILEY_DOC_INTEL_RETRY_ATTEMPTS: int = 5
    RILEY_DOC_INTEL_RETRY_BACKOFF_SECONDS: float = 2.5
    RILEY_DOC_INTEL_MAX_TIMEOUT_SECONDS: int = 360
    RILEY_DOC_INTEL_MULTIPASS_ENABLED: bool = True
    RILEY_DOC_INTEL_MULTIPASS_MIN_CHUNKS: int = 70
    RILEY_DOC_INTEL_MULTIPASS_MIN_CHARS: int = 45000
    RILEY_DOC_INTEL_MULTIPASS_MIN_PAGES: int = 35
    RILEY_DOC_INTEL_MULTIPASS_BAND_TARGET_CHUNKS: int = 26
    RILEY_DOC_INTEL_MULTIPASS_BAND_MAX_CHUNKS: int = 34
    RILEY_DOC_INTEL_MULTIPASS_BAND_TARGET_CHARS: int = 17000
    RILEY_DOC_INTEL_MULTIPASS_BAND_MAX_CHARS: int = 24000
    RILEY_CAMPAIGN_INTEL_ENABLED: bool = True
    RILEY_CAMPAIGN_INTEL_USE_CLOUD_TASKS: bool = True
    RILEY_CAMPAIGN_INTEL_TASKS_QUEUE: str = "riley-campaign-intel-jobs"
    RILEY_CAMPAIGN_INTEL_TASKS_LOCATION: str = "us-west1"
    RILEY_CAMPAIGN_INTEL_WORKER_URL: Optional[str] = None
    RILEY_CAMPAIGN_INTEL_WORKER_TOKEN: Optional[str] = None
    RILEY_CAMPAIGN_INTEL_TASKS_SERVICE_ACCOUNT_EMAIL: Optional[str] = None
    DEADLINE_REMINDERS_ENABLED: bool = True
    DEADLINE_REMINDER_WORKER_TOKEN: Optional[str] = None

    # Google Cloud Storage configuration
    GCS_BUCKET_NAME: str = "riley-assets-riley-ai-479422"
    GOOGLE_APPLICATION_CREDENTIALS: Optional[str] = None  # Path to GCP service account JSON

    # OCR configuration
    OCR_ENABLED_SYSTEMWIDE: bool = False  # Master switch for OCR functionality
    OCR_MIN_CONFIDENCE: float = 0.50  # Minimum confidence threshold (0-1)
    OCR_MAX_CHARS: int = 10000  # Maximum characters to extract from OCR

    # Upload limits (server-side hard cap)
    MAX_UPLOAD_MB: int = 25
    MAX_UPLOAD_BATCH_SIZE: int = 10

    # Preview generation configuration (Office/HTML -> PDF)
    ENABLE_PREVIEW_GENERATION: bool = True
    PREVIEW_MAX_MB: int = 25
    PREVIEW_BUCKET_PATH_PREFIX: str = "previews"
    SIGN_PREVIEW_URLS: bool = True
    PREVIEW_URL_TTL_SECONDS: int = 3600
    SIGNING_SERVICE_ACCOUNT_EMAIL: Optional[str] = None  # Service account email for IAM-based signed URLs

    # Durable ingestion worker (Cloud Tasks)
    INGESTION_USE_CLOUD_TASKS: bool = True
    GCP_PROJECT_ID: Optional[str] = None
    INGESTION_TASKS_QUEUE: str = "riley-ingestion-jobs"
    INGESTION_TASKS_LOCATION: str = "us-west1"
    INGESTION_WORKER_URL: Optional[str] = None
    INGESTION_WORKER_TOKEN: Optional[str] = None
    INGESTION_TASKS_SERVICE_ACCOUNT_EMAIL: Optional[str] = None

    # Clerk JWT authentication configuration
    # Either CLERK_JWKS_URL (direct JWKS endpoint) or CLERK_ISSUER (to derive JWKS URL)
    CLERK_JWKS_URL: Optional[str] = None
    CLERK_ISSUER: Optional[str] = None
    
    # Clerk Backend API secret key (for user directory lookups)
    CLERK_SECRET_KEY: Optional[str] = None
    # Mission Control admin allowlists (comma-separated values).
    MISSION_CONTROL_ADMIN_USER_IDS: Optional[str] = None
    MISSION_CONTROL_ADMIN_EMAILS: Optional[str] = None

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance so configuration is read once."""
    return Settings()



