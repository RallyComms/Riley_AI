import os
from contextlib import asynccontextmanager
from pathlib import Path

from anyio import CapacityLimiter, to_thread
from fastapi import FastAPI, Request, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from app.routers import chat, files, search, campaign
from app.dependencies.auth import verify_clerk_token
from app.services.graph import GraphService
from app.services.qdrant import vector_service
from app.core.config import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan: startup and shutdown events."""
    # Startup: Initialize Neo4j connection and store in app.state
    app.state.graph = GraphService()

    # Startup: Log Qdrant connection mode
    settings = get_settings()
    if settings.QDRANT_URL:
        # Sanitize URL for logging (remove API key if accidentally included)
        sanitized_url = settings.QDRANT_URL.split("?")[0]  # Remove query params
        print(f"✅ Qdrant: URL mode ({sanitized_url})")
    else:
        print(f"✅ Qdrant: host/port mode ({settings.QDRANT_HOST}:{settings.QDRANT_PORT})")

    # Startup: Ensure Qdrant collections exist (non-destructive).
    # NOTE: Use scripts/reset_qdrant_collections.py for one-time destructive reset to fix dim mismatch.
    try:
        await vector_service.ensure_collections()
    except Exception as exc:
        # Don't hard-crash app startup; surface clearly so deploy logs explain Qdrant issues.
        print(f"⚠️ Qdrant collection ensure failed: {exc}")
    
    # Ensure static directory exists for file uploads
    static_dir = Path("static")
    static_dir.mkdir(exist_ok=True)
    
    # Increase thread pool capacity for high-concurrency I/O
    # This prevents request queuing and thread starvation under load
    limiter = to_thread.current_default_thread_limiter()
    if isinstance(limiter, CapacityLimiter):
        limiter.total_tokens = 100
    
    yield
    # Shutdown: Close Neo4j connection
    if hasattr(app.state, "graph") and app.state.graph:
        await app.state.graph.close()


app = FastAPI(lifespan=lifespan)

# CORS: Read allowed origins from environment variable
def get_allowed_origins() -> list[str]:
    """Parse ALLOWED_ORIGINS from environment variable.
    
    Reads comma-separated list of origins from ALLOWED_ORIGINS env var.
    If not set, defaults to localhost origins for development.
    
    Returns:
        List of allowed origin strings (sanitized and deduplicated)
    """
    default_origins = ["http://localhost:3000", "http://127.0.0.1:3000"]
    
    allowed_origins_env = os.getenv("ALLOWED_ORIGINS")
    if not allowed_origins_env:
        return default_origins
    
    # Parse: split on commas, strip whitespace, drop empty strings
    origins = [
        origin.strip()
        for origin in allowed_origins_env.split(",")
        if origin.strip()
    ]
    
    # If parsing resulted in empty list, fall back to defaults
    if not origins:
        return default_origins
    
    return origins

# Get and log allowed origins
allowed_origins = get_allowed_origins()
print(f"✅ CORS: Allowed origins: {', '.join(allowed_origins)}")

# CORS: Configure middleware with environment-driven origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Production-safe: read from ALLOWED_ORIGINS env var
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# CRITICAL: This puts the search route at /api/v1/search
# All /api/v1/* routes require Clerk JWT authentication
app.include_router(search.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# CRITICAL: This puts the chat route at /api/v1/chat
app.include_router(chat.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# CRITICAL: This puts the files route at /api/v1/files/{tenant_id}
app.include_router(files.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# CRITICAL: This puts the campaign route at /api/v1/campaign/{tenant_id}
app.include_router(campaign.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])

# Ensure static directory exists BEFORE mounting (required for Cloud Run)
# This must happen synchronously at module load time, not in lifespan
Path("static").mkdir(parents=True, exist_ok=True)

# Mount static file directory for serving uploaded files
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def root():
    return {"status": "Riley Doorman Online"}


@app.get("/healthz")
async def healthz():
    """Health check endpoint for Cloud Run smoke tests and uptime monitoring.
    
    This endpoint requires no authentication and does not call external services.
    Returns a simple status to indicate the service is running.
    """
    return {"status": "ok"}


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler to catch unhandled errors.
    
    This prevents the frontend from seeing raw Internal Server Errors
    and provides a user-friendly error message while logging the actual error.
    """
    print(f"🔥 UNHANDLED ERROR: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "System encountered an error. Engineering has been notified."},
    )