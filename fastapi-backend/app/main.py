import os
import time
import uuid
import asyncio
from asyncio import create_task
from contextlib import asynccontextmanager
import logging
from pathlib import Path

from anyio import CapacityLimiter, to_thread
from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from app.routers import (
    chat,
    files,
    search,
    campaign,
    conversations,
    ingestion_worker,
    reports,
    report_worker,
    document_intel_worker,
    campaign_intelligence,
    campaign_intel_worker,
    comparisons,
    notifications,
    mission_control,
    clerk_webhooks,
    uploads,
)
from app.dependencies.auth import verify_clerk_token, extract_tenant_id
from app.services.graph import GraphService
from app.services.pricing_registry import estimate_worker_runtime_cost
from app.services.qdrant import vector_service
from app.services.llm_cost_guardrail import (
    configure_guardrail_graph_service,
    close_guardrail_driver,
)
from app.services.request_observability import (
    classify_operation_type,
    cloud_task_retry_count,
    request_event_type_raw,
)
from app.core.config import get_settings

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan: startup and shutdown events."""
    # Startup: Initialize Neo4j connection and store in app.state
    settings = get_settings()
    app.state.graph = GraphService()
    configure_guardrail_graph_service(app.state.graph)
    if getattr(settings, "MISSION_CONTROL_AUTO_SCHEMA_SETUP", False):
        timeout_seconds = max(
            1,
            int(getattr(settings, "MISSION_CONTROL_SCHEMA_SETUP_TIMEOUT_SECONDS", 20)),
        )
        try:
            await asyncio.wait_for(
                app.state.graph.ensure_mission_control_schema(),
                timeout=timeout_seconds,
            )
            logger.info(
                "mission_control_schema_auto_setup_completed timeout_seconds=%s",
                timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "mission_control_schema_auto_setup_timed_out timeout_seconds=%s",
                timeout_seconds,
            )
        except Exception as exc:
            logger.warning(
                "mission_control_schema_auto_setup_failed error=%s",
                exc,
            )
    else:
        logger.info("mission_control_schema_auto_setup_disabled")

    # Startup: Log Qdrant connection mode
    if (
        settings.NEO4J_URI
        and "localhost" not in settings.NEO4J_URI
        and settings.NEO4J_USER == "neo4j"
        and settings.NEO4J_PASSWORD == "password"
    ):
        logger.warning(
            "neo4j_credentials_using_defaults uri=%s user=%s",
            settings.NEO4J_URI,
            settings.NEO4J_USER,
        )

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

    if settings.MISSION_CONTROL_ROLLUP_AUTO_REFRESH_ENABLED:
        create_task(
            app.state.graph.rebuild_analytics_daily_rollups(
                days_back=max(7, int(settings.MISSION_CONTROL_ROLLUP_DAYS_BACK))
            )
        )
    
    yield
    # Shutdown: Close Neo4j connection
    configure_guardrail_graph_service(None)
    await close_guardrail_driver()
    if hasattr(app.state, "graph") and app.state.graph:
        await app.state.graph.close()


app = FastAPI(lifespan=lifespan)


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(str(value))
    except Exception:
        return default


@app.middleware("http")
async def request_cost_observability_middleware(request: Request, call_next):
    """Capture Cloud Run request behavior for operation-level cost attribution."""
    path = str(request.url.path or "")
    # Skip observability for internal Cloud Tasks worker routes:
    # - they already emit their own fine-grained analytics events on success/failure
    # - avoid adding Neo4j load on every Cloud Tasks retry when Aura is degraded
    # - prevents noisy request_cost_observability_failed logs on worker paths
    is_internal_worker_path = (
        path.startswith("/internal/")
        or path.startswith("/api/v1/internal/")
    )
    if (
        request.method == "OPTIONS"
        or is_internal_worker_path
        or (not path.startswith("/api/v1/") and not path.startswith("/internal/"))
    ):
        return await call_next(request)

    started = time.perf_counter()
    response = None
    error_type = ""
    try:
        response = await call_next(request)
        return response
    except Exception as exc:
        error_type = type(exc).__name__
        raise
    finally:
        graph = getattr(request.app.state, "graph", None)
        if graph is not None:
            duration_ms = max(0, int((time.perf_counter() - started) * 1000))
            retry_count = cloud_task_retry_count(dict(request.headers))
            is_retry = retry_count > 0
            base_operation_type = classify_operation_type(method=request.method, path=path)
            operation_type = "retry" if is_retry else base_operation_type
            status_code = response.status_code if response is not None else 500
            request_bytes = _safe_int(request.headers.get("content-length"), default=0)
            response_bytes = _safe_int(
                response.headers.get("content-length") if response is not None else 0,
                default=0,
            )
            runtime_cost = estimate_worker_runtime_cost(duration_ms=duration_ms)
            user_id = str(getattr(request.state, "auth_user_id", "") or "").strip() or None
            campaign_id = extract_tenant_id(request)
            event_id = f"http_request:{path}:{request.method}:{uuid.uuid4()}"
            try:
                await graph.append_analytics_event(
                    event_id=event_id,
                    source_event_type_raw=request_event_type_raw(
                        operation_type=base_operation_type,
                        is_retry=is_retry,
                    ),
                    source_entity="HttpRequest",
                    campaign_id=campaign_id,
                    user_id=user_id,
                    actor_user_id=user_id,
                    object_id=path,
                    status=str(status_code),
                    provider="cloud_run",
                    model="default_worker_profile",
                    latency_ms=duration_ms,
                    cost_estimate_usd=(
                        float(runtime_cost["cost_estimate_usd"])
                        if runtime_cost.get("cost_estimate_usd") is not None
                        else None
                    ),
                    pricing_version=str(runtime_cost.get("pricing_version") or "cost-accounting-v1"),
                    cost_confidence=str(runtime_cost.get("cost_confidence") or "proxy_only"),
                    metadata={
                        "service": "http_request",
                        "method": request.method,
                        "endpoint": path,
                        "operation_type": operation_type,
                        "base_operation_type": base_operation_type,
                        "is_retry": is_retry,
                        "retry_count": retry_count,
                        "duration_ms": duration_ms,
                        "request_bytes": request_bytes,
                        "response_bytes": response_bytes,
                        "resource_usage_proxy": {
                            "cpu_ms_estimate": duration_ms,
                            "memory_mb_estimate": 512,
                        },
                        "error_type": error_type,
                        "requests_count": 1,
                    },
                )
                logger.info(
                    "request_cost_observed operation_type=%s endpoint=%s method=%s status=%s duration_ms=%s retry_count=%s",
                    operation_type,
                    path,
                    request.method,
                    status_code,
                    duration_ms,
                    retry_count,
                )
            except Exception as analytics_exc:
                logger.warning(
                    "request_cost_observability_failed endpoint=%s method=%s error=%s",
                    path,
                    request.method,
                    analytics_exc,
                )

# CORS: Read allowed origins from environment variable
def get_allowed_origins() -> list[str]:
    """Parse ALLOWED_ORIGINS from environment variable.
    
    Reads comma-separated list of origins from ALLOWED_ORIGINS env var.
    If not set, defaults to localhost origins for development.
    
    Returns:
        List of allowed origin strings (sanitized and deduplicated)
    """
    required_origins = [
        "https://rileyplatform.com",
        "https://www.rileyplatform.com",
    ]
    dev_origins = ["http://localhost:3000", "http://127.0.0.1:3000"]
    
    allowed_origins_env = os.getenv("ALLOWED_ORIGINS")
    if not allowed_origins_env:
        return [*required_origins, *dev_origins]
    
    # Parse: split on commas, strip whitespace, drop empty strings
    origins = [
        origin.strip()
        for origin in allowed_origins_env.split(",")
        if origin.strip()
    ]
    
    # If parsing resulted in empty list, fall back to required + dev defaults
    if not origins:
        return [*required_origins, *dev_origins]

    # Always include required production origins and de-duplicate
    merged = []
    for origin in [*origins, *required_origins]:
        if origin not in merged:
            merged.append(origin)
    return merged

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

# Clerk webhook route (signature-verified, no Clerk JWT dependency)
app.include_router(clerk_webhooks.router, prefix="/api/v1")

# CRITICAL: This puts the search route at /api/v1/search
# All /api/v1/* routes require Clerk JWT authentication
app.include_router(search.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# CRITICAL: This puts the chat route at /api/v1/chat
app.include_router(chat.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# CRITICAL: This puts the files route at /api/v1/files/{tenant_id}
app.include_router(files.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# Direct-to-GCS upload session endpoints (signed URL init + metadata-only complete)
app.include_router(uploads.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# CRITICAL: This puts the campaign route at /api/v1/campaign/{tenant_id}
app.include_router(campaign.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# Conversations v2 route (authenticated)
app.include_router(conversations.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# Riley report jobs route (authenticated)
app.include_router(reports.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# Riley campaign intelligence route (authenticated)
app.include_router(campaign_intelligence.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# Riley comparison tables route (authenticated)
app.include_router(comparisons.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# Notifications route (authenticated)
app.include_router(notifications.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# Mission Control admin analytics route (authenticated + admin-guarded at endpoint level)
app.include_router(mission_control.router, prefix="/api/v1", dependencies=[Depends(verify_clerk_token)])
# Internal worker route for Cloud Tasks ingestion callbacks (token-protected, not Clerk-protected)
app.include_router(ingestion_worker.router, prefix="/api/v1")
# Internal worker route for Cloud Tasks report callbacks (token-protected, not Clerk-protected)
app.include_router(report_worker.router, prefix="/api/v1")
# Internal worker route for Cloud Tasks document intelligence callbacks (token-protected)
app.include_router(document_intel_worker.router, prefix="/api/v1")
# Internal worker route for Cloud Tasks campaign intelligence callbacks (token-protected)
app.include_router(campaign_intel_worker.router, prefix="/api/v1")
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


@app.get("/healthz/graph")
async def healthz_graph(request: Request):
    """Lightweight graph readiness probe for Neo4j cutover verification."""
    graph = getattr(request.app.state, "graph", None)
    if graph is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Graph service not initialized",
        )
    try:
        await graph.verify_connectivity()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Neo4j connectivity check failed: {exc}",
        ) from exc
    return {"status": "ok", "graph": "reachable"}


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