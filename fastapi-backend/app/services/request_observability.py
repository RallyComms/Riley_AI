from __future__ import annotations

from typing import Dict


POLLING_PATH_HINTS = (
    "/api/v1/riley/reports",
    "/api/v1/riley/index-summary",
    "/api/v1/mission-control/",
)


def _to_operation_slug(value: str) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    return "".join(ch for ch in normalized if ch.isalnum() or ch == "_") or "other"


def cloud_task_retry_count(headers: Dict[str, str]) -> int:
    """Extract Cloud Tasks retry count across common header variants."""
    candidates = (
        headers.get("x-cloudtasks-taskretrycount"),
        headers.get("x-appengine-taskretrycount"),
        headers.get("x-task-retry-count"),
    )
    for value in candidates:
        if value is None:
            continue
        try:
            return max(0, int(str(value).strip()))
        except Exception:
            continue
    return 0


def classify_operation_type(*, method: str, path: str) -> str:
    """Map API request path/method to a cost-attribution operation bucket."""
    normalized_method = str(method or "").upper().strip()
    normalized_path = str(path or "").strip().lower()

    if normalized_path == "/internal/ingestion/run":
        return "ingestion_job"
    if normalized_path == "/internal/reports/run":
        return "report_generation"
    if normalized_path == "/internal/document-intelligence/run":
        return "document_processing"
    if normalized_path == "/internal/campaign-intelligence/run":
        return "document_processing"

    if normalized_path.startswith("/api/v1/chat"):
        return "chat_request"
    if (
        normalized_path.startswith("/api/v1/riley/conversations")
        and normalized_path.endswith("/messages")
        and normalized_method == "POST"
    ):
        return "chat_request"

    if normalized_path.startswith("/api/v1/riley/reports") and normalized_method == "POST":
        return "report_generation"

    if normalized_path == "/api/v1/upload":
        return "document_upload"

    if normalized_path.startswith("/api/v1/files/") and normalized_path.endswith("/ocr"):
        return "document_processing"

    if normalized_method == "GET":
        for hint in POLLING_PATH_HINTS:
            if normalized_path.startswith(hint):
                return "polling_cycle"

    return "other"


def request_event_type_raw(*, operation_type: str, is_retry: bool) -> str:
    op = "retry" if is_retry else operation_type
    return f"http_request_{_to_operation_slug(op)}_completed"

