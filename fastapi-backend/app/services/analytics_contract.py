import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple


def canonical_campaign_id(
    *,
    campaign_id: Optional[str] = None,
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    is_global: Optional[bool] = None,
) -> str:
    """Canonical campaign scope resolver for analytics."""
    for value in (campaign_id, tenant_id, client_id):
        normalized = str(value or "").strip()
        if normalized:
            return normalized
    if bool(is_global):
        return "global"
    return "unknown_campaign"

def classify_chat_type(*, source_entity: str, is_private_thread: Optional[bool] = None) -> Optional[str]:
    """Normalize dual chat systems into canonical chat classes."""
    entity = (source_entity or "").strip()
    if entity in {"ChatSession", "Message"}:
        return "assistant_chat"
    if entity in {"TeamMessage"}:
        return "campaign_team_chat"
    if entity in {"ThreadMessage", "ChatThread"}:
        return "private_thread_chat" if is_private_thread is not False else "campaign_team_chat"
    return None


def normalize_event_type(
    *,
    source_event_type_raw: str,
    source_entity: str,
    chat_type: Optional[str] = None,
) -> Tuple[str, str, str]:
    """Return (event_type_normalized, feature_area, object_type)."""
    raw = str(source_event_type_raw or "").strip().lower() or "unknown"
    source = str(source_entity or "").strip()

    campaign_event_mapping: Dict[str, Tuple[str, str, str]] = {
        "access_request_created": ("campaign.access.request.created", "campaign_access", "campaign_access_request"),
        "access_request_approved": ("campaign.access.request.approved", "campaign_access", "campaign_access_request"),
        "access_request_denied": ("campaign.access.request.denied", "campaign_access", "campaign_access_request"),
        "campaign_member_added": ("campaign.membership.added", "campaign_membership", "campaign_member"),
        "campaign_member_added_notification": ("campaign.membership.added_notification", "campaign_membership", "campaign_member"),
        "campaign_message_mentioned_user": ("chat.mention.created", "collaboration_chat", "message_mention"),
        "deadline_created": ("deadline.created", "deadlines", "campaign_deadline"),
        "document_mentioned_user": ("document.mention.created", "document_collaboration", "document_comment"),
        "document_tagged_for_review": ("document.review.tagged", "document_workflow", "document"),
        "document_assigned_to_user": ("document.assigned.user", "document_workflow", "document"),
        "document_assigned": ("document.assigned", "document_workflow", "document"),
        "document_moved_needs_review": ("document.status.needs_review", "document_workflow", "document"),
        "document_moved_in_review": ("document.status.in_review", "document_workflow", "document"),
    }

    if raw in campaign_event_mapping:
        return campaign_event_mapping[raw]

    if source == "RileyReportJob":
        if raw == "report_job_created":
            return ("report.job.created", "reports", "report_job")
        if raw == "report_job_status_changed":
            return ("report.job.status_changed", "reports", "report_job")

    if source in {"TeamMessage", "ThreadMessage", "Message"}:
        normalized_chat_type = chat_type or classify_chat_type(source_entity=source)
        if normalized_chat_type == "assistant_chat":
            return ("chat.assistant.message.created", "assistant_chat", "message")
        if normalized_chat_type == "private_thread_chat":
            return ("chat.private_thread.message.created", "collaboration_chat", "thread_message")
        return ("chat.team.message.created", "collaboration_chat", "team_message")

    if source == "CampaignAccessRequest":
        return (f"campaign.access.request.{raw}", "campaign_access", "campaign_access_request")
    if source == "CampaignDeadline":
        return (f"deadline.{raw}", "deadlines", "campaign_deadline")
    if source == "ChatSession":
        return ("chat.assistant.session.created", "assistant_chat", "chat_session")

    return (f"event.{raw}", "other", "unknown")


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def infer_provider_from_model(model: Optional[str]) -> Optional[str]:
    normalized = str(model or "").strip().lower()
    if not normalized:
        return None
    if normalized.startswith("gpt") or "openai" in normalized:
        return "openai"
    if normalized.startswith("gemini"):
        return "google_gemini"
    return None


def safe_metadata_json(metadata: Optional[Dict[str, Any]]) -> str:
    payload = metadata or {}
    try:
        return json.dumps(payload, separators=(",", ":"), sort_keys=True, default=str)
    except Exception:
        return "{}"
