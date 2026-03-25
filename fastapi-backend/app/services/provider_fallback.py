import asyncio
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from app.services.genai_client import get_genai_client


@dataclass
class ProviderFailureDetails:
    fallback_eligible: bool
    error_type: str
    http_status: Optional[int] = None
    provider_error_code: Optional[str] = None
    provider_error_type: Optional[str] = None
    response_body_excerpt: Optional[str] = None


def classify_openai_generation_failure(exc: Exception, *, response_excerpt_limit: int = 1200) -> ProviderFailureDetails:
    details = ProviderFailureDetails(
        fallback_eligible=False,
        error_type=type(exc).__name__,
    )

    try:
        import httpx  # type: ignore
    except Exception:
        return details

    timeout_classes = tuple(
        cls
        for cls in (
            getattr(httpx, "TimeoutException", None),
            getattr(httpx, "ReadTimeout", None),
            getattr(httpx, "ConnectTimeout", None),
            getattr(httpx, "WriteTimeout", None),
            getattr(httpx, "PoolTimeout", None),
        )
        if cls is not None
    )
    if timeout_classes and isinstance(exc, timeout_classes):
        details.fallback_eligible = True
        return details

    transport_classes = tuple(
        cls
        for cls in (
            getattr(httpx, "TransportError", None),
            getattr(httpx, "NetworkError", None),
            getattr(httpx, "ConnectError", None),
            getattr(httpx, "ReadError", None),
            getattr(httpx, "WriteError", None),
            getattr(httpx, "ProtocolError", None),
            getattr(httpx, "RemoteProtocolError", None),
        )
        if cls is not None
    )
    if transport_classes and isinstance(exc, transport_classes):
        details.fallback_eligible = True
        return details

    if isinstance(exc, httpx.HTTPStatusError):
        response = exc.response
        status_code = int(response.status_code) if response is not None else None
        details.http_status = status_code
        body_text = ""
        if response is not None:
            try:
                body_text = str(response.text or "")
            except Exception:
                body_text = ""
        details.response_body_excerpt = body_text[:response_excerpt_limit] if body_text else None

        provider_error_code: Optional[str] = None
        provider_error_type: Optional[str] = None
        provider_error_message: Optional[str] = None
        if response is not None:
            try:
                payload = response.json()
                if isinstance(payload, dict):
                    err = payload.get("error")
                    if isinstance(err, dict):
                        code = err.get("code")
                        err_type = err.get("type")
                        message = err.get("message")
                        if code is not None:
                            provider_error_code = str(code).strip().lower() or None
                        if err_type is not None:
                            provider_error_type = str(err_type).strip().lower() or None
                        if message is not None:
                            provider_error_message = str(message).strip().lower() or None
            except Exception:
                pass

        details.provider_error_code = provider_error_code
        details.provider_error_type = provider_error_type

        provider_failure_codes = {
            "insufficient_quota",
            "rate_limit_exceeded",
        }
        provider_failure_types = {
            "insufficient_quota",
            "rate_limit_exceeded",
        }
        provider_failure_hints = (
            "rate limit",
            "quota",
            "overloaded",
            "unavailable",
            "gateway",
            "upstream",
            "timeout",
            "temporarily",
        )
        message_blob = " ".join(
            part
            for part in (
                provider_error_message or "",
                (details.response_body_excerpt or "").lower(),
            )
            if part
        )

        if status_code == 429 or (status_code is not None and status_code >= 500):
            details.fallback_eligible = True
        if provider_error_code in provider_failure_codes:
            details.fallback_eligible = True
        if provider_error_type in provider_failure_types:
            details.fallback_eligible = True
        if message_blob and any(hint in message_blob for hint in provider_failure_hints):
            details.fallback_eligible = True

    return details


def classify_gemini_generation_failure(exc: Exception) -> ProviderFailureDetails:
    details = ProviderFailureDetails(
        fallback_eligible=False,
        error_type=type(exc).__name__,
    )

    provider_failure_hints = (
        "rate limit",
        "quota",
        "overloaded",
        "unavailable",
        "gateway",
        "upstream",
        "timeout",
        "temporarily",
        "resource exhausted",
        "service unavailable",
        "deadline exceeded",
    )

    if isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
        details.fallback_eligible = True
        return details

    # Best-effort extraction for HTTP-like status fields across SDK exceptions.
    status_candidate = getattr(exc, "status_code", None)
    if status_candidate is None:
        status_candidate = getattr(exc, "code", None)
    if status_candidate is not None:
        try:
            details.http_status = int(getattr(status_candidate, "value", status_candidate))
        except Exception:
            details.http_status = None

    # Google API Core exception classes (if available in runtime).
    try:
        from google.api_core import exceptions as gexc  # type: ignore

        fallback_types = tuple(
            cls
            for cls in (
                getattr(gexc, "TooManyRequests", None),
                getattr(gexc, "ResourceExhausted", None),
                getattr(gexc, "ServiceUnavailable", None),
                getattr(gexc, "DeadlineExceeded", None),
                getattr(gexc, "GatewayTimeout", None),
                getattr(gexc, "InternalServerError", None),
                getattr(gexc, "BadGateway", None),
            )
            if cls is not None
        )
        if fallback_types and isinstance(exc, fallback_types):
            details.fallback_eligible = True
    except Exception:
        pass

    message_blob = str(exc or "").lower()
    if message_blob and any(hint in message_blob for hint in provider_failure_hints):
        details.fallback_eligible = True

    if details.http_status == 429 or (details.http_status is not None and details.http_status >= 500):
        details.fallback_eligible = True

    # Network transport issues should be treated as provider-class failures.
    if isinstance(exc, (ConnectionError, OSError)):
        details.fallback_eligible = True

    return details


async def generate_text_with_gemini(*, prompt: str, model_name: str, timeout_seconds: int) -> str:
    text, _ = await generate_text_with_gemini_with_usage(
        prompt=prompt,
        model_name=model_name,
        timeout_seconds=timeout_seconds,
    )
    return text


async def generate_text_with_gemini_with_usage(
    *, prompt: str, model_name: str, timeout_seconds: int
) -> Tuple[str, Dict[str, Any]]:
    def _generate_sync() -> Tuple[str, Dict[str, Any]]:
        client = get_genai_client()
        response = client.models.generate_content(model=model_name, contents=prompt)

        text = getattr(response, "text", None)
        if isinstance(text, str) and text.strip():
            return text.strip(), _extract_gemini_usage(response)

        candidates = getattr(response, "candidates", None)
        if isinstance(candidates, list):
            for candidate in candidates:
                content = getattr(candidate, "content", None)
                parts = getattr(content, "parts", None) if content is not None else None
                if not isinstance(parts, list):
                    continue
                for part in parts:
                    part_text = getattr(part, "text", None)
                    if isinstance(part_text, str) and part_text.strip():
                        usage = _extract_gemini_usage(response)
                        return part_text.strip(), usage

        raise RuntimeError("Gemini response did not contain text output")

    timeout = max(5, int(timeout_seconds))
    return await asyncio.wait_for(asyncio.to_thread(_generate_sync), timeout=timeout)


def _extract_gemini_usage(response: Any) -> Dict[str, Any]:
    usage_obj = getattr(response, "usage_metadata", None) or getattr(response, "usage", None)
    if usage_obj is None:
        return {}

    def _usage_field(*names: str) -> Optional[int]:
        for name in names:
            value = getattr(usage_obj, name, None)
            if value is None and isinstance(usage_obj, dict):
                value = usage_obj.get(name)
            if isinstance(value, (int, float)):
                return int(value)
        return None

    input_tokens = _usage_field("prompt_token_count", "input_token_count", "prompt_tokens")
    output_tokens = _usage_field("candidates_token_count", "output_token_count", "completion_tokens")
    total_tokens = _usage_field("total_token_count", "total_tokens")
    usage: Dict[str, Any] = {}
    if input_tokens is not None:
        usage["input_tokens"] = int(input_tokens)
        usage["prompt_tokens"] = int(input_tokens)
    if output_tokens is not None:
        usage["output_tokens"] = int(output_tokens)
        usage["completion_tokens"] = int(output_tokens)
    if total_tokens is not None:
        usage["total_tokens"] = int(total_tokens)
    return usage
