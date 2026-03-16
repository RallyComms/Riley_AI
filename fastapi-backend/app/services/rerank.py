import asyncio
import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from app.core.config import get_settings
from app.services.token_utils import estimate_tokens, truncate_text_to_token_budget

try:
    import google.generativeai as genai
except Exception:  # pragma: no cover - handled at runtime per provider
    genai = None

try:
    import httpx
except Exception:  # pragma: no cover - handled at runtime per provider
    httpx = None


logger = logging.getLogger(__name__)

RERANK_TIMEOUT_SECONDS = 3.0
DEFAULT_RERANK_MAX_SNIPPET_TOKENS = 96
DEFAULT_RERANK_MAX_TOTAL_INPUT_TOKENS = 2500
MAX_RERANK_CANDIDATE_COUNT_IN = 20
MAX_RERANK_CANDIDATE_COUNT_SENT = 12


def _extract_json_object(text: str) -> Dict[str, Any]:
    if not text:
        return {}
    cleaned = text.strip()
    # Handle fenced JSON blocks.
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        parsed = json.loads(cleaned)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass

    # Fallback: first {...} object in text.
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(cleaned[start : end + 1])
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _safe_location(payload: Dict[str, Any]) -> str:
    for key in ("page", "page_number", "slide", "slide_number", "section"):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return f"{key}:{value}"
    chunk_index = payload.get("chunk_index")
    if chunk_index is not None:
        return f"chunk_index:{chunk_index}"
    return "unknown"


def _safe_snippet(payload: Dict[str, Any]) -> str:
    text = payload.get("text") or payload.get("content") or payload.get("content_preview") or ""
    return str(text).strip().replace("\n", " ")


def _candidate_id(candidate: Dict[str, Any]) -> str:
    payload = candidate.get("payload", {}) or {}
    chunk_id = payload.get("chunk_id")
    if chunk_id:
        return str(chunk_id)
    parent = payload.get("parent_file_id")
    idx = payload.get("chunk_index")
    if parent is not None and idx is not None:
        return f"{parent}::chunk::{idx}"
    return str(candidate.get("id", ""))


def _build_prompt(query: str, prepared_candidates: List[Dict[str, str]], top_k: int) -> str:
    return (
        "You are a retrieval reranker for RAG. Rank candidate chunks by relevance to the user query.\n"
        "Rules:\n"
        "- Prefer direct lexical and semantic relevance.\n"
        "- Prefer chunks that answer the query with concrete evidence.\n"
        "- Use only provided candidate fields.\n"
        "- Return STRICT JSON only.\n\n"
        f"User Query:\n{query}\n\n"
        f"Candidates (JSON array):\n{json.dumps(prepared_candidates, ensure_ascii=True)}\n\n"
        "Return format (preferred):\n"
        '{\n'
        '  "ranked": [\n'
        '    {"id": "candidate_id_1", "score": 0.93, "reason": "short reason"},\n'
        '    {"id": "candidate_id_2", "score": 0.81, "reason": "short reason"}\n'
        "  ],\n"
        '  "notes": "optional"\n'
        "}\n"
        "Backward-compatible alternative accepted:\n"
        '{"ranked_ids": ["candidate_id_1", "candidate_id_2"], "notes": "optional"}\n'
        f"Limit output to at most {top_k} candidates."
    )


def _run_gemini_rerank_sync(*, prompt: str, model: str, api_key: str) -> str:
    if genai is None:
        raise RuntimeError("google.generativeai dependency is not installed")
    genai.configure(api_key=api_key)
    rerank_model = genai.GenerativeModel(model)
    response = rerank_model.generate_content(prompt)
    return getattr(response, "text", "") or ""


def _extract_openai_output_text(response_json: Dict[str, Any]) -> str:
    output_text = response_json.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    outputs = response_json.get("output")
    if isinstance(outputs, list):
        for output in outputs:
            content = output.get("content") if isinstance(output, dict) else None
            if not isinstance(content, list):
                continue
            for item in content:
                if not isinstance(item, dict):
                    continue
                text = item.get("text")
                if isinstance(text, str) and text.strip():
                    return text
    return ""


def _run_openai_rerank_sync(*, prompt: str, model: str, api_key: str) -> str:
    if httpx is None:
        raise RuntimeError("httpx dependency is not installed")
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "ranked": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "id": {"type": "string"},
                        "score": {"type": "number"},
                        "reason": {"type": "string"},
                    },
                    "required": ["id", "score"],
                },
            },
            "ranked_ids": {
                "type": "array",
                "items": {"type": "string"},
            },
            "notes": {"type": "string"},
        },
        "anyOf": [
            {"required": ["ranked"]},
            {"required": ["ranked_ids"]},
        ],
    }
    payload = {
        "model": model,
        "input": prompt,
        "text": {
            "format": {
                "type": "json_schema",
                "name": "rerank_response",
                "strict": True,
                "schema": schema,
            }
        },
    }
    with httpx.Client(timeout=RERANK_TIMEOUT_SECONDS) as client:
        response = client.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
        return _extract_openai_output_text(response.json())


def _truncate_snippet_to_tokens(text: str, max_tokens: int) -> str:
    return truncate_text_to_token_budget(text, max_tokens)


def _normalize_score(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except Exception:
        return None
    if 0.0 <= numeric <= 1.0:
        return numeric
    if 1.0 < numeric <= 100.0:
        return round(numeric / 100.0, 6)
    return None


def _prepare_candidates_with_budget(
    *,
    query: str,
    candidates: List[Dict[str, Any]],
    max_snippet_tokens: int,
    max_total_input_tokens: int,
    max_candidates_sent: int,
) -> Tuple[List[Dict[str, str]], int]:
    prepared: List[Dict[str, str]] = []
    total_tokens = estimate_tokens(query)
    for candidate in candidates:
        if len(prepared) >= max_candidates_sent:
            break
        payload = candidate.get("payload", {}) or {}
        candidate_id = _candidate_id(candidate)
        filename = str(payload.get("filename", "unknown"))
        location = _safe_location(payload)
        snippet = _truncate_snippet_to_tokens(_safe_snippet(payload), max_snippet_tokens)
        static_tokens = (
            estimate_tokens(candidate_id)
            + estimate_tokens(filename)
            + estimate_tokens(location)
            + 12
        )
        remaining_budget = max_total_input_tokens - total_tokens - static_tokens
        if remaining_budget <= 0:
            break
        if estimate_tokens(snippet) > remaining_budget:
            snippet = _truncate_snippet_to_tokens(snippet, remaining_budget)
            if not snippet:
                break
        candidate_payload = {
            "id": candidate_id,
            "filename": filename,
            "location": location,
            "snippet": snippet,
        }
        prepared.append(candidate_payload)
        total_tokens += static_tokens + estimate_tokens(snippet)
    return prepared, total_tokens


def _validate_ranked_ids(
    ranked_ids: Any,
    *,
    candidate_ids: set[str],
    top_k: int,
) -> Optional[List[str]]:
    if not isinstance(ranked_ids, list):
        return None
    normalized: List[str] = []
    seen: set[str] = set()
    for item in ranked_ids:
        value = str(item).strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    if not normalized:
        return None
    if any(candidate_id not in candidate_ids for candidate_id in normalized):
        return None
    return normalized[:top_k]


def _extract_ranked_ids_from_response(
    parsed: Dict[str, Any],
    *,
    candidate_ids: set[str],
    top_k: int,
) -> Optional[Tuple[List[str], int]]:
    ranked = parsed.get("ranked")
    if isinstance(ranked, list):
        normalized: List[str] = []
        seen: set[str] = set()
        for item in ranked:
            if not isinstance(item, dict):
                return None
            candidate_id = str(item.get("id", "")).strip()
            if not candidate_id or candidate_id in seen:
                continue
            if candidate_id not in candidate_ids:
                return None
            score = _normalize_score(item.get("score"))
            if score is None:
                return None
            reason = item.get("reason")
            if reason is not None and not isinstance(reason, str):
                return None
            seen.add(candidate_id)
            normalized.append(candidate_id)
        if not normalized:
            return None
        result = normalized[:top_k]
        return result, len(result)

    ranked_ids = _validate_ranked_ids(
        parsed.get("ranked_ids"),
        candidate_ids=candidate_ids,
        top_k=top_k,
    )
    if ranked_ids is None:
        return None
    return ranked_ids, len(ranked_ids)


async def rerank_candidates(query: str, candidates: List[Dict[str, Any]], top_k: int) -> Optional[List[str]]:
    """Rerank candidates with an LLM; returns ranked candidate IDs or None on failure."""
    settings = get_settings()
    if not settings.RERANK_ENABLED or not candidates:
        return None

    provider = (settings.RERANK_PROVIDER or "gemini").strip().lower()
    input_candidates = candidates[:MAX_RERANK_CANDIDATE_COUNT_IN]
    max_snippet_tokens = min(
        DEFAULT_RERANK_MAX_SNIPPET_TOKENS,
        max(32, int(getattr(settings, "RERANK_MAX_SNIPPET_TOKENS", DEFAULT_RERANK_MAX_SNIPPET_TOKENS))),
    )
    max_total_input_tokens = min(
        DEFAULT_RERANK_MAX_TOTAL_INPUT_TOKENS,
        max(512, int(getattr(settings, "RERANK_MAX_TOTAL_INPUT_TOKENS", DEFAULT_RERANK_MAX_TOTAL_INPUT_TOKENS))),
    )
    prepared_candidates, estimated_input_tokens = _prepare_candidates_with_budget(
        query=query,
        candidates=input_candidates,
        max_snippet_tokens=max_snippet_tokens,
        max_total_input_tokens=max_total_input_tokens,
        max_candidates_sent=MAX_RERANK_CANDIDATE_COUNT_SENT,
    )
    if not prepared_candidates:
        logger.warning(
            "reranker_failed provider=%s model=%s candidate_count_in=%s candidate_count_sent=%s estimated_input_tokens=%s latency_ms=%s fallback_used=%s error_type=%s",
            provider,
            settings.RERANK_MODEL,
            len(input_candidates),
            0,
            0,
            0,
            True,
            "InputBudgetExceeded",
        )
        return None
    candidate_ids = {candidate["id"] for candidate in prepared_candidates}

    prompt = _build_prompt(query=query, prepared_candidates=prepared_candidates, top_k=top_k)

    started = time.perf_counter()
    try:
        if provider == "gemini":
            if not settings.GOOGLE_API_KEY:
                raise RuntimeError("GOOGLE_API_KEY is required for gemini reranker")
            runner = lambda: _run_gemini_rerank_sync(
                prompt=prompt,
                model=settings.RERANK_MODEL,
                api_key=settings.GOOGLE_API_KEY,
            )
        elif provider == "openai":
            if not settings.OPENAI_API_KEY:
                raise RuntimeError("OPENAI_API_KEY is required for openai reranker")
            runner = lambda: _run_openai_rerank_sync(
                prompt=prompt,
                model=settings.RERANK_MODEL,
                api_key=settings.OPENAI_API_KEY,
            )
        else:
            logger.warning("reranker_provider_unsupported provider=%s; using fallback order", provider)
            return None

        raw = await asyncio.wait_for(
            asyncio.to_thread(runner),
            timeout=RERANK_TIMEOUT_SECONDS,
        )
        parsed = _extract_json_object(raw)
        extracted = _extract_ranked_ids_from_response(
            parsed,
            candidate_ids=candidate_ids,
            top_k=top_k,
        )
        if extracted is None:
            raise ValueError("reranker returned invalid ranking payload")
        ranked_ids, candidate_count_out = extracted
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        logger.info(
            "reranker_success provider=%s model=%s candidate_count_in=%s candidate_count_sent=%s candidate_count_out=%s estimated_input_tokens=%s latency_ms=%s fallback_used=%s",
            provider,
            settings.RERANK_MODEL,
            len(input_candidates),
            len(prepared_candidates),
            candidate_count_out,
            estimated_input_tokens,
            elapsed_ms,
            False,
        )
        return ranked_ids[:top_k]
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        logger.warning(
            "reranker_failed provider=%s model=%s candidate_count_in=%s candidate_count_sent=%s estimated_input_tokens=%s latency_ms=%s fallback_used=%s error_type=%s",
            provider,
            settings.RERANK_MODEL,
            len(input_candidates),
            len(prepared_candidates),
            estimated_input_tokens,
            elapsed_ms,
            True,
            type(exc).__name__,
        )
        return None
