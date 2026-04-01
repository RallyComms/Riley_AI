import base64
import json
import logging
from typing import Any, Dict, Optional

import httpx

from app.core.config import get_settings
from app.services.llm_cost_guardrail import enforce_monthly_llm_cost_guardrail

logger = logging.getLogger(__name__)


def _extract_json_object(raw: str) -> Dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        raise ValueError("Empty vision response")
    try:
        return json.loads(text)
    except Exception:
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            return json.loads(text[start : end + 1])
        raise


def _normalize_visual_type(value: Any) -> Optional[str]:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    allowed = {
        "chart",
        "table",
        "infographic",
        "diagram",
        "screenshot",
        "poster",
        "photo",
        "slide",
        "document_page",
        "other",
    }
    return normalized if normalized in allowed else "other"


async def summarize_visual_content(
    *,
    image_bytes: bytes,
    model: Optional[str] = None,
    timeout_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """Summarize visual communication of an image using OpenAI multimodal."""
    await enforce_monthly_llm_cost_guardrail()

    settings = get_settings()
    api_key = settings.OPENAI_API_KEY
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for visual understanding")

    used_model = model or settings.RILEY_VISION_MODEL
    used_timeout = int(timeout_seconds or settings.RILEY_VISION_TIMEOUT_SECONDS)
    image_b64 = base64.b64encode(image_bytes).decode("utf-8")
    data_url = f"data:image/png;base64,{image_b64}"

    prompt = (
        "You analyze campaign research visuals. Return ONLY JSON with keys: "
        'has_visual_content (boolean), visual_type (one of: chart, table, infographic, diagram, screenshot, poster, photo, slide, document_page, other), '
        'vision_caption (string <= 80 words). '
        "Be factual, concise, and avoid speculation."
    )

    payload = {
        "model": used_model,
        "input": [
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": data_url},
                ],
            }
        ],
    }

    timeout = httpx.Timeout(max(5, used_timeout))
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            "https://api.openai.com/v1/responses",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
        body = response.json()
        raw_text = ""
        if isinstance(body.get("output_text"), str):
            raw_text = body.get("output_text") or ""
        if not raw_text:
            output = body.get("output") or []
            for item in output:
                content = item.get("content") if isinstance(item, dict) else None
                if not isinstance(content, list):
                    continue
                for segment in content:
                    if isinstance(segment, dict) and isinstance(segment.get("text"), str):
                        raw_text = segment["text"]
                        break
                if raw_text:
                    break
        parsed = _extract_json_object(raw_text)
        has_visual = bool(parsed.get("has_visual_content"))
        visual_type = _normalize_visual_type(parsed.get("visual_type"))
        caption_raw = parsed.get("vision_caption")
        caption = str(caption_raw).strip() if isinstance(caption_raw, str) else ""
        if len(caption) > 500:
            caption = caption[:500].rstrip()
        if not caption:
            caption = "No significant visual communication detected."
            has_visual = False
        return {
            "has_visual_content": has_visual,
            "visual_type": visual_type or ("other" if has_visual else None),
            "vision_caption": caption,
        }
