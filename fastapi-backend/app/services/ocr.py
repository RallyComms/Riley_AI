"""Google Cloud Vision OCR helpers used by ingestion and OCR endpoint."""

import json
import logging
import uuid
from typing import Any, Dict, List, Optional
from urllib.parse import unquote, urlparse

from fastapi.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)


def is_image_ext(filename: str) -> bool:
    if not filename:
        return False
    ext = filename.split(".")[-1].lower() if "." in filename else ""
    return ext in ("png", "jpg", "jpeg", "webp", "tiff")


def gcs_uri_from_url(url: str, default_bucket: Optional[str] = None) -> Optional[str]:
    """Normalize gs/public/signed storage URLs to gs://bucket/object URI."""
    if not url:
        return None
    parsed = urlparse(url)
    if parsed.scheme == "gs":
        bucket = parsed.netloc
        name = unquote(parsed.path.lstrip("/"))
        return f"gs://{bucket}/{name}" if bucket and name else None
    if "storage.googleapis.com" in parsed.netloc:
        netloc = parsed.netloc.strip()
        path = parsed.path.lstrip("/")
        if netloc.endswith(".storage.googleapis.com") and netloc != "storage.googleapis.com":
            bucket = netloc[: -len(".storage.googleapis.com")]
            name = unquote(path)
            return f"gs://{bucket}/{name}" if bucket and name else None
        if not path:
            return None
        parts = path.split("/", 1)
        if len(parts) == 1 and default_bucket:
            return f"gs://{default_bucket}/{unquote(parts[0])}"
        if len(parts) >= 2:
            return f"gs://{parts[0]}/{unquote(parts[1])}"
    if default_bucket and parsed.path:
        return f"gs://{default_bucket}/{unquote(parsed.path.lstrip('/'))}"
    return None


def _extract_annotation_confidence(annotation: Any) -> Optional[float]:
    try:
        pages = list(getattr(annotation, "pages", []) or [])
        scores: List[float] = []
        for page in pages:
            for block in getattr(page, "blocks", []) or []:
                for para in getattr(block, "paragraphs", []) or []:
                    for word in getattr(para, "words", []) or []:
                        conf = getattr(word, "confidence", None)
                        if isinstance(conf, (float, int)) and conf >= 0:
                            scores.append(float(conf))
        if not scores:
            return None
        return max(0.0, min(1.0, sum(scores) / len(scores)))
    except Exception:
        return None


def _extract_annotation_language(annotation: Any) -> Optional[str]:
    try:
        pages = list(getattr(annotation, "pages", []) or [])
        if not pages:
            return None
        prop = getattr(pages[0], "property", None)
        langs = list(getattr(prop, "detected_languages", []) or [])
        if not langs:
            return None
        code = getattr(langs[0], "language_code", None)
        return str(code) if code else None
    except Exception:
        return None


async def run_ocr(image_bytes: bytes, max_chars: int = 8000) -> Dict[str, Optional[str | float]]:
    """OCR image bytes via Google Cloud Vision document_text_detection."""
    try:
        from google.cloud import vision
    except ImportError as exc:
        raise ImportError("google-cloud-vision is required for OCR") from exc

    def _perform() -> Dict[str, Optional[str | float]]:
        client = vision.ImageAnnotatorClient()
        image = vision.Image(content=image_bytes)
        result = client.document_text_detection(image=image)
        if result.error.message:
            raise RuntimeError(f"Vision OCR failed: {result.error.message}")
        annotation = result.full_text_annotation
        text = (getattr(annotation, "text", "") or "").strip()
        if len(text) > max_chars:
            text = text[:max_chars]
        return {
            "text": text,
            "confidence": _extract_annotation_confidence(annotation),
            "language": _extract_annotation_language(annotation),
        }

    return await run_in_threadpool(_perform)


async def run_ocr_document_from_gcs(
    *,
    gcs_source_uri: str,
    output_bucket: str,
    output_prefix: str,
    mime_type: str = "application/pdf",
    timeout_seconds: int = 120,
    max_chars: int = 20000,
    max_pages: int = 200,
) -> Dict[str, Any]:
    """OCR PDF/TIFF from GCS using Vision async batch API."""
    try:
        from google.cloud import storage, vision
    except ImportError as exc:
        raise ImportError("google-cloud-vision and google-cloud-storage are required") from exc

    def _run() -> Dict[str, Any]:
        client = vision.ImageAnnotatorClient()
        storage_client = storage.Client()
        clean_prefix = output_prefix.strip("/").rstrip("/")
        temp_prefix = f"{clean_prefix}/{uuid.uuid4()}"
        destination_uri = f"gs://{output_bucket}/{temp_prefix}/"

        feature = vision.Feature(type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION)
        gcs_source = vision.GcsSource(uri=gcs_source_uri)
        input_config = vision.InputConfig(gcs_source=gcs_source, mime_type=mime_type)
        output_config = vision.OutputConfig(
            gcs_destination=vision.GcsDestination(uri=destination_uri),
            batch_size=2,
        )
        request = vision.AsyncAnnotateFileRequest(
            features=[feature],
            input_config=input_config,
            output_config=output_config,
        )

        operation = client.async_batch_annotate_files(requests=[request])
        operation.result(timeout=timeout_seconds)

        bucket = storage_client.bucket(output_bucket)
        blobs = list(storage_client.list_blobs(output_bucket, prefix=f"{temp_prefix}/"))
        pages: List[Dict[str, Any]] = []
        try:
            for blob in blobs:
                if not blob.name.endswith(".json"):
                    continue
                payload = json.loads(blob.download_as_text())
                responses = payload.get("responses", [])
                for idx, response in enumerate(responses):
                    if len(pages) >= max_pages:
                        break
                    full = response.get("fullTextAnnotation") or {}
                    text = str(full.get("text") or "").strip()
                    if not text:
                        continue
                    page_num = (
                        response.get("context", {}).get("pageNumber")
                        or len(pages) + 1
                        or idx + 1
                    )
                    pages.append(
                        {
                            "page": int(page_num),
                            "text": text,
                            "confidence": None,
                            "language": None,
                        }
                    )
                if len(pages) >= max_pages:
                    break
        finally:
            # Best-effort cleanup of OCR temp output.
            for blob in blobs:
                try:
                    bucket.blob(blob.name).delete()
                except Exception:
                    pass

        merged = "\n\n".join(p["text"] for p in pages).strip()
        if len(merged) > max_chars:
            merged = merged[:max_chars]
        return {
            "text": merged,
            "confidence": None,
            "language": None,
            "pages": pages,
            "status": "complete" if pages else "failed",
        }

    return await run_in_threadpool(_run)
