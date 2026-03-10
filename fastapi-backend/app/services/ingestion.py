import io
import logging
import csv
import json
import re
import uuid
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import UploadFile, HTTPException
from fastapi.concurrency import run_in_threadpool
from qdrant_client.http.models import PointStruct, Filter, FieldCondition, MatchValue
from google.cloud import tasks_v2
from google.api_core.exceptions import AlreadyExists

from app.core.config import get_settings
from app.services.genai_client import get_genai_client
from app.services.ocr import is_image_ext
from app.services.preview import is_office_or_html, generate_pdf_preview
from app.services.qdrant import vector_service
from app.services.storage import StorageService

# Configure logging
logger = logging.getLogger(__name__)

# Constants for text extraction limits
MAX_CHARS_TOTAL = 20_000
CONTENT_PREVIEW_LENGTH = 1_000
XLSX_MAX_SHEETS = 3
XLSX_MAX_ROWS = 100
XLSX_MAX_COLS = 25
PPTX_MAX_SLIDES = 50
CSV_MAX_ROWS = 300
CSV_MAX_COLS = 40
CHUNK_SIZE_TOKENS = 850
CHUNK_OVERLAP_TOKENS = 120
MIN_EXTRACTED_CHARS = 120
EMBEDDING_COST_PER_1K_TOKENS_USD = 0.0001

TEXT_NATIVE_EXTENSIONS = {
    "pdf", "docx", "doc", "pptx", "ppt", "txt", "md", "rtf",
    "html", "htm", "csv", "xlsx", "xls", "json", "tsv",
}

IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "tiff"}


def _format_file_size(size_bytes: int) -> str:
    """Format file size in bytes to human-readable string.
    
    Args:
        size_bytes: File size in bytes
        
    Returns:
        Formatted string like "5.2 MB" or "1.5 KB"
    """
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes / (1024 * 1024):.1f} MB"




async def extract_text(
    file_bytes: bytes, filename: str
) -> str:
    """Router function to extract text from various file types.
    
    Note: OCR is NOT performed during extraction. OCR must be explicitly
    requested via the dedicated POST /files/{file_id}/ocr endpoint.
    
    Args:
        file_bytes: File content as bytes
        filename: Original filename (used to determine file type)
        
    Returns:
        Extracted text string, truncated to MAX_CHARS_TOTAL
        Returns "[Binary file — no text extracted]" on failure
    """
    # Normalize file extension
    file_ext = filename.split(".")[-1].lower() if "." in filename else ""
    
    try:
        if file_ext == "pdf":
            return await _extract_text_from_pdf(file_bytes)
        elif file_ext == "docx":
            return await _extract_text_from_docx(file_bytes)
        elif file_ext == "doc":
            return await _extract_text_from_doc(file_bytes)
        elif file_ext == "pptx":
            return await _extract_text_from_pptx(file_bytes)
        elif file_ext == "ppt":
            return await _extract_text_from_ppt(file_bytes)
        elif file_ext == "xlsx":
            return await _extract_text_from_xlsx(file_bytes)
        elif file_ext == "xls":
            return await _extract_text_from_xls(file_bytes)
        elif file_ext in ("html", "htm"):
            return await _extract_text_from_html(file_bytes)
        elif file_ext in ("txt", "md"):
            return await _extract_text_from_plain_text(file_bytes)
        elif file_ext == "csv":
            return await _extract_text_from_csv_like(file_bytes, delimiter=",")
        elif file_ext == "tsv":
            return await _extract_text_from_csv_like(file_bytes, delimiter="\t")
        elif file_ext == "json":
            return await _extract_text_from_json(file_bytes)
        elif file_ext == "rtf":
            return await _extract_text_from_rtf(file_bytes)
        elif file_ext in ("png", "jpg", "jpeg", "webp", "tiff"):
            # OCR is never performed during extraction - must use dedicated endpoint
            return await _extract_text_from_image(file_bytes, enable_ocr=False)
        else:
            return "[Unsupported file type for text extraction]"
    except Exception as exc:
        logger.warning(f"Text extraction failed for {filename}: {type(exc).__name__}: {exc}")
        return "[Binary file — no text extracted]"


async def _extract_text_from_pdf(file_content: bytes) -> str:
    """Extract text from a PDF file using pypdf from file content in memory."""
    try:
        from pypdf import PdfReader
        
        def _read_pdf():
            pdf_file = io.BytesIO(file_content)
            reader = PdfReader(pdf_file)
            text = ""
            for page in reader.pages:
                text += page.extract_text() + "\n"
            # Cap at MAX_CHARS_TOTAL
            if len(text) > MAX_CHARS_TOTAL:
                text = text[:MAX_CHARS_TOTAL]
            return text
        
        # Run blocking I/O in threadpool
        return await run_in_threadpool(_read_pdf)
    except ImportError:
        logger.error("pypdf is not installed. Install with: pip install pypdf")
        return "[PDF extraction unavailable — pypdf not installed]"
    except Exception as exc:
        logger.warning(f"PDF extraction failed: {exc}")
        return "[Binary file — no text extracted]"


async def _extract_text_from_docx(file_content: bytes) -> str:
    """Extract text from a DOCX file using python-docx from file content in memory.
    
    Args:
        file_content: The DOCX file content as bytes
        
    Returns:
        Extracted text, capped at 20,000 characters
        
    Raises:
        HTTPException: If extraction fails or library is not installed
    """
    try:
        from docx import Document
        
        def _read_docx():
            docx_file = io.BytesIO(file_content)
            doc = Document(docx_file)
            text_parts = []
            
            # Extract text from all paragraphs
            for paragraph in doc.paragraphs:
                if paragraph.text.strip():
                    text_parts.append(paragraph.text.strip())
            
            # Join paragraphs with newlines
            text = "\n".join(text_parts)
            
            # Cap at MAX_CHARS_TOTAL
            if len(text) > MAX_CHARS_TOTAL:
                text = text[:MAX_CHARS_TOTAL]
            
            return text
        
        # Run blocking I/O in threadpool
        return await run_in_threadpool(_read_docx)
    except ImportError:
        logger.error("python-docx is not installed. Install with: pip install python-docx")
        return "[DOCX extraction unavailable — python-docx not installed]"
    except Exception as exc:
        logger.warning(f"DOCX extraction failed: {exc}")
        return "[Binary file — no text extracted]"


async def _extract_text_from_xlsx(file_content: bytes) -> str:
    """Extract text from an XLSX file using openpyxl from file content in memory.
    
    Processes up to 3 sheets, 100 rows per sheet, and 25 columns per row.
    Values are joined as CSV-style text.
    
    Args:
        file_content: The XLSX file content as bytes
        
    Returns:
        Extracted text, capped at 20,000 characters
        
    Raises:
        HTTPException: If extraction fails or library is not installed
    """
    try:
        from openpyxl import load_workbook
        
        def _read_xlsx():
            xlsx_file = io.BytesIO(file_content)
            workbook = load_workbook(xlsx_file, data_only=True)
            text_parts = []
            
            # Process up to XLSX_MAX_SHEETS sheets
            sheet_count = 0
            for sheet_name in workbook.sheetnames:
                if sheet_count >= XLSX_MAX_SHEETS:
                    break
                
                sheet = workbook[sheet_name]
                sheet_text_parts = []
                
                # Process up to XLSX_MAX_ROWS rows
                for row_idx, row in enumerate(sheet.iter_rows(values_only=True)):
                    if row_idx >= XLSX_MAX_ROWS:
                        break
                    
                    # Process up to XLSX_MAX_COLS columns per row
                    row_values = []
                    for col_idx, cell_value in enumerate(row):
                        if col_idx >= XLSX_MAX_COLS:
                            break
                        
                        # Convert cell value to string, handling None
                        if cell_value is not None:
                            row_values.append(str(cell_value))
                        else:
                            row_values.append("")
                    
                    # Join row values as CSV-style (comma-separated)
                    if any(row_values):  # Only add non-empty rows
                        sheet_text_parts.append(",".join(row_values))
                
                # Add sheet content with sheet name header
                if sheet_text_parts:
                    text_parts.append(f"Sheet: {sheet_name}")
                    text_parts.extend(sheet_text_parts)
                
                sheet_count += 1
            
            # Join all text parts with newlines
            text = "\n".join(text_parts)
            
            # Cap at MAX_CHARS_TOTAL
            if len(text) > MAX_CHARS_TOTAL:
                text = text[:MAX_CHARS_TOTAL]
            
            return text
        
        # Run blocking I/O in threadpool
        return await run_in_threadpool(_read_xlsx)
    except ImportError:
        logger.error("openpyxl is not installed. Install with: pip install openpyxl")
        return "[XLSX extraction unavailable — openpyxl not installed]"
    except Exception as exc:
        logger.warning(f"XLSX extraction failed: {exc}")
        return "[Binary file — no text extracted]"


async def _extract_text_from_doc(file_content: bytes) -> str:
    """Extract text from a DOC file using textract (legacy binary format).
    
    Args:
        file_content: The DOC file content as bytes
        
    Returns:
        Extracted text, capped at MAX_CHARS_TOTAL
    """
    try:
        import textract
        
        def _read_doc():
            doc_file = io.BytesIO(file_content)
            # textract can work with BytesIO
            text = textract.process(doc_file, extension='doc', encoding='utf-8')
            if isinstance(text, bytes):
                text = text.decode('utf-8', errors='ignore')
            # Cap at MAX_CHARS_TOTAL
            if len(text) > MAX_CHARS_TOTAL:
                text = text[:MAX_CHARS_TOTAL]
            return text
        
        # Run blocking I/O in threadpool
        return await run_in_threadpool(_read_doc)
    except ImportError:
        logger.error("textract is not installed. Install with: pip install textract")
        return "[DOC extraction unavailable — textract not installed]"
    except Exception as exc:
        logger.warning(f"DOC extraction failed: {exc}")
        return "[Binary file — no text extracted]"


async def _extract_text_from_pptx(file_content: bytes) -> str:
    """Extract text from a PPTX file using python-pptx.
    
    Processes up to PPTX_MAX_SLIDES slides, extracting text from shapes.
    
    Args:
        file_content: The PPTX file content as bytes
        
    Returns:
        Extracted text, capped at MAX_CHARS_TOTAL
    """
    try:
        from pptx import Presentation
        
        def _read_pptx():
            pptx_file = io.BytesIO(file_content)
            prs = Presentation(pptx_file)
            text_parts = []
            
            # Process up to PPTX_MAX_SLIDES slides
            for slide_idx, slide in enumerate(prs.slides):
                if slide_idx >= PPTX_MAX_SLIDES:
                    break
                
                slide_text_parts = []
                
                # Extract text from all shapes in the slide
                for shape in slide.shapes:
                    if hasattr(shape, "text") and shape.text:
                        slide_text_parts.append(shape.text.strip())
                
                # Add slide content with slide number header
                if slide_text_parts:
                    text_parts.append(f"Slide {slide_idx + 1}:")
                    text_parts.extend(slide_text_parts)
            
            # Join all text parts with newlines
            text = "\n".join(text_parts)
            
            # Cap at MAX_CHARS_TOTAL
            if len(text) > MAX_CHARS_TOTAL:
                text = text[:MAX_CHARS_TOTAL]
            
            return text
        
        # Run blocking I/O in threadpool
        return await run_in_threadpool(_read_pptx)
    except ImportError:
        logger.error("python-pptx is not installed. Install with: pip install python-pptx")
        return "[PPTX extraction unavailable — python-pptx not installed]"
    except Exception as exc:
        logger.warning(f"PPTX extraction failed: {exc}")
        return "[Binary file — no text extracted]"


async def _extract_text_from_html(file_content: bytes) -> str:
    """Extract text from an HTML file using BeautifulSoup.
    
    Strips scripts, styles, and other non-content elements.
    
    Args:
        file_content: The HTML file content as bytes
        
    Returns:
        Extracted text, capped at MAX_CHARS_TOTAL
    """
    try:
        from bs4 import BeautifulSoup
        
        def _read_html():
            html_file = io.BytesIO(file_content)
            soup = BeautifulSoup(html_file, 'html.parser')
            
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            
            # Get text and clean up whitespace
            text = soup.get_text()
            # Clean up multiple newlines and spaces
            lines = [line.strip() for line in text.splitlines()]
            lines = [line for line in lines if line]  # Remove empty lines
            text = "\n".join(lines)
            
            # Cap at MAX_CHARS_TOTAL
            if len(text) > MAX_CHARS_TOTAL:
                text = text[:MAX_CHARS_TOTAL]
            
            return text
        
        # Run blocking I/O in threadpool
        return await run_in_threadpool(_read_html)
    except ImportError:
        logger.error("beautifulsoup4 is not installed. Install with: pip install beautifulsoup4")
        return "[HTML extraction unavailable — beautifulsoup4 not installed]"
    except Exception as exc:
        logger.warning(f"HTML extraction failed: {exc}")
        return "[Binary file — no text extracted]"


async def _extract_text_from_plain_text(file_content: bytes) -> str:
    """Extract text from plain UTF-family text files."""
    def _read_text() -> str:
        text = file_content.decode("utf-8", errors="ignore")
        return text[:MAX_CHARS_TOTAL]
    return await run_in_threadpool(_read_text)


async def _extract_text_from_csv_like(file_content: bytes, delimiter: str) -> str:
    """Extract text from CSV/TSV files as row-wise comma-separated lines."""
    def _read_csv() -> str:
        decoded = file_content.decode("utf-8", errors="ignore")
        stream = io.StringIO(decoded)
        reader = csv.reader(stream, delimiter=delimiter)
        lines: List[str] = []
        for row_idx, row in enumerate(reader):
            if row_idx >= CSV_MAX_ROWS:
                break
            clipped = [str(value) for value in row[:CSV_MAX_COLS]]
            if any(cell.strip() for cell in clipped):
                lines.append(", ".join(clipped))
        text = "\n".join(lines)
        return text[:MAX_CHARS_TOTAL]
    return await run_in_threadpool(_read_csv)


async def _extract_text_from_json(file_content: bytes) -> str:
    """Extract text from JSON by pretty-printing parsed content."""
    def _read_json() -> str:
        decoded = file_content.decode("utf-8", errors="ignore")
        try:
            obj = json.loads(decoded)
            normalized = json.dumps(obj, ensure_ascii=True, indent=2)
        except Exception:
            normalized = decoded
        return normalized[:MAX_CHARS_TOTAL]
    return await run_in_threadpool(_read_json)


async def _extract_text_from_rtf(file_content: bytes) -> str:
    """Extract text from RTF using striprtf if available, fallback regex cleanup."""
    def _read_rtf() -> str:
        raw = file_content.decode("utf-8", errors="ignore")
        try:
            from striprtf.striprtf import rtf_to_text
            cleaned = rtf_to_text(raw)
        except Exception:
            cleaned = re.sub(r"\\[a-zA-Z]+-?\d* ?", " ", raw)
            cleaned = cleaned.replace("{", " ").replace("}", " ")
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned[:MAX_CHARS_TOTAL]
    return await run_in_threadpool(_read_rtf)


async def _extract_text_from_ppt(file_content: bytes) -> str:
    """Extract text from legacy PPT via textract."""
    try:
        import textract

        def _read_ppt() -> str:
            payload = textract.process(io.BytesIO(file_content), extension="ppt", encoding="utf-8")
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8", errors="ignore")
            return payload[:MAX_CHARS_TOTAL]

        return await run_in_threadpool(_read_ppt)
    except ImportError:
        return "[PPT extraction unavailable — textract not installed]"
    except Exception as exc:
        logger.warning(f"PPT extraction failed: {exc}")
        return "[Binary file — no text extracted]"


async def _extract_text_from_xls(file_content: bytes) -> str:
    """Extract text from legacy XLS via textract."""
    try:
        import textract

        def _read_xls() -> str:
            payload = textract.process(io.BytesIO(file_content), extension="xls", encoding="utf-8")
            if isinstance(payload, bytes):
                payload = payload.decode("utf-8", errors="ignore")
            return payload[:MAX_CHARS_TOTAL]

        return await run_in_threadpool(_read_xls)
    except ImportError:
        return "[XLS extraction unavailable — textract not installed]"
    except Exception as exc:
        logger.warning(f"XLS extraction failed: {exc}")
        return "[Binary file — no text extracted]"


async def _extract_text_from_image(file_content: bytes, enable_ocr: bool = False) -> str:
    """Extract text from an image file using OCR (if enabled).
    
    NOTE: This function is deprecated in favor of the OCR service.
    For images, we now return placeholder text and handle OCR separately.
    
    Args:
        file_content: The image file content as bytes
        enable_ocr: If True, perform OCR using pytesseract (deprecated)
        
    Returns:
        Placeholder text - OCR is handled separately via OCR endpoint
    """
    # Images should not have OCR run during upload
    # OCR is handled via dedicated endpoint
    return "[Image file — OCR not requested]"


async def _generate_embedding(text: str) -> List[float]:
    """Generate an embedding vector for the given text using Google Gemini.
    
    This function is used for:
    - Initial file upload embeddings
    - OCR re-embedding after OCR completes (in files.py OCR endpoint)
    
    Uses the new google-genai SDK via the shared client.
    """
    settings = get_settings()
    model_name = settings.EMBEDDING_MODEL
    
    def _embed_sync(truncated_text: str) -> List[float]:
        """Synchronous embedding function to run in threadpool."""
        try:
            # Get client (lazy initialization)
            client = get_genai_client()
            # Use the shared client to generate embedding
            response = client.models.embed_content(
                model=model_name,
                contents=truncated_text
            )
            
            # Extract embedding vector from response
            if not response.embeddings or len(response.embeddings) == 0:
                raise RuntimeError(
                    f"Embedding response did not contain any embeddings for model '{model_name}'."
                )
            
            embedding = response.embeddings[0].values
            if not isinstance(embedding, list):
                raise RuntimeError(
                    f"Embedding response did not contain a valid vector for model '{model_name}'."
                )
            
            return embedding
        except RuntimeError:
            # Re-raise RuntimeError as-is
            raise
        except Exception as exc:
            error_msg = (
                f"Embedding failed using model '{model_name}': {exc}"
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg) from exc
    
    try:
        # Truncate text to 9000 characters to avoid token limits
        truncated_text = text[:9000]
        
        # Run blocking GenAI call in threadpool to avoid blocking event loop
        return await run_in_threadpool(_embed_sync, truncated_text)
    except RuntimeError:
        # Re-raise RuntimeError as-is
        raise
    except Exception as exc:
        error_msg = (
            f"Embedding failed using model '{model_name}': {exc}"
        )
        logger.error(error_msg)
        raise HTTPException(
            status_code=500,
            detail=error_msg
        ) from exc


def _estimate_tokens(text: str) -> int:
    """Rough token estimate used for chunk/cost telemetry."""
    if not text:
        return 0
    return max(1, int(len(text) / 4))


def _clean_extracted_text(text: str) -> str:
    """Normalize whitespace to improve quality checks and chunking stability."""
    normalized = (text or "").replace("\x00", " ").strip()
    normalized = re.sub(r"\r\n?", "\n", normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized


def _assess_extraction_quality(text: str, filename: str, file_size_bytes: int) -> Tuple[str, str]:
    """Classify extraction quality and return (status, reason)."""
    cleaned = _clean_extracted_text(text)
    ext = filename.split(".")[-1].lower() if "." in filename else ""

    if not cleaned:
        if ext == "pdf":
            return "ocr_needed", "No extractable native PDF text detected"
        return "low_text", "No extractable text detected"

    if len(cleaned) < MIN_EXTRACTED_CHARS and file_size_bytes > 10_000:
        if ext == "pdf":
            return "ocr_needed", "PDF appears image-based or text-poor"
        return "low_text", "Extracted text too short for file size"

    unique_ratio = len(set(cleaned.lower())) / max(1, len(cleaned))
    if unique_ratio < 0.03:
        return "low_text", "Low-information repetitive text"

    lowered = cleaned.lower()
    placeholder_markers = [
        "[binary file",
        "no text extracted",
        "extraction unavailable",
        "unsupported file type",
    ]
    if any(marker in lowered for marker in placeholder_markers):
        return "low_text", "Extraction returned placeholder text"

    return "indexed", "Extraction passed quality gate"


def _chunk_text_by_tokens(text: str, chunk_size_tokens: int = CHUNK_SIZE_TOKENS, overlap_tokens: int = CHUNK_OVERLAP_TOKENS) -> List[str]:
    """Split text into overlapping token-ish chunks using whitespace tokenization."""
    normalized = _clean_extracted_text(text)
    tokens = normalized.split()
    if not tokens:
        return []

    chunks: List[str] = []
    step = max(1, chunk_size_tokens - overlap_tokens)
    start = 0
    while start < len(tokens):
        end = min(len(tokens), start + chunk_size_tokens)
        chunk = " ".join(tokens[start:end]).strip()
        if chunk:
            chunks.append(chunk)
        if end >= len(tokens):
            break
        start += step
    return chunks


async def _delete_chunk_points(collection_name: str, parent_file_id: str) -> None:
    """Delete all chunk points for a parent file before reindexing."""
    chunk_filter = Filter(
        must=[
            FieldCondition(key="parent_file_id", match=MatchValue(value=parent_file_id)),
            FieldCondition(key="record_type", match=MatchValue(value="chunk")),
        ]
    )
    points, _ = await vector_service.client.scroll(
        collection_name=collection_name,
        scroll_filter=chunk_filter,
        limit=5000,
        with_payload=False,
        with_vectors=False,
    )
    if points:
        await vector_service.client.delete(
            collection_name=collection_name,
            points_selector=[str(point.id) for point in points],
        )


def _build_worker_payload(job_id: str, file_id: str, collection_name: str) -> Dict[str, str]:
    return {
        "job_id": job_id,
        "file_id": file_id,
        "collection_name": collection_name,
    }


async def _enqueue_cloud_task(job_id: str, file_id: str, collection_name: str) -> None:
    """Create a durable Cloud Task for ingestion worker execution."""
    settings = get_settings()
    if not settings.INGESTION_USE_CLOUD_TASKS:
        return
    if not settings.GCP_PROJECT_ID or not settings.INGESTION_WORKER_URL:
        raise RuntimeError("Cloud Tasks ingestion is enabled but GCP_PROJECT_ID/INGESTION_WORKER_URL is missing")

    payload = _build_worker_payload(job_id, file_id, collection_name)

    def _create_task_sync() -> None:
        client = tasks_v2.CloudTasksClient()
        parent = client.queue_path(
            settings.GCP_PROJECT_ID,
            settings.INGESTION_TASKS_LOCATION,
            settings.INGESTION_TASKS_QUEUE,
        )
        task_name = f"{parent}/tasks/{job_id}"
        headers = {"Content-Type": "application/json"}
        if settings.INGESTION_WORKER_TOKEN:
            headers["X-Ingestion-Worker-Token"] = settings.INGESTION_WORKER_TOKEN

        http_request: Dict[str, Any] = {
            "http_method": tasks_v2.HttpMethod.POST,
            "url": settings.INGESTION_WORKER_URL,
            "headers": headers,
            "body": json.dumps(payload).encode("utf-8"),
        }

        if settings.INGESTION_TASKS_SERVICE_ACCOUNT_EMAIL:
            http_request["oidc_token"] = {
                "service_account_email": settings.INGESTION_TASKS_SERVICE_ACCOUNT_EMAIL,
                "audience": settings.INGESTION_WORKER_URL,
            }

        task = {"name": task_name, "http_request": http_request}
        try:
            client.create_task(request={"parent": parent, "task": task})
        except AlreadyExists:
            # Task already exists for this job_id; treat as idempotent enqueue.
            return

    await run_in_threadpool(_create_task_sync)


async def create_ingestion_job(
    *,
    collection_name: str,
    file_id: str,
    tenant_id: str,
    initial_status: str = "queued",
) -> str:
    """Create/attach a durable ingestion job record on the parent file payload."""
    job_id = str(uuid.uuid4())
    now = datetime.now().isoformat()
    await vector_service.client.set_payload(
        collection_name=collection_name,
        payload={
            "ingestion_job_id": job_id,
            "ingestion_job_status": initial_status,
            "ingestion_job_attempt_count": 0,
            "ingestion_job_created_at": now,
            "ingestion_job_started_at": None,
            "ingestion_job_completed_at": None,
            "ingestion_job_error_message": None,
            "ingestion_job_processing_duration_ms": None,
            "ingestion_status": initial_status,
            "ingestion_queue_provider": "cloud_tasks" if get_settings().INGESTION_USE_CLOUD_TASKS else "inline",
            "tenant_scope": tenant_id,
        },
        points=[file_id],
    )
    return job_id


async def enqueue_ingestion_job(
    *,
    collection_name: str,
    file_id: str,
    tenant_id: str,
) -> str:
    """Create job metadata and enqueue durable worker task."""
    job_id = await create_ingestion_job(
        collection_name=collection_name,
        file_id=file_id,
        tenant_id=tenant_id,
        initial_status="queued",
    )
    await _enqueue_cloud_task(job_id, file_id, collection_name)
    return job_id


async def _run_ingestion_pipeline(
    *,
    job_id: Optional[str],
    collection_name: str,
    point_id: str,
    file_bytes: bytes,
    filename: str,
    file_type: str,
    tenant_id: str,
    is_global: bool,
    tags: List[str],
    file_url: str,
    size_str: str,
    file_size: int,
    upload_date: str,
    preview_url: Optional[str],
    preview_type: Optional[str],
    preview_status: str,
    preview_error: Optional[str],
) -> None:
    """Async extraction -> quality gate -> chunk embedding pipeline."""
    settings = get_settings()
    started = time.perf_counter()

    try:
        started_at = datetime.now().isoformat()
        attempt_count = 1
        if job_id:
            existing = await vector_service.client.retrieve(
                collection_name=collection_name,
                ids=[point_id],
                with_payload=True,
                with_vectors=False,
            )
            payload = existing[0].payload if existing else {}
            attempt_count = int((payload or {}).get("ingestion_job_attempt_count") or 0) + 1
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={
                "ingestion_status": "processing",
                "ingestion_started_at": started_at,
                "ingestion_job_status": "processing" if job_id else None,
                "ingestion_job_started_at": started_at if job_id else None,
                "ingestion_job_attempt_count": attempt_count if job_id else None,
                "ingestion_job_error_message": None if job_id else None,
            },
            points=[point_id],
        )

        extracted_text = await extract_text(file_bytes, filename)
        cleaned_text = _clean_extracted_text(extracted_text)[:MAX_CHARS_TOTAL]
        quality_status, quality_reason = _assess_extraction_quality(cleaned_text, filename, file_size)

        if quality_status != "indexed":
            await _delete_chunk_points(collection_name, point_id)
            duration_ms = int((time.perf_counter() - started) * 1000)
            completed_at = datetime.now().isoformat()
            await vector_service.client.set_payload(
                collection_name=collection_name,
                payload={
                    "record_type": "file",
                    "filename": filename,
                    "file_type": file_type,
                    "type": file_type,
                    "url": file_url,
                    "is_global": is_global,
                    "client_id": None if is_global else tenant_id,
                    "tags": tags,
                    "size": size_str,
                    "size_bytes": file_size,
                    "upload_date": upload_date,
                    "uploaded_at": upload_date,
                    "preview_url": preview_url,
                    "preview_type": preview_type,
                    "preview_status": preview_status,
                    "preview_error": preview_error,
                    "content": cleaned_text[:MAX_CHARS_TOTAL],
                    "content_preview": cleaned_text[:CONTENT_PREVIEW_LENGTH],
                    "ai_enabled": False,
                    "ingestion_status": quality_status,
                    "ingestion_error": quality_reason,
                    "extracted_char_count": len(cleaned_text),
                    "chunk_count": 0,
                    "embedding_model": settings.EMBEDDING_MODEL,
                    "embedding_tokens_estimate": 0,
                    "embedding_cost_estimate_usd": 0.0,
                    "processing_duration_ms": duration_ms,
                    "ingestion_job_status": quality_status if job_id else None,
                    "ingestion_job_completed_at": completed_at if job_id else None,
                    "ingestion_job_error_message": quality_reason if job_id else None,
                    "ingestion_job_processing_duration_ms": duration_ms if job_id else None,
                    "ocr_status": "not_requested" if file_type in IMAGE_EXTENSIONS else None,
                },
                points=[point_id],
            )
            logger.info(
                "ingestion_complete file_id=%s tenant=%s status=%s chars=%s chunks=0 reason=%s duration_ms=%s",
                point_id, tenant_id, quality_status, len(cleaned_text), quality_reason, duration_ms
            )
            return

        chunks = _chunk_text_by_tokens(cleaned_text)
        if not chunks:
            chunks = [cleaned_text[:MAX_CHARS_TOTAL]]

        await _delete_chunk_points(collection_name, point_id)

        chunk_points: List[PointStruct] = []
        total_tokens = 0
        for chunk_idx, chunk_text in enumerate(chunks):
            vector = await _generate_embedding(chunk_text)
            token_estimate = _estimate_tokens(chunk_text)
            total_tokens += token_estimate
            chunk_payload: Dict[str, Any] = {
                "record_type": "chunk",
                "parent_file_id": point_id,
                "chunk_id": f"{point_id}::chunk::{chunk_idx}",
                "chunk_index": chunk_idx,
                "filename": filename,
                "file_type": file_type,
                "type": file_type,
                "client_id": None if is_global else tenant_id,
                "is_global": is_global,
                "tenant_id": tenant_id,
                "url": file_url,
                "tags": tags,
                "ai_enabled": True,
                "ingestion_status": "indexed",
                "text": chunk_text,
                "content": chunk_text,
                "content_preview": chunk_text[:CONTENT_PREVIEW_LENGTH],
                "upload_date": upload_date,
                "uploaded_at": upload_date,
                "chunk_token_estimate": token_estimate,
            }
            chunk_points.append(
                PointStruct(
                    id=f"{point_id}::chunk::{chunk_idx}",
                    vector=vector,
                    payload=chunk_payload,
                )
            )

        await vector_service.client.upsert(
            collection_name=collection_name,
            points=chunk_points,
        )

        duration_ms = int((time.perf_counter() - started) * 1000)
        embedding_cost_estimate = round((total_tokens / 1000.0) * EMBEDDING_COST_PER_1K_TOKENS_USD, 6)
        parent_payload: Dict[str, Any] = {
            "record_type": "file",
            "filename": filename,
            "file_type": file_type,
            "type": file_type,
            "url": file_url,
            "is_global": is_global,
            "tags": tags,
            "content": cleaned_text[:MAX_CHARS_TOTAL],
            "content_preview": cleaned_text[:CONTENT_PREVIEW_LENGTH],
            "size": size_str,
            "size_bytes": file_size,
            "upload_date": upload_date,
            "uploaded_at": upload_date,
            "preview_url": preview_url,
            "preview_type": preview_type,
            "preview_status": preview_status,
            "preview_error": preview_error,
            "ai_enabled": True,
            "ingestion_status": "indexed",
            "ingestion_error": None,
            "extracted_char_count": len(cleaned_text),
            "chunk_count": len(chunk_points),
            "embedding_model": settings.EMBEDDING_MODEL,
            "embedding_tokens_estimate": total_tokens,
            "embedding_cost_estimate_usd": embedding_cost_estimate,
            "processing_duration_ms": duration_ms,
        }
        if not is_global:
            parent_payload["client_id"] = tenant_id
        completed_at = datetime.now().isoformat()
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload=parent_payload,
            points=[point_id],
        )
        if job_id:
            await vector_service.client.set_payload(
                collection_name=collection_name,
                payload={
                    "ingestion_job_status": "indexed",
                    "ingestion_job_completed_at": completed_at,
                    "ingestion_job_error_message": None,
                    "ingestion_job_processing_duration_ms": duration_ms,
                },
                points=[point_id],
            )
        logger.info(
            "ingestion_complete file_id=%s tenant=%s status=indexed chars=%s chunks=%s duration_ms=%s tokens=%s est_cost_usd=%s",
            point_id, tenant_id, len(cleaned_text), len(chunk_points), duration_ms, total_tokens, embedding_cost_estimate
        )
    except Exception as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        logger.exception("ingestion_failed file_id=%s tenant=%s error=%s", point_id, tenant_id, exc)
        completed_at = datetime.now().isoformat()
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={
                "ingestion_status": "failed",
                "ingestion_error": str(exc),
                "processing_duration_ms": duration_ms,
                "ai_enabled": False,
                "ingestion_job_status": "failed" if job_id else None,
                "ingestion_job_completed_at": completed_at if job_id else None,
                "ingestion_job_error_message": str(exc) if job_id else None,
                "ingestion_job_processing_duration_ms": duration_ms if job_id else None,
            },
            points=[point_id],
        )


async def process_upload(
    file: UploadFile,
    tenant_id: str,
    tags: List[str],
    overwrite: bool = False,
) -> dict:
    """
    Process an uploaded file: upload to GCS, extract text, vectorize, and index in Qdrant.
    
    STANDARDIZED CONTENT STORAGE (SINGLE SOURCE OF TRUTH):
    - payload["content"]: FULL extracted text (capped at MAX_CHARS_TOTAL=20,000) for RAG
    - payload["content_preview"]: First 1000 chars for UI display
    - For images: content = "[Image file — OCR not requested]" (placeholder)
    
    This ensures RAG has access to full text, not just 1000 chars.
    
    Args:
        file: The uploaded file
        tenant_id: Tenant/client identifier for data isolation
        tags: List of tags associated with the file
        overwrite: If True, replace existing file with same name. If False, raise 409 on conflict.
        Note: OCR is NOT performed during upload. OCR must be explicitly
        requested via the dedicated POST /files/{file_id}/ocr endpoint.
        
    Returns:
        Dictionary containing:
            - id: The UUID of the indexed document
            - url: The public GCS URL to access the file
            - filename: The original filename
            - type: The file type/extension
    """
    settings = get_settings()

    if file.filename:
        file_extension = "." + file.filename.split(".")[-1] if "." in file.filename else ""
        base_name = file.filename.rsplit(".", 1)[0] if "." in file.filename else file.filename
        unique_filename = f"{uuid.uuid4()}_{base_name}{file_extension}"
    else:
        unique_filename = str(uuid.uuid4())

    try:
        file_content = await file.read()
        file_size = len(file_content)
        size_str = _format_file_size(file_size)
        upload_date = datetime.now().isoformat()
        filename = file.filename or "unknown"
        file_type = filename.split(".")[-1].lower() if "." in filename else "unknown"
        is_global_upload = tenant_id == "global"
        is_supported_text = file_type in TEXT_NATIVE_EXTENSIONS
        is_image = is_image_ext(filename)

        point_id = str(uuid.uuid4())

        # Upload original bytes first.
        file_url = await StorageService.upload_bytes(
            unique_filename,
            file_content,
            content_type=file.content_type or "application/octet-stream",
        )

        preview_url: Optional[str] = None
        preview_type: Optional[str] = None
        preview_status: str = "not_requested"
        preview_error: Optional[str] = None
        if settings.ENABLE_PREVIEW_GENERATION and is_office_or_html(filename):
            preview_status = "processing"
            try:
                pdf_bytes = await generate_pdf_preview(file_content, filename)
                preview_object_name = f"{settings.PREVIEW_BUCKET_PATH_PREFIX}/{point_id}.pdf"
                preview_public_url = await StorageService.upload_bytes(
                    preview_object_name,
                    pdf_bytes,
                    content_type="application/pdf",
                )
                if settings.SIGN_PREVIEW_URLS:
                    preview_url = await StorageService.generate_signed_url(
                        preview_object_name,
                        settings.PREVIEW_URL_TTL_SECONDS,
                    )
                else:
                    preview_url = preview_public_url
                preview_type = "pdf"
                preview_status = "complete"
            except HTTPException as exc:
                preview_status = "failed"
                preview_error = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
            except Exception as exc:
                preview_status = "failed"
                preview_error = str(exc)

        target_collection = (
            settings.QDRANT_COLLECTION_TIER_1 if is_global_upload
            else settings.QDRANT_COLLECTION_TIER_2
        )
        ingestion_status = "uploaded"
        if is_image:
            ingestion_status = "ocr_needed"

        payload: Dict[str, Any] = {
            "record_type": "file",
            "filename": filename,
            "file_type": file_type,
            "type": file_type,
            "url": file_url,
            "is_global": is_global_upload,
            "tags": tags,
            "size": size_str,
            "size_bytes": file_size,
            "upload_date": upload_date,
            "uploaded_at": upload_date,
            "ai_enabled": False,
            "content": "",
            "content_preview": "",
            "ingestion_status": ingestion_status,
            "ingestion_error": None,
            "extracted_char_count": 0,
            "chunk_count": 0,
            "embedding_model": settings.EMBEDDING_MODEL,
            "embedding_tokens_estimate": 0,
            "embedding_cost_estimate_usd": 0.0,
            "preview_url": preview_url,
            "preview_type": preview_type,
            "preview_status": preview_status,
            "preview_error": preview_error,
            "ocr_enabled": False,
            "ocr_status": "not_requested" if is_image else None,
        }
        if not is_global_upload:
            payload["client_id"] = tenant_id
        if is_image:
            payload["content"] = "[Image file — OCR not requested]"
            payload["content_preview"] = payload["content"]

        placeholder_vector = [0.0] * int(settings.EMBEDDING_DIM)
        await vector_service.client.upsert(
            collection_name=target_collection,
            points=[PointStruct(id=point_id, vector=placeholder_vector, payload=payload)],
        )

        if is_supported_text:
            try:
                job_id = await enqueue_ingestion_job(
                    collection_name=target_collection,
                    file_id=point_id,
                    tenant_id=tenant_id,
                )
                await vector_service.client.set_payload(
                    collection_name=target_collection,
                    payload={
                        "ingestion_status": "queued",
                        "ingestion_job_id": job_id,
                    },
                    points=[point_id],
                )
            except Exception as enqueue_exc:
                await vector_service.client.set_payload(
                    collection_name=target_collection,
                    payload={
                        "ingestion_status": "failed",
                        "ingestion_error": f"Failed to enqueue ingestion job: {enqueue_exc}",
                        "ingestion_job_status": "failed",
                        "ingestion_job_error_message": str(enqueue_exc),
                    },
                    points=[point_id],
                )
                logger.exception("ingestion_enqueue_failed file_id=%s tenant=%s", point_id, tenant_id)
        elif not is_image:
            await vector_service.client.set_payload(
                collection_name=target_collection,
                payload={
                    "ingestion_status": "failed",
                    "ingestion_error": f"Unsupported file type for Phase 1A: {file_type}",
                },
                points=[point_id],
            )

        logger.info(
            "upload_staged file_id=%s tenant=%s file_type=%s collection=%s async_ingestion=%s status=%s",
            point_id,
            tenant_id,
            file_type,
            target_collection,
            is_supported_text,
            ingestion_status,
        )
        return {
            "id": point_id,
            "url": file_url,
            "filename": filename,
            "type": file_type,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process upload: {exc}"
        ) from exc


async def reindex_existing_file(
    file_id: str,
    collection_name: str,
) -> Optional[str]:
    """Create and enqueue a durable reindex job for an existing file."""
    points = await vector_service.client.retrieve(
        collection_name=collection_name,
        ids=[file_id],
        with_payload=True,
        with_vectors=False,
    )
    if not points:
        raise HTTPException(status_code=404, detail=f"File with ID {file_id} not found")

    point = points[0]
    payload = point.payload or {}
    if payload.get("record_type") == "chunk":
        return None

    file_type = (payload.get("file_type") or payload.get("type") or "").lower()
    if file_type not in TEXT_NATIVE_EXTENSIONS:
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={
                "ingestion_status": "failed",
                "ingestion_error": f"Unsupported file type for Phase 1A: {file_type or 'unknown'}",
            },
            points=[file_id],
        )
        return None

    tenant_id = payload.get("client_id") or "global"
    job_id = await enqueue_ingestion_job(
        collection_name=collection_name,
        file_id=str(file_id),
        tenant_id=tenant_id,
    )
    return job_id


async def run_ingestion_job(
    *,
    job_id: str,
    file_id: str,
    collection_name: str,
) -> None:
    """Worker entrypoint: load job context and execute ingestion pipeline."""
    points = await vector_service.client.retrieve(
        collection_name=collection_name,
        ids=[file_id],
        with_payload=True,
        with_vectors=False,
    )
    if not points:
        raise HTTPException(status_code=404, detail=f"File with ID {file_id} not found")
    payload = points[0].payload or {}
    if payload.get("record_type") == "chunk":
        return

    current_job_id = payload.get("ingestion_job_id")
    if current_job_id and current_job_id != job_id:
        # Stale task; a newer job exists for this file.
        return

    filename = payload.get("filename") or "unknown"
    file_type = (payload.get("file_type") or payload.get("type") or "").lower()
    file_url = payload.get("url")
    if not file_url:
        raise HTTPException(status_code=400, detail=f"File {file_id} has no URL")

    if file_type not in TEXT_NATIVE_EXTENSIONS:
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={
                "ingestion_status": "failed",
                "ingestion_error": f"Unsupported file type for Phase 1A: {file_type or 'unknown'}",
                "ingestion_job_status": "failed",
                "ingestion_job_error_message": f"Unsupported file type: {file_type or 'unknown'}",
                "ingestion_job_completed_at": datetime.now().isoformat(),
            },
            points=[file_id],
        )
        return

    file_bytes = await StorageService.download_file(file_url)
    tenant_id = payload.get("client_id") or "global"
    is_global = bool(payload.get("is_global")) or collection_name == get_settings().QDRANT_COLLECTION_TIER_1
    tags = payload.get("tags", []) if isinstance(payload.get("tags"), list) else []
    upload_date = payload.get("upload_date") or datetime.now().isoformat()
    size_bytes = payload.get("size_bytes") or len(file_bytes)
    size_str = payload.get("size") or _format_file_size(int(size_bytes))

    await _run_ingestion_pipeline(
        job_id=job_id,
        collection_name=collection_name,
        point_id=str(file_id),
        file_bytes=file_bytes,
        filename=filename,
        file_type=file_type,
        tenant_id=tenant_id,
        is_global=is_global,
        tags=tags,
        file_url=file_url,
        size_str=size_str,
        file_size=int(size_bytes),
        upload_date=upload_date,
        preview_url=payload.get("preview_url"),
        preview_type=payload.get("preview_type"),
        preview_status=payload.get("preview_status", "not_requested"),
        preview_error=payload.get("preview_error"),
    )


async def backfill_reindex_supported_files(
    tenant_id: Optional[str] = None,
    include_global: bool = True,
    limit: int = 500,
) -> Dict[str, int]:
    """Backfill existing file points by enqueueing durable Phase 1A jobs."""
    settings = get_settings()
    totals = {"seen": 0, "enqueued": 0, "skipped": 0, "failed": 0}

    targets: List[Tuple[str, Optional[Filter]]] = []
    tenant_filter = None
    if tenant_id and tenant_id != "global":
        tenant_filter = Filter(
            must=[FieldCondition(key="client_id", match=MatchValue(value=tenant_id))]
        )
    targets.append((settings.QDRANT_COLLECTION_TIER_2, tenant_filter))
    if include_global:
        global_filter = Filter(must=[FieldCondition(key="is_global", match=MatchValue(value=True))])
        targets.append((settings.QDRANT_COLLECTION_TIER_1, global_filter))

    for collection_name, collection_filter in targets:
        points, _ = await vector_service.client.scroll(
            collection_name=collection_name,
            scroll_filter=collection_filter,
            limit=limit,
            with_payload=True,
            with_vectors=False,
        )
        for point in points:
            totals["seen"] += 1
            payload = point.payload or {}
            if payload.get("record_type") == "chunk":
                totals["skipped"] += 1
                continue
            file_type = (payload.get("file_type") or payload.get("type") or "").lower()
            if file_type not in TEXT_NATIVE_EXTENSIONS:
                totals["skipped"] += 1
                continue
            try:
                job_id = await reindex_existing_file(str(point.id), collection_name)
                if job_id:
                    totals["enqueued"] += 1
                else:
                    totals["skipped"] += 1
            except Exception:
                totals["failed"] += 1
                logger.exception("backfill_reindex_failed file_id=%s collection=%s", point.id, collection_name)
    return totals


async def delete_file(file_id: str, collection_name: str) -> None:
    """
    Permanently delete a file from GCS and Qdrant.
    
    Args:
        file_id: The Qdrant point ID (UUID) of the file to delete
        collection_name: Name of the Qdrant collection
        
    Raises:
        HTTPException: If file not found or deletion fails
    """
    # Retrieve the point to get file metadata
    try:
        points = await vector_service.client.retrieve(
            collection_name=collection_name,
            ids=[file_id],
            with_payload=True,
            with_vectors=False
        )
        
        if not points:
            raise HTTPException(
                status_code=404,
                detail=f"File with ID {file_id} not found in Qdrant"
            )
        
        point = points[0]
        payload = point.payload or {}
        file_url = payload.get("url")
        
        if not file_url:
            raise HTTPException(
                status_code=404,
                detail=f"File URL not found for ID {file_id}"
            )
        
        # Delete file from GCS
        try:
            await StorageService.delete_file(file_url)
        except HTTPException as exc:
            # If file not found in GCS, log but continue with Qdrant deletion
            # (file might have been manually deleted from GCS)
            if exc.status_code == 404:
                pass  # Continue with Qdrant deletion
            else:
                raise
        
        # Delete point from Qdrant
        await vector_service.client.delete(
            collection_name=collection_name,
            points_selector=[file_id]
        )

        # Delete chunk points linked to this parent file
        await _delete_chunk_points(collection_name, file_id)
        
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to delete file: {exc}"
        ) from exc


async def untag_file(file_id: str, tag: str | None, collection_name: str) -> None:
    """
    Remove a specific tag from a file's metadata in Qdrant, or clear all tags.
    
    Args:
        file_id: The Qdrant point ID (UUID) of the file
        tag: The tag to remove (e.g., "Messaging", "Research"). If None, clears all tags.
        collection_name: Name of the Qdrant collection
        
    Raises:
        HTTPException: If file not found or update fails
    """
    # Retrieve the point to get current payload
    try:
        points = await vector_service.client.retrieve(
            collection_name=collection_name,
            ids=[file_id],
            with_payload=True,
            with_vectors=False
        )
        
        if not points:
            raise HTTPException(
                status_code=404,
                detail=f"File with ID {file_id} not found in Qdrant"
            )
        
        point = points[0]
        payload = point.payload or {}
        current_tags = payload.get("tags", [])
        
        # If tag is None, clear all tags
        if tag is None:
            updated_tags = []
        else:
            # Remove the specific tag if it exists
            if isinstance(current_tags, list) and tag in current_tags:
                updated_tags = [t for t in current_tags if t != tag]
            else:
                # Tag doesn't exist, but that's okay - just return success
                return
        
        # Update the payload with the new tags list
        await vector_service.client.set_payload(
            collection_name=collection_name,
            payload={"tags": updated_tags},
            points=[file_id]
        )
        
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to untag file: {exc}"
        ) from exc

