import io
import logging
import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import UploadFile, HTTPException
from fastapi.concurrency import run_in_threadpool
from qdrant_client.http.models import PointStruct

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
        elif file_ext == "xlsx":
            return await _extract_text_from_xlsx(file_bytes)
        elif file_ext in ("html", "htm"):
            return await _extract_text_from_html(file_bytes)
        elif file_ext in ("png", "jpg", "jpeg", "webp", "tiff"):
            # OCR is never performed during extraction - must use dedicated endpoint
            return await _extract_text_from_image(file_bytes, enable_ocr=False)
        else:
            return f"File: {filename}"
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


async def process_upload(
    file: UploadFile,
    tenant_id: str,
    tags: List[str],
    overwrite: bool = False
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
    
    # Generate unique filename
    if file.filename:
        # Use original filename but make it unique with UUID prefix to avoid collisions
        file_extension = "." + file.filename.split(".")[-1] if "." in file.filename else ""
        base_name = file.filename.rsplit(".", 1)[0] if "." in file.filename else file.filename
        unique_filename = f"{uuid.uuid4()}_{base_name}{file_extension}"
    else:
        # Generate unique filename if no filename provided
        file_extension = ""
        unique_filename = f"{uuid.uuid4()}{file_extension}"
    
    try:
        # Read file content into memory for text extraction
        file_content = await file.read()
        
        # Calculate file size and upload date before upload
        file_size = len(file_content)
        size_str = _format_file_size(file_size)
        upload_date = datetime.now().isoformat()
        
        # Extract text using the router function
        file_type = file.filename.split(".")[-1].lower() if file.filename else "unknown"
        filename = file.filename or "unknown"
        is_image = is_image_ext(filename)
        
        # Extract text (OCR is never performed during upload - must use dedicated endpoint)
        text = await extract_text(file_content, filename)
        
        # Ensure text doesn't exceed MAX_CHARS_TOTAL (safety check)
        if len(text) > MAX_CHARS_TOTAL:
            text = text[:MAX_CHARS_TOTAL]
        
        # Rewind file stream before uploading to GCS
        await file.seek(0)
        
        # Generate point_id early so it can be used for preview object naming
        point_id = str(uuid.uuid4())
        
        # Upload original to GCS
        file_url = await StorageService.upload_file(file, unique_filename)

        # Optional: generate and upload PDF preview for Office/HTML documents
        preview_url: Optional[str] = None
        preview_type: Optional[str] = None
        preview_status: str = "not_requested"
        preview_error: Optional[str] = None

        if settings.ENABLE_PREVIEW_GENERATION and is_office_or_html(filename):
            preview_status = "processing"
            try:
                pdf_bytes = await generate_pdf_preview(file_content, filename)

                # Use point_id-based name so previews are stable per document
                preview_object_name = f"{settings.PREVIEW_BUCKET_PATH_PREFIX}/{point_id}.pdf"

                # Upload preview PDF
                preview_public_url = await StorageService.upload_bytes(
                    preview_object_name,
                    pdf_bytes,
                    content_type="application/pdf",
                )

                # Optionally sign the preview URL
                if settings.SIGN_PREVIEW_URLS:
                    preview_url = await StorageService.generate_signed_url(
                        preview_object_name,
                        settings.PREVIEW_URL_TTL_SECONDS,
                    )
                else:
                    preview_url = preview_public_url

                preview_type = "pdf"
                preview_status = "complete"
                preview_error = None
            except HTTPException as exc:
                # Controlled failure - record status and let upload succeed
                preview_status = "failed"
                preview_error = exc.detail if isinstance(exc.detail, str) else str(exc.detail)
            except Exception as exc:
                preview_status = "failed"
                preview_error = str(exc)
        
        # Generate embedding
        if text.strip():
            vector = await _generate_embedding(text)
        else:
            # If no text extracted, generate a minimal embedding from filename
            # This ensures we can still index the file even without extractable text
            fallback_text = f"File: {file.filename or 'unknown'}"
            vector = await _generate_embedding(fallback_text)
        
        # Prepare Qdrant payload
        # (point_id already generated above for preview naming)
        
        # Determine ai_enabled based on file type
        # Default: pdf, docx, doc, pptx, xlsx, html -> ai_enabled = true
        # Images -> ai_enabled = false by default (conservative, requires explicit OCR)
        # All other file types -> ai_enabled = false
        # Special case: .doc files that fail extraction get ai_enabled = false
        ai_enabled_file_types = {"pdf", "docx", "doc", "pptx", "xlsx", "html", "htm"}
        image_types = {"png", "jpg", "jpeg", "webp", "tiff"}
        
        # Check if .doc extraction failed (returns fallback string)
        doc_extraction_failed = (
            file_type == "doc" and 
            (text.startswith("[DOC extraction unavailable") or 
             text.startswith("[Binary file — no text extracted]"))
        )
        
        if doc_extraction_failed:
            ai_enabled = False
            logger.warning(
                f"DOC extraction failed for {filename}; "
                f"ai_enabled set to False. Recommend converting to .docx format."
            )
        elif file_type in ai_enabled_file_types:
            ai_enabled = True
        elif file_type in image_types:
            ai_enabled = False  # Images default to false, require explicit OCR + ai_enabled toggle
        else:
            ai_enabled = False
        
        # Initialize OCR fields for images (or any file type for consistency)
        ocr_enabled = False
        ocr_status = "not_requested"
        ocr_text: Optional[str] = None
        ocr_confidence: Optional[float] = None
        ocr_language: Optional[str] = None
        ocr_error: Optional[str] = None
        ocr_extracted_at: Optional[str] = None
        
        # For images, set OCR fields even if not requested
        if is_image:
            ocr_status = "not_requested"
        
        # Prepare content fields:
        # - content: full extracted text (capped at MAX_CHARS_TOTAL) for RAG
        # - content_preview: first 1000 chars for UI display
        content_full = text  # Already capped at MAX_CHARS_TOTAL
        content_preview = text[:CONTENT_PREVIEW_LENGTH]
        
        # For images, keep placeholder in content
        if is_image:
            content_full = "[Image file — OCR not requested]"
            content_preview = content_full
        
        # Determine collection and payload flags based on tenant_id
        # Global uploads (tenant_id == "global") -> Tier 1 with is_global=True
        # Campaign uploads (tenant_id != "global") -> Tier 2 with client_id=tenant_id, is_global=False
        is_global_upload = tenant_id == "global"
        
        if is_global_upload:
            # Global archive: Tier 1, is_global=True, no client_id
            target_collection = settings.QDRANT_COLLECTION_TIER_1
            is_global_flag = True
            client_id_value = None  # Don't set client_id for global files
        else:
            # Campaign upload: Tier 2, is_global=False, client_id=tenant_id
            target_collection = settings.QDRANT_COLLECTION_TIER_2
            is_global_flag = False
            client_id_value = tenant_id
        
        payload = {
            "filename": file.filename or "unknown",
            "file_type": file_type,  # Normalized extension
            "type": file_type,  # Keep for backward compatibility
            "url": file_url,  # GCS public URL
            "is_global": is_global_flag,  # Critical flag for collection routing
            "tags": tags,
            "content": content_full,  # FULL TEXT for RAG (capped at MAX_CHARS_TOTAL)
            "content_preview": content_preview,  # Preview for UI (first 1000 chars)
            "size": size_str,  # Human-readable file size (e.g., "5.2 MB")
            "size_bytes": file_size,  # Raw size in bytes for sorting/filtering
            "upload_date": upload_date,  # ISO format timestamp
            "uploaded_at": upload_date,  # Alias for consistency
            "ai_enabled": ai_enabled,  # AI Memory toggle
            # Preview fields (read-only PDF previews for Office/HTML)
            "preview_url": preview_url,
            "preview_type": preview_type,
            "preview_status": preview_status,
            "preview_error": preview_error,
            # OCR fields (always included for consistency, especially for images)
            "ocr_enabled": ocr_enabled,
            "ocr_status": ocr_status,
            "ocr_text": ocr_text,
            "ocr_confidence": ocr_confidence,
            "ocr_language": ocr_language,
            "ocr_error": ocr_error,
            "ocr_extracted_at": ocr_extracted_at,
        }
        
        # Only set client_id for campaign uploads (not global)
        if client_id_value is not None:
            payload["client_id"] = client_id_value
        
        # Create Qdrant point
        point = PointStruct(
            id=point_id,
            vector=vector,
            payload=payload
        )
        
        # Upsert into Qdrant with collection routing based on tenant_id
        await vector_service.client.upsert(
            collection_name=target_collection,
            points=[point]
        )
        
        # Log collection routing for debugging
        logger.info(
            f"UPSERT collection={target_collection} tenant_id={tenant_id} id={point_id} "
            f"is_global={is_global_flag}"
        )
        
        return {
            "id": point_id,
            "url": file_url,
            "filename": file.filename or "unknown",
            "type": file_type,
        }
        
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process upload: {exc}"
        ) from exc


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

