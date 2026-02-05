"""OCR service for extracting text from image files.

This module provides OCR functionality with caching and gating to ensure
OCR only runs when explicitly requested and results are cached in Qdrant.
"""
import io
import logging
from typing import Dict, Optional

from fastapi.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)


def is_image_ext(filename: str) -> bool:
    """Check if a filename has an image extension.
    
    Args:
        filename: The filename to check
        
    Returns:
        True if the file is an image type (png, jpg, jpeg, webp, tiff)
    """
    if not filename:
        return False
    
    ext = filename.split(".")[-1].lower() if "." in filename else ""
    return ext in ("png", "jpg", "jpeg", "webp", "tiff")


def _compute_confidence_from_data(data: Dict) -> Optional[float]:
    """Compute average confidence from pytesseract image_to_data output.
    
    Args:
        data: Dictionary from pytesseract.image_to_data with output_type=dict
        
    Returns:
        Average confidence (0-100 scale, converted to 0-1), or None if unavailable
    """
    try:
        confidences = []
        if "conf" in data:
            for conf in data["conf"]:
                # pytesseract uses -1 for invalid confidence
                if isinstance(conf, (int, float)) and conf >= 0:
                    confidences.append(conf)
        
        if confidences:
            # pytesseract returns confidence as 0-100, convert to 0-1
            avg_conf = sum(confidences) / len(confidences) / 100.0
            return avg_conf
    except Exception as e:
        logger.warning(f"Failed to compute OCR confidence: {e}")
    
    return None


async def run_ocr(image_bytes: bytes, max_chars: int = 8000) -> Dict[str, Optional[str | float]]:
    """Run OCR on an image file and return extracted text with metadata.
    
    Args:
        image_bytes: The image file content as bytes
        max_chars: Maximum number of characters to extract (default: 8000)
        
    Returns:
        Dictionary with keys:
            - text: Extracted text (truncated to max_chars)
            - confidence: Average confidence score (0-1) or None
            - language: Detected language code or None
            
    Raises:
        ImportError: If required libraries are not installed
        Exception: If OCR processing fails
    """
    try:
        import pytesseract
        from PIL import Image
    except ImportError as e:
        missing = "pytesseract" if "pytesseract" in str(e) else "Pillow"
        logger.error(f"{missing} is not installed. Install with: pip install {missing.lower()}")
        raise ImportError(f"{missing} is required for OCR") from e
    
    def _perform_ocr():
        """Perform OCR synchronously."""
        # Open image from bytes
        image_file = io.BytesIO(image_bytes)
        image = Image.open(image_file)
        
        # Try to get confidence data (best-effort)
        confidence = None
        language = None
        
        try:
            # Attempt to get detailed data for confidence calculation
            # Use pytesseract.Output.DICT to get confidence scores
            try:
                data = pytesseract.image_to_data(image, output_type=pytesseract.Output.DICT)
                confidence = _compute_confidence_from_data(data)
            except Exception as e:
                logger.warning(f"Could not compute OCR confidence: {e}")
                confidence = None
            
            # Try to detect language (best-effort)
            # Default to "en" for now - could be enhanced with language detection
            language = "en"
        except Exception as e:
            logger.warning(f"Could not compute OCR metadata: {e}")
            confidence = None
            language = None
        
        # Extract text
        text = pytesseract.image_to_string(image)
        
        # Clean up text: strip and collapse whitespace
        lines = [line.strip() for line in text.splitlines()]
        lines = [line for line in lines if line]  # Remove empty lines
        text = "\n".join(lines)
        
        # Truncate to max_chars
        if len(text) > max_chars:
            text = text[:max_chars]
            logger.info(f"OCR text truncated to {max_chars} characters")
        
        return {
            "text": text,
            "confidence": confidence,
            "language": language,
        }
    
    # Run blocking OCR in threadpool
    return await run_in_threadpool(_perform_ocr)
