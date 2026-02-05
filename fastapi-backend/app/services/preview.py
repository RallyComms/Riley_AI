"""Document preview generation service.

Generates read-only PDF previews for Office/HTML documents using LibreOffice.
"""

import asyncio
import os
import tempfile
from pathlib import Path
from typing import Final

from fastapi import HTTPException

from app.core.config import get_settings


OFFICE_EXTENSIONS: Final[set[str]] = {
    "doc",
    "docx",
    "xls",
    "xlsx",
    "ppt",
    "pptx",
    "html",
    "htm",
}


def is_office_or_html(filename: str) -> bool:
    """Return True if the filename looks like an Office or HTML document."""
    ext = Path(filename).suffix.lower().lstrip(".")
    return ext in OFFICE_EXTENSIONS


async def generate_pdf_preview(file_bytes: bytes, filename: str) -> bytes:
    """Generate a PDF preview for an Office/HTML document using LibreOffice.

    Workflow:
    - Write the incoming bytes to a temp file with the correct extension.
    - Run `soffice --headless --convert-to pdf --outdir /tmp <input>`.
    - Read the resulting PDF bytes.
    - Clean up temp files.

    Raises HTTPException(500) on failure with a clear message.
    """
    settings = get_settings()

    # Enforce preview size limit separately from upload size
    max_preview_bytes = int(getattr(settings, "PREVIEW_MAX_MB", 25)) * 1024 * 1024
    if len(file_bytes) > max_preview_bytes:
        raise HTTPException(
            status_code=500,
            detail=f"Preview generation skipped: file exceeds preview limit ({settings.PREVIEW_MAX_MB}MB).",
        )

    ext = Path(filename).suffix or ""
    if not ext:
        # Default to .docx if extension missing but still requested
        ext = ".docx"

    # Use a dedicated temp directory so we can clean up easily
    with tempfile.TemporaryDirectory(prefix="riley_preview_") as tmpdir:
        input_path = Path(tmpdir) / f"source{ext}"
        output_path = Path(tmpdir) / "source.pdf"

        # Write input bytes
        input_path.write_bytes(file_bytes)

        # Build LibreOffice command
        cmd = [
            "soffice",
            "--headless",
            "--nologo",
            "--nofirststartwizard",
            "--convert-to",
            "pdf",
            "--outdir",
            str(tmpdir),
            str(input_path),
        ]

        try:
            # Run with a hard timeout to avoid hanging
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            try:
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30.0)
            except asyncio.TimeoutError:
                proc.kill()
                raise HTTPException(
                    status_code=500,
                    detail="Preview generation timed out while converting to PDF.",
                )

            if proc.returncode != 0:
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "Preview generation failed via LibreOffice. "
                        f"Return code: {proc.returncode}. Stderr: {stderr.decode('utf-8', errors='ignore')[:500]}"
                    ),
                )

            if not output_path.exists():
                raise HTTPException(
                    status_code=500,
                    detail="Preview generation failed: output PDF not found.",
                )

            pdf_bytes = output_path.read_bytes()
            if not pdf_bytes:
                raise HTTPException(
                    status_code=500,
                    detail="Preview generation produced an empty PDF.",
                )

            return pdf_bytes

        except HTTPException:
            raise
        except FileNotFoundError:
            # soffice not installed or not in PATH
            raise HTTPException(
                status_code=500,
                detail=(
                    "Preview generation is not available: LibreOffice (soffice) is not installed. "
                    "Install it with 'apt-get update && apt-get install -y libreoffice'."
                ),
            )
        except Exception as exc:  # pragma: no cover - OS/LibreOffice edge cases
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error during preview generation: {exc}",
            )
