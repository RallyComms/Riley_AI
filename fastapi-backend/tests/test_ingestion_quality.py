import unittest

from app.services.ingestion import (
    _assess_extraction_quality,
    _compute_content_signals,
    _decide_final_ingestion_status,
)


class IngestionQualityDecisionTests(unittest.TestCase):
    def test_simple_text_pdf_is_indexed(self) -> None:
        text = "This is a text-heavy campaign research PDF with clear readable content. " * 10
        native_status, _ = _assess_extraction_quality(text, "memo.pdf", 120_000)
        signals = _compute_content_signals(
            native_text=text,
            merged_text=text,
            segments=[
                {
                    "location_type": "page",
                    "location_value": "1",
                    "text": text,
                }
            ],
        )
        status, _, _ = _decide_final_ingestion_status(
            file_type="pdf",
            quality_status=native_status,
            quality_reason="native extracted",
            signals=signals,
        )
        self.assertEqual(status, "indexed")

    def test_scanned_pdf_with_ocr_text_is_not_low_text(self) -> None:
        native_text = ""
        ocr_text = "OCR recovered substantial campaign language and policy details. " * 6
        signals = _compute_content_signals(
            native_text=native_text,
            merged_text=ocr_text,
            segments=[
                {
                    "location_type": "page",
                    "location_value": "1",
                    "text": ocr_text,
                    "ocr_text_present": True,
                    "ocr_status": "complete",
                }
            ],
        )
        status, _, _ = _decide_final_ingestion_status(
            file_type="pdf",
            quality_status="ocr_needed",
            quality_reason="native weak",
            signals=signals,
        )
        self.assertIn(status, {"indexed", "partial"})

    def test_image_heavy_pdf_with_vision_captions_is_not_low_text(self) -> None:
        signals = _compute_content_signals(
            native_text="",
            merged_text="",
            segments=[
                {
                    "location_type": "page",
                    "location_value": "1",
                    "text": "",
                    "vision_caption": "Chart shows public safety concern rising among likely voters.",
                },
                {
                    "location_type": "page",
                    "location_value": "2",
                    "text": "",
                    "vision_caption": "Infographic emphasizes economic anxiety and healthcare costs.",
                },
            ],
        )
        status, _, _ = _decide_final_ingestion_status(
            file_type="pdf",
            quality_status="ocr_needed",
            quality_reason="visual-heavy",
            signals=signals,
        )
        self.assertIn(status, {"indexed", "partial"})

    def test_mixed_quality_multipage_pdf_with_strong_pages_not_low_text(self) -> None:
        signals = _compute_content_signals(
            native_text="",
            merged_text="Strong extracted text from several pages. " * 5,
            segments=[
                {"location_type": "page", "location_value": "1", "text": ""},
                {"location_type": "page", "location_value": "2", "text": "Substantial policy analysis text." * 3},
                {"location_type": "page", "location_value": "3", "text": ""},
                {"location_type": "page", "location_value": "4", "text": "Readable messaging and framing notes." * 3},
                {"location_type": "page", "location_value": "5", "text": "Audience implications and risks." * 3},
            ],
        )
        status, _, _ = _decide_final_ingestion_status(
            file_type="pdf",
            quality_status="indexed",
            quality_reason="native partial pages",
            signals=signals,
        )
        self.assertIn(status, {"indexed", "partial"})

    def test_unreadable_document_remains_low_text(self) -> None:
        signals = _compute_content_signals(
            native_text="",
            merged_text="",
            segments=[],
        )
        status, _, _ = _decide_final_ingestion_status(
            file_type="pdf",
            quality_status="ocr_needed",
            quality_reason="no text",
            signals=signals,
        )
        self.assertEqual(status, "low_text")
