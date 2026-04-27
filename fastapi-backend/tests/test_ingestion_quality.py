import unittest

from app.services.ingestion import (
    _assess_extraction_quality,
    _build_pptx_slide_micro_chunks,
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


class PptxSlideChunkingTests(unittest.TestCase):
    def test_pptx_slide_chunk_preserves_title_notes_metadata(self) -> None:
        chunks, stats = _build_pptx_slide_micro_chunks(
            [
                {
                    "text": "Campaign Priority Update\nSpeaker notes: Focus on suburban voters first.",
                    "location_type": "slide",
                    "location_value": "1",
                    "slide_title": "Campaign Priority Update",
                    "char_start": 0,
                    "char_end": 80,
                    "pptx_total_slides": 1,
                }
            ],
            chunk_size_tokens=120,
            overlap_tokens=20,
            max_chars_per_chunk=6000,
        )
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["slide_number"], 1)
        self.assertEqual(chunks[0]["slide_title"], "Campaign Priority Update")
        self.assertEqual(chunks[0]["slide_subchunk_index"], 0)
        self.assertEqual(stats["total_slides"], 1)
        self.assertEqual(stats["indexed_slides"], 1)
        self.assertEqual(stats["skipped_slides"], 0)

    def test_pptx_long_slide_is_split_into_slide_subchunks(self) -> None:
        long_slide_text = " ".join(f"token{i}" for i in range(1200))
        chunks, stats = _build_pptx_slide_micro_chunks(
            [
                {
                    "text": long_slide_text,
                    "location_type": "slide",
                    "location_value": "3",
                    "slide_title": "Deep Dive Notes",
                    "char_start": 0,
                    "char_end": len(long_slide_text),
                    "pptx_total_slides": 3,
                }
            ],
            chunk_size_tokens=180,
            overlap_tokens=40,
            max_chars_per_chunk=1200,
        )
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(chunk["slide_number"] == 3 for chunk in chunks))
        self.assertTrue(all(chunk["slide_title"] == "Deep Dive Notes" for chunk in chunks))
        self.assertTrue(all(len(chunk["text"]) <= 1200 for chunk in chunks))
        self.assertEqual([chunk["slide_subchunk_index"] for chunk in chunks], list(range(len(chunks))))
        self.assertEqual(stats["total_slides"], 3)
        self.assertEqual(stats["indexed_slides"], 1)
        self.assertEqual(stats["skipped_slides"], 2)

    def test_pptx_empty_slides_not_chunked_but_counted(self) -> None:
        chunks, stats = _build_pptx_slide_micro_chunks(
            [
                {
                    "text": "Only this slide has text.",
                    "location_type": "slide",
                    "location_value": "2",
                    "slide_title": "Only Content Slide",
                    "char_start": 20,
                    "char_end": 44,
                    "pptx_total_slides": 4,
                }
            ],
            chunk_size_tokens=120,
            overlap_tokens=20,
            max_chars_per_chunk=6000,
        )
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0]["slide_number"], 2)
        self.assertEqual(stats["total_slides"], 4)
        self.assertEqual(stats["indexed_slides"], 1)
        self.assertEqual(stats["skipped_slides"], 3)
