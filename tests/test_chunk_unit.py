from __future__ import annotations

from pipeline.chunk import header_sections, slides_to_chunks


def test_header_sections_overlap():
    text = "A" * 1800
    chunks = header_sections(text, target=200, overlap=20)
    # Ensure overlap yields more than one chunk
    assert len(chunks) >= 2
    # Ensure non-empty chunks
    assert all(len(c) > 0 for c in chunks)


def test_header_sections_short():
    text = "short"
    chunks = header_sections(text, target=200, overlap=20)
    assert len(chunks) == 1
    assert chunks[0].startswith("short")


def test_slides_to_chunks_basic():
    slides = {
        1: {"title": "Intro", "body": "Welcome\nAgenda"},
        2: {"title": "Findings", "body": "We saw improvements."},
    }
    chunks = slides_to_chunks(slides, target=100, overlap=10)
    flat = " ".join(chunks)
    assert "[Slide 1]" in flat
    assert "Intro" in flat
    assert "Findings" in flat
