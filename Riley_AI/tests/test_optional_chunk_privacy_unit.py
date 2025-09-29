import pytest

chunk_mod = pytest.importorskip("pipeline.chunk", reason="wire pipeline.chunk")
privacy_mod = pytest.importorskip("pipeline.privacy", reason="wire pipeline.privacy")

def test_chunk_roundtrip():
    text = " ".join(["alpha"] * 1200)
    chunks = chunk_mod.chunk_text(text, max_tokens=200, overlap_tokens=20)
    assert len(chunks) >= 2
    assert all(isinstance(c, str) and c for c in chunks)

def test_privacy_detect_and_redact():
    text = "Email me at a@b.com or call +1 202 555 0142 at 123 Main Street"
    pii = privacy_mod.detect_pii(text)
    assert pii["emails"] >= 1 and pii["phones"] >= 1 and pii["addresses"] >= 1
    red = privacy_mod.redact_text(text)
    assert "@" not in red and "555" not in red and "Main Street" not in red
