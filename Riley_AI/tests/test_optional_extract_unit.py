import pytest

extract = pytest.importorskip("pipeline.extract", reason="wire pipeline.extract or set RILEY_CI_STRICT=0")

def test_readers_exist():
    assert hasattr(extract, "read_pdf")
    assert hasattr(extract, "read_docx")
    assert hasattr(extract, "read_pptx")
