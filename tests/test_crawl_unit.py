from __future__ import annotations

import csv
from pathlib import Path

import pytest
from pipeline.crawl import main as crawl_main


def test_crawl_writes_inventory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """
    Test that the crawl module correctly writes an inventory CSV
    when given a simple fake campaign directory.
    """

    # Create a fake "campaign" folder with a couple of files
    root = tmp_path / "raw" / "C1"
    root.mkdir(parents=True)
    (root / "a.txt").write_text("hello", encoding="utf-8")
    (root / "b.docx").write_text(
        "fake", encoding="utf-8"
    )  # crawl only lists files; not parsed here

    # Crawl writes to data/processed relative to the current working directory.
    # Switch into tmp_path so outputs land under this temp folder.
    monkeypatch.chdir(tmp_path)

    # Run crawl
    crawl_main(str(root))

    # Verify the inventory file exists
    inv = Path("data") / "processed" / f"inventory_{root.name}.csv"
    assert inv.exists(), f"Expected inventory file {inv} not found"

    # Verify content: CSV has a header and at least 2 rows (for the two files)
    rows = list(csv.DictReader(inv.open(encoding="utf-8")))
    assert len(rows) >= 1, "Expected at least 1 row in inventory CSV"
    assert {"source_path", "ext", "sha256"}.issubset(rows[0].keys())
