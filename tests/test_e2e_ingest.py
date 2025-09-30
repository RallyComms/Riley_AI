import json
import subprocess
import sys


def test_smoke_ingest(sample_campaign, ci_out):
    cmd = [
        sys.executable,
        "scripts/ingest_campaign.py",
        "--input",
        str(sample_campaign),
        "--out",
        str(ci_out),
        "--max_tokens",
        "200",
        "--overlap_tokens",
        "20",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    assert res.returncode == 0, res.stderr

    chunks = ci_out / "chunks.jsonl"
    manifest = ci_out / "artifacts" / "manifest.csv"
    assert chunks.exists(), "chunks.jsonl not produced"
    assert manifest.exists(), "manifest.csv not produced"

    labels = set()
    with open(chunks, encoding="utf-8") as f:
        lines = [json.loads(line) for line in f if line.strip()]
    assert len(lines) >= 4
    for rec in lines:
        assert "doc_id" in rec and "text" in rec and "taxonomy_label" in rec
        assert isinstance(rec["approx_tokens"], int) and rec["approx_tokens"] > 0
        labels.add(rec["taxonomy_label"])
        pii = rec.get("pii", {})
        assert all(k in pii for k in ("emails", "phones", "addresses"))

    assert (
        len(labels.intersection({"Fundraising", "Comms", "Field", "Policy", "General"}))
        >= 2
    )
