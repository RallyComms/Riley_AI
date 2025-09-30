"""
Chunk + classify runner.
- Extracts, redacts, chunks, classifies docs.
- Emits chunks.jsonl + manifest.csv + run_metadata.json
- Skips unchanged files using hashes.
"""

from __future__ import annotations
import argparse
import csv
import hashlib
import json
import time
from pathlib import Path
from pipeline.extract import sniff_and_read
from pipeline.privacy import detect_pii, redact_text
from pipeline.chunk import chunk_text
from pipeline.classify_llm import classify

ALLOWED = {".pdf", ".docx", ".pptx", ".txt", ".md"}


def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def approx_tokens(s: str) -> int:
    return max(1, len(s) // 4)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    artifacts = args.out / "artifacts"
    artifacts.mkdir(exist_ok=True)
    chunks_path = args.out / "chunks.jsonl"
    manifest_path = artifacts / "manifest.csv"

    hashes_csv = artifacts / "file_hashes.csv"
    seen = {}
    if hashes_csv.exists():
        for r in csv.DictReader(hashes_csv.open("r", encoding="utf-8")):
            seen[r["rel_path"]] = r["sha256"]

    cj = open(chunks_path, "a", encoding="utf-8")
    mf_exists = manifest_path.exists()
    mf = manifest_path.open("a", newline="", encoding="utf-8")
    mw = csv.DictWriter(
        mf,
        fieldnames=[
            "doc_id",
            "rel_path",
            "sha256",
            "bytes",
            "chars",
            "approx_tokens",
            "chunks",
            "emails",
            "phones",
            "addresses",
            "skipped",
        ],
    )
    if not mf_exists:
        mw.writeheader()

    new_seen = {}
    start = time.time()
    processed = 0
    skipped = 0

    for p in sorted(args.input.rglob("*")):
        if not p.is_file():
            continue
        if p.suffix.lower() not in ALLOWED:
            continue
        rel = str(p.relative_to(args.input))
        h = sha256_file(p)
        if seen.get(rel) == h:
            skipped += 1
            mw.writerow(
                {
                    "doc_id": p.stem,
                    "rel_path": rel,
                    "sha256": h,
                    "bytes": p.stat().st_size,
                    "chars": "",
                    "approx_tokens": "",
                    "chunks": 0,
                    "emails": "",
                    "phones": "",
                    "addresses": "",
                    "skipped": True,
                }
            )
            new_seen[rel] = h
            continue
        text = sniff_and_read(p)
        if not text.strip():
            continue
        pii = detect_pii(text)
        red = redact_text(text)
        pieces = chunk_text(red)
        for i, ch in enumerate(pieces):
            cj.write(
                json.dumps(
                    {
                        "doc_id": p.stem,
                        "source_path": rel,
                        "chunk_index": i,
                        "text": ch,
                        "approx_tokens": approx_tokens(ch),
                        "taxonomy_label": classify(ch),
                        "pii": pii,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
        mw.writerow(
            {
                "doc_id": p.stem,
                "rel_path": rel,
                "sha256": h,
                "bytes": p.stat().st_size,
                "chars": len(text),
                "approx_tokens": approx_tokens(text),
                "chunks": len(pieces),
                "emails": pii["emails"],
                "phones": pii["phones"],
                "addresses": pii["addresses"],
                "skipped": False,
            }
        )
        new_seen[rel] = h
        processed += 1

    cj.close()
    mf.close()
    with hashes_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["rel_path", "sha256"])
        w.writeheader()
        for rel, h in new_seen.items():
            w.writerow({"rel_path": rel, "sha256": h})

    meta = {
        "input": str(args.input),
        "out": str(args.out),
        "stats": {
            "processed": processed,
            "skipped": skipped,
            "duration_sec": round(time.time() - start, 2),
        },
    }
    (args.out / "run_metadata.json").write_text(json.dumps(meta, indent=2), "utf-8")
    print(f"Done. processed={processed}, skipped={skipped}")


if __name__ == "__main__":
    main()
