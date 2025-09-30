# Why this is better than the first version (what its doing)

# 1. sys.path shim ensures pipeline is always found when running as a script.
# 2. safe_print prevents encoding crashes on Windows consoles.
# 3. Dual-mode CLI makes my tests happy (--input/--out) and keeps backward compatibility ("data/raw/<Campaign>").
# 4. Uses my existing pipeline pieces (sniff_and_read, detect_pii, chunk_text, classify) in smoke mode, so it’s consistent with CI.
# ruff: noqa: E402

"""

Riley ingest runner with two modes:

1) Smoke mode (used by CI and tests):
    python scripts/ingest_campaign.py --input <folder> --out <outdir> --max_tokens 200 --overlap_tokens 20
    - Extracts text via pipeline.extract.sniff_and_read
    - Detects & redacts PII via pipeline.privacy
    - Chunks via pipeline.chunk.chunk_text
    - Classifies via pipeline.classify_llm.classify (heuristic fallback)
    - Writes:
        <outdir>/chunks.jsonl
        <outdir>/artifacts/manifest.csv

2) Legacy orchestrator mode (backward-compatible):
    python scripts/ingest_campaign.py "data/raw/<Campaign Name>"
    - Calls the original per-module drivers:
        pipeline.crawl, pipeline.extract, pipeline.classify_llm, pipeline.privacy, pipeline.chunk, pipeline.catalog
"""

from __future__ import annotations

import sys
from pathlib import Path


def safe_print(*args, **kwargs):
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        msg = " ".join(str(a) for a in args)
        sys.stdout.write(
            msg.encode(sys.stdout.encoding, errors="replace").decode(
                sys.stdout.encoding
            )
            + "\n"
        )


# Ensure repo root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import argparse
import csv
import json
import time

...


# --- imports from pipeline packages (must exist in your repo) ---
from pipeline.extract import sniff_and_read
from pipeline.privacy import detect_pii, redact_text
from pipeline.chunk import chunk_text

try:
    # heuristic fallback we added earlier
    from pipeline.classify_llm import classify
except Exception:
    # ultimate fallback if classify_llm lacks a simple classify()
    def classify(text: str) -> str:
        tl = (text or "").lower()
        if any(k in tl for k in ("donate", "fundraising", "contribute", "finance")):
            return "Fundraising"
        if any(k in tl for k in ("press", "media", "announce", "statement", "release")):
            return "Comms"
        if any(
            k in tl
            for k in ("volunteer", "door knock", "canvass", "phonebank", "rally")
        ):
            return "Field"
        if any(
            k in tl
            for k in ("policy", "plan", "healthcare", "education", "tax", "climate")
        ):
            return "Policy"
        return "General"


ALLOWED = {".pdf", ".docx", ".pptx", ".txt", ".md", ".xls", ".xlsx"}


# ------------------ Smoke mode (CI/test) ------------------


def run_smoke(
    input_dir: Path, out_dir: Path, max_tokens: int = 400, overlap_tokens: int = 40
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    artifacts = out_dir / "artifacts"
    artifacts.mkdir(exist_ok=True)

    chunks_path = out_dir / "chunks.jsonl"
    manifest_path = artifacts / "manifest.csv"

    cj = chunks_path.open("w", encoding="utf-8")
    mf = manifest_path.open("w", newline="", encoding="utf-8")
    mw = csv.DictWriter(
        mf,
        fieldnames=[
            "doc_id",
            "rel_path",
            "bytes",
            "chars",
            "approx_tokens",
            "chunks",
            "emails",
            "phones",
            "addresses",
        ],
    )
    mw.writeheader()

    processed = 0
    start = time.time()

    for p in sorted(input_dir.rglob("*")):
        if not p.is_file():
            continue
        ext = p.suffix.lower()
        if ext not in ALLOWED:
            continue

        rel = str(p.relative_to(input_dir))
        try:
            raw = sniff_and_read(p)
        except Exception:
            # record an empty row so we can see failures in the manifest
            mw.writerow(
                {
                    "doc_id": p.stem,
                    "rel_path": rel,
                    "bytes": p.stat().st_size,
                    "chars": 0,
                    "approx_tokens": 0,
                    "chunks": 0,
                    "emails": 0,
                    "phones": 0,
                    "addresses": 0,
                }
            )
            continue

        text = raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False)
        text = (text or "").strip()
        if not text:
            mw.writerow(
                {
                    "doc_id": p.stem,
                    "rel_path": rel,
                    "bytes": p.stat().st_size,
                    "chars": 0,
                    "approx_tokens": 0,
                    "chunks": 0,
                    "emails": 0,
                    "phones": 0,
                    "addresses": 0,
                }
            )
            continue

        # PII
        pii = detect_pii(text)
        red = redact_text(text)

        # Chunk + classify
        pieces = chunk_text(red, max_tokens=max_tokens, overlap_tokens=overlap_tokens)
        for i, ch in enumerate(pieces):
            rec = {
                "doc_id": p.stem,
                "source_path": rel,
                "chunk_index": i,
                "text": ch,
                "approx_tokens": max(1, len(ch) // 4),
                "taxonomy_label": classify(ch),
                "pii": pii,
            }
            cj.write(json.dumps(rec, ensure_ascii=False) + "\n")

        mw.writerow(
            {
                "doc_id": p.stem,
                "rel_path": rel,
                "bytes": p.stat().st_size,
                "chars": len(text),
                "approx_tokens": max(1, len(text) // 4),
                "chunks": len(pieces),
                "emails": pii.get("emails", 0),
                "phones": pii.get("phones", 0),
                "addresses": pii.get("addresses", 0),
            }
        )
        processed += 1

    cj.close()
    mf.close()
    dur = round(time.time() - start, 2)
    safe_print(f"[smoke] processed={processed} → {out_dir} in {dur}s")


# ------------------ Legacy orchestrator mode ------------------


def run_legacy_orchestrator(campaign_root: str) -> None:
    camp = campaign_root
    if not Path(camp).exists():
        print(f"[ingest_campaign] ERROR: Path not found: {camp}")
        sys.exit(1)

    py = sys.executable
    steps = [
        [py, "-m", "pipeline.crawl", camp],
        [py, "-m", "pipeline.extract", camp],
        [py, "-m", "pipeline.classify_llm", camp],
        [py, "-m", "pipeline.privacy", camp],
        [py, "-m", "pipeline.chunk", camp],
        [py, "-m", "pipeline.catalog", camp],
    ]

    for s in steps:
        print("\n>>", " ".join(s))
        # If any step fails, bubble up
        try:
            # use the already-loaded module if available
            sys.modules.get("subprocess").run(s, check=True)  # type: ignore[attr-defined]
        except Exception:
            # fallback import (very defensive)
            import subprocess as sp  # noqa: E402

            sp.run(s, check=True)

    safe_print("\nDONE")  # <-- after the loop, prints once


# ------------------ CLI ------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Riley ingest runner (smoke + legacy modes)"
    )
    ap.add_argument(
        "--input", type=Path, help="Input folder (campaign root) for smoke mode"
    )
    ap.add_argument("--out", type=Path, help="Output folder for smoke mode")
    ap.add_argument("--max_tokens", type=int, default=400)
    ap.add_argument("--overlap_tokens", type=int, default=40)
    ap.add_argument(
        "legacy_campaign", nargs="?", help="Legacy mode: data/raw/<Campaign Name>"
    )
    return ap.parse_args(argv)


def main() -> None:
    args = parse_args(sys.argv[1:])
    # Smoke mode if both flags present
    if args.input and args.out:
        run_smoke(
            args.input,
            args.out,
            max_tokens=args.max_tokens,
            overlap_tokens=args.overlap_tokens,
        )
        return

    # Legacy mode: one positional argument
    if args.legacy_campaign:
        run_legacy_orchestrator(args.legacy_campaign)
        return

    # Otherwise show usage similar to your old script
    print(
        "Usage:\n"
        "  Smoke:  python scripts/ingest_campaign.py --input <folder> --out <outdir> [--max_tokens N --overlap_tokens M]\n"
        '  Legacy: python scripts/ingest_campaign.py "data/raw/<Campaign Name>"'
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
