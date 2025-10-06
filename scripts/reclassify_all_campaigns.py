#!/usr/bin/env python
"""
Re-run LLM classification across ALL processed campaigns with a gentle pause
between campaigns to avoid hammering the API.

Usage:
  python scripts/reclassify_all_campaigns.py --input data/processed --model gpt-4o-mini --sleep-seconds 2 --log artifacts/classify_batch_log.txt
"""

from __future__ import annotations

import argparse
import subprocess
import time
from pathlib import Path

import sys


def reclassify_campaign(
    camp_dir: Path, model: str | None = None, retries: int = 1
) -> int:
    """Run pipeline.classify_llm for a single campaign. Returns process returncode."""
    print(f"\n[classify] {camp_dir.name}")
    cmd = [sys.executable, "-m", "pipeline.classify_llm", str(camp_dir)]
    if model:
        cmd += ["--model", model]

    # Force a fresh sweep: delete old classified.csv
    old_csv = camp_dir / "classified.csv"
    if old_csv.exists():
        try:
            old_csv.unlink()
        except Exception as e:
            print(f"[warn] Could not delete {old_csv}: {e}")

    # Try once + optional retry
    attempts = 0
    while True:
        attempts += 1
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"[OK] {camp_dir.name}")
            return 0
        print(f"[ERROR] {camp_dir.name} (attempt {attempts}):\n{result.stderr.strip()}")
        if attempts > retries:
            return result.returncode
        # backoff
        time.sleep(2 * attempts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Root processed dir (e.g., data/processed)",
    )
    ap.add_argument(
        "--model",
        type=str,
        default=None,
        help="Optional model override (else uses env CLASSIFY_MODEL)",
    )
    ap.add_argument(
        "--sleep-seconds",
        type=float,
        default=2.0,
        help="Pause between campaigns (default: 2.0s)",
    )
    ap.add_argument(
        "--retries",
        type=int,
        default=1,
        help="Retries per campaign on failure (default: 1)",
    )
    ap.add_argument(
        "--log",
        type=Path,
        default=None,
        help="Optional log file to append status lines",
    )
    args = ap.parse_args()

    root = args.input
    if not root.exists() or not root.is_dir():
        raise SystemExit(
            f"[fatal] Input path does not exist or is not a directory: {root}"
        )

    failures = []
    camps = [d for d in sorted(root.iterdir()) if d.is_dir()]
    print(f"[batch] Found {len(camps)} campaigns under {root}")

    def log_line(s: str):
        print(s)
        if args.log:
            args.log.parent.mkdir(parents=True, exist_ok=True)
            with args.log.open("a", encoding="utf-8") as f:
                f.write(s + "\n")

    for camp_dir in camps:
        extracted = camp_dir / "extracted.jsonl"
        if not extracted.exists():
            log_line(f"[skip] {camp_dir.name} (no extracted.jsonl)")
            continue

        rc = reclassify_campaign(camp_dir, model=args.model, retries=args.retries)
        time.sleep(max(0.0, args.sleep_seconds))
        if rc != 0:
            failures.append(camp_dir.name)
            log_line(f"[fail] {camp_dir.name}")
        else:
            log_line(f"[done] {camp_dir.name}")

    print("\n==== Reclassification complete ====")
    if failures:
        print(f"[DONE] Some campaigns failed: {', '.join(failures)}")
    else:
        print("[DONE] All campaigns processed successfully.")


if __name__ == "__main__":
    main()
