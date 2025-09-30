"""
Preflight audit for an ingested campaign folder.
Produces audit_report.json, audit_report.md, by_ext.csv, and sample_failures.
"""

from __future__ import annotations
import argparse
import json
import csv
import statistics as stats
from pathlib import Path
from typing import Dict
from pipeline.extract import sniff_and_read
from pipeline.privacy import detect_pii

import sys

# Ensure repo root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


ALLOWED = {".pdf", ".docx", ".pptx", ".txt", ".md"}


def human(n: int) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--sample-per-ext", type=int, default=5)
    args = ap.parse_args()

    out = args.out
    out.mkdir(parents=True, exist_ok=True)
    (out / "sample_failures").mkdir(exist_ok=True)

    totals = {"files": 0, "bytes": 0}
    by_ext: Dict[str, Dict] = {}
    lengths, pii_samples = [], {"emails": [], "phones": [], "addresses": []}

    for p in args.input.rglob("*"):
        if not p.is_file():
            continue
        ext = p.suffix.lower() or "<none>"
        by_ext.setdefault(ext, {"count": 0, "bytes": 0})
        size = p.stat().st_size
        totals["files"] += 1
        totals["bytes"] += size
        by_ext[ext]["count"] += 1
        by_ext[ext]["bytes"] += size

    # write csv
    with open(out / "by_ext.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ext", "count", "bytes", "human"])
        for e, rec in by_ext.items():
            w.writerow([e, rec["count"], rec["bytes"], human(rec["bytes"])])

    # sample parse
    parseability = {}
    for ext in list(by_ext.keys()):
        if ext not in ALLOWED:
            continue
        paths = list(args.input.rglob(f"*{ext}"))[: args.sample_per_ext]
        ok = fail = 0
        for p in paths:
            try:
                t = sniff_and_read(p)
                if t.strip():
                    ok += 1
                    lengths.append(len(t))
                    pii = detect_pii(t)
                    for k, v in pii.items():
                        pii_samples[k].append(v)
                else:
                    fail += 1
            except Exception as e:
                fail += 1
                (out / "sample_failures" / f"{p.name}.txt").write_text(str(e), "utf-8")
        parseability[ext] = {"ok": ok, "fail": fail, "rate_ok": ok / max(1, ok + fail)}

    med_len = int(stats.median(lengths)) if lengths else 0
    avg_len = int(stats.mean(lengths)) if lengths else 0
    avg_pii = {k: (stats.mean(v) if v else 0.0) for k, v in pii_samples.items()}

    report = {
        "totals": totals,
        "by_ext": by_ext,
        "parseability": parseability,
        "length": {"median": med_len, "mean": avg_len},
        "pii_avg": avg_pii,
    }
    (out / "audit_report.json").write_text(json.dumps(report, indent=2), "utf-8")

    md = [
        f"# Audit for {args.input.name}",
        f"- Files: {totals['files']}  Size: {human(totals['bytes'])}",
        "## Parseability",
    ]
    for e, r in parseability.items():
        md.append(f"- {e}: {r['ok']}/{r['ok']+r['fail']} ok ({r['rate_ok']:.0%})")
    md.append(f"## Avg PII: {avg_pii}")
    md.append(f"## Median length: {med_len}, mean length: {avg_len}")
    (out / "audit_report.md").write_text("\n".join(md), "utf-8")
    print(f"Audit complete → {out}")


if __name__ == "__main__":
    main()
