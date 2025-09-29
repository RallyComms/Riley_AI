#!/usr/bin/env python3
# This script validates ingestion results: taxonomy, duplicates, chunk stats, PII flags
# ensure we go into chunking and classification with good data
import argparse, json, hashlib
from pathlib import Path
import pandas as pd

# Allowed taxonomy values
ALLOWED_DOC_TYPES = {
    "work_plan","story_bank","fact_sheet","editorial_calendar",
    "pitch_email","press_release","budget","report","deck","notes"
}

def sha1_bytes(b: bytes) -> str:
    h = hashlib.sha1(); h.update(b); return h.hexdigest()

def file_hash(p: Path) -> str:
    try:
        return sha1_bytes(p.read_bytes())
    except Exception:
        return ""

def load_manifest(manifest_path: Path) -> pd.DataFrame:
    # Expect your extractor/classifier to write a CSV with these columns:
    # source_path, rel_path, doc_type, clf_confidence, n_chunks, pii_flag
    return pd.read_csv(manifest_path)

def validate(df: pd.DataFrame, root: Path) -> dict:
    issues = {"missing_doc_type":[], "unknown_doc_type":[], "low_confidence":[],
            "duplicate_path":[], "duplicate_content":[], "taxonomy_mismatch":[],
            "empty_or_short":[], "chunk_anomaly":[], "pii_flagged":[]}

    # 1) missing / unknown doc_type
    for _, r in df.iterrows():
        if pd.isna(r.get("doc_type")) or str(r.get("doc_type")).strip()=="":
            issues["missing_doc_type"].append(r.get("rel_path"))
        elif str(r["doc_type"]) not in ALLOWED_DOC_TYPES:
            issues["unknown_doc_type"].append({"path": r.get("rel_path"), "value": r["doc_type"]})

    # 2) low confidence classifications
    low = df[df.get("clf_confidence", 1.0) < 0.60]
    for _, r in low.iterrows():
        issues["low_confidence"].append({"path": r.get("rel_path"), "doc_type": r.get("doc_type"), "conf": r.get("clf_confidence")})

    # 3) duplicate paths
    dup_paths = df["rel_path"].value_counts()
    for pth, cnt in dup_paths.items():
        if cnt > 1: issues["duplicate_path"].append({"path": pth, "count": int(cnt)})

    # 4) duplicate content (by SHA1)
    content_hashes = []
    for _, r in df.iterrows():
        p = root / str(r.get("rel_path"))
        content_hashes.append(file_hash(p))
    df["_hash"] = content_hashes
    dup_hash = df.groupby("_hash").size().sort_values(ascending=False)
    for h, cnt in dup_hash.items():
        if h and cnt > 1:
            paths = df[df["_hash"]==h]["rel_path"].tolist()
            issues["duplicate_content"].append({"hash": h, "count": int(cnt), "paths": paths})

    # 5) empty/short text & chunk anomalies
    if "n_chunks" in df.columns:
        for _, r in df.iterrows():
            n = int(r.get("n_chunks") or 0)
            if n == 0: issues["empty_or_short"].append(r.get("rel_path"))
            if n > 300: issues["chunk_anomaly"].append({"path": r.get("rel_path"), "n_chunks": n})
    # 6) privacy flags
    flagged = df[df.get("pii_flag", 0) == 1]
    for _, r in flagged.iterrows():
        issues["pii_flagged"].append(r.get("rel_path"))

    # 7) taxonomy mismatch summary (values not in allowed set)
    if "doc_type" in df.columns:
        bad_vals = sorted(set(df["doc_type"].dropna()) - ALLOWED_DOC_TYPES)
        for v in bad_vals:
            issues["taxonomy_mismatch"].append(v)

    return issues

def main():
    ap = argparse.ArgumentParser(description="Post‑ingestion health check")
    ap.add_argument("--manifest", required=True, help="CSV from classify/chunk step")
    ap.add_argument("--root", required=True, help="Root of extracted campaign (e.g., data/raw/211_LA)")
    ap.add_argument("--out", default="health_report.json", help="Output JSON path")
    args = ap.parse_args()

    root = Path(args.root)
    df = load_manifest(Path(args.manifest))
    report = validate(df, root)

    # quick OK/FAIL gate
    fail = any(len(v) for k,v in report.items() if k not in ("taxonomy_mismatch",))
    report["status"] = "FAIL" if fail else "OK"

    Path(args.out).write_text(json.dumps(report, indent=2))
    print(json.dumps({"status": report["status"], "report": args.out}, indent=2))

if __name__ == "__main__":
    main()
