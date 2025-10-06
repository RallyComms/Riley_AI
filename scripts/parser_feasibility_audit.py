#!/usr/bin/env python
"""
Check feasibility of targeted parsers by scanning ingested files.

Usage:
  python scripts/parser_feasibility_audit.py --input data/processed --out artifacts/parser_audit

This audit:
- Scans each campaign's extracted.jsonl
- For each record, uses filename & content heuristics to detect likely document types
- Writes a CSV of matches and a JSON summary with counts per label
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Tuple, Any

# ----------------------------
# Heuristic libraries
# ----------------------------

MEDIA_LIST_COLS = {"outlet", "reporter", "email", "beat", "phone", "title"}
PRESS_RELEASE_TOKENS = {
    "for immediate release",
    "media contact",
    "press release",
    "press statement",
    "embargoed until",
    "news release",
    "press office",
}

# Label heuristics:
# Each label has:
# - file_kw: filename keywords (lowercase substrings)
# - text_kw: content keywords (lowercase substrings)
# - text_regex: TODO (not used here for simplicity)
# - xlsx_sheet_kw: match sheet names
# - xlsx_cols_kw: match column names
# - type_bonus: adds weight if extract type matches (e.g., slides → deck/trainings)
#
# We keep weights simple: each keyword hit = +1, xlsx col hits scale with count.
LABELS: Dict[str, Dict[str, Any]] = {
    "press_release": {
        "file_kw": ["release", "press", "statement", "media"],
        "text_kw": list(PRESS_RELEASE_TOKENS),
    },
    "press_advisory": {
        "file_kw": ["advisory"],
        "text_kw": [
            "press advisory",
            "media advisory",
            "who:",
            "what:",
            "when:",
            "where:",
        ],
    },
    "deck_training": {
        "file_kw": ["deck", "slides", "ppt", "training"],
        "text_kw": ["workshop", "training objectives", "slide deck", "facilitator"],
        "type_bonus": {"slides": 2.0},
    },
    "messaging_one_pager": {
        "file_kw": [
            "one-pager",
            "one pager",
            "fact sheet",
            "messaging",
            "message framework",
        ],
        "text_kw": ["key messages", "message", "audience", "tone", "boilerplate"],
    },
    "meeting_notes": {
        "file_kw": ["agenda", "minutes", "notes", "meeting"],
        "text_kw": ["agenda", "attendees", "action items", "next steps", "minutes"],
    },
    "stakeholder_interview": {
        "file_kw": ["interview", "discussion guide"],
        "text_kw": [
            "interview guide",
            "stakeholder interview",
            "discussion guide",
            "respondent",
        ],
    },
    "landscape_analysis": {
        "file_kw": ["landscape", "situation analysis", "peer audit", "positioning"],
        "text_kw": [
            "landscape analysis",
            "situation analysis",
            "swot",
            "peer audit",
            "positioning",
        ],
    },
    "policy_brief": {
        "file_kw": ["policy brief", "issue brief"],
        "text_kw": [
            "policy recommendations",
            "executive summary",
            "background",
            "policy",
        ],
    },
    "fundraising": {
        "file_kw": ["fundraising", "donate", "appeal", "contribute"],
        "text_kw": ["donate", "contribute", "your gift", "fundraising", "pledge"],
    },
    "field_volunteer": {
        "file_kw": ["volunteer", "canvass", "phonebank", "rally"],
        "text_kw": ["volunteer", "canvass", "phonebank", "door-knock", "door knock"],
    },
    "social_calendar": {
        "file_kw": ["calendar", "editorial", "content calendar"],
        "text_kw": ["content calendar", "editorial calendar"],
        "xlsx_cols_kw": ["month", "date", "channel", "copy", "status"],
    },
    "media_monitoring": {
        "file_kw": ["coverage report", "clip report", "media monitoring"],
        "text_kw": ["coverage report", "clips", "mentions", "impressions"],
    },
    "contract_sow_invoice": {
        "file_kw": ["statement of work", "sow", "invoice", "contract"],
        "text_kw": [
            "statement of work",
            "deliverables",
            "scope",
            "compensation",
            "invoice",
        ],
    },
    "survey_results": {
        "file_kw": ["survey", "poll"],
        "text_kw": [
            "survey results",
            "respondents",
            "sample size",
            "margin of error",
            "n=",
        ],
    },
    "email_newsletter": {
        "file_kw": ["newsletter", "email"],
        "text_kw": ["unsubscribe", "subject:", "view in browser", "newsletter"],
    },
    # Keep the original two for parity:
    "media_list": {
        "xlsx_cols_kw": list(MEDIA_LIST_COLS),
        "xlsx_sheet_kw": ["media", "contacts", "press list", "outlets"],
    },
}

# A threshold: how many points a label needs to count as matched
MATCH_THRESHOLD = 2.0


# ----------------------------
# Utility helpers
# ----------------------------


def _safe_lower(s: str | None) -> str:
    return (s or "").lower()


def _name_from_path(source_path: str | None) -> str:
    try:
        return Path(source_path or "").name
    except Exception:
        return source_path or ""


def _text_from_extract(extract: dict) -> str:
    """
    Turn an `extract` object into a text snippet for scoring.
    Handles:
      - type == "text"
      - type == "slides" (first few slides)
      - type == "xlsx_schema" (sheet names + columns)
    """
    ekind = extract.get("type")
    if ekind == "text":
        return (extract.get("text") or "")[:4000]
    if ekind == "slides":
        slides = extract.get("slides", {}) or {}
        parts: List[str] = []
        for i in range(1, 6):  # first 5 slides max
            s = slides.get(i, {})
            t = s.get("title", "")
            b = s.get("body", "")
            if t or b:
                parts.append(f"[Slide {i}] {t}\n{b}")
        return "\n\n".join(parts)[:4000]
    if ekind == "xlsx_schema":
        schema = extract.get("schema", {}) or {}
        parts: List[str] = []
        for sheet in schema.get("sheets", []):
            sname = _safe_lower(sheet.get("sheet"))
            cols = [str(c) for c in sheet.get("columns", [])]
            parts.append(f"Sheet: {sname} | Cols: {', '.join(cols)}")
        return "\n".join(parts)[:2000]
    return ""


def _xlsx_schema_info(extract: dict) -> Tuple[List[str], List[str]]:
    """
    Return (sheet_names, all_columns) from an xlsx_schema extract, normalized to lower-case.
    """
    sheets = []
    cols_all = []
    schema = extract.get("schema", {}) or {}
    for sh in schema.get("sheets", []):
        sheets.append(_safe_lower(sh.get("sheet")))
        for c in sh.get("columns", []) or []:
            cols_all.append(_safe_lower(str(c)))
    return sheets, cols_all


def _score_label(
    label: str,
    cfg: Dict[str, Any],
    *,
    filename: str,
    text: str,
    extract_type: str | None,
    xlsx_sheets: List[str],
    xlsx_cols: List[str],
) -> float:
    score = 0.0
    name_l = _safe_lower(filename)
    text_l = _safe_lower(text)

    # filename hits
    for kw in cfg.get("file_kw", []) or []:
        if kw in name_l:
            score += 1.0

    # text hits
    for kw in cfg.get("text_kw", []) or []:
        if kw in text_l:
            score += 1.0

    # xlsx sheet/col hits
    if cfg.get("xlsx_cols_kw"):
        hits = sum(1 for c in cfg["xlsx_cols_kw"] if c in xlsx_cols)
        # Strong signal if >=2 columns match
        if hits >= 2:
            score += 2.0
        elif hits == 1:
            score += 0.5

    if cfg.get("xlsx_sheet_kw"):
        sh_hits = sum(
            1 for s in cfg["xlsx_sheet_kw"] if any(s in sh for sh in xlsx_sheets)
        )
        if sh_hits >= 1:
            score += 1.0

    # type bonus
    tbonus = cfg.get("type_bonus", {})
    if tbonus and extract_type:
        score += float(tbonus.get(extract_type, 0.0))

    return score


def classify_record(rec: dict) -> Tuple[List[Tuple[str, float]], str | None]:
    """
    Score each label for this record and return:
      - matches: list of (label, score) sorted descending
      - top_label: label with highest score if >= threshold, else None
    """
    extract = rec.get("extract", {}) or {}
    ekind = extract.get("type")
    spath = rec.get("source_path") or ""
    filename = _name_from_path(spath)
    text = _text_from_extract(extract)

    xlsx_sheets: List[str] = []
    xlsx_cols: List[str] = []
    if ekind == "xlsx_schema":
        xlsx_sheets, xlsx_cols = _xlsx_schema_info(extract)

    # Score all labels
    scored: List[Tuple[str, float]] = []
    for label, cfg in LABELS.items():
        sc = _score_label(
            label,
            cfg,
            filename=filename,
            text=text,
            extract_type=ekind,
            xlsx_sheets=xlsx_sheets,
            xlsx_cols=xlsx_cols,
        )
        scored.append((label, sc))
    scored.sort(key=lambda t: t[1], reverse=True)

    # Choose top label if score >= threshold
    top_label = scored[0][0] if scored and scored[0][1] >= MATCH_THRESHOLD else None
    return scored, top_label


# ----------------------------
# Main audit routine
# ----------------------------


def audit_processed(processed_root: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    # initialize summary with all labels
    summary: Dict[str, int] = {"total": 0, "unmatched": 0}
    for lab in LABELS.keys():
        summary[lab] = 0

    results: List[Dict[str, Any]] = []

    for camp_dir in processed_root.iterdir():
        if not camp_dir.is_dir():
            continue
        extracted = camp_dir / "extracted.jsonl"
        if not extracted.exists():
            continue

        with extracted.open(encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                summary["total"] += 1

                scored, top = classify_record(rec)
                labels_matched = [lab for lab, sc in scored if sc >= MATCH_THRESHOLD]
                top_label = top or ""
                if top_label:
                    summary[top_label] += 1
                else:
                    summary["unmatched"] += 1

                results.append(
                    {
                        "campaign": camp_dir.name,
                        "source_path": rec.get("source_path") or "",
                        "extract_type": rec.get("extract", {}).get("type"),
                        "top_label": top_label,
                        "labels": ";".join(labels_matched),
                        "top_score": (scored[0][1] if scored else 0.0),
                    }
                )

    # Write CSV
    csv_path = out_dir / "parser_audit.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "campaign",
                "source_path",
                "extract_type",
                "top_label",
                "labels",
                "top_score",
            ],
        )
        writer.writeheader()
        writer.writerows(results)
    print(f"[audit] Wrote detailed matches → {csv_path}")

    # Write summary
    summary_path = out_dir / "summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print("[audit] Summary:")
    print(json.dumps(summary, indent=2))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input", type=Path, required=True, help="Processed root (data/processed)"
    )
    ap.add_argument(
        "--out", type=Path, required=True, help="Output dir for audit results"
    )
    args = ap.parse_args()

    audit_processed(args.input, args.out)


if __name__ == "__main__":
    main()
