import sys
import re
import json
from pathlib import Path
import pandas as pd

# --- Regex patterns ---
EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", re.I)
PHONE_RE = re.compile(
    r"(?:(?:\+?1[\s\.-]?)?(?:\(?\d{3}\)?|\d{3})[\s\.-]?)?\d{3}[\s\.-]?\d{4}"
)
ADDR_RE = re.compile(
    r"\b(\d{1,5}\s+[A-Za-z0-9.\s]+"
    r"(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Way))\b",
    re.I,
)

# --- Redaction tokens (configurable) ---
REDACTION_TOKENS = {
    "email": "[EMAIL]",
    "phone": "[PHONE]",
    "address": "[ADDR]",
}


# --- Detection + redaction helpers ---
def detect_pii(text: str) -> dict:
    """Return counts of emails, phones, and addresses detected."""
    return {
        "emails": len(EMAIL_RE.findall(text or "")),
        "phones": len(PHONE_RE.findall(text or "")),
        "addresses": len(ADDR_RE.findall(text or "")),
    }


def redact_text(text: str) -> str:
    """Replace all detected PII with safe placeholders."""
    if not text:
        return ""
    # Strict redaction order: email → phone → address
    t = EMAIL_RE.sub(REDACTION_TOKENS["email"], text)
    t = PHONE_RE.sub(REDACTION_TOKENS["phone"], t)
    t = ADDR_RE.sub(REDACTION_TOKENS["address"], t)
    return t


# --- Indexing mode decisions (campaign-specific) ---
SUMMARY_ONLY_TYPES = {"story_bank", "media_list"}


def decide_index_mode(doc_type, extract_type, sample):
    """
    Decide indexing mode based on doc_type, extract_type, and content sample.
    Returns (index_mode, pii_flag).
    """
    if doc_type in SUMMARY_ONLY_TYPES or extract_type == "xlsx_schema":
        return "summary_only", "Contacts"
    if any(k in (sample or "").lower() for k in ["media contact", "press contact"]):
        return "redacted", "MediaContacts"
    if EMAIL_RE.search(sample or "") or PHONE_RE.search(sample or ""):
        return "redacted", "Contacts"
    return "full", "None"


# --- Batch pipeline driver ---
def main(campaign_root: str):
    """
    Campaign-level privacy analysis:
    Reads extracted.jsonl + classified.csv, writes privacy.csv
    with index mode and PII flags.
    """
    camp = Path(campaign_root).name
    extracted = Path("data/processed") / camp / "extracted.jsonl"
    classified = Path("data/processed") / camp / "classified.csv"

    if not extracted.exists() or not classified.exists():
        print(f"[privacy] skip {camp} (missing inputs)")
        return

    cls = pd.read_csv(classified, dtype=str).fillna("")
    out = Path("data/processed") / camp / "privacy.csv"
    rows = []

    with extracted.open(encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            ekind = r.get("extract", {}).get("type")
            # Get sample text depending on type
            if ekind == "text":
                sample = r["extract"].get("text", "")[:5000]
            elif ekind == "slides":
                slides = r["extract"].get("slides", {})
                sample = (
                    slides.get(1, {}).get("title", "")
                    + " "
                    + slides.get(1, {}).get("body", "")
                )[:5000]
            elif ekind == "xlsx_schema":
                sample = json.dumps(r["extract"].get("schema", {}))[:1500]
            else:
                sample = ""

            # Lookup doc_type from classified.csv
            doc_row = cls[cls["source_path"] == r["source_path"]]
            doc_type = (
                doc_row["doc_type"].iloc[0]
                if not doc_row.empty
                else "messaging_one_pager"
            )

            index_mode, pii = decide_index_mode(doc_type, ekind, sample or "")
            rows.append(
                {
                    "source_path": r["source_path"],
                    "doc_type": doc_type,
                    "extract_type": ekind,
                    "index_mode": index_mode,
                    "pii_flags": pii,
                }
            )

    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"[privacy] wrote {out}")


if __name__ == "__main__":
    main(sys.argv[1])


## 1. Explicit redaction order avoids misfires.
# 2. Centralized tokens → easy to audit/replace.
# 3. Graceful main(): skips if inputs missing, instead of crashing.
# 4. CI/CD safe: only uses stdlib + pandas. No network, no side effects.
# 5. Compatible with both:
# 6. New unit tests (detect_pii + redact_text).
