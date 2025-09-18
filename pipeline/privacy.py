import sys, re, json
from pathlib import Path
import pandas as pd

EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+", re.I)
PHONE_RE = re.compile(r"(?:(?:\+?1[\s\.-]?)?(?:\(?\d{3}\)?|\d{3})[\s\.-]?)?\d{3}[\s\.-]?\d{4}")

SUMMARY_ONLY_TYPES = {"story_bank","media_list"}

def decide_index_mode(doc_type, extract_type, sample):
    if doc_type in SUMMARY_ONLY_TYPES or extract_type=="xlsx_schema":
        return "summary_only", "Contacts"
    if any(k in (sample or "").lower() for k in ["media contact","press contact"]):
        return "redacted", "MediaContacts"
    if EMAIL_RE.search(sample or "") or PHONE_RE.search(sample or ""):
        return "redacted", "Contacts"
    return "full", "None"

def main(campaign_root: str):
    camp = Path(campaign_root).name
    extracted = Path("data/processed")/camp/"extracted.jsonl"
    classified = Path("data/processed")/camp/"classified.csv"
    cls = pd.read_csv(classified, dtype=str).fillna("")
    out = Path("data/processed")/camp/"privacy.csv"
    rows=[]
    with extracted.open(encoding="utf-8") as f:
        for line in f:
            r = json.loads(line); ekind = r.get("extract",{}).get("type")
            if ekind=="text":
                sample = r["extract"]["text"][:5000]
            elif ekind=="slides":
                slides = r["extract"]["slides"]
                sample = (slides.get(1, {}).get("title","")+" "+slides.get(1, {}).get("body",""))[:5000]
            elif ekind=="xlsx_schema":
                sample = json.dumps(r["extract"]["schema"])[:1500]
            else:
                sample = ""
            doc_row = cls[cls["source_path"]==r["source_path"]]
            doc_type = doc_row["doc_type"].iloc[0] if not doc_row.empty else "messaging_one_pager"
            index_mode, pii = decide_index_mode(doc_type, ekind, sample or "")
            rows.append({
                "source_path": r["source_path"],
                "doc_type": doc_type,
                "extract_type": ekind,
                "index_mode": index_mode,
                "pii_flags": pii
            })
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"[privacy] wrote {out}")

if __name__=="__main__":
    main(sys.argv[1])
