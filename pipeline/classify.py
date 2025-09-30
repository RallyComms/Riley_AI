import sys
import json
import yaml
from pathlib import Path
import pandas as pd


def load_yaml(p):
    return yaml.safe_load(Path(p).read_text(encoding="utf-8"))


def best_rule_label(text, taxonomy, patterns, filename=""):
    text_low = (text or "").lower()
    name_low = (filename or "").lower()
    scores = {}
    for label, pat in patterns.get("patterns", {}).items():
        must_any = [m.lower() for m in pat.get("must_any", [])]
        if any(m in text_low for m in must_any) or any(m in name_low for m in must_any):
            scores[label] = scores.get(label, 0) + 0.6
    for label, syns in taxonomy.get("synonyms", {}).items():
        hits = sum(1 for s in syns if s.lower() in text_low) + sum(
            1 for s in syns if s.lower() in name_low
        )
        if hits >= 2:
            scores[label] = scores.get(label, 0) + 0.3
        elif hits == 1:
            scores[label] = scores.get(label, 0) + 0.15

    if not scores:
        return "messaging_one_pager", 0.3
    label = max(scores, key=scores.get)
    conf = min(1.0, scores[label])
    return label, conf


def main(campaign_root: str):
    camp = Path(campaign_root).name
    taxonomy = load_yaml("configs/taxonomy.yaml")
    patterns = load_yaml("configs/patterns.yaml")
    extracted = Path("data/processed") / camp / "extracted.jsonl"
    out = Path("data/processed") / camp / "classified.csv"
    rows = []
    with extracted.open(encoding="utf-8") as f:
        for line in f:
            r = json.loads(line)
            ekind = r.get("extract", {}).get("type")
            if ekind == "text":
                sample = r["extract"]["text"][:3000]
            elif ekind == "slides":
                slides = r["extract"]["slides"]
                first = slides.get(1, {})
                sample = (first.get("title", "") + " " + first.get("body", ""))[:3000]
            elif ekind == "xlsx_schema":
                sample = json.dumps(r["extract"]["schema"])[:2000]
            else:
                sample = ""
            name = Path(r["source_path"]).name
            label, conf = best_rule_label(sample, taxonomy, patterns, filename=name)
            rows.append(
                {
                    "source_path": r["source_path"],
                    "sha256": r.get("sha256", ""),
                    "doc_type": label,
                    "doc_subtype": "",
                    "clf_confidence": round(conf, 2),
                    "extract_type": ekind,
                }
            )
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"[classify] wrote {out} ({len(rows)} rows)")


if __name__ == "__main__":
    main(sys.argv[1])
