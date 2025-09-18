import sys
from pathlib import Path
import pandas as pd
from hashlib import md5
import yaml

SCHEMA = yaml.safe_load(Path("configs/schema.yaml").read_text(encoding="utf-8"))

def stable_id(campaign_id: str, source_path: str) -> str:
    h = md5((campaign_id+"|"+source_path).encode("utf-8")).hexdigest()[:12]
    return f"{campaign_id.replace(' ','_')}-{h}"

def update_master(campaign_root: str):
    campaign_id = Path(campaign_root).name
    out_dir = Path("data/processed")/campaign_id
    cls = pd.read_csv(out_dir/"classified.csv", dtype=str).fillna("")
    prv = pd.read_csv(out_dir/"privacy.csv", dtype=str).fillna("")
    df = cls.merge(prv, on=["source_path","doc_type","extract_type"], how="left").fillna("")
    rows=[]
    for _, r in df.iterrows():
        rows.append({
            "doc_id": stable_id(campaign_id, r["source_path"]),
            "campaign_id": campaign_id.replace(" ","_"),
            "doc_type": r["doc_type"],
            "doc_subtype": r.get("doc_subtype",""),
            "clf_confidence": r.get("clf_confidence",""),
            "title": Path(r["source_path"]).name,
            "date": "",
            "language": "en",
            "index_mode": r["index_mode"],
            "pii_flags": r["pii_flags"],
            "canonical": "true",
            "source_path": r["source_path"]
        })
    new_df = pd.DataFrame(rows)
    master = Path("catalog/master_catalog.csv")
    if master.exists():
        m = pd.read_csv(master, dtype=str).fillna("")
        all_cols = sorted(set(m.columns) | set(new_df.columns))
        m = m.reindex(columns=all_cols, fill_value="")
        new_df = new_df.reindex(columns=all_cols, fill_value="")
        m = m[~m["doc_id"].isin(new_df["doc_id"])]
        m = pd.concat([m, new_df], ignore_index=True)
    else:
        m = new_df
    m.to_csv(master, index=False)
    print(f"[catalog] updated {master} (+{len(new_df)} rows)")

if __name__=="__main__":
    update_master(sys.argv[1])
