import sys, json
from pathlib import Path
import pandas as pd
import pdfplumber
from docx import Document
from pptx import Presentation

import logging
for name in ("pdfminer", "pdfplumber"):
    logging.getLogger(name).setLevel(logging.ERROR)


def read_docx(p: Path) -> str:
    d = Document(p.as_posix())
    return "\n".join(para.text for para in d.paragraphs)

def read_pdf(p: Path) -> str:
    out = []
    with pdfplumber.open(p.as_posix()) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            out.append(page.extract_text() or "")
    return "\n\n".join(out)

def read_pptx(p: Path):
    prs = Presentation(p.as_posix())
    slides = {}
    for i, s in enumerate(prs.slides, start=1):
        title = ""
        body = []
        for shp in s.shapes:
            if hasattr(shp, "text") and shp.text:
                if not title:
                    title = shp.text.strip()
                else:
                    body.append(shp.text.strip())
        slides[i] = {"title": title, "body": "\n".join(body)}
    return slides

def summarize_xlsx(p: Path):
    xls = pd.ExcelFile(p.as_posix())
    out = {"sheets": [], "pii_any": False}
    for name in xls.sheet_names:
        try:
            df = xls.parse(name)
            cols = [str(c) for c in df.columns]
            out["sheets"].append({"sheet": name, "rows": int(df.shape[0]), "cols": int(df.shape[1]), "columns": cols[:25] + (["..."] if len(cols)>25 else [])})
        except Exception as e:
            out["sheets"].append({"sheet": name, "error": str(e)})
    return out

def main(campaign_root: str):
    camp = Path(campaign_root).name
    inv = Path("data/processed") / f"inventory_{camp}.csv"

    # --- Guard 1: inventory file must exist ---
    if not inv.exists():
        print(f"[extract] skip {camp} (inventory not found: {inv})")
        return  # exit gracefully so the driver continues

    # --- Guard 2: inventory must have content ---
    if inv.stat().st_size == 0:
        print(f"[extract] skip {camp} (0 files in inventory)")
        return

    # --- Read inventory safely (handle empty/garbled CSVs) ---
    try:
        df = pd.read_csv(inv)
    except pd.errors.EmptyDataError:
        print(f"[extract] skip {camp} (inventory empty/unreadable: {inv})")
        return
    except Exception as e:
        print(f"[extract] skip {camp} (failed to read inventory: {e})")
        return

    # --- If df ended up empty for any reason, skip ---
    if df.empty:
        print(f"[extract] skip {camp} (inventory has no rows)")
        return

    out_dir = Path("data/processed") / camp
    out_dir.mkdir(parents=True, exist_ok=True)
    out_jsonl = out_dir / "extracted.jsonl"

    with out_jsonl.open("w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            p = Path(row["source_path"])
            rec = {"source_path": row["source_path"], "sha256": row["sha256"], "ext": row["ext"]}
            try:
                if p.suffix.lower() == ".docx":
                    rec["extract"] = {"type": "text", "text": read_docx(p)}
                elif p.suffix.lower() == ".pdf":
                    rec["extract"] = {"type": "text", "text": read_pdf(p)}
                elif p.suffix.lower() == ".pptx":
                    rec["extract"] = {"type": "slides", "slides": read_pptx(p)}
                elif p.suffix.lower() in [".xlsx", ".xls"]:
                    rec["extract"] = {"type": "xlsx_schema", "schema": summarize_xlsx(p)}
                else:
                    rec["extract"] = {"type": "skip"}
            except Exception as e:
                rec["error"] = str(e)
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"[extract] wrote {out_jsonl}")

if __name__ == "__main__":
    main(sys.argv[1])
