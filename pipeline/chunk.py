import sys
import json
import yaml
from pathlib import Path
import pandas as pd
import tiktoken


def header_sections(text, target=800, overlap=80):
    enc = tiktoken.get_encoding("cl100k_base")
    ids = enc.encode(text or "")
    chunks = []
    i = 0
    step = max(1, target - overlap)
    while i < len(ids):
        sl = ids[i : i + target]
        chunks.append(enc.decode(sl))
        i += step
    return chunks or [""]


def slides_to_chunks(slides: dict, target=800, overlap=80):
    out = []
    for i, payload in slides.items():
        t = (payload.get("title", "") or "").strip()
        b = (payload.get("body", "") or "").strip()
        text = f"[Slide {i}] {t}\n{b}".strip()
        out.extend(header_sections(text, target=target, overlap=overlap))
    return out


# --- Adding this lightweight wrapper for CI & scripts ---
def chunk_text(
    text: str,
    max_tokens: int = 400,
    overlap_tokens: int = 40,
    strategy: str = "sentence",
    **kwargs,
):
    """
    Generic chunker used by CI/smoke and run_chunk_and_classify.py.
    We reuse header_sections() using token counts via tiktoken.
    """
    # strategy is reserved for future (e.g., sentence vs window)
    return header_sections(text or "", target=max_tokens, overlap=overlap_tokens)


def main(campaign_root: str, cfg_path="configs/chunking.yaml"):
    camp = Path(campaign_root).name
    cfg = yaml.safe_load(Path(cfg_path).read_text(encoding="utf-8"))
    tgt = cfg["defaults"]["target_tokens"]
    ov = cfg["defaults"]["overlap_tokens"]
    out_dir = Path("data/processed") / camp
    extracted = out_dir / "extracted.jsonl"
    classified = pd.read_csv(out_dir / "classified.csv", dtype=str).fillna("")
    privacy = pd.read_csv(out_dir / "privacy.csv", dtype=str).fillna("")
    meta = classified.merge(privacy, on=["source_path", "doc_type", "extract_type"])
    out_chunks = out_dir / "chunks.jsonl"
    with open(extracted, encoding="utf-8") as fin, open(
        out_chunks, "w", encoding="utf-8"
    ) as fout:
        for line in fin:
            r = json.loads(line)
            ekind = r.get("extract", {}).get("type")
            sp = r["source_path"]
            mrow = meta[meta["source_path"] == sp]
            if mrow.empty:
                doc_type = "messaging_one_pager"
                index_mode = "full"
                pii_flags = "None"
            else:
                doc_type = mrow["doc_type"].iloc[0]
                index_mode = mrow["index_mode"].iloc[0]
                pii_flags = mrow["pii_flags"].iloc[0]
            if ekind == "text":
                pieces = header_sections(r["extract"]["text"], tgt, ov)
            elif ekind == "slides":
                pieces = slides_to_chunks(r["extract"]["slides"], tgt, ov)
            elif ekind == "xlsx_schema":
                pieces = [json.dumps(r["extract"]["schema"], ensure_ascii=False)]
            else:
                pieces = []
            for i, ch in enumerate(pieces, start=1):
                fout.write(
                    json.dumps(
                        {
                            "chunk_id": f"{sp}#c{i}",
                            "source_path": sp,
                            "doc_type": doc_type,
                            "index_mode": index_mode,
                            "pii_flags": pii_flags,
                            "text": ch,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
    print(f"[chunk] wrote {out_chunks}")


if __name__ == "__main__":
    main(sys.argv[1])
