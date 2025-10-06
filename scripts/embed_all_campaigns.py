#!/usr/bin/env python
"""
Chunk + Embed all campaigns under data/processed/.

- Reads each campaign's extracted.jsonl and classified.csv
- Chunks redacted text into ~600-token windows (with overlap)
- Generates embeddings via OpenAI text-embedding-3-large
- Writes per-campaign parquet part files: data/embeddings/<campaign>/parts/part_XXXXX.parquet
- Maintains a checkpoint of processed chunk_ids to support resumable runs

Usage:
  python scripts/embed_all_campaigns.py \
    --processed-root data/processed \
    --embeddings-root data/embeddings \
    --model text-embedding-3-large \
    --chunk-tokens 600 \
    --overlap-tokens 60 \
    --batch-size 64 \
    --sleep-batch 0.4 \
    --sleep-campaign 2.0 \
    --resume 1
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple, Any

import pandas as pd

# Requires openai>=1.0.0
from openai import OpenAI

# Use your existing chunker for consistency
try:
    from pipeline.chunk import chunk_text
except Exception as e:
    print(f"[fatal] Could not import pipeline.chunk.chunk_text: {e}")
    sys.exit(1)


def load_classified(camp_dir: Path) -> pd.DataFrame:
    """Load classified.csv if present; returns empty DF if not."""
    f = camp_dir / "classified.csv"
    if not f.exists():
        return pd.DataFrame(
            columns=[
                "source_path",
                "sha256",
                "doc_type",
                "clf_confidence",
                "extract_type",
            ]
        )
    df = pd.read_csv(f, dtype=str).fillna("")
    return df


def iter_extracted_records(camp_dir: Path):
    """Yield records from extracted.jsonl."""
    ext = camp_dir / "extracted.jsonl"
    if not ext.exists():
        return
    with ext.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def build_chunks_for_record(
    rec: dict, max_tokens: int, overlap_tokens: int
) -> List[Tuple[str, int, str]]:
    """
    Return list of (chunk_text, chunk_index, extract_type) for a single extracted record.
    Handles 'text' and 'slides' and 'xlsx_schema' conservatively.
    """
    extract = rec.get("extract", {}) or {}
    ekind = extract.get("type")
    out: List[Tuple[str, int, str]] = []

    if ekind == "text":
        text = extract.get("text", "") or ""
        pieces = chunk_text(text, max_tokens=max_tokens, overlap_tokens=overlap_tokens)
        for i, ch in enumerate(pieces):
            out.append((ch, i, ekind))
    elif ekind == "slides":
        # Flatten slides text; then chunk
        slides = extract.get("slides", {}) or {}
        parts = []
        for i in range(1, 100):
            s = slides.get(i, {})
            t = s.get("title", "")
            b = s.get("body", "")
            if not (t or b):
                continue
            parts.append(f"[Slide {i}] {t}\n{b}")
        slab = "\n\n".join(parts)
        pieces = chunk_text(slab, max_tokens=max_tokens, overlap_tokens=overlap_tokens)
        for i, ch in enumerate(pieces):
            out.append((ch, i, ekind))
    elif ekind == "xlsx_schema":
        # Schema text is short; treat as a single piece
        schema_text = json.dumps(extract.get("schema", {}) or {}, ensure_ascii=False)
        if schema_text.strip():
            out.append((schema_text[:3000], 0, ekind))
    else:
        # Unknown: skip
        pass

    return out


def load_checkpoint(checkpoint_file: Path) -> set:
    if not checkpoint_file.exists():
        return set()
    done = set()
    with checkpoint_file.open("r", encoding="utf-8") as f:
        for line in f:
            cid = line.strip()
            if cid:
                done.add(cid)
    return done


def append_checkpoint(checkpoint_file: Path, chunk_ids: List[str]) -> None:
    if not chunk_ids:
        return
    with checkpoint_file.open("a", encoding="utf-8") as f:
        for cid in chunk_ids:
            f.write(cid + "\n")


def ensure_parents(p: Path):
    p.parent.mkdir(parents=True, exist_ok=True)


def make_part_file(emb_root: Path, campaign: str, part_index: int) -> Path:
    parts_dir = emb_root / campaign / "parts"
    parts_dir.mkdir(parents=True, exist_ok=True)
    return parts_dir / f"part_{part_index:05d}.parquet"


def embed_batch(
    client: OpenAI, model: str, texts: List[str], retries: int = 3, backoff: float = 1.5
) -> List[List[float]]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = client.embeddings.create(model=model, input=texts)
            # resp.data is a list of objects with 'embedding'
            embs = [d.embedding for d in resp.data]
            if len(embs) != len(texts):
                raise RuntimeError("Embedding response length mismatch")
            return embs
        except Exception as e:
            last_err = e
            sleep_for = backoff ** (attempt - 1)
            print(
                f"[embed] WARN attempt {attempt}/{retries} failed: {e} → sleeping {sleep_for:.2f}s"
            )
            time.sleep(sleep_for)
    raise RuntimeError(f"Embedding failed after {retries} attempts: {last_err}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed-root", type=Path, default=Path("data/processed"))
    ap.add_argument("--embeddings-root", type=Path, default=Path("data/embeddings"))
    ap.add_argument("--model", type=str, default="text-embedding-3-large")
    ap.add_argument("--chunk-tokens", type=int, default=600)
    ap.add_argument("--overlap-tokens", type=int, default=60)
    ap.add_argument("--batch-size", type=int, default=64)
    ap.add_argument(
        "--sleep-batch", type=float, default=0.4, help="Pause between batches (seconds)"
    )
    ap.add_argument(
        "--sleep-campaign",
        type=float,
        default=2.0,
        help="Pause between campaigns (seconds)",
    )
    ap.add_argument(
        "--resume",
        type=int,
        default=1,
        help="Resume from checkpoint if present (1=yes, 0=no)",
    )
    args = ap.parse_args()

    # Init client
    client = OpenAI()

    processed_root = args.processed_root
    emb_root = args.embeddings_root

    if not processed_root.exists():
        print(f"[fatal] Processed root not found: {processed_root}")
        sys.exit(1)

    campaigns = [d for d in sorted(processed_root.iterdir()) if d.is_dir()]
    print(f"[embed] Found {len(campaigns)} campaigns under {processed_root}")

    for c_idx, camp_dir in enumerate(campaigns, start=1):
        campaign = camp_dir.name
        extracted = camp_dir / "extracted.jsonl"
        if not extracted.exists():
            print(f"[embed] SKIP {campaign}: no extracted.jsonl")
            continue

        # Load classification metadata (for doc_type, confidence)
        cls = load_classified(camp_dir)
        meta_map: Dict[str, Dict[str, Any]] = {}
        for _, row in cls.iterrows():
            sp = row.get("source_path", "")
            meta_map[sp] = {
                "doc_type": row.get("doc_type", ""),
                "clf_confidence": row.get("clf_confidence", ""),
                "extract_type": row.get("extract_type", ""),
            }

        # Prepare checkpoint
        checkpoint_file = emb_root / campaign / "checkpoint.txt"
        done_ids = load_checkpoint(checkpoint_file) if args.resume else set()
        part_index = 1
        # If parts already exist, bump part_index to avoid overwrite
        parts_dir = emb_root / campaign / "parts"
        if parts_dir.exists():
            existing = sorted(parts_dir.glob("part_*.parquet"))
            if existing:
                # find next index
                last_name = existing[-1].name
                try:
                    last_idx = int(
                        last_name.replace("part_", "").replace(".parquet", "")
                    )
                    part_index = last_idx + 1
                except Exception:
                    pass

        print(f"\n[embed] Campaign {c_idx}/{len(campaigns)}: {campaign}")
        print(
            f"[embed] Resume mode: {'ON' if args.resume else 'OFF'} | Already processed chunks: {len(done_ids)}"
        )

        # Build all chunks → embed in batches → write per-batch to parts
        batch_texts: List[str] = []
        batch_rows: List[Dict[str, Any]] = []
        batch_chunk_ids: List[str] = []
        batches_written = 0
        total_new_chunks = 0

        for rec in iter_extracted_records(camp_dir):
            spath = rec.get("source_path") or ""
            sha = rec.get("sha256") or ""
            meta = meta_map.get(spath, {})
            doc_type = meta.get("doc_type", "")
            clf_conf = meta.get("clf_confidence", "")
            ekind = meta.get("extract_type", rec.get("extract", {}).get("type", ""))

            chunks = build_chunks_for_record(
                rec, args.chunk_tokens, args.overlap_tokens
            )
            for ch_text, ch_idx, ch_type in chunks:
                chunk_id = f"{sha}:{ch_idx}"
                if args.resume and (chunk_id in done_ids):
                    continue
                if not ch_text.strip():
                    continue

                batch_texts.append(ch_text)
                batch_rows.append(
                    {
                        "campaign": campaign,
                        "source_path": spath,
                        "sha256": sha,
                        "chunk_index": ch_idx,
                        "chunk_id": chunk_id,
                        "doc_type": doc_type,
                        "clf_confidence": clf_conf,
                        "extract_type": ch_type or ekind,
                        "text": ch_text,
                    }
                )
                batch_chunk_ids.append(chunk_id)

                if len(batch_texts) >= args.batch_size:
                    # embed and flush
                    try:
                        embs = embed_batch(client, args.model, batch_texts)
                    except Exception as e:
                        print(f"[embed] ERROR embedding batch: {e}")
                        # drop this batch; continue
                        batch_texts, batch_rows, batch_chunk_ids = [], [], []
                        time.sleep(1.0)
                        continue

                    # assemble df
                    df = pd.DataFrame(batch_rows)
                    df["embedding"] = embs  # list[float]
                    part_file = make_part_file(emb_root, campaign, part_index)
                    ensure_parents(part_file)
                    df.to_parquet(part_file, index=False)
                    append_checkpoint(checkpoint_file, batch_chunk_ids)

                    total_new_chunks += len(batch_rows)
                    batches_written += 1
                    part_index += 1
                    print(f"[embed] wrote {part_file.name} ({len(batch_rows)} chunks)")

                    # reset batch
                    batch_texts, batch_rows, batch_chunk_ids = [], [], []
                    time.sleep(max(0.0, args.sleep_batch))

        # flush remainder
        if batch_texts:
            try:
                embs = embed_batch(client, args.model, batch_texts)
                df = pd.DataFrame(batch_rows)
                df["embedding"] = embs
                part_file = make_part_file(emb_root, campaign, part_index)
                ensure_parents(part_file)
                df.to_parquet(part_file, index=False)
                append_checkpoint(checkpoint_file, batch_chunk_ids)
                total_new_chunks += len(batch_rows)
                batches_written += 1
                print(f"[embed] wrote {part_file.name} ({len(batch_rows)} chunks)")
                part_index += 1
            except Exception as e:
                print(f"[embed] ERROR embedding final batch: {e}")

        print(
            f"[embed] Campaign done: {campaign} | new chunks embedded: {total_new_chunks} | parts: {batches_written}"
        )
        time.sleep(max(0.0, args.sleep_campaign))

    print("\n[embed] ALL DONE.")
    print(f"[embed] Embeddings written under: {emb_root}")
    print("[embed] Each campaign has a /parts/ directory with per-batch parquet files.")
    print("[embed] Use a separate consolidation script to merge parts if desired.")


if __name__ == "__main__":
    main()
