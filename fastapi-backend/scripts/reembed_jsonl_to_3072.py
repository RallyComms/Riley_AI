import os, json, uuid, time, asyncio
from typing import Iterator, List, Dict, Any, Tuple

from google.cloud import storage
from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models

from app.services.genai_client import get_genai_client
from app.core.config import get_settings


GS_FILES = [
    "gs://riley-tier1-hot/vectors_part1.jsonl",
    "gs://riley-tier1-hot/vectors_part2.jsonl",
]

DEST_COLLECTION = os.getenv("DEST_COLLECTION", "riley_campaigns")  # 3072
BATCH = int(os.getenv("BATCH_SIZE", "64"))
CHECKPOINT = os.getenv("CHECKPOINT_PATH", "reembed_checkpoint.txt")

def parse_gs(gs_uri: str) -> Tuple[str, str]:
    _, rest = gs_uri.split("gs://", 1)
    b, p = rest.split("/", 1)
    return b, p

def iter_lines(gs_uri: str, skip: int = 0) -> Iterator[str]:
    b, p = parse_gs(gs_uri)
    blob = storage.Client().bucket(b).blob(p)
    with blob.open("r") as f:
        for i, line in enumerate(f):
            if i < skip:
                continue
            line = line.strip()
            if line:
                yield line

def stable_uuid(orig_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"riley:{orig_id}"))

def load_checkpoint() -> Tuple[int, int]:
    if not os.path.exists(CHECKPOINT):
        return 0, 0
    try:
        raw = open(CHECKPOINT, "r").read().strip()
        if not raw:
            return 0, 0
        a, b = raw.split(",", 1)
        return int(a), int(b)
    except Exception:
        return 0, 0

def save_checkpoint(file_idx: int, line_no: int):
    with open(CHECKPOINT, "w") as f:
        f.write(f"{file_idx},{line_no}")

async def main():
    settings = get_settings()
    model = settings.EMBEDDING_MODEL  # models/gemini-embedding-001
    expected_dim = settings.EMBEDDING_DIM  # 3072

    qurl = os.popen("gcloud secrets versions access latest --secret=QDRANT_URL --project riley-ai-479422").read().strip()
    qkey = os.popen("gcloud secrets versions access latest --secret=QDRANT_API_KEY --project riley-ai-479422").read().strip()
    qdrant = AsyncQdrantClient(url=qurl, api_key=qkey, timeout=120)

    start_file, start_line = load_checkpoint()
    client = get_genai_client()

    total = 0
    for fi, gs_uri in enumerate(GS_FILES):
        if fi < start_file:
            continue
        skip = start_line if fi == start_file else 0
        line_no = skip

        batch: List[models.PointStruct] = []

        for line in iter_lines(gs_uri, skip=skip):
            line_no += 1
            obj = json.loads(line)
            md = obj.get("metadata", {}) or {}
            text = (md.get("text") or "").strip()
            if len(text) < 20:
                continue

            # embed (3072)
            try:
                resp = client.models.embed_content(model=model, contents=text[:9000])
                vec = resp.embeddings[0].values
                if len(vec) != expected_dim:
                    continue
            except Exception as e:
                # simple backoff for rate limits/transient errors
                time.sleep(2)
                continue

            payload: Dict[str, Any] = {
                "source": md.get("source", ""),
                "chunk_index": md.get("chunk_index", None),
                "text_snippet": text[:600],
                "orig_id": obj.get("id"),
                "is_global": True,
            }
            payload = {k: v for k, v in payload.items() if v is not None}

            pid = stable_uuid(str(obj.get("id")))
            batch.append(models.PointStruct(id=pid, vector=vec, payload=payload))
            total += 1

            if len(batch) >= BATCH:
                await qdrant.upsert(collection_name=DEST_COLLECTION, points=batch)
                batch = []
                save_checkpoint(fi, line_no)
                print("upserted", total)

        if batch:
            await qdrant.upsert(collection_name=DEST_COLLECTION, points=batch)
            save_checkpoint(fi, line_no)
            print("upserted", total)

        save_checkpoint(fi + 1, 0)

    info = await qdrant.get_collection(DEST_COLLECTION)
    print("DONE. points_count:", info.points_count)

if __name__ == "__main__":
    asyncio.run(main())
