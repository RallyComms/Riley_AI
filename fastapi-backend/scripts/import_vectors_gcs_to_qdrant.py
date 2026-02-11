import os, json, uuid
from typing import Dict, Any, Iterator, Optional, List, Tuple

from google.cloud import storage
from qdrant_client import QdrantClient
from qdrant_client.http import models
from tqdm import tqdm


def env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Not a gs:// URI: {gs_uri}")
    _, rest = gs_uri.split("gs://", 1)
    bucket, path = rest.split("/", 1)
    return bucket, path


def iter_gcs_lines(gs_uri: str, skip_lines: int = 0) -> Iterator[str]:
    bucket, path = parse_gs_uri(gs_uri)
    blob = storage.Client().bucket(bucket).blob(path)
    with blob.open("r") as f:
        for i, line in enumerate(f):
            if i < skip_lines:
                continue
            line = line.strip()
            if line:
                yield line


def to_uuid(orig_id: str) -> str:
    # Stable UUID from original id string -> reruns are idempotent
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"riley:{orig_id}"))


def normalize_payload(md: Dict[str, Any], orig_id: str) -> Dict[str, Any]:
    # Keep payload lean. Full text belongs in GCS; store a snippet for UI only.
    text = md.get("text", "") or ""
    payload = {
        "source": md.get("source", "") or "",
        "chunk_index": md.get("chunk_index", None),
        "text_snippet": text[:600],
        "orig_id": orig_id,
    }
    return {k: v for k, v in payload.items() if v is not None}


def main():
    qdrant_url = env("QDRANT_URL")
    qdrant_key = env("QDRANT_API_KEY")
    dest_collection = env("DEST_COLLECTION", "riley_campaigns_768")
    gcs_files = [s.strip() for s in env("GCS_JSONL_FILES").split(",") if s.strip()]

    batch_size = int(env("BATCH_SIZE", "256"))
    checkpoint_path = env("CHECKPOINT_PATH", "import_checkpoint.txt")
    expected_dim = int(env("EXPECTED_DIM", "768"))

    client = QdrantClient(url=qdrant_url, api_key=qdrant_key, timeout=60)

    # Resume state: file_idx,line_no (0-based line index within file)
    file_idx_start, line_no_start = 0, 0
    if os.path.exists(checkpoint_path):
        try:
            raw = open(checkpoint_path, "r").read().strip()
            if raw:
                a, b = raw.split(",", 1)
                file_idx_start, line_no_start = int(a), int(b)
        except Exception:
            pass

    print(f"Qdrant URL: {qdrant_url}")
    print(f"Destination: {dest_collection}")
    print(f"GCS files: {gcs_files}")
    print(f"Resume from file_idx={file_idx_start}, line_no={line_no_start}")
    print(f"Batch size: {batch_size}, expected_dim={expected_dim}")

    total_upserted = 0

    for file_idx, gs_uri in enumerate(gcs_files):
        if file_idx < file_idx_start:
            continue

        skip = line_no_start if file_idx == file_idx_start else 0
        line_no = skip

        batch: List[models.PointStruct] = []

        # Use tqdm without total (streaming)
        for line in tqdm(iter_gcs_lines(gs_uri, skip_lines=skip), desc=f"Import {gs_uri}", unit="lines"):
            line_no += 1
            try:
                obj = json.loads(line)
                vec = obj.get("values")
                orig_id = str(obj.get("id"))
                md = obj.get("metadata", {}) or {}

                if not isinstance(vec, list) or len(vec) != expected_dim:
                    continue

                qid = to_uuid(orig_id)
                payload = normalize_payload(md, orig_id)

                batch.append(models.PointStruct(id=qid, vector=vec, payload=payload))
            except Exception:
                continue

            if len(batch) >= batch_size:
                client.upsert(collection_name=dest_collection, points=batch)
                total_upserted += len(batch)
                batch = []
                # checkpoint every batch
                with open(checkpoint_path, "w") as f:
                    f.write(f"{file_idx},{line_no}")

        if batch:
            client.upsert(collection_name=dest_collection, points=batch)
            total_upserted += len(batch)
            with open(checkpoint_path, "w") as f:
                f.write(f"{file_idx},{line_no}")

        # file done -> next file, reset line to 0
        with open(checkpoint_path, "w") as f:
            f.write(f"{file_idx+1},0")

    info = client.get_collection(dest_collection)
    print(f"Done. Upserted ~{total_upserted} points. Qdrant points_count now: {info.points_count}")


if __name__ == "__main__":
    main()