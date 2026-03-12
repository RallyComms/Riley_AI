"""Verify Tier 1/Tier 2 collections have sparse vector config `bm25`.

Usage:
  python -m scripts.verify_qdrant_bm25_config
  python -m scripts.verify_qdrant_bm25_config --require
"""

from __future__ import annotations

import argparse
import sys
from typing import Any, Dict

from qdrant_client import QdrantClient

from app.core.config import get_settings


def _extract_sparse_vectors(collection_info: Any) -> Dict[str, Any]:
    if hasattr(collection_info, "model_dump"):
        dumped = collection_info.model_dump()
    elif hasattr(collection_info, "dict"):
        dumped = collection_info.dict()
    else:
        return {}
    return ((dumped.get("config") or {}).get("params") or {}).get("sparse_vectors") or {}


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Qdrant BM25 sparse vector configuration.")
    parser.add_argument(
        "--require",
        action="store_true",
        help="Exit non-zero if bm25 sparse config is missing in any target collection.",
    )
    args = parser.parse_args()

    settings = get_settings()
    if settings.QDRANT_URL:
        client = QdrantClient(url=settings.QDRANT_URL, api_key=settings.QDRANT_API_KEY)
    else:
        client = QdrantClient(host=settings.QDRANT_HOST, port=settings.QDRANT_PORT)

    collections = [settings.QDRANT_COLLECTION_TIER_1, settings.QDRANT_COLLECTION_TIER_2]
    missing = []

    for name in collections:
        info = client.get_collection(name)
        sparse_vectors = _extract_sparse_vectors(info)
        bm25 = sparse_vectors.get("bm25", {})
        modifier = ((bm25.get("modifier") if isinstance(bm25, dict) else None) or "").lower()
        has_bm25 = "bm25" in sparse_vectors
        print(f"{name}: bm25_configured={has_bm25} modifier={modifier or 'unknown'}")
        if not has_bm25:
            missing.append(name)

    if args.require and missing:
        print(f"Missing bm25 sparse config for: {', '.join(missing)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
