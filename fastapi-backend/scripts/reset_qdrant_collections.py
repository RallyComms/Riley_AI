"""One-time destructive reset of Qdrant Tier 1 & Tier 2 collections.

This deletes and recreates the configured collections with the configured embedding dimension.

SAFETY:
- Requires either `--force` OR environment variable `QDRANT_RESET_CONFIRM=YES`.

Usage:
  QDRANT_RESET_CONFIRM=YES python -m scripts.reset_qdrant_collections
  python -m scripts.reset_qdrant_collections --force
"""

from __future__ import annotations

import argparse
import os
import sys

from qdrant_client import QdrantClient
from qdrant_client.http import models

from app.core.config import get_settings


def _confirm_or_exit(force: bool) -> None:
    if force:
        return
    if os.getenv("QDRANT_RESET_CONFIRM") == "YES":
        return

    print("Refusing to reset Qdrant collections without explicit confirmation.")
    print("")
    print("To proceed, set:")
    print("  QDRANT_RESET_CONFIRM=YES")
    print("or pass:")
    print("  --force")
    print("")
    print("Example:")
    print("  QDRANT_RESET_CONFIRM=YES python -m scripts.reset_qdrant_collections")
    sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset Qdrant collections (DELETES DATA).")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Actually delete and recreate collections (dangerous).",
    )
    args = parser.parse_args()
    _confirm_or_exit(args.force)

    settings = get_settings()

    distance_str = (settings.QDRANT_DISTANCE or "Cosine").strip().lower()
    distance = models.Distance.COSINE
    if distance_str == "dot":
        distance = models.Distance.DOT
    elif distance_str == "euclid":
        distance = models.Distance.EUCLID

    client = QdrantClient(
        host=settings.QDRANT_HOST,
        port=settings.QDRANT_PORT,
    )

    for name in (settings.QDRANT_COLLECTION_TIER_1, settings.QDRANT_COLLECTION_TIER_2):
        # Delete if exists
        try:
            client.get_collection(name)
            client.delete_collection(name)
        except Exception:
            pass

        # Create fresh
        client.create_collection(
            collection_name=name,
            vectors_config=models.VectorParams(
                size=settings.EMBEDDING_DIM,
                distance=distance,
            ),
        )

        print(f"Deleted {name} (if existed), created with dim={settings.EMBEDDING_DIM}")

    print("")
    print("Done.")
    print("If the API is running, restart it to ensure it reconnects cleanly.")


if __name__ == "__main__":
    main()

