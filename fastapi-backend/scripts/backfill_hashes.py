import argparse
import asyncio
import logging

from qdrant_client.http.models import FieldCondition, Filter, MatchValue

from app.core.config import get_settings
from app.services.qdrant import vector_service


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def scan_parent_hash_coverage(
    *,
    collection_name: str,
    batch_size: int,
) -> dict:
    offset = None
    total_batches_processed = 0
    total_parent_files = 0
    total_with_hash = 0
    total_without_hash = 0
    total_backfill_eligible = 0

    parent_filter = Filter(
        must_not=[FieldCondition(key="record_type", match=MatchValue(value="chunk"))]
    )

    while True:
        points, next_offset = await vector_service.client.scroll(
            collection_name=collection_name,
            scroll_filter=parent_filter,
            limit=batch_size,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        if not points:
            break

        total_batches_processed += 1
        total_parent_files += len(points)
        for point in points:
            payload = point.payload or {}
            existing_hash = str(payload.get("indexed_content_sha256") or "").strip()
            if existing_hash:
                total_with_hash += 1
            else:
                total_without_hash += 1

            status_now = str(payload.get("ingestion_status") or "").strip().lower()
            chunk_count = int(payload.get("chunk_count") or 0)
            if status_now == "indexed" and chunk_count > 0 and not existing_hash:
                total_backfill_eligible += 1

        offset = next_offset
        if offset is None:
            break

    logger.info(
        "total_parent_files collection=%s total_parent_files=%s",
        collection_name,
        total_parent_files,
    )
    logger.info(
        "total_with_hash collection=%s total_with_hash=%s",
        collection_name,
        total_with_hash,
    )
    logger.info(
        "total_without_hash collection=%s total_without_hash=%s",
        collection_name,
        total_without_hash,
    )
    logger.info(
        "total_batches_processed collection=%s total_batches_processed=%s",
        collection_name,
        total_batches_processed,
    )

    return {
        "collection": collection_name,
        "total_parent_files": total_parent_files,
        "total_with_hash": total_with_hash,
        "total_without_hash": total_without_hash,
        "total_backfill_eligible": total_backfill_eligible,
        "total_batches_processed": total_batches_processed,
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="One-time backfill for indexed_content_sha256 on already-indexed files."
    )
    parser.add_argument(
        "--tenant-id",
        dest="tenant_id",
        default=None,
        help="Optional tenant_id to scope Tier 2 backfill. Defaults to all tenants.",
    )
    parser.add_argument(
        "--no-global",
        action="store_true",
        help="Skip Tier 1 global collection backfill.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size per scroll page (clamped to 50-100).",
    )
    parser.add_argument(
        "--max-points-per-collection",
        type=int,
        default=None,
        help="Optional scan cap per collection for controlled runs.",
    )
    parser.add_argument(
        "--debug-scan-only",
        action="store_true",
        help="Only run full parent/hash coverage scan; do not perform backfill updates.",
    )
    parser.add_argument(
        "--debug-collections",
        nargs="*",
        default=None,
        help="Optional explicit collection names for debug scan (defaults to configured Tier 2/Tier 1).",
    )
    args = parser.parse_args()

    settings = get_settings()
    debug_batch_size = max(50, min(100, int(args.batch_size or 100)))
    debug_collections = args.debug_collections or [
        settings.QDRANT_COLLECTION_TIER_2,
        settings.QDRANT_COLLECTION_TIER_1,
    ]

    coverage_rows = []
    for collection_name in debug_collections:
        try:
            coverage = await scan_parent_hash_coverage(
                collection_name=collection_name,
                batch_size=debug_batch_size,
            )
            coverage_rows.append(coverage)
        except Exception as exc:
            logger.error(
                "backfill_errors collection=%s error=%s",
                collection_name,
                type(exc).__name__,
            )

    if coverage_rows:
        logger.info("coverage_summary %s", coverage_rows)
    if args.debug_scan_only:
        return

    from app.services.ingestion import backfill_indexed_content_hashes

    totals = await backfill_indexed_content_hashes(
        tenant_id=args.tenant_id,
        include_global=not args.no_global,
        batch_size=args.batch_size,
        max_points_per_collection=args.max_points_per_collection,
    )
    logger.info("backfill_hashes_result %s", totals)


if __name__ == "__main__":
    asyncio.run(main())
