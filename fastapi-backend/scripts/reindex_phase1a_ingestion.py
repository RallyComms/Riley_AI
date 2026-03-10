import argparse
import asyncio
import logging

from app.services.ingestion import backfill_reindex_supported_files


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill Phase 1A chunked ingestion for existing files."
    )
    parser.add_argument(
        "--tenant-id",
        dest="tenant_id",
        default=None,
        help="Optional tenant_id to scope Tier 2 backfill.",
    )
    parser.add_argument(
        "--no-global",
        action="store_true",
        help="Skip Tier 1 global collection backfill.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max points to scan per collection in this run.",
    )
    args = parser.parse_args()

    totals = await backfill_reindex_supported_files(
        tenant_id=args.tenant_id,
        include_global=not args.no_global,
        limit=args.limit,
    )
    logger.info("Backfill complete: %s", totals)


if __name__ == "__main__":
    asyncio.run(main())
