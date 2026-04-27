import argparse
import asyncio
import json
import logging

from app.services.ingestion import reset_stuck_processing_ingestion_jobs


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Manual ingestion reliability utility: detect stale processing files and "
            "reset their status safely. This script does not run automatically."
        )
    )
    parser.add_argument(
        "--older-than-minutes",
        type=int,
        default=30,
        help="Only consider processing jobs older than this many minutes (default: 30).",
    )
    parser.add_argument(
        "--reenqueue-safe",
        action="store_true",
        help="If set, re-enqueue only payloads that pass safe re-enqueue checks.",
    )
    parser.add_argument(
        "--limit-per-collection",
        type=int,
        default=500,
        help="Scroll page size per collection while scanning processing payloads (default: 500).",
    )
    return parser.parse_args()


async def main() -> None:
    args = parse_args()
    result = await reset_stuck_processing_ingestion_jobs(
        older_than_minutes=max(1, int(args.older_than_minutes)),
        reenqueue_safe=bool(args.reenqueue_safe),
        limit_per_collection=max(1, int(args.limit_per_collection)),
    )
    logger.info("stuck_processing_reset_summary %s", json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
