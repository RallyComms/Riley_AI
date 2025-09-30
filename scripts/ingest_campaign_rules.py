import sys
import subprocess as sp
from pathlib import Path


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <campaign_folder_path>")
        sys.exit(1)

    # Join all args after the script name, in case the path has spaces
    camp = " ".join(sys.argv[1:]).strip('"')

    camp_path = Path(camp)
    if not camp_path.exists() or not camp_path.is_dir():
        print(
            f"[ingest_campaign_rules] ERROR: Path not found or not a directory: {camp_path}"
        )
        sys.exit(1)

    py = sys.executable
    steps = [
        [py, "-m", "pipeline.crawl", str(camp_path)],
        [py, "-m", "pipeline.extract", str(camp_path)],
        [py, "-m", "pipeline.classify", str(camp_path)],  # rules-based classifier
        [py, "-m", "pipeline.privacy", str(camp_path)],
        [py, "-m", "pipeline.chunk", str(camp_path)],
        [py, "-m", "pipeline.catalog", str(camp_path)],
    ]

    print(f"\n[ingest_campaign_rules] Starting pipeline for: {camp_path}\n")

    for s in steps:
        print("\n>>", " ".join(s))
        sp.run(s, check=True)

    print(f"\n[ingest_campaign_rules] DONE ✓  ({camp_path})")


if __name__ == "__main__":
    main()
