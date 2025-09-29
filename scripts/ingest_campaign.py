import sys
import subprocess as sp
from pathlib import Path

def main():
    if len(sys.argv) != 2:
        print('Usage: python scripts/ingest_campaign.py "data/raw/<Campaign Name>"')
        sys.exit(1)

    camp = sys.argv[1]
    if not Path(camp).exists():
        print(f'[ingest_campaign] ERROR: Path not found: {camp}')
        sys.exit(1)

    py = sys.executable  # use the same Python env
    steps = [
        [py, "-m", "pipeline.crawl", camp],
        [py, "-m", "pipeline.extract", camp],
        [py, "-m", "pipeline.classify_llm", camp],
        [py, "-m", "pipeline.privacy", camp],
        [py, "-m", "pipeline.chunk", camp],
        [py, "-m", "pipeline.catalog", camp],
    ]

    for s in steps:
        print("\n>>", " ".join(s))
        sp.run(s, check=True)

    print("\nDONE ✓")

if __name__ == "__main__":
    main()
