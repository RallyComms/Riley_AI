

# --source is data/raw_dedup, which is a folder of campaign folders (not zips).
#The current version of ingest_from_nested_zip.py only knows how to:
#   1. Take a mega zip (and look for inner zips), or
#   2. Take a folder full of inner zip files.scripts/ingest_from_folder.py

#SO we need a ingest from folders not zips script.
#Since we’ve already flattened and deduped to data/raw_dedup/, we don’t need the “zip-of-zips” logic anymore. 
#All you need is a loop over each subfolder in data/raw_dedup/ that calls the campaign ingest script.

import argparse, subprocess, sys
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", required=True, help="Folder of campaign folders (e.g. data/raw_dedup)")
    ap.add_argument("--campaign-cmd", required=True, help="Command template (use {campaign_dir})")
    ap.add_argument("--max-campaigns", type=int, default=0, help="Limit campaigns per run")
    args = ap.parse_args()

    root = Path(args.source)
    if not root.exists() or not root.is_dir():
        sys.exit(f"[error] Source not found: {root}")

    campaigns = [d for d in root.iterdir() if d.is_dir()]
    if args.max_campaigns:
        campaigns = campaigns[:args.max_campaigns]

    for camp in campaigns:
        cmd = args.campaign_cmd.format(campaign_dir=str(camp))
        print(f"[run] {cmd}")
        rc = subprocess.run(cmd, shell=True).returncode
        if rc != 0:
            print(f"[warn] campaign failed: {camp} (code {rc})")

if __name__ == "__main__":
    main()
