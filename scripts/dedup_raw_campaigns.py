# Hybrid merge + deduplication
#   1. Use campaign folder name as key (strip __1/__2).
#   2. Merge contents from suffixed duplicates.
#   3. Deduplicate by SHA256 hash inside each merged campaign.
#   4. Sanitize filenames/paths for Windows safety.

import argparse
import hashlib
import shutil
import os
from pathlib import Path

RAW = Path("data/raw")
DEDUP = Path("data/raw_dedup")


def log(msg):
    print(msg, flush=True)


def sanitize_filename(name: str) -> str:
    """Sanitize a filename or folder component for Windows/NTFS."""
    invalid = '<>:"/\\|?*'
    safe = "".join(c for c in name if c not in invalid)
    return safe.rstrip(" .")  # strip trailing spaces/dots


def file_hash(path, block_size=65536):
    """Compute SHA256 hash for deduplication."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)
    return h.hexdigest()


def dedup_campaigns(src_root: Path, out_root: Path, dry_run=False):
    out_root.mkdir(parents=True, exist_ok=True)

    # Group campaign folders by "base name" (strip __1/__2)
    groups = {}
    for d in src_root.iterdir():
        if not d.is_dir():
            continue
        base = d.name.split("__")[0]
        groups.setdefault(base, []).append(d)

    total_campaigns = len(groups)
    log(
        f"[plan] Found {len(list(src_root.iterdir()))} raw folders → {total_campaigns} unique campaigns"
    )

    for base, dirs in groups.items():
        out_dir = out_root / sanitize_filename(base)
        if not dry_run:
            out_dir.mkdir(parents=True, exist_ok=True)
        seen_hashes = set()
        merged_files, skipped = 0, 0

        for d in dirs:
            for root, _, files in os.walk(d):
                for f in files:
                    src = Path(root) / f
                    try:
                        h = file_hash(src)
                    except Exception as e:
                        log(f"[warn] could not hash {src}: {e}")
                        continue

                    if h in seen_hashes:
                        skipped += 1
                        continue
                    seen_hashes.add(h)

                    # replicate relative path, sanitizing each part
                    rel = src.relative_to(d)
                    clean_parts = [sanitize_filename(p) for p in rel.parts]
                    dest = out_dir.joinpath(*clean_parts)

                    if not dry_run:
                        dest.parent.mkdir(parents=True, exist_ok=True)
                        try:
                            shutil.copy2(src, dest)
                        except Exception as e:
                            log(f"[warn] could not copy {src} -> {dest}: {e}")
                            continue
                    merged_files += 1

        log(
            f"[dedup] {base}: merged {merged_files} files (skipped {skipped} dups) from {len(dirs)} folders"
        )

    log(f"[done] Deduplicated {total_campaigns} campaigns into {out_root}")


def main():
    ap = argparse.ArgumentParser(description="Deduplicate raw campaign folders")
    ap.add_argument(
        "--src", default=str(RAW), help="Source raw campaigns folder (default=data/raw)"
    )
    ap.add_argument(
        "--out",
        default=str(DEDUP),
        help="Output deduplicated folder (default=data/raw_dedup)",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="Show plan only, don’t copy files"
    )
    args = ap.parse_args()

    dedup_campaigns(Path(args.src), Path(args.out), dry_run=args.dry_run)


if __name__ == "__main__":
    main()
