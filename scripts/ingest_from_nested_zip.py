# scripts/ingest_from_nested_zip.py
# Process a "zip-of-zips" archive: one mega ZIP that contains many inner ZIPs.
# For each inner ZIP, find the campaign folder root and run your per-campaign ingester.

import argparse
import json
import sys
import zipfile
import shutil
import fnmatch
import shlex
import os
from pathlib import Path
from datetime import datetime, timezone


def log(msg: str):
    print(msg, flush=True)


def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def is_zip(p: Path) -> bool:
    return p.suffix.lower() == ".zip"


def copy_inner_zips_from_mega(mega_zip: Path, out_dir: Path):
    """Copy *.zip files OUT of the mega zip as files (no decompression)."""
    ensure_dir(out_dir)
    with zipfile.ZipFile(mega_zip, "r") as z:
        inner = [
            i
            for i in z.infolist()
            if (not i.is_dir()) and i.filename.lower().endswith(".zip")
        ]
        if not inner:
            log(f"[warn] No inner .zip files found in {mega_zip.name}")
        for i in inner:
            target = out_dir / Path(i.filename).name
            if target.exists():
                log(f"[skip] already have {target.name}")
                continue
            log(f"[copy] {i.filename} -> {target}")
            with z.open(i, "r") as src, open(target, "wb") as dst:
                shutil.copyfileobj(src, dst)


def discover_inner_zip_files(source: Path, staging_zips: Path):
    """Return a list of inner zip files to process."""
    if source.is_dir():
        return sorted([p for p in source.glob("*.zip")])
    elif is_zip(source):
        copy_inner_zips_from_mega(source, staging_zips)
        return sorted([p for p in staging_zips.glob("*.zip")])
    else:
        raise SystemExit(
            f"[error] Source must be a .zip or a folder with .zip files: {source}"
        )


def load_resume(path: Path):
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "started_at": None,
        "completed_inner": [],
        "completed_campaigns": [],
        "failed": [],
    }


def save_resume(path: Path, data: dict):
    ensure_dir(path.parent)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)


# --- Windows long-path helpers ------------------------------------------------


def _win_long_path(s: str) -> str:
    """Return a Windows extended-length path (\\?\...) if needed."""
    if os.name != "nt":
        return s
    s = s.replace("/", "\\")
    if s.startswith("\\\\?\\"):
        return s
    if s.startswith("\\\\"):  # UNC
        return "\\\\?\\UNC\\" + s.lstrip("\\")
    return "\\\\?\\" + s


def _sanitize_name(name: str) -> str:
    """
    Clean up paths from ZIPs so Windows can extract them.
    - Strip trailing spaces/dots (invalid on Windows)
    - Replace backslashes and colons
    - Strip a couple of invisible directional marks sometimes found in names
    """
    name = name.rstrip(" .")
    name = name.replace("\\", "_").replace(":", "_")
    # Remove common bidi/invisible marks that can sneak into names
    name = name.replace("\u202a", "").replace("\u202c", "").replace("\ufeff", "")
    return name


def _safe_extract(zf: zipfile.ZipFile, dest: Path):
    """
    Long-path-safe unzip with filename sanitization.
    Works best when dest is a short path (e.g., C:\\rs\\extracted\\<inner>).
    """
    for m in zf.infolist():
        clean_name = _sanitize_name(m.filename)
        if not clean_name:
            continue  # skip weird empty entries

        target = dest / clean_name

        if m.is_dir():
            if os.name == "nt":
                os.makedirs(_win_long_path(str(target)), exist_ok=True)
            else:
                target.mkdir(parents=True, exist_ok=True)
            continue

        # Ensure parent exists
        if os.name == "nt":
            os.makedirs(_win_long_path(str(target.parent)), exist_ok=True)
            out_path = _win_long_path(str(target))
            with zf.open(m, "r") as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(m, "r") as src, open(str(target), "wb") as dst:
                shutil.copyfileobj(src, dst)


# --- Per-campaign runner (no shell; robust quoting) ---------------------------


def run_per_campaign(campaign_dir: Path, cmd_template: str) -> int:
    """
    Run the per-campaign command robustly (no shell) so spaces/&/() don't break on Windows.
    Use {campaign_dir} placeholder in the template.
    Examples:
    "python scripts\\ingest_campaign_rules.py \"{campaign_dir}\""
    "python -m pipeline.classify \"{campaign_dir}\""
    """
    import subprocess  # local import to avoid "unused import" lint at module level

    filled = cmd_template.format(campaign_dir=str(campaign_dir))
    argv = shlex.split(filled, posix=False)  # keep Windows backslashes
    if argv and argv[0].lower() == "python":
        argv[0] = sys.executable  # run with current venv/python
    log(f"  [run] {' '.join(argv)}")
    return subprocess.run(argv, shell=False).returncode


# --- Utility to locate the campaign root inside an inner ZIP ------------------


def find_campaign_root(extracted_dir: Path, preferred_name: str) -> Path:
    """
    Return the folder that actually holds the campaign subfolders.
    If 'Complete Campaigns/' exists, use it; else if there's only one folder, use that.
    """
    exact = extracted_dir / preferred_name
    if exact.exists() and exact.is_dir():
        return exact
    # case-insensitive match
    for child in extracted_dir.iterdir():
        if child.is_dir() and child.name.lower() == preferred_name.lower():
            return child
    dirs = [d for d in extracted_dir.iterdir() if d.is_dir()]
    if len(dirs) == 1:
        return dirs[0]
    return extracted_dir


# --- Main ---------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(
        description="Ingest campaigns from a mega zip that contains inner zip parts."
    )
    ap.add_argument(
        "--source",
        required=True,
        help="Path to the mega zip (or a folder that contains the inner zip parts).",
    )
    ap.add_argument(
        "--staging",
        default="data/_nested_staging",
        help="Working directory used for inner zips and extraction.",
    )
    ap.add_argument(
        "--resume-file",
        default="data/reports/nested_resume.json",
        help="Where progress is stored (safe to keep across runs).",
    )
    ap.add_argument(
        "--campaign-root-name",
        default="Complete Campaigns",
        help="Folder name inside each inner zip that contains the campaign folders.",
    )
    ap.add_argument(
        "--include",
        default="*",
        help="Glob of campaign folder names to include (default: all)",
    )
    ap.add_argument(
        "--exclude",
        default="",
        help="Comma-separated globs to exclude (e.g., 'Archive*,_old*')",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=0,
        help="If >0, limit number of campaign folders processed per inner zip in this run.",
    )
    ap.add_argument(
        "--max-campaigns",
        type=int,
        default=0,
        help="If >0, stop after processing this many campaigns in total.",
    )
    ap.add_argument(
        "--delete-extracted",
        action="store_true",
        help="Delete extracted inner zip folder after processing (reclaims disk).",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="Plan only; do not run the ingester."
    )
    ap.add_argument(
        "--campaign-cmd",
        required=True,
        help="Per-campaign command (REQUIRED). Use {campaign_dir} where the folder path should go. "
        'Example: "python scripts/ingest_campaign_rules.py \\"{campaign_dir}\\""',
    )
    args = ap.parse_args()

    source = Path(args.source)
    staging = Path(args.staging)
    staging_zips = staging / "inner_zips"
    extracted_parent = staging / "extracted"
    ensure_dir(staging_zips)
    ensure_dir(extracted_parent)

    resume_path = Path(args.resume_file)
    state = load_resume(resume_path)
    if state["started_at"] is None:
        state["started_at"] = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        save_resume(resume_path, state)

    # 1) Find inner zips
    inner_zips = discover_inner_zip_files(source, staging_zips)
    if not inner_zips:
        log("[error] No inner zip files found.")
        sys.exit(2)

    # 2) Iterate inner zips
    completed_inner = set(state.get("completed_inner", []))
    total_done = 0
    include_glob = args.include
    exclude_globs = [g.strip() for g in args.exclude.split(",") if g.strip()]

    log(
        f"[plan] inner zips found: {len(inner_zips)} (skipping {len(completed_inner)} already done)"
    )
    for inner in inner_zips:
        if inner.name in completed_inner:
            log(f"[skip] inner zip already processed: {inner.name}")
            continue

        # Extract this inner zip
        extracted_dir = extracted_parent / inner.stem
        if not extracted_dir.exists():
            log(f"[extract] {inner.name} -> {extracted_dir}")
            ensure_dir(extracted_dir)
            with zipfile.ZipFile(inner, "r") as z:
                _safe_extract(z, extracted_dir)

        # Find the campaign root inside
        campaign_root = find_campaign_root(extracted_dir, args.campaign_root_name)
        if not campaign_root.exists():
            log(
                f"[warn] campaign root not found in {inner.name}; using {extracted_dir}"
            )
            campaign_root = extracted_dir

        # List campaign folders (immediate children)
        all_campaigns = [d for d in sorted(campaign_root.iterdir()) if d.is_dir()]

        # Filter include/exclude
        campaigns = []
        for d in all_campaigns:
            if not fnmatch.fnmatch(d.name, include_glob):
                continue
            if any(fnmatch.fnmatch(d.name, g) for g in exclude_globs):
                continue
            campaigns.append(d)

        # Apply per-run batch cap for this inner zip
        if args.batch_size > 0:
            campaigns = campaigns[: args.batch_size]

        if not campaigns:
            log(f"[warn] no campaigns matched in {inner.name}")
            # Mark inner as completed to avoid looping forever
            state["completed_inner"].append(inner.name)
            save_resume(resume_path, state)
            # Optionally delete extraction
            if args.delete_extracted and extracted_dir.exists():
                shutil.rmtree(extracted_dir, ignore_errors=True)
            continue

        log(f"[inner] {inner.name} -> {len(campaigns)} campaign(s) scheduled")

        # 3) Run ingestion per campaign
        for cdir in campaigns:
            # Global stop after N campaigns
            if args.max_campaigns and total_done >= args.max_campaigns:
                log(f"[stop] Reached max-campaigns={args.max_campaigns}")
                save_resume(resume_path, state)
                sys.exit(0)

            # Avoid re-running same campaign path
            key = str(cdir.resolve())
            if key in state.get("completed_campaigns", []):
                log(f"[skip] already processed: {cdir.name}")
                continue

            log(f"[campaign] {cdir.name}")
            if args.dry_run:
                state["completed_campaigns"].append(key)
                total_done += 1
                save_resume(resume_path, state)
                continue

            rc = run_per_campaign(cdir, args.campaign_cmd)
            if rc == 0:
                state["completed_campaigns"].append(key)
                total_done += 1
            else:
                state["failed"].append(
                    {
                        "inner_zip": inner.name,
                        "campaign": cdir.name,
                        "path": key,
                        "code": rc,
                        "ts": datetime.now(timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z"),
                    }
                )
            save_resume(resume_path, state)

        # 4) Mark inner zip done
        state["completed_inner"].append(inner.name)
        save_resume(resume_path, state)

        # 5) Optional cleanup
        if args.delete_extracted and extracted_dir.exists():
            log(f"[cleanup] deleting {extracted_dir}")
            shutil.rmtree(extracted_dir, ignore_errors=True)

    log("[done] All inner zips processed.")
    save_resume(resume_path, state)


if __name__ == "__main__":
    main()
