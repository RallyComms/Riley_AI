import argparse
import os
import shutil
import zipfile
from pathlib import Path

INNER_ZIPS = Path("data/_nested_staging/inner_zips")
EXTRACTED = Path("data/_nested_staging/flatten_extract")
RAW_OUT = Path("data/raw")


def log(msg):
    print(msg, flush=True)


def ensure(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def is_zip(p: Path) -> bool:
    return p.suffix.lower() == ".zip"


def sanitize_name(name: str) -> str:
    # strip trailing spaces/dots (illegal on Windows), replace backslashes/colons
    name = name.rstrip(" .").replace("\\", "_").replace(":", "_")
    # remove a few invisible bidi marks
    for ch in ("\u202a", "\u202c", "\ufeff"):
        name = name.replace(ch, "")
    return name


def win_long(s: str) -> str:
    if os.name != "nt":
        return s
    s = os.path.abspath(s)
    s = s.replace("/", "\\")
    if s.startswith("\\\\?\\"):
        return s
    if s.startswith("\\\\"):  # UNC path
        return "\\\\?\\UNC\\" + s.lstrip("\\")
    return "\\\\?\\" + s


def safe_extract(zf: zipfile.ZipFile, dest: Path):
    """
    Long-path-safe unzip with sanitization of *every path component*.
    Strips trailing spaces/dots from folder and file names.
    """
    for m in zf.infolist():
        # Split into parts and sanitize each
        parts = Path(m.filename).parts
        clean_parts = [sanitize_name(p) for p in parts if p not in ("", "/", "\\")]
        if not clean_parts:
            continue
        target = dest.joinpath(*clean_parts)

        if m.is_dir():
            if os.name == "nt":
                os.makedirs(win_long(str(target)), exist_ok=True)
            else:
                target.mkdir(parents=True, exist_ok=True)
            continue

        # Ensure parent exists
        if os.name == "nt":
            os.makedirs(win_long(str(target.parent)), exist_ok=True)
            out_path = win_long(str(target))
            with zf.open(m, "r") as src, open(out_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
        else:
            target.parent.mkdir(parents=True, exist_ok=True)
            with zf.open(m, "r") as src, open(str(target), "wb") as dst:
                shutil.copyfileobj(src, dst)


def copy_inner_zips_from_mega(mega_zip: Path, out_dir: Path):
    ensure(out_dir)
    with zipfile.ZipFile(mega_zip, "r") as z:
        inner = [
            i
            for i in z.infolist()
            if (not i.is_dir()) and i.filename.lower().endswith(".zip")
        ]
        if not inner:
            log(f"[warn] No inner .zip files found in {mega_zip}")
        for i in inner:
            dest = out_dir / Path(i.filename).name
            if dest.exists():
                log(f"[skip] already have {dest.name}")
                continue
            log(f"[copy] {i.filename} -> {dest}")
            with z.open(i, "r") as src, open(dest, "wb") as dst:
                shutil.copyfileobj(src, dst)


def find_complete_campaigns_root(root: Path) -> Path:
    # exact match
    cand = root / "Complete Campaigns"
    if cand.exists() and cand.is_dir():
        return cand
    # case-insensitive match
    for d in root.iterdir():
        if d.is_dir() and d.name.lower() == "complete campaigns":
            return d
    # fallback: if one directory, use it
    dirs = [d for d in root.iterdir() if d.is_dir()]
    if len(dirs) == 1:
        return dirs[0]
    return root


def unique_dest(base_dir: Path, name: str) -> Path:
    name = sanitize_name(name.strip())
    dest = base_dir / name
    if not dest.exists():
        return dest
    # add numeric disambiguator
    i = 1
    while True:
        cand = base_dir / f"{name}__{i}"
        if not cand.exists():
            return cand
        i += 1


def flatten_inner_zip(
    inner_zip: Path, extracted_tmp_root: Path, raw_out: Path, dry_run: bool
):
    # 1) extract inner zip
    inner_dir = extracted_tmp_root / inner_zip.stem
    if not inner_dir.exists():
        log(f"[extract] {inner_zip.name} -> {inner_dir}")
        ensure(inner_dir)
        with zipfile.ZipFile(inner_zip, "r") as z:
            safe_extract(z, inner_dir)
    else:
        log(f"[skip] already extracted: {inner_zip.name}")

    # 2) find 'Complete Campaigns' folder
    camp_root = find_complete_campaigns_root(inner_dir)
    # 3) each immediate subfolder is a campaign
    campaigns = [d for d in sorted(camp_root.iterdir()) if d.is_dir()]
    log(f"[inner] {inner_zip.name} -> {len(campaigns)} campaign(s)")

    ensure(raw_out)
    moved = 0

    for cdir in campaigns:
        dest = unique_dest(raw_out, cdir.name)
        if dry_run:
            log(f"  [plan] {cdir}  ->  {dest}")
            continue
        log(f"  [copy] {cdir.name} -> {dest}")

        # Always resolve to absolute paths
        src_abs = cdir.resolve()
        dst_abs = dest.resolve()

        if os.name == "nt":
            shutil.copytree(win_long(str(src_abs)), win_long(str(dst_abs)))
        else:
            shutil.copytree(src_abs, dst_abs)
        moved += 1

    return moved


def main():
    ap = argparse.ArgumentParser(
        description="Flatten inner zips into data/raw/<Campaign> folders"
    )
    ap.add_argument(
        "--mega", help="Path to the mega zip that contains the 18 inner zips (optional)"
    )
    ap.add_argument(
        "--inner-zips",
        default=str(INNER_ZIPS),
        help="Folder containing the 18 inner zip files",
    )
    ap.add_argument(
        "--out",
        default=str(RAW_OUT),
        help="Destination for flat campaign folders (data/raw)",
    )
    ap.add_argument(
        "--tmp",
        default=str(EXTRACTED),
        help="Temp extraction root (short path recommended)",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="Plan only; do not copy campaigns"
    )
    ap.add_argument(
        "--delete-tmp",
        action="store_true",
        help="Delete temp extracted trees when done",
    )
    args = ap.parse_args()

    inner_dir = Path(args.inner_zips)
    tmp_root = ensure(Path(args.tmp))
    raw_out = ensure(Path(args.out))

    # optionally copy inner zips out of the mega zip
    if args.mega:
        copy_inner_zips_from_mega(Path(args.mega), inner_dir)

    zips = sorted(p for p in inner_dir.glob("*.zip"))
    if not zips:
        log(f"[error] No inner zip files found in {inner_dir}")
        raise SystemExit(2)

    total = 0
    for z in zips:
        total += flatten_inner_zip(z, tmp_root, raw_out, args.dry_run)

    log(f"[done] Flattened campaigns planned/copied: {total}")
    if args.delete_tmp and not args.dry_run:
        log(f"[cleanup] deleting {tmp_root}")
        shutil.rmtree(tmp_root, ignore_errors=True)


if __name__ == "__main__":
    main()
