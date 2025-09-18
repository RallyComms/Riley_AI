import hashlib, sys
from pathlib import Path
import pandas as pd

ALLOWED = {'.docx','.pdf','.pptx','.xlsx','.xls'}

def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda: f.read(1<<20), b''):
            h.update(chunk)
    return h.hexdigest()

def main(root_path: str):
    root = Path(root_path)
    rows = []
    for p in root.rglob('*'):
        if p.is_file() and p.suffix.lower() in ALLOWED:
            rows.append({
                "source_path": str(p),
                "ext": p.suffix.lower(),
                "size": p.stat().st_size,
                "sha256": sha256(p)
            })
    out = Path('data/processed') / f"inventory_{root.name}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out, index=False)
    print(f"[crawl] {len(rows)} files -> {out}")

if __name__ == "__main__":
    main(sys.argv[1])
