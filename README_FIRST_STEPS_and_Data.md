# Rally KB — Starter (v3, LLM-powered classification)

This version adds **GPT-powered classification** so we don't need to predefine every label.
- The model picks a **coarse `doc_type`** from our stable list (used for chunking/privacy).
- It also proposes a **free-text `doc_subtype`** for new/unknown types.
- We keep the same **privacy** and **chunking** rules as v2.

## 0) Open in VS Code
Unzip, then **File → Open Folder…** and select this folder.

## 1) Create & activate a Python environment
**Windows (PowerShell):**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
# If blocked, run once: Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

**macOS / Linux / WSL:**
```bash
python -m venv .venv
source .venv/bin/activate
```

## 2) Install requirements
```bash
pip install -r requirements.txt
```

## 3) Add your OpenAI key
```bash
cp .env.example .env
# this is the fi;e where you would paste open ai key
```

## 4) Put a campaign under data/raw/
```
data/
  raw/
    211 LA/
      (your .docx / .pdf / .pptx / .xlsx files)
```

## 5) Run the pipeline (LLM classifier)
**VS Code Task:** Ctrl/Cmd+Shift+P → Run Task → **"Ingest: Single Campaign (LLM classify)"** → enter `data/raw/211 LA`

**Or manual commands:**
```bash
python -m pipeline.crawl "data/raw/211 LA"
python -m pipeline.extract "data/raw/211 LA"
python -m pipeline.classify_llm "data/raw/211 LA"
python -m pipeline.privacy "data/raw/211 LA"
python -m pipeline.chunk "data/raw/211 LA"
python -m pipeline.catalog "data/raw/211 LA"
```

## 6) Check outputs
- `data/processed/<Campaign>/classified.csv` now includes **doc_subtype** + **clf_confidence**.
- `catalog/master_catalog.csv` includes those columns too (added automatically).

## Optional: Rules-only classifier
A second task **"Ingest: Single Campaign (rules-only)"** runs the old heuristic classifier.
----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
09/22/2025

## Main Bulk Data Ingestion Pipeline
  **Strategy Implemented*

Classify everything, but:

  1. If the model suggests a label that’s not in the current taxonomy, do not error.

  2. Route it to a triage file with the model’s suggested label + evidence, while assigning a safe working label (unmapped_other) so the rest of the pipeline can proceed.

  3. Also triage low‑confidence items (e.g., ≤ 0.65) regardless of label.

  4. Chunk everything, including unmapped_other using conservative chunk rules.

  5. After each batch, you review triage_needs_review.csv, decide on new labels or rules, update configs/taxonomy.yaml / heuristics, and re‑run. The runner skips already‑handled files by SHA.

This gives you a schema‑evolution loop without stopping the automation.

----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------
09/24/2025

### Filename Sanitization in Nested Ingest
Some campaigns include folders/files with trailing spaces or other
characters that are invalid on Windows (e.g. `"Design "` or `"Assets "`).
To prevent extraction errors, `ingest_from_nested_zip.py` now sanitizes
all names:
- Trailing spaces/dots are removed
- Backslashes are replaced with `_`
- Colons are replaced with `_`

This affects only the *temporary extracted paths*; metadata inside
catalog outputs still retains the original filenames where possible.

-------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------
09/25/2025

### Flatten & Deduplicate Campaigns (zip-of-zips → unique folders)

Some exports arrive as a “zip-of-zips”: a mega ZIP containing ~18 inner ZIPs, each with
a `Complete Campaigns/` folder and 18–35 campaign subfolders. We normalize this into a
flat corpus of unique campaign folders, with safe filenames and no duplicate files.

**Steps**

1. **Stage inner ZIPs**  
   The helper copies all inner ZIPs to `data/_nested_staging/inner_zips/`.

2. **Long-path-safe extraction with sanitization**  
   Each inner ZIP is extracted to `data/_nested_staging/extracted/<part>/`, sanitizing every
   path component (remove trailing spaces/dots; replace illegal characters) to avoid Windows
   filesystem errors.

3. **Flatten to `data/raw/`**  
   Every immediate subfolder in `Complete Campaigns/` is treated as a campaign and copied to
   `data/raw/<Campaign Name>/`. Name collisions are preserved as `<name>__1`, `<name>__2`, … rather
   than overwritten.

4. **Hybrid deduplication → `data/raw_dedup/`**  
   We collapse suffixed copies into a single canonical folder per campaign and deduplicate files
   by SHA-256. Filenames are sanitized for Windows safety. The original `data/raw/` is left intact.

**Commands**

```bash
# Flatten from mega ZIP to data/raw
python scripts/extract_all_inner_zips.py \
  --mega "C:\path\Complete Campaigns_Mega Zip.zip" \
  --inner-zips data/_nested_staging/inner_zips \
  --out data/raw \
  --tmp data/_nested_staging/extracted \
  --delete-tmp

# Deduplicate into data/raw_dedup
python scripts/dedup_raw_campaigns.py
