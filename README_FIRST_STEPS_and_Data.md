# Rally KB — Starter (v3, LLM-Powered Classification)

This repository implements a campaign knowledge-base ingestion pipeline with **GPT-powered classification**, privacy handling, and chunking.

---

## 🔑 What’s New in v2

* Adds **LLM-based classification**:

  * Model selects a stable `doc_type` (for chunking & privacy).
  * Also proposes a free-text `doc_subtype` for novel types.
* Preserves the **privacy** and **chunking** logic from v2.
* Introduces **schema evolution loop**: unmapped or low-confidence docs are triaged but the pipeline continues.

---

## 🚀 Quickstart

### 0. Open in VS Code

* Unzip this repo.
* **File → Open Folder…** and select the repo root.

### 1. Create & activate a Python environment

**Windows (PowerShell):**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
# If blocked, run once:
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
```

**macOS / Linux / WSL:**

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. Install requirements

```bash
pip install -r requirements.txt
```

### 3. Add your OpenAI key

```bash
cp .env.example .env
# Paste your OpenAI API key into the new .env file
```

### 4. Add campaign data

Place your campaign folder under `data/raw/`:

```
data/
  raw/
    211 LA/
      (your .docx / .pdf / .pptx / .xlsx files)
```

### 5. Run the pipeline (LLM classifier)

**VS Code task:**

* Press Ctrl/Cmd+Shift+P → **Run Task**
* Choose **"Ingest: Single Campaign (LLM classify)"**
* Enter the path: `data/raw/211 LA`

**Or manual commands:**

```bash
python -m pipeline.crawl "data/raw/211 LA"
python -m pipeline.extract "data/raw/211 LA"
python -m pipeline.classify_llm "data/raw/211 LA"
python -m pipeline.privacy "data/raw/211 LA"
python -m pipeline.chunk "data/raw/211 LA"
python -m pipeline.catalog "data/raw/211 LA"
```

### 6. Check outputs

* `data/processed/<Campaign>/classified.csv` → contains `doc_subtype` + `clf_confidence`.
* `catalog/master_catalog.csv` → includes those new columns automatically.

---

## ⚙️ Optional: Rules-Only Classifier

You can also run the legacy heuristic classifier via task **"Ingest: Single Campaign (rules-only)"**.

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
(09/22/2025)

## 🛠️ Bulk Ingestion Strategy 

* Classify everything, but:

  1. If model suggests a label not in taxonomy → **triage** it with evidence + assign `unmapped_other`.
  2. Triage any **low-confidence** results (≤ 0.65).
  3. Chunk everything (including `unmapped_other`) with conservative rules.
  4. After each batch: review `triage_needs_review.csv`, update `configs/taxonomy.yaml` or rules, and re-run.
     The runner skips already-handled files by SHA.
* **Result:** Schema evolves continuously without blocking automation.

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
(09/24/2025)

## 🗂️ Filename Sanitization

* Nested ZIP campaigns sometimes include invalid Windows filenames (`"Design "`, `"Assets "`, etc.).
* The pipeline sanitizes temporary extracted paths:

  * Trailing spaces/dots removed
  * Backslashes → `_`
  * Colons → `_`
* Catalog metadata retains original names when possible.

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
(09/25/2025)

## 📦 Flatten & Deduplicate 

Some exports arrive as “zip-of-zips”: a mega ZIP with ~18 inner ZIPs, each containing campaigns.
We normalize this to a flat corpus of unique, safe campaign folders.

**Steps:**

1. Stage inner ZIPs → `data/_nested_staging/inner_zips/`
2. Extract long-path-safe with sanitization → `data/_nested_staging/extracted/<part>/`
3. Flatten into `data/raw/<Campaign Name>/`

   * Collisions → suffixed (`__1`, `__2`)
4. Deduplicate into `data/raw_dedup/` by SHA-256

   * Keeps one canonical folder per campaign
   * Leaves `data/raw/` intact

**Commands:**

```bash
# Flatten mega ZIP into data/raw
python scripts/extract_all_inner_zips.py \
  --mega "C:\path\Complete Campaigns_Mega Zip.zip" \
  --inner-zips data/_nested_staging/inner_zips \
  --out data/raw \
  --tmp data/_nested_staging/extracted \
  --delete-tmp

# Deduplicate into data/raw_dedup
python scripts/dedup_raw_campaigns.py
```

-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
(09/29/2025)

## 🔒 Privacy Pipeline Notes 

**Improvements:**

* **Address regex** detects common US-style street names (e.g., `"123 Main Street"`).
  (Doesn’t yet cover PO Boxes or exotic suffixes.)
* `detect_pii()` and `redact_text()` added for CI/CD.
* **Redaction order** is deterministic: emails → phones → addresses.
* Redaction tokens are configurable (`[EMAIL]`, `[PHONE]`, `[ADDR]`).
* Legacy `decide_index_mode()` + `main()` remain for `privacy.csv`.

## 📑 Extract Pipeline Notes 

**Key Improvements:**

* `read_*` functions wrap in `try/except` → always return strings.
* `sniff_and_read` handles XLSX schemas as JSON string → safe for audits.
* `main()` now:

  * Guards against missing/empty inventories.
  * Covers `.txt` and `.md`.
  * Guarantees output per file (extract or error).
* CI/CD compatibility:

  * `sniff_and_read` = single universal entry point.
  * Deterministic, no network calls.

---