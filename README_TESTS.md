

# Riley_AI – What Our Tests Do (in simple words)

This project turns lots of campaign files (Word docs, PDFs, slide decks, spreadsheets) into clean, searchable “chunks” of text.
To make sure nothing breaks as we improve the pipeline, we run different **tests**.
Each test checks a small part of the system and makes sure it does what we expect.

Think of tests like a *checklist we run automatically* every time we change the code.

---

## 1) `conftest.py` (shared test helpers)

* **What it does:**
  Provides shared setup used by multiple tests. In particular, it **builds a tiny fake campaign** (Word file, PDF, slides, etc.) and gives each test access to a scratch output folder.
* **Why it matters:**
  Ensures all tests have a consistent, safe place to work — no real files, no secrets, and no mess.
  The tests are fast, repeatable, and don’t depend on the internet.

---

## 2) `test_e2e_ingest.py` (end‑to‑end “smoke” test)

* **What it checks:**
  Runs the command‐line tool `scripts/ingest_campaign.py` in **smoke mode** on the tiny fake campaign.
  Verifies that:

  * It writes a **manifest** (a CSV summary) about what it processed.
  * It writes **chunks.jsonl** with small, searchable segments of text.
* **Why it matters:**
  Confirms the **entire pipeline** can run from start to finish on a small, safe sample.
  This catches big wiring problems (like missing imports or broken paths) early.

---

## 3) `test_extract_unit.py` (file reading)

* **What it checks:**
  Makes **fake**:

  * Word document (DOCX)
  * PDF
  * PowerPoint deck (PPTX)
  * Spreadsheet (XLSX)
  * Plain text file (TXT)

  Then it tests our “extractor” functions:

  * Can we **read** the text from each file?
  * Do we **summarize** spreadsheet structure (sheet names and columns)?
  * Does our “sniff and read” helper pick the right method for each file type?
* **Why it matters:**
  The pipeline can’t work if we can’t **open and read** files.
  These tests keep extraction reliable and fast.

---

## 4) `test_privacy_unit.py` (PII detection & redaction)

* **What it checks:**
  Looks for **PII** (personally identifiable information) like:

  * Emails (`name@site.com`)
  * Phone numbers (`(202) 555-0123`)
  * Street addresses (`123 Main Street`)

  Also checks that **redaction** replaces those details with `[EMAIL]`, `[PHONE]`, and `[ADDR]`.
  And verifies we choose the right **indexing mode**:

  * `full` (safe to index),
  * `redacted` (mask the risky bits),
  * `summary_only` (for spreadsheets/contacts).
* **Why it matters:**
  Protects privacy.
  Prevents putting sensitive contacts into search or embeddings.

---

## 5) `test_chunk_unit.py` (turning long text into chunks)

* **What it checks:**
  Confirms that:

  * Long text gets split into **overlapping chunks** (helps the AI keep context).
  * Short text stays in **one chunk** (no over-splitting).
  * Slide content becomes chunks labeled like `[Slide 1] Intro`.
* **Why it matters:**
  Good chunking is the foundation for search, RAG (retrieval‑augmented generation), and embeddings.
  These tests ensure we don’t break chunk boundaries or lose context.

---

## 6) `test_classify_llm_utils.py` (classifier helper smarts)

* **What it checks:**
  Just the **pure helper functions** that support classification, including:

  * Cleaning up **JSON** that comes back from a model (even if noisy / wrapped in code blocks).
  * Building the **sample content** that the model sees (first and last parts of a doc).
  * Guessing **context** from file path segments.
  * Generating **label hints** from synonyms (e.g., “press release” ≈ “news release”).
  * **Coercing** weird results into safe defaults (so the pipeline never crashes).
  * Smart **overrides**: e.g., if a file says “For immediate release”, treat it as a press release—even if the AI was unsure.
  * Enforcing **format constraints**: e.g., spreadsheets should classify as “media list/editorial calendar/etc.” not “press release”.
* **Why it matters:**
  This is where our **human rules** and **business logic** live.
  We test them without using any real AI—fast, safe, and very reliable.

---

## 7) `test_classify_llm_main_smoke.py` (classification end‑to‑end without network)

* **What it checks:**
  Runs the **top‑level classification function** but **pretends** to be the AI (we monkey‑patch a fake response).
  Confirms it writes the right outputs (`classified.csv`, optional `triage_needs_review.csv`) and uses the taxonomy correctly.
* **Why it matters:**
  Guarantees the **classification pipeline** works even when the real AI service isn’t available.
  Perfect for GitHub CI and for local dev without API keys.

---

## 8) `test_crawl_unit.py` (inventory builder)

* **What it checks:**
  Runs the **crawler** on a small folder and verifies it writes an **inventory CSV** listing files and their hashes (fingerprints).
* **Why it matters:**
  The entire pipeline relies on a correct inventory.
  If crawling breaks, nothing else can run.

---

## 9) The “optional” tests

* `test_optional_extract_unit.py`
* `test_optional_chunk_privacy_unit.py`

These are **extra** mini‑checks that overlap with the others.
They’re kept small and safe—good to run during quick edits.
You can remove them later if you prefer the larger unit tests above.

---

## How to run everything

```bash
# 1) Check code quality
python -m ruff check .
python -m black --check pipeline scripts tests

# 2) Run tests
python -m pytest -q --disable-warnings --cov --cov-report=term-missing --cov-fail-under=0
```

* **Green check** = safe to ship changes.
* If something fails, the test tells you **what broke** and **why**, so you can fix it quickly.

---

## Why this matters to RALLY

* Keeps **Riley** safe: no PII leaks, no broken parsers.
* Lets us **move fast**: we can refactor, add features, and trust the tests to catch mistakes.
* Builds the foundation for **multi‑agent workflows**, semantic search, and future automations.


