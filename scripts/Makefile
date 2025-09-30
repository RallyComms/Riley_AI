# --- currently working in dedupe data folder (09/29/25)---
INGESTED_DIR := data/raw_dedup
AUDIT_OUT    := artifacts/audit
RUN_OUT      := .runs/latest

.PHONY: install test lint ci audit chunk

install:
\tpython -m pip install -U pip
\tpip install -r requirements.txt

lint:
\truff check .
\tblack --check .

test:
\tpytest -q --maxfail=1 --disable-warnings

ci: lint test

audit:
\tpython scripts/preflight_ingest_audit.py --input $(INGESTED_DIR) --out $(AUDIT_OUT)

chunk:
\tpython scripts/run_chunk_and_classify.py --input $(INGESTED_DIR) --out $(RUN_OUT)
