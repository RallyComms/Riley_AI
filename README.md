# Riley_AI

**Riley** is RALLY’s internal AI assistant designed to rapidly ingest, classify, and structure decades of campaign materials into a knowledge base that can be searched, chunked, and extended into agentic workflows.

This repository contains the **core ingestion pipeline**, CI/CD scaffolding, and testing framework for Riley’s document processing system.

---

## 🚀 Features

### Automated Ingestion
- Extracts text from PDFs, DOCX, PPTX, TXT, and more  
- Handles nested campaign folder structures and zips

### Classification & Chunking
- Lightweight LLM (mocked for CI) assigns taxonomy labels (Fundraising, Comms, Field, Policy, General)  
- Robust chunking into token-bounded segments for RAG / embeddings

### Privacy Protection
- PII detection (emails, phone numbers, addresses)  
- Redaction pipeline to ensure sensitive data is masked before indexing

### CI/CD Pipeline
- GitHub Actions: linting, formatting, pytest smoke tests  
- Synthetic campaign fixtures for deterministic regression testing  
- Strict/fallback mode toggle via environment variables

### Scalable Foundation
- Modular structure (`pipeline.extract`, `pipeline.chunk`, `pipeline.privacy`, `pipeline.classify_llm`)  
- Designed to evolve toward multi-agent orchestration and advanced semantic search

---

## 📂 Repository Structure

```

.github/workflows   # CI/CD pipelines
riley_ci/           # Integration stubs / import mapping
scripts/            # CLI runner (ingest_campaign.py)
tests/              # Pytest suite with synthetic fixtures
LICENSE             # Proprietary – All rights reserved
requirements.txt    # Python dependencies
Makefile            # Dev convenience commands

````

---

## 🧪 Development Workflow

### 1. Install Dependencies
```bash
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
````

### 2. Run Tests

```bash
pytest -q
```

### 3. Lint & Format

```bash
ruff check .
black .
```

### 4. Smoke-Run Ingest

```bash
python scripts/ingest_campaign.py --input tests/.tmp-sample --out .ci_out
```

---

## ⚙️ CI/CD

Pull requests and pushes to `main` automatically trigger the **ci.yml** workflow:

1. Dependency install
2. Lint + format check
3. Unit + integration tests (synthetic campaign fixture)

Nightly or manual runs of **llm-integration.yml** use real model APIs if secrets are configured.

---

## 🔒 Licensing

This project is **proprietary**.
Copyright © 2025 **Rally Communications**. All rights reserved.
Unauthorized use, reproduction, or distribution is prohibited.

---

## 🌟 Roadmap

* [ ] Wire to production `pipeline.*` modules (replace fallbacks)
* [ ] Extend taxonomy classification with fine-tuned models
* [ ] Build campaign-level dashboards for ingestion metrics
* [ ] Evolve into multi-agent **Riley** system that generates proposals, decks, and strategy outlines

---

## 👥 Maintainers

RALLY AI Department — 2025
**Lead Engineer:** Anova Youngers

