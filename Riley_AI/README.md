# Riley_AI

Proprietary repository for Riley AI ingestion/classification pipeline scaffolding.

## Quick start

```bash
python -m pip install -U pip
pip install -r requirements.txt

# run tests locally
pytest -q

# run smoke runner manually
python scripts/ingest_campaign.py --input ./tests/.tmp-sample --out .ci_out
```

## CI
- GitHub Actions workflow in `.github/workflows/ci.yml`
- Strict mode can be enabled via `RILEY_CI_STRICT=1` once real modules are wired.
