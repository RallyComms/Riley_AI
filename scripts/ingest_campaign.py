import sys, subprocess as sp

if len(sys.argv) != 2:
    print("Usage: python scripts/ingest_campaign.py "data/raw/<Campaign Name>"")
    sys.exit(1)

camp = sys.argv[1]
steps = [
  ["python","-m","pipeline.crawl", camp],
  ["python","-m","pipeline.extract", camp],
  ["python","-m","pipeline.classify_llm", camp],
  ["python","-m","pipeline.privacy", camp],
  ["python","-m","pipeline.chunk", camp],
  ["python","-m","pipeline.catalog", camp],
]
for s in steps:
    print("\n>>", " ".join(s))
    sp.check_call(s)
print("\nDONE ✓")
