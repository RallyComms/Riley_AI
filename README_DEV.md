# Developer Notes

 **CRITICAL COST FLAG  EMBEDDINGS** 

This project currently uses **LLM classification + tagging** (gpt-4o-mini) for documents.  
That part is **cheap** (fractions of a cent per doc).

BUT: when we move to **chunk embeddings for RAG / agent search**, costs can explode if we embed millions of tokens with an API model.

## Action Required BEFORE Embedding
- **Estimate token counts** for the dataset (per campaign and overall).
- **Run a cost model**:
  - 	ext-embedding-3-small  .02 / 1M tokens
  - 	ext-embedding-3-large  .13 / 1M tokens
- **Evaluate alternatives**:
  - Open-source embeddings (e.g., BGE, Instructor, MiniLM) on local GPU (RTX 5080 available).
  - Hybrid setup: cheap open-source embeddings + LLM reranker for accuracy.

## Bottom Line
**Do not launch large-scale embeddings until costs are modeled and alternatives are reviewed.**
This step is CRITICAL to prevent runaway API bills.
