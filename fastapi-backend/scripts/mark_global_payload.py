import os, asyncio
from qdrant_client import AsyncQdrantClient
from qdrant_client.http import models

COLLECTION = os.getenv("COLLECTION", "riley_campaigns_768")
BATCH = int(os.getenv("BATCH", "512"))

async def main():
    qurl = os.popen("gcloud secrets versions access latest --secret=QDRANT_URL --project riley-ai-479422").read().strip()
    qkey = os.popen("gcloud secrets versions access latest --secret=QDRANT_API_KEY --project riley-ai-479422").read().strip()
    client = AsyncQdrantClient(url=qurl, api_key=qkey, timeout=60)

    offset = None
    updated = 0
    while True:
        points, next_offset = await client.scroll(
            collection_name=COLLECTION,
            scroll_filter=None,
            limit=BATCH,
            offset=offset,
            with_payload=False,
            with_vectors=False,
        )
        if not points:
            break

        ids = [p.id for p in points]
        await client.set_payload(
            collection_name=COLLECTION,
            payload={"is_global": True},
            points=ids,
        )
        updated += len(ids)
        print("updated", updated)

        if next_offset is None:
            break
        offset = next_offset

    print("DONE updated", updated)

if __name__ == "__main__":
    asyncio.run(main())
