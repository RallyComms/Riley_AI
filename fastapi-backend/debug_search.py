import qdrant_client
from qdrant_client import QdrantClient

print(f"📦 Qdrant Client Version: {qdrant_client.__version__}")

try:
    client = QdrantClient(host="localhost", port=6333)
    print("✅ Client initialized.")
    
    # Check if 'search' exists in the toolbox
    if hasattr(client, 'search'):
        print("✅ Function 'search' FOUND.")
    else:
        print("❌ Function 'search' MISSING.")
        print("👀 Available attributes:", [m for m in dir(client) if not m.startswith('_')])

    # Try a fake search
    print("🧪 Attempting test search...")
    client.search(
        collection_name="riley_production_v1",
        query_vector=[0.1] * 768,
        limit=1
    )
    print("🎉 Test search SUCCESS.")

except Exception as e:
    print(f"💥 CRASH: {e}")