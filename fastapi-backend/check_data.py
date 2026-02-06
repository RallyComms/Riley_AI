from qdrant_client import QdrantClient

# Connect to the local database
client = QdrantClient(host="localhost", port=6333)
collection = "riley_production_v1"

print(f"📡 Connecting to {collection}...")

try:
    # Get the first record to see what the ID looks like
    res = client.scroll(
        collection_name=collection,
        limit=1,
        with_payload=True
    )
    
    if res[0]:
        payload = res[0][0].payload
        print("\n🔎 DATA FOUND:")
        
        # Check which ID key is being used
        if 'client_id' in payload:
            print(f"🔑 ID KEY: 'client_id'")
            print(f"🆔 VALUE:  {payload['client_id']}")
        elif 'tenant_id' in payload:
            print(f"🔑 ID KEY: 'tenant_id'")
            print(f"🆔 VALUE:  {payload['tenant_id']}")
        else:
            print("❌ No ID key found. Payload keys:", list(payload.keys()))
            
    else:
        print("❌ Collection is empty.")

except Exception as e:
    print(f"❌ Error: {e}")