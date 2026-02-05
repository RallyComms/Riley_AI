import psycopg2
import google.generativeai as genai
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct
from tqdm import tqdm
import os
import json
import time

# --- CONFIGURATION ---
DB_NAME = "riley_archive"
DB_USER = "riley_admin"
DB_PASS = "riley_password" # Ensure this matches the docker setup
DB_HOST = "localhost"

QDRANT_HOST = "localhost"
QDRANT_PORT = 6333
COLLECTION_NAME = "riley_production_v1"

# API KEY (Ensure this is set in your environment or paste here for temporary run)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
genai.configure(api_key=GOOGLE_API_KEY)

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST
    )

def main():
    print("--- STARTING LAZARUS PROTOCOL: SYNC POSTGRES TO QDRANT ---")
    
    # 1. Connect to Services
    try:
        pg_conn = get_db_connection()
        pg_cursor = pg_conn.cursor()
        client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        print("[*] Databases Connected.")
    except Exception as e:
        print(f"[!] Connection Failed: {e}")
        return

    # 2. Check how many we have to do
    pg_cursor.execute("SELECT COUNT(*) FROM archive_docs WHERE content IS NOT NULL AND content != '';")
    total_docs = pg_cursor.fetchone()[0]
    print(f"[*] Found {total_docs} valid documents in Archive to sync.")

    # 3. Fetch Data in Batches
    BATCH_SIZE = 50
    pg_cursor.execute("SELECT id, filename, content, metadata, gcs_path FROM archive_docs WHERE content IS NOT NULL AND content != '';")

    pbar = tqdm(total=total_docs, desc="Hydrating Vectors")
    
    while True:
        rows = pg_cursor.fetchmany(BATCH_SIZE)
        if not rows:
            break

        points = []
        for row in rows:
            doc_id, filename, content, metadata, gcs_path = row
            
            # Skip if content is too short to be useful
            if len(content) < 50:
                continue

            try:
                # 4. Generate Embedding
                # We use the text-embedding-004 model
                embedding_result = genai.embed_content(
                    model="models/text-embedding-004",
                    content=content[:9000], # Truncate to avoid token limits
                    task_type="retrieval_document"
                )
                vector = embedding_result['embedding']

                # 5. Prepare Payload
                # We normalize metadata to ensure it's a flat dict for Qdrant
                payload = {
                    "filename": filename,
                    "content": content[:1000], # Store preview text
                    "gcs_path": gcs_path,
                    "client_id": "demo_client_001", # TEMPORARY: Assigning all to a demo client for now
                    "type": "document"
                }
                # Merge existing metadata if it exists
                if metadata:
                    if isinstance(metadata, str):
                        payload.update(json.loads(metadata))
                    else:
                        payload.update(metadata)

                points.append(PointStruct(id=str(doc_id), vector=vector, payload=payload))

            except Exception as e:
                # Rate limit handling or API error
                if "429" in str(e):
                    time.sleep(5)
                continue

        # 6. Upload Batch to Qdrant
        if points:
            try:
                client.upsert(
                    collection_name=COLLECTION_NAME,
                    points=points
                )
            except Exception as e:
                print(f" [!] Insert Error: {e}")

        pbar.update(len(rows))

    print("\n[*] Lazarus Protocol Complete. Qdrant is now in sync with Archive.")
    pg_conn.close()

if __name__ == "__main__":
    main()