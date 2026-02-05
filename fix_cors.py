from google.cloud import storage
import os

# --- CONFIGURATION ---
BUCKET_NAME = "riley-platform-assets-live" # This is the bucket name for the assets
CREDENTIALS_FILE = "gcp-key.json"          # This is the credentials file for the assets

def configure_cors():
    print(f"🔧 Configuring CORS for bucket: {BUCKET_NAME}...")
    
    # Authenticate
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_FILE
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)

    # Define the CORS policy
    # This tells Google: "Let ANY website (*) download files using GET requests."
    cors_configuration = [
        {
            "origin": ["*"],
            "responseHeader": [
                "Content-Type",
                "x-goog-resumable"
            ],
            "method": ["GET", "OPTIONS", "HEAD"],
            "maxAgeSeconds": 3600
        }
    ]

    # Apply and Save
    bucket.cors = cors_configuration
    bucket.patch()

    print("✅ CORS configured successfully!")
    print("   Origins allowed: *")
    print("   Methods allowed: GET, OPTIONS, HEAD")
    print("   Note: It may take up to 5 minutes for changes to propagate globally.")

if __name__ == "__main__":
    configure_cors()