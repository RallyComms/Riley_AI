# 🧠 Riley Platform | RALLY

**Riley** is RALLY’s internal AI Operating System. It is a secure, collaborative intelligence platform designed to streamline campaign strategy through two distinct modes:

1.  **Global Riley:** Access to firm-wide knowledge and "Golden Standard" archival documents.
2.  **Campaign Riley:** Tenant-isolated workspaces where teams upload assets, view documents, and collaborate within a secure silo.

---

## 🏗️ Architecture Stack

*   **Frontend:** Next.js 14 (App Router) + Tailwind + Framer Motion.
*   **Backend:** FastAPI (Python 3.11) running on **Google Cloud Run**.
*   **Auth:** Clerk (JWT Verification).
*   **Graph DB:** Neo4j (Campaign Registry + Chat Memory).
*   **Vector DB:** Qdrant Cloud (Semantic Search).
*   **Storage:** Google Cloud Storage (Asset Vault).

---

## ⚡ Core Capabilities

### 1. Campaign Workspaces (The Silo)
*   **Source of Truth:** Campaigns and Memberships are managed in **Neo4j**.
*   **Isolation:** Strict RLS (Row Level Security) enforcement.
    *   Campaign Riley sees **Tier 2** (Client Assets) + **Tier 1** (Global).
    *   Global Riley sees **Tier 1** ONLY.

### 2. Document Intelligence
*   **Ingestion:** Supports PDF, DOCX, PPTX, XLSX, HTML, and Images.
*   **Previews:** Automatic PDF preview generation for Office files using headless LibreOffice.
*   **OCR:** On-demand text extraction for image-heavy assets.

### 3. Persistent Memory
*   **Chat History:** Tenant-scoped session history stored in Neo4j.
*   **Context:** Hybrid RAG approach combining **Graph relationships** (Who is the client?) with **Vector search** (What is in the docs?).

---

## 🛠️ Configuration & Environment

### Frontend Variables
Create `.env.local` in `next-frontend/`:

```bash
# Production API URL
NEXT_PUBLIC_API_URL=https://riley-api-786327046070.us-east4.run.app

# Clerk Auth
NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY=pk_test_...
CLERK_SECRET_KEY=sk_test_...
```

### Backend Variables
Set these in **Google Cloud Run** (Variables & Secrets):

```bash
# Infrastructure
GOOGLE_API_KEY=...             # Gemini Models
QDRANT_URL=...                 # Vector DB Endpoint
QDRANT_API_KEY=...             # Vector DB Key
GCS_BUCKET_NAME=...            # Asset Storage Bucket

# Vector Collections
QDRANT_COLLECTION_TIER_1=riley_production_v1  # Firm Archive
QDRANT_COLLECTION_TIER_2=riley_campaigns      # Campaign Assets

# Security
ALLOWED_ORIGINS=https://riley-platform.vercel.app,http://localhost:3000
```

---

## 🚀 Running Locally

### 1. Start the Backend (Brain)
```bash
cd fastapi-backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Run server with hot-reload
uvicorn app.main:app --reload --port 8000
```

### 2. Start the Frontend (Body)
```bash
cd next-frontend
npm install
npm run dev
```
*Access the platform at `http://localhost:3000`*

---

## 📦 Deployment Strategy

### Backend (Google Cloud Run)
*   **Container:** Dockerfile builds Python env + LibreOffice (for previews).
*   **Scaling:** Set `min-instances=1` during high-traffic windows to prevent cold starts.

### Frontend (Vercel)
*   Deploys from `next-frontend/`.
*   Must have `NEXT_PUBLIC_API_URL` pointing to the Cloud Run service.

---

## 🗺️ Engineering Roadmap

*   [ ] **Async Ingestion:** Move heavy OCR/Embedding tasks to Cloud Tasks to prevent HTTP timeouts.
*   [ ] **Real-Time Team Chat:** Move from polling to WebSockets or robust Neo4j persistence.
*   [ ] **Mission Control:** Director-level dashboard for Token Usage, Latency, and Adoption metrics.

---

## 👨‍💻 Maintainers

**RALLY AI Engineering**
*   **Lead Engineer:** Anova Youngers
