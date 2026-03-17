# 🧠 Riley Platform | RALLY

**Riley** is an internal AI operating system designed for high-stakes campaign strategy, enabling teams to transform fragmented documents, research, and communications into structured, actionable intelligence.

It combines **document intelligence, semantic retrieval, and collaborative workflows** into a unified platform purpose-built for strategic teams.

Riley operates across two layers:

* **Global Intelligence Layer** — firm-wide knowledge, archival strategy, and institutional memory
* **Campaign Intelligence Layer** — isolated, secure workspaces for active campaign execution

---

## ⚙️ System Architecture

Riley is built as a modern, cloud-native AI system optimized for performance, isolation, and extensibility.

**Frontend**

* Next.js (App Router)
* Tailwind CSS + Framer Motion
* Real-time UX patterns with streaming and event-driven updates

**Backend**

* FastAPI (Python 3.11)
* Deployed on Google Cloud Run (autoscaling, stateless execution)

**Authentication**

* Clerk (JWT-based identity and session management)

**Data Layer**

* **Neo4j** — campaign graph, user relationships, access control, and event system
* **Qdrant** — vector search for semantic retrieval across campaign and global corpora
* **Google Cloud Storage** — asset vault for documents and generated outputs

---

## 🧠 Core System Capabilities

### 1. Campaign Workspaces (Secure Collaboration Layer)

* Campaigns function as **isolated intelligence environments**
* Membership and permissions enforced through graph relationships
* Shared asset layer prevents duplication while maintaining workspace integrity

---

### 2. Document Intelligence Pipeline

* Multi-format ingestion: PDF, DOCX, PPTX, XLSX, HTML, images
* Hybrid extraction:

  * native parsing
  * OCR for image-heavy documents
  * vision-assisted understanding
* Outputs structured intelligence artifacts used downstream in strategy and retrieval

---

### 3. Hybrid Retrieval (Graph + Vector)

Riley combines:

* **Graph retrieval**

  * relationships (campaign → documents → users → decisions)
* **Vector retrieval**

  * semantic similarity across documents

This enables:

* context-aware search
* cross-document synthesis
* strategic pattern detection

---

### 4. Multipass Intelligence Engine

Large documents are analyzed using a multi-stage pipeline:

* segmentation into structured bands
* per-band analysis
* cross-band contradiction detection
* full-document synthesis

This allows Riley to reason over **complex, multi-source campaign materials**, not just summarize them.

---

### 5. Adaptive Generation Layer

Riley uses a **multi-provider LLM architecture**:

* **Primary:** Gemini (fast, stable generation)
* **Fallback:** OpenAI (resilience layer)

Features:

* provider-level failover
* context-aware prompt construction
* structured vs conversational response control
* strict grounding in source documents

---

### 6. Event-Driven Collaboration System

Riley includes a unified event model powering:

* campaign activity feeds
* cross-campaign intelligence feed (“Riley Bot”)
* access requests and approvals
* document workflow (review / assignment)
* deadline tracking

All activity is:

* persisted
* scoped by campaign and user
* queryable in real time

---

### 7. User Identity & Presence Layer

* Riley-native user profiles (independent of auth provider)
* global status system:

  * Active
  * Away
  * In Meeting
* consistent identity across campaigns and collaboration surfaces

---

## 🧩 Product Features

* Campaign dashboards with real-time activity and team visibility
* AI-assisted strategic analysis across campaign assets
* Structured reporting (strategy memos, summaries, audience analysis)
* Cross-campaign intelligence feed with actionable notifications
* Access control and onboarding flows for secure collaboration
* Document tagging and workflow routing (e.g., Needs Review → In Review)

---

## 🚀 System Characteristics

* **Tenant-isolated architecture** for campaign security
* **Event-driven design** for real-time collaboration surfaces
* **Hybrid retrieval system** combining graph and vector reasoning
* **Provider-agnostic LLM layer** with failover resilience
* **Cloud-native scaling** via serverless infrastructure

---

## 🗺️ Engineering Direction

* Deeper real-time collaboration (event streaming / presence signals)
* Advanced intelligence synthesis across campaigns
* Mission Control layer for system-wide observability
* Adaptive retrieval + reranking optimization

---

## 👨‍💻 Built By

**RALLY AI Engineering**
Lead Engineer: **Anova Youngers**

