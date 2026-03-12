#!/usr/bin/env bash
set -euo pipefail

# Pilot-safe Cloud Tasks throttling profile for Riley ingestion.
# Usage:
#   GCP_PROJECT_ID=... INGESTION_TASKS_QUEUE=... INGESTION_TASKS_LOCATION=... \
#   bash scripts/configure_ingestion_queue_pilot.sh

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is required}"
: "${INGESTION_TASKS_QUEUE:=riley-ingestion-jobs}"
: "${INGESTION_TASKS_LOCATION:=us-west1}"

echo "Applying pilot queue profile:"
echo "  project:  ${GCP_PROJECT_ID}"
echo "  queue:    ${INGESTION_TASKS_QUEUE}"
echo "  location: ${INGESTION_TASKS_LOCATION}"

gcloud tasks queues update "${INGESTION_TASKS_QUEUE}" \
  --project="${GCP_PROJECT_ID}" \
  --location="${INGESTION_TASKS_LOCATION}" \
  --max-concurrent-dispatches=2 \
  --max-dispatches-per-second=1 \
  --max-attempts=100 \
  --min-backoff=10s \
  --max-backoff=3600s \
  --max-doublings=16

echo ""
echo "Queue updated. Current settings:"
gcloud tasks queues describe "${INGESTION_TASKS_QUEUE}" \
  --project="${GCP_PROJECT_ID}" \
  --location="${INGESTION_TASKS_LOCATION}" \
  --format="yaml(name,rateLimits,retryConfig,state)"
