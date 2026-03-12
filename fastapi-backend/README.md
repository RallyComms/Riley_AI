## Riley Backend Notes

### Ingestion Queue Pilot Hardening (Cloud Tasks)

Current queue assumptions in code:
- Queue name is read from `INGESTION_TASKS_QUEUE`
- Queue location is read from `INGESTION_TASKS_LOCATION`
- Tasks are created by `app/services/ingestion.py` and rely on Cloud Tasks queue-level retry/backoff settings
- The app does **not** set per-task retry policy; operational throttling must be configured on the queue

Recommended pilot profile (burst-safe):
- max concurrent dispatches: `2`
- max dispatches per second: `1`
- high retry count with exponential backoff
- long max backoff (`3600s`)

Exact command:

```bash
gcloud tasks queues update "$INGESTION_TASKS_QUEUE" \
  --project="$GCP_PROJECT_ID" \
  --location="$INGESTION_TASKS_LOCATION" \
  --max-concurrent-dispatches=2 \
  --max-dispatches-per-second=1 \
  --max-attempts=100 \
  --min-backoff=10s \
  --max-backoff=3600s \
  --max-doublings=16
```

Verify queue settings:

```bash
gcloud tasks queues describe "$INGESTION_TASKS_QUEUE" \
  --project="$GCP_PROJECT_ID" \
  --location="$INGESTION_TASKS_LOCATION" \
  --format="yaml(name,rateLimits,retryConfig,state)"
```

Helper script:

```bash
GCP_PROJECT_ID=your-project \
INGESTION_TASKS_QUEUE=riley-ingestion-jobs \
INGESTION_TASKS_LOCATION=us-west1 \
bash scripts/configure_ingestion_queue_pilot.sh
```

Why this protects the system:
- Dispatch throttling prevents ingestion workers from stampeding Qdrant/embedding APIs during upload bursts.
- Low sustained QPS smooths spikes into a manageable queue.
- High retries with long backoff absorb transient upstream failures without dropping jobs.