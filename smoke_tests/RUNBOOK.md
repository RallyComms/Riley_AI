# Riley Regression Harness Runbook

Use this runbook for every production deploy.

## Purpose

Provide one repeatable release-validation flow that combines:

- automated smoke tests
- required env/config
- test users and campaign setup
- short manual critical-path checks
- failure recording

---

## Prerequisites

### 1) Local setup

- Python 3.11+ available
- Repo checked out at the deployed commit
- Smoke harness files present:
  - `smoke_tests/run_smoke.py`
  - `smoke_tests/run.sh`
  - `smoke_tests/config.example.env`
  - `smoke_tests/README.md`

### 2) Smoke config

Create env file:

```bash
cp smoke_tests/config.example.env smoke_tests/.env
```

Required vars:

- `SMOKE_BASE_URL`
- `SMOKE_ADMIN_TOKEN`
- `SMOKE_TENANT_ID`

Optional vars (recommended):

- `SMOKE_USER_TOKEN`
- `SMOKE_NON_ADMIN_TOKEN`
- `SMOKE_REQUESTER_TOKEN`
- `SMOKE_LEAD_TOKEN`
- `SMOKE_ASSET_FILE_ID`
- `SMOKE_ENABLE_MUTATION` (`true` only in controlled environments)

### 3) Test identities/data

- Admin user token for Mission Control checks
- Standard member token for campaign/chat/assets flows
- Non-admin token for authz negative check (optional)
- Requester + lead tokens for access-request lifecycle (optional)
- One known test campaign (`SMOKE_TENANT_ID`)
- One known test file id (`SMOKE_ASSET_FILE_ID`) if running AI toggle mutation

---

## Final Run Sequence

## 1) Deploy

- Deploy backend/frontend normally.
- Confirm deployment target URL is correct and set in `SMOKE_BASE_URL`.

## 2) Run Automated Smoke Tests

Run:

```bash
bash smoke_tests/run.sh
```

This also writes:

- `smoke_tests/last_smoke_report.json`

Interpretation:

- `PASS` = scenario contract validated
- `FAIL` = blocking regression candidate
- `SKIP` = scenario not configured (usually missing optional env vars)

Gate:

- If any `FAIL`, treat release as **failed validation** until triaged.

v1.1 blocking API checks now include:

- Mission Control adoption excludes system actors in user drilldowns (`24h/7d/30d`)

## 3) Run Manual Release Checklist

Use the short manual checklist from prior step (access/auth, Riley core, collaboration/notifications, assets/ingestion, Mission Control).

Minimum manual actions:

- Admin/non-admin access sanity check
- Fast + deep chat sanity check
- One report job lifecycle sanity check
- Asset upload + Riley Memory toggle sanity check
- Mission Control all-tab/timeframe sanity check
- Workflow Health table/date rendering sanity check

v1.1 additional **blocking** manual checks:

- Global Riley first-turn contract:
  - new Global Riley conversation must answer the first prompt directly (no canned intro replacing response)
- Asset status live-update contract:
  - upload + toggle ON should live-update status badges without full page reload
- Team Chat identity resolution:
  - mention/reply must resolve to correct teammate identity (no Unknown User author)
- Notification routing contract:
  - access request/deadline -> Riley Bot/workflow surfaces
  - team-chat mention/document assignment -> Recent Activity (not noisy Riley Bot)

## 4) Record Failures

For each failed automated or manual check, capture:

- Scenario name
- Environment and timestamp
- Endpoint/page + request payload (if API)
- Actual vs expected result
- Error snippet (status code / traceback / screenshot)
- Suspected owner (`backend`, `frontend`, `infra`, `data`)

Suggested format:

```text
[REGRESSION] <scenario-name>
time: <UTC timestamp>
env: production
commit: <sha>
expected: <one line>
actual: <one line>
evidence: <error/log/screenshot link>
owner: <team or person>
status: open
```

For release ledger tracking, use:

- `smoke_tests/EXECUTION_LEDGER_TEMPLATE.md`

---

## Repeatability Notes

- Keep `smoke_tests/.env` as local-only test config (do not commit secrets).
- Reuse the same test campaign/users each release to reduce noise.
- Only enable mutation scenarios when test data is safe to modify.
- If smoke output changes, update this runbook and `smoke_tests/README.md` together.
