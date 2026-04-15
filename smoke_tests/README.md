# Riley Smoke Tests

Minimal, release-critical smoke harness for post-deploy validation.

## What this covers

Focused API-level checks for highest-risk flows:

- Auth/access control
- Campaign membership access paths
- Notifications feed
- Assets listing + optional Riley Memory toggle roundtrip
- Riley chat (fast + deep)
- Reports list contract
- Mission Control endpoints across `24h`, `7d`, `30d`
- Workflow Health datetime serialization contract
- Mission Control adoption drilldown excludes system actors

Manual/UI blocking checks (kept outside API harness):

- Global Riley first-turn behavior (no canned intro replacing first prompt response)
- Asset status badges live-update without page refresh
- Team Chat mention/reply identity resolution (no Unknown User)
- Notification routing to correct feed surfaces

## Setup

1. Copy config template:

```bash
cp smoke_tests/config.example.env smoke_tests/.env
```

2. Fill required values in `smoke_tests/.env`:

- `SMOKE_BASE_URL`
- `SMOKE_ADMIN_TOKEN`
- `SMOKE_TENANT_ID`

Optional values enable extra checks:

- `SMOKE_NON_ADMIN_TOKEN` (non-admin authz test)
- `SMOKE_USER_TOKEN` (separate normal member token)
- `SMOKE_REQUESTER_TOKEN`, `SMOKE_LEAD_TOKEN` (access-request lifecycle mutation)
- `SMOKE_ASSET_FILE_ID` (AI toggle mutation)
- `SMOKE_ENABLE_MUTATION=true` (required for mutation scenarios)

## Run

```bash
bash smoke_tests/run.sh
```

Or with explicit config path:

```bash
bash smoke_tests/run.sh /path/to/.env
```

Runner also writes a JSON execution artifact by default:

- `smoke_tests/last_smoke_report.json`

## Output

Each scenario prints one line:

- `[PASS]` contract validated
- `[FAIL]` blocking regression
- `[SKIP]` optional test not configured

The process exits non-zero if any scenario fails.

## Seeded test data expectations

For full coverage (including optional mutation tests), prepare:

- one admin token for Mission Control checks
- one campaign tenant id where token has membership
- optional non-admin token
- optional requester + lead tokens for access-request lifecycle
- optional known asset `file_id` for toggle roundtrip

## Release-critical v1.1 addendum mapping

- **Automated (blocking):**
  - Mission Control adoption excludes system actors
- **Manual/UI (blocking):**
  - Global Riley first-turn contract
  - Asset status live-update contract
  - Team Chat identity resolution contract
  - Notification routing contract

