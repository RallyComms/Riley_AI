# Riley Manual Release Checklist (Critical Only)

Run after automated smoke tests. These checks are release-blocking.

## Access / Auth

- **Action:** Validate admin vs non-admin Mission Control visibility/access.
  - **Expected:** Admin allowed; non-admin denied.
  - **Why:** Prevents authz regressions in admin surfaces.

## Riley Core

- **Action:** Start a brand-new Global Riley chat; send first prompt.
  - **Expected:** First assistant message answers the prompt directly.
  - **Why:** Prevents first-turn regression where intro message consumes first interaction.

- **Action:** Send one fast chat and one deep chat prompt in campaign Riley.
  - **Expected:** Both succeed with real response (no 422/500/spinner hang).
  - **Why:** Primary user workflow reliability.

- **Action:** Create one Riley report and verify status progression.
  - **Expected:** Job moves to terminal state with usable output or clear failure.
  - **Why:** Report pipeline is release-critical async flow.

## Collaboration / Notifications

- **Action:** User A mentions User B in Team Chat; User B replies.
  - **Expected:** Reply shows correct User B identity (name/avatar fallback), not Unknown User.
  - **Why:** Identity resolution is critical for collaboration trust.

- **Action:** Trigger one access request, one team-chat mention, one document assignment/mention, and one deadline reminder.
  - **Expected:** Routing is correct:
    - access request -> Riley Bot/admin visibility
    - deadline reminder -> Riley Bot/workflow surfaces
    - team-chat mention -> Recent Activity (not Riley Bot)
    - document assignment/mention -> Recent Activity
  - **Why:** Prevents noisy feeds and missed operational events.

## Assets / Ingestion

- **Action:** Upload asset, toggle Riley Memory ON, keep page open.
  - **Expected:** Badge/status updates live (`uploaded -> queued -> processing -> terminal`) without manual refresh.
  - **Why:** Confirms live state sync and avoids false “stuck processing” UX.

## Mission Control

- **Action:** Open all Mission Control tabs and switch timeframe `24h/7d/30d`.
  - **Expected:** No backend 500/CORS-like errors; data loads consistently.
  - **Why:** Historically high-regression admin surface.

- **Action:** Open Adoption & Engagement tab.
  - **Expected:** Human adoption metrics/drilldowns exclude system actors.
  - **Why:** Prevents inflated adoption counts from system traffic.

