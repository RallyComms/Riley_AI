#!/usr/bin/env python3
"""Lightweight release smoke harness for Riley.

Focused on release-critical, high-risk workflows only.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

HARNESS_VERSION = "v1.1"


@dataclass
class SmokeConfig:
    base_url: str
    admin_token: str
    tenant_id: str
    user_token: Optional[str]
    non_admin_token: Optional[str]
    requester_token: Optional[str]
    lead_token: Optional[str]
    asset_file_id: Optional[str]
    chat_query: str
    timeout_seconds: float
    enable_mutation: bool


@dataclass
class ScenarioResult:
    name: str
    category: str
    status: str  # PASS / FAIL / SKIP
    detail: str
    duration_ms: int


def load_env_file(path: str) -> None:
    if not path or not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
                value = value[1:-1]
            os.environ.setdefault(key, value)


def get_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def make_config() -> SmokeConfig:
    base_url = (os.getenv("SMOKE_BASE_URL") or "").strip().rstrip("/")
    admin_token = (os.getenv("SMOKE_ADMIN_TOKEN") or "").strip()
    tenant_id = (os.getenv("SMOKE_TENANT_ID") or "").strip()
    user_token = (os.getenv("SMOKE_USER_TOKEN") or "").strip() or None
    non_admin_token = (os.getenv("SMOKE_NON_ADMIN_TOKEN") or "").strip() or None
    requester_token = (os.getenv("SMOKE_REQUESTER_TOKEN") or "").strip() or None
    lead_token = (os.getenv("SMOKE_LEAD_TOKEN") or "").strip() or None
    asset_file_id = (os.getenv("SMOKE_ASSET_FILE_ID") or "").strip() or None
    chat_query = (os.getenv("SMOKE_CHAT_QUERY") or "Smoke test: confirm Riley is responsive.").strip()
    timeout_seconds = float(os.getenv("SMOKE_TIMEOUT_SECONDS", "45").strip())
    enable_mutation = get_bool("SMOKE_ENABLE_MUTATION", default=False)

    missing = []
    if not base_url:
        missing.append("SMOKE_BASE_URL")
    if not admin_token:
        missing.append("SMOKE_ADMIN_TOKEN")
    if not tenant_id:
        missing.append("SMOKE_TENANT_ID")
    if missing:
        raise ValueError(f"Missing required env vars: {', '.join(missing)}")

    return SmokeConfig(
        base_url=base_url,
        admin_token=admin_token,
        tenant_id=tenant_id,
        user_token=user_token,
        non_admin_token=non_admin_token,
        requester_token=requester_token,
        lead_token=lead_token,
        asset_file_id=asset_file_id,
        chat_query=chat_query,
        timeout_seconds=timeout_seconds,
        enable_mutation=enable_mutation,
    )


def _join_url(base_url: str, path: str, params: Optional[Dict[str, Any]]) -> str:
    url = path if path.startswith("http") else f"{base_url}{path}"
    if params:
        qp = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        if qp:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{qp}"
    return url


def http_json(
    cfg: SmokeConfig,
    method: str,
    path: str,
    *,
    token: Optional[str] = None,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Dict[str, Any], str]:
    url = _join_url(cfg.base_url, path, params)
    payload: Optional[bytes] = None
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, method=method.upper(), data=payload, headers=headers)

    try:
        with urllib.request.urlopen(req, timeout=cfg.timeout_seconds) as resp:
            text = resp.read().decode("utf-8", errors="replace")
            data = json.loads(text) if text else {}
            if not isinstance(data, dict):
                data = {"_raw": data}
            return resp.status, data, text
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        try:
            data = json.loads(text) if text else {}
            if not isinstance(data, dict):
                data = {"_raw": data}
        except Exception:
            data = {"_raw_text": text}
        return exc.code, data, text


def expect(status: int, allowed: List[int], msg: str) -> None:
    if status not in allowed:
        raise AssertionError(f"{msg}; got HTTP {status}")


def scenario_auth_valid_admin_access(cfg: SmokeConfig) -> str:
    status, data, _ = http_json(cfg, "GET", "/api/v1/mission-control/access", token=cfg.admin_token)
    expect(status, [200], "Mission Control access check failed for admin token")
    if data.get("allowed") is not True:
        raise AssertionError(f"Expected allowed=true for admin, got payload={data}")
    return "admin token has mission control access"


def scenario_auth_invalid_token_blocked(cfg: SmokeConfig) -> str:
    status, _, _ = http_json(cfg, "GET", "/api/v1/mission-control/access", token=None)
    expect(status, [401], "Unauthenticated access should be blocked")
    return "unauthenticated request correctly blocked"


def scenario_auth_non_admin_blocked(cfg: SmokeConfig) -> str:
    if not cfg.non_admin_token:
        raise RuntimeError("SKIP: set SMOKE_NON_ADMIN_TOKEN to validate non-admin enforcement")
    status, data, _ = http_json(cfg, "GET", "/api/v1/mission-control/access", token=cfg.non_admin_token)
    if status == 200:
        # Support either contract: {allowed:false} or an authz denial code.
        if data.get("allowed") is not False:
            raise AssertionError(f"Expected allowed=false for non-admin; got payload={data}")
    else:
        expect(status, [401, 403], "Non-admin token should not have Mission Control access")
    return "non-admin token is blocked from mission control"


def scenario_campaign_events_access(cfg: SmokeConfig) -> str:
    token = cfg.user_token or cfg.admin_token
    status, data, _ = http_json(
        cfg,
        "GET",
        f"/api/v1/campaigns/{cfg.tenant_id}/events",
        token=token,
        params={"limit": 5},
    )
    expect(status, [200], "Campaign events fetch failed for campaign member token")
    if "events" not in data or not isinstance(data["events"], list):
        raise AssertionError(f"Expected events list; got payload={data}")
    return f"campaign events reachable (events={len(data['events'])})"


def scenario_campaign_feed_notifications(cfg: SmokeConfig) -> str:
    token = cfg.user_token or cfg.admin_token
    status, data, _ = http_json(
        cfg,
        "GET",
        "/api/v1/campaigns/events/feed",
        token=token,
        params={"limit": 10},
    )
    expect(status, [200], "Campaign notifications feed fetch failed")
    if "events" not in data or not isinstance(data["events"], list):
        raise AssertionError(f"Expected feed events list; got payload={data}")
    return f"feed reachable (events={len(data['events'])})"


def scenario_assets_messaging_list(cfg: SmokeConfig) -> str:
    token = cfg.user_token or cfg.admin_token
    status, data, _ = http_json(
        cfg,
        "GET",
        "/api/v1/files/messaging/list",
        token=token,
        params={"tenant_id": cfg.tenant_id},
    )
    expect(status, [200], "Messaging files list failed")
    files = data.get("files")
    if not isinstance(files, list):
        raise AssertionError(f"Expected files list; got payload={data}")
    return f"messaging assets listed (files={len(files)})"


def scenario_assets_ai_toggle_roundtrip(cfg: SmokeConfig) -> str:
    if not cfg.enable_mutation:
        raise RuntimeError("SKIP: set SMOKE_ENABLE_MUTATION=true to run mutation tests")
    if not cfg.asset_file_id:
        raise RuntimeError("SKIP: set SMOKE_ASSET_FILE_ID for AI toggle roundtrip")
    token = cfg.user_token or cfg.admin_token

    # Read current value from messaging list payload.
    status, data, _ = http_json(
        cfg,
        "GET",
        "/api/v1/files/messaging/list",
        token=token,
        params={"tenant_id": cfg.tenant_id},
    )
    expect(status, [200], "Failed to fetch files before AI toggle test")
    files = data.get("files") if isinstance(data.get("files"), list) else []
    match = next((f for f in files if str(f.get("id")) == cfg.asset_file_id), None)
    if not match:
        raise AssertionError(f"Asset file_id={cfg.asset_file_id} not found in messaging list")
    original = bool(match.get("ai_enabled", False))
    flipped = not original

    status1, _, _ = http_json(
        cfg,
        "PATCH",
        f"/api/v1/files/{cfg.asset_file_id}/ai_enabled",
        token=token,
        params={"tenant_id": cfg.tenant_id},
        body={"ai_enabled": flipped},
    )
    expect(status1, [200], "First AI toggle request failed")

    status2, _, _ = http_json(
        cfg,
        "PATCH",
        f"/api/v1/files/{cfg.asset_file_id}/ai_enabled",
        token=token,
        params={"tenant_id": cfg.tenant_id},
        body={"ai_enabled": original},
    )
    expect(status2, [200], "Restore AI toggle request failed")
    return f"ai_enabled toggled and restored for file {cfg.asset_file_id}"


def scenario_chat_fast(cfg: SmokeConfig) -> str:
    token = cfg.user_token or cfg.admin_token
    status, data, _ = http_json(
        cfg,
        "POST",
        "/api/v1/chat",
        token=token,
        body={"query": cfg.chat_query, "tenant_id": cfg.tenant_id, "mode": "fast"},
    )
    expect(status, [200], "Fast chat request failed")
    if not isinstance(data.get("response"), str) or not data.get("response", "").strip():
        raise AssertionError(f"Chat response missing/empty; payload={data}")
    return f"fast chat ok (model={data.get('model_used', 'unknown')})"


def scenario_chat_deep(cfg: SmokeConfig) -> str:
    token = cfg.user_token or cfg.admin_token
    status, data, _ = http_json(
        cfg,
        "POST",
        "/api/v1/chat",
        token=token,
        body={"query": cfg.chat_query, "tenant_id": cfg.tenant_id, "mode": "deep"},
    )
    expect(status, [200], "Deep chat request failed")
    if not isinstance(data.get("response"), str) or not data.get("response", "").strip():
        raise AssertionError(f"Deep chat response missing/empty; payload={data}")
    return f"deep chat ok (model={data.get('model_used', 'unknown')})"


def scenario_reports_list(cfg: SmokeConfig) -> str:
    token = cfg.user_token or cfg.admin_token
    status, data, _ = http_json(
        cfg,
        "GET",
        "/api/v1/riley/reports",
        token=token,
        params={"tenant_id": cfg.tenant_id, "limit": 5},
    )
    expect(status, [200], "Report listing failed")
    jobs = data.get("jobs")
    if not isinstance(jobs, list):
        raise AssertionError(f"Expected jobs list; got payload={data}")
    return f"reports list reachable (jobs={len(jobs)})"


def scenario_mission_control_all_tabs(cfg: SmokeConfig) -> str:
    endpoints = [
        "/api/v1/mission-control/overview",
        "/api/v1/mission-control/riley-performance",
        "/api/v1/mission-control/cost-summary",
        "/api/v1/mission-control/adoption-summary",
        "/api/v1/mission-control/workflow-health-summary",
        "/api/v1/mission-control/system-health-summary",
    ]
    timeframe_values = ["24h", "7d", "30d"]
    checked = 0
    for endpoint in endpoints:
        for tf in timeframe_values:
            status, data, _ = http_json(
                cfg,
                "GET",
                endpoint,
                token=cfg.admin_token,
                params={"timeframe": tf},
            )
            expect(status, [200], f"Mission Control endpoint failed ({endpoint}?timeframe={tf})")
            if data.get("timeframe") != tf:
                raise AssertionError(
                    f"Unexpected timeframe echo for {endpoint}?timeframe={tf}; payload.timeframe={data.get('timeframe')}"
                )
            checked += 1
    return f"mission control tab/timeframe contract ok ({checked} requests)"


def scenario_adoption_excludes_system_events(cfg: SmokeConfig) -> str:
    timeframe_values = ["24h", "7d", "30d"]
    inspected_rows = 0
    for tf in timeframe_values:
        status, data, _ = http_json(
            cfg,
            "GET",
            "/api/v1/mission-control/adoption-summary",
            token=cfg.admin_token,
            params={"timeframe": tf},
        )
        expect(status, [200], f"Adoption summary failed ({tf})")

        rows = data.get("user_activity_30d")
        if not isinstance(rows, list):
            raise AssertionError(f"user_activity_30d must be a list for timeframe={tf}; payload={data}")

        for row in rows:
            inspected_rows += 1
            user_id = str(row.get("user_id") or "").strip().lower()
            user_label = str(row.get("user_label") or "").strip().lower()
            if user_id in {"system:deadline-reminder", "unknown_user"}:
                raise AssertionError(
                    f"System/unknown actor leaked into adoption user drilldown (timeframe={tf}, user_id={user_id})"
                )
            if user_label.startswith("system:"):
                raise AssertionError(
                    f"System actor leaked into adoption user labels (timeframe={tf}, user_label={user_label})"
                )
    return f"adoption drilldowns exclude system actors across 24h/7d/30d (rows={inspected_rows})"


def scenario_workflow_datetime_serialization(cfg: SmokeConfig) -> str:
    status, data, _ = http_json(
        cfg,
        "GET",
        "/api/v1/mission-control/workflow-health-summary",
        token=cfg.admin_token,
        params={"timeframe": "30d"},
    )
    expect(status, [200], "Workflow health summary failed")

    pending = data.get("pending_access_request_list") or []
    overdue = data.get("overdue_deadline_list") or []
    stale = data.get("stale_assignment_list") or []

    if not isinstance(pending, list) or not isinstance(overdue, list) or not isinstance(stale, list):
        raise AssertionError("Workflow summary list fields are not lists")

    for row in pending:
        value = row.get("created_at")
        if value is not None and not isinstance(value, str):
            raise AssertionError(f"pending_access_request_list.created_at must be string or null; got {type(value)}")
    for row in overdue:
        value = row.get("due_at")
        if value is not None and not isinstance(value, str):
            raise AssertionError(f"overdue_deadline_list.due_at must be string or null; got {type(value)}")
    for row in stale:
        value = row.get("due_at")
        if value is not None and not isinstance(value, str):
            raise AssertionError(f"stale_assignment_list.due_at must be string or null; got {type(value)}")

    return (
        "workflow datetime fields are JSON-safe "
        f"(pending={len(pending)}, overdue={len(overdue)}, stale={len(stale)})"
    )


def scenario_access_request_lifecycle(cfg: SmokeConfig) -> str:
    if not cfg.enable_mutation:
        raise RuntimeError("SKIP: set SMOKE_ENABLE_MUTATION=true to run mutation tests")
    if not cfg.requester_token or not cfg.lead_token:
        raise RuntimeError("SKIP: set SMOKE_REQUESTER_TOKEN and SMOKE_LEAD_TOKEN for access request lifecycle")

    create_status, create_data, _ = http_json(
        cfg,
        "POST",
        f"/api/v1/campaigns/{cfg.tenant_id}/access-requests",
        token=cfg.requester_token,
        body={"message": "smoke-harness access request"},
    )
    expect(create_status, [200], "Access request creation failed")
    request_id = str(create_data.get("request_id") or "").strip()
    if not request_id:
        raise AssertionError(f"Missing request_id in create response payload={create_data}")

    list_status, list_data, _ = http_json(
        cfg,
        "GET",
        f"/api/v1/campaigns/{cfg.tenant_id}/access-requests",
        token=cfg.lead_token,
        params={"status": "pending"},
    )
    expect(list_status, [200], "Access request list failed")
    rows = list_data.get("requests")
    if not isinstance(rows, list):
        raise AssertionError(f"Expected requests list; got payload={list_data}")
    if not any(str(item.get("id") or "") == request_id for item in rows):
        raise AssertionError(f"Created request_id={request_id} not found in pending list")

    # Deny to avoid accidentally granting access through smoke.
    decision_status, decision_data, _ = http_json(
        cfg,
        "POST",
        f"/api/v1/campaigns/{cfg.tenant_id}/access-requests/{request_id}/decision",
        token=cfg.lead_token,
        body={"action": "deny"},
    )
    expect(decision_status, [200], "Access request decision failed")
    decided_status = str(decision_data.get("status") or "").strip().lower()
    if decided_status != "denied":
        raise AssertionError(f"Expected denied status after decision; payload={decision_data}")
    return f"access-request lifecycle ok (request_id={request_id})"


def run_scenario(category: str, name: str, fn: Callable[[SmokeConfig], str], cfg: SmokeConfig) -> ScenarioResult:
    start = time.time()
    try:
        detail = fn(cfg)
        return ScenarioResult(
            name=name,
            category=category,
            status="PASS",
            detail=detail,
            duration_ms=int((time.time() - start) * 1000),
        )
    except RuntimeError as exc:
        text = str(exc)
        if text.startswith("SKIP:"):
            return ScenarioResult(
                name=name,
                category=category,
                status="SKIP",
                detail=text.replace("SKIP:", "", 1).strip(),
                duration_ms=int((time.time() - start) * 1000),
            )
        return ScenarioResult(
            name=name,
            category=category,
            status="FAIL",
            detail=text,
            duration_ms=int((time.time() - start) * 1000),
        )
    except AssertionError as exc:
        return ScenarioResult(
            name=name,
            category=category,
            status="FAIL",
            detail=str(exc),
            duration_ms=int((time.time() - start) * 1000),
        )
    except Exception as exc:
        return ScenarioResult(
            name=name,
            category=category,
            status="FAIL",
            detail=f"unexpected error: {type(exc).__name__}: {exc}",
            duration_ms=int((time.time() - start) * 1000),
        )


def print_result(result: ScenarioResult) -> None:
    print(f"[{result.status}] {result.category} :: {result.name} ({result.duration_ms}ms)")
    if result.detail:
        print(f"       {result.detail}")


def write_json_report(
    report_path: str,
    cfg: SmokeConfig,
    results: List[ScenarioResult],
    passed: int,
    failed: int,
    skipped: int,
) -> None:
    if not report_path:
        return
    payload = {
        "harness_version": HARNESS_VERSION,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "base_url": cfg.base_url,
        "tenant_id": cfg.tenant_id,
        "summary": {
            "pass": passed,
            "fail": failed,
            "skip": skipped,
            "total": len(results),
        },
        "results": [
            {
                "category": r.category,
                "name": r.name,
                "status": r.status,
                "detail": r.detail,
                "duration_ms": r.duration_ms,
            }
            for r in results
        ],
    }
    report_dir = os.path.dirname(report_path)
    if report_dir:
        os.makedirs(report_dir, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def main() -> int:
    parser = argparse.ArgumentParser(description="Riley release-critical smoke harness")
    parser.add_argument(
        "--config",
        default="smoke_tests/.env",
        help="Path to env-style config file (default: smoke_tests/.env)",
    )
    parser.add_argument(
        "--report-file",
        default="smoke_tests/last_smoke_report.json",
        help="Write JSON report to this path (default: smoke_tests/last_smoke_report.json)",
    )
    args = parser.parse_args()

    load_env_file(args.config)
    try:
        cfg = make_config()
    except ValueError as exc:
        print(f"[FAIL] Config :: {exc}")
        return 2

    scenarios: List[Tuple[str, str, Callable[[SmokeConfig], str]]] = [
        ("Auth / Access", "admin mission-control access", scenario_auth_valid_admin_access),
        ("Auth / Access", "unauthenticated blocked", scenario_auth_invalid_token_blocked),
        ("Auth / Access", "non-admin blocked", scenario_auth_non_admin_blocked),
        ("Campaign / Membership", "campaign events for member", scenario_campaign_events_access),
        ("Campaign / Membership", "access request lifecycle (optional mutation)", scenario_access_request_lifecycle),
        ("Notifications", "campaign feed notifications", scenario_campaign_feed_notifications),
        ("Assets / Riley Memory", "messaging asset list", scenario_assets_messaging_list),
        ("Assets / Riley Memory", "ai_enabled toggle roundtrip (optional mutation)", scenario_assets_ai_toggle_roundtrip),
        ("Chat / Reports", "chat fast mode", scenario_chat_fast),
        ("Chat / Reports", "chat deep mode", scenario_chat_deep),
        ("Chat / Reports", "report list contract", scenario_reports_list),
        ("Mission Control", "adoption excludes system actors", scenario_adoption_excludes_system_events),
        ("Mission Control", "all tabs across 24h/7d/30d", scenario_mission_control_all_tabs),
        ("Mission Control", "workflow datetime serialization", scenario_workflow_datetime_serialization),
    ]

    print("Riley smoke harness starting...")
    print(f"Harness version: {HARNESS_VERSION}")
    print(f"Base URL: {cfg.base_url}")
    print(f"Tenant: {cfg.tenant_id}")
    print(f"Mutation scenarios enabled: {cfg.enable_mutation}")
    print("")

    results: List[ScenarioResult] = []
    for category, name, fn in scenarios:
        result = run_scenario(category, name, fn, cfg)
        results.append(result)
        print_result(result)

    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status == "FAIL")
    skipped = sum(1 for r in results if r.status == "SKIP")

    print("\n=== Smoke Summary ===")
    print(f"PASS: {passed}")
    print(f"FAIL: {failed}")
    print(f"SKIP: {skipped}")
    print(f"TOTAL: {len(results)}")
    write_json_report(args.report_file, cfg, results, passed, failed, skipped)
    print(f"JSON report: {args.report_file}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
