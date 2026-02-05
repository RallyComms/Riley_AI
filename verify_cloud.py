import os
import time
import json
import urllib.request
import urllib.error

DEFAULT_URL = "https://riley-api-km6eaiohua-uk.a.run.app"
BASE_URL = os.environ.get("RILEY_API_URL", DEFAULT_URL).rstrip("/")

TESTS = [
    ("healthz", f"{BASE_URL}/healthz"),
    ("list_global", f"{BASE_URL}/api/v1/list?tenant_id=global"),
]


def fetch(url: str, timeout: int = 20):
    req = urllib.request.Request(url, headers={"User-Agent": "riley-smoke-test/1.0"})
    start = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            ms = int((time.time() - start) * 1000)
            return resp.status, ms, body
    except urllib.error.HTTPError as e:
        body = e.read() if hasattr(e, "read") else b""
        ms = int((time.time() - start) * 1000)
        return e.code, ms, body
    except Exception as e:
        ms = int((time.time() - start) * 1000)
        return None, ms, str(e).encode("utf-8")

def pretty(body: bytes, limit: int = 600) -> str:
    s = body.decode("utf-8", errors="replace")
    s = s.strip()
    if not s:
        return "(empty)"
    try:
        obj = json.loads(s)
        out = json.dumps(obj, indent=2)[:limit]
        return out + ("..." if len(out) >= limit else "")
    except Exception:
        return (s[:limit] + ("..." if len(s) > limit else ""))

def main():
    print(f"Riley Cloud Smoke Test")
    print(f"BASE_URL = {BASE_URL}\n")

    all_ok = True
    for name, url in TESTS:
        status, ms, body = fetch(url)
        ok = (status is not None) and (200 <= status < 300)
        all_ok = all_ok and ok
        print(f"[{name}] {status} in {ms}ms")
        print(f"  {url}")
        print(f"  body: {pretty(body)}\n")

    if all_ok:
        print("✅ SMOKE TEST PASSED")
        raise SystemExit(0)
    else:
        print("❌ SMOKE TEST FAILED (see above)")
        raise SystemExit(1)

if __name__ == "__main__":
    main()
