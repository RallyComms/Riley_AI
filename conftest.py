## this is ignore logic, it tells pytest to stop going into the data and artifacts folders

from __future__ import annotations

def pytest_ignore_collect(path, config):  # type: ignore[override]
    """
    Prevent pytest from collecting tests or files in unwanted directories.
    """
    p = str(path).replace("\\", "/").lower()
    blocked = [
        "/data/",
        "/artifacts/",
        "/.runs/",
        "/.venv/",
        "/riley_ai/",
    ]
    return any(b in p for b in blocked)
