# this is ignore logic, it tells pytest to stop going into the data and artifacts folders

from __future__ import annotations

from pathlib import Path

_BLOCK_TOP = {"data", "artifacts", ".runs", ".venv"}

def pytest_ignore_collect(path, config):  # type: ignore[override]
    """
    Prevent pytest from collecting tests or files in heavyweight directories.
    Only block when those are top-level under the repo root.
    """
    p = Path(str(path)).resolve()
    try:
        repo_root = Path(__file__).resolve().parent
        rel = p.relative_to(repo_root)
    except Exception:
        # Path is outside the repo; don't block
        return False

    parts = [part.lower() for part in rel.parts]
    # Block only if the first segment is a blocked folder
    return bool(parts) and parts[0] in _BLOCK_TOP


