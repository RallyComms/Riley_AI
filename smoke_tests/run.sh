#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG_PATH="${1:-$ROOT_DIR/smoke_tests/.env}"

python3 "$ROOT_DIR/smoke_tests/run_smoke.py" --config "$CONFIG_PATH"
