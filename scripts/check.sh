#!/usr/bin/env bash
# The gates CI runs on every pull request (.github/workflows/ci.yml), in one
# command, so "done" means the same locally as in CI:
#   ruff, black --check, pytest with the coverage floor, eslint, vitest.
# Runs every gate and reports each, then exits non-zero if any failed.
#
#   scripts/check.sh          # or: npm run check
#   scripts/check.sh --fast   # skip pytest (the slowest gate)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# The project's virtualenv first: a python on PATH may be another version.
if [[ -x .venv/bin/python ]]; then
  PY=.venv/bin/python
else
  PY="${PYTHON:-python3}"
fi
PY_PATHS=(backend albis_launcher.py tests scripts test_scripts)

failed=()
gate() {
  local name="$1"
  shift
  echo "== $name"
  if "$@"; then
    echo "   ok"
  else
    echo "   FAILED: $name"
    failed+=("$name")
  fi
}

gate "ruff" "$PY" -m ruff check "${PY_PATHS[@]}"
gate "black" "$PY" -m black --check --quiet "${PY_PATHS[@]}"
if [[ "${1:-}" != "--fast" ]]; then
  gate "pytest" env PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 "$PY" -m pytest -q -p pytest_cov --cov=backend --cov-report= --cov-fail-under=77
fi
gate "eslint" npm run --silent lint:js
gate "vitest" npm run --silent test:js

echo
if ((${#failed[@]})); then
  echo "Failed: ${failed[*]}"
  exit 1
fi
echo "All checks passed."
