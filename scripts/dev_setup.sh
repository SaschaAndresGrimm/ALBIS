#!/usr/bin/env bash
# One-step development setup for a fresh checkout, for contributors and for
# coding agents starting in a clean sandbox:
#   - a Python 3.13 virtualenv in .venv with the dev requirements,
#   - the frontend tooling (npm ci),
#   - the pre-commit hooks, when a git checkout.
# Safe to run again; it reuses what is there.
#
#   scripts/dev_setup.sh            # PYTHON=python3.13 to pick the interpreter
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# 3.13 is the floor and the ceiling (see docs/DEVELOPER_GUIDE.md). Prefer an
# explicit python3.13 over whatever `python3` happens to be first on PATH
# (an Anaconda install, for one, shadows it with another version).
PYTHON="${PYTHON:-}"
if [[ -z "$PYTHON" ]]; then
  for candidate in python3.13 python3 python; do
    if command -v "$candidate" >/dev/null 2>&1 &&
      "$candidate" -c 'import sys; sys.exit(sys.version_info[:2] != (3, 13))' 2>/dev/null; then
      PYTHON="$candidate"
      break
    fi
  done
fi
if [[ -z "$PYTHON" ]]; then
  echo "Python 3.13 not found. Install it, or set PYTHON=/path/to/python3.13." >&2
  exit 1
fi

if [[ ! -x .venv/bin/python ]]; then
  echo "Creating .venv with $("$PYTHON" --version)"
  "$PYTHON" -m venv .venv
fi
.venv/bin/python -m pip install --quiet --upgrade pip
.venv/bin/python -m pip install --quiet -r requirements-dev.txt

# Node 22+ is required (package.json engines); the npm scripts run node@24.
if ! command -v npm >/dev/null 2>&1; then
  echo "npm not found. Install Node.js 22 or later (see .nvmrc)." >&2
  exit 1
fi
npm ci --no-audit --no-fund

if [[ -d .git ]] && .venv/bin/python -m pre_commit --version >/dev/null 2>&1; then
  .venv/bin/python -m pre_commit install
fi

echo
echo "Ready. Run the checks with:  npm run check"
echo "Start ALBIS with:            .venv/bin/python albis_launcher.py"
