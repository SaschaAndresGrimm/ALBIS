#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"

if [ -r /etc/os-release ]; then
  # shellcheck disable=SC1091
  . /etc/os-release
  LINUX_ID="${ID:-linux}"
  LINUX_VER="${VERSION_ID:-unknown}"
elif command -v lsb_release >/dev/null 2>&1; then
  LINUX_ID="$(lsb_release -si | tr '[:upper:]' '[:lower:]')"
  LINUX_VER="$(lsb_release -sr)"
else
  LINUX_ID="linux"
  LINUX_VER="$(uname -r)"
fi
OS_TAG="$(printf '%s_%s' "$LINUX_ID" "$LINUX_VER" | tr '.-' '_' | tr -cd '[:alnum:]_')"

"$PYTHON_BIN" -m pip install --upgrade pyinstaller
"$PYTHON_BIN" -m PyInstaller ALBIS.spec

OUT="dist/ALBIS-linux-${OS_TAG}-${TAG}.tar.gz"
rm -f "$OUT"
tar -czf "$OUT" -C dist ALBIS
echo "Output: dist/ALBIS and ${OUT}"
