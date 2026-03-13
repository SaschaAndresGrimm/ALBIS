#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DMG_PATH="${1:-}"
STARTUP_TIMEOUT="${2:-60}"

if [ -z "$DMG_PATH" ] || [ ! -f "$DMG_PATH" ]; then
  echo "Usage: ./scripts/smoke_macos_dmg.sh <path-to-dmg> [startup-timeout]"
  exit 1
fi

TEMP_DIR="$(mktemp -d)"
MOUNT_DIR="$TEMP_DIR/mount"
mkdir -p "$MOUNT_DIR"

cleanup() {
  hdiutil detach "$MOUNT_DIR" -force >/dev/null 2>&1 || true
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

hdiutil attach "$DMG_PATH" -mountpoint "$MOUNT_DIR" -nobrowse -quiet

APP_PATH="$MOUNT_DIR/ALBIS.app"
if [ ! -d "$APP_PATH" ]; then
  APP_PATH="$(find "$MOUNT_DIR" -maxdepth 1 -type d -name '*.app' | head -n1 || true)"
fi
if [ -z "$APP_PATH" ] || [ ! -d "$APP_PATH" ]; then
  echo "No .app bundle found in mounted DMG"
  exit 1
fi

BINARY_PATH="$APP_PATH/Contents/MacOS/ALBIS"
if [ ! -x "$BINARY_PATH" ]; then
  echo "App bundle binary not found: $BINARY_PATH"
  exit 1
fi

python3 ./scripts/smoke_packaged_binary.py --binary "$BINARY_PATH" --startup-timeout "$STARTUP_TIMEOUT"
