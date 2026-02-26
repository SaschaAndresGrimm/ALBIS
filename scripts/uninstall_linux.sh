#!/usr/bin/env bash
set -euo pipefail

PREFIX="${ALBIS_PREFIX:-$HOME/.local}"
if [ -z "$PREFIX" ] || [ "$PREFIX" = "/" ]; then
  echo "Refusing to uninstall with ALBIS_PREFIX='$PREFIX'."
  exit 1
fi

APP_DIR="$PREFIX/share/albis"
BIN_DIR="$PREFIX/bin"
DESKTOP_DIR="$PREFIX/share/applications"
ICON_DIR="$PREFIX/share/icons/hicolor/512x512/apps"

rm -rf "$APP_DIR"
rm -f "$BIN_DIR/albis"
rm -f "$DESKTOP_DIR/albis.desktop"
rm -f "$ICON_DIR/albis.png"

rmdir "$ICON_DIR" 2>/dev/null || true
rmdir "$PREFIX/share/icons/hicolor/512x512" 2>/dev/null || true
rmdir "$PREFIX/share/icons/hicolor" 2>/dev/null || true
rmdir "$PREFIX/share/icons" 2>/dev/null || true
rmdir "$DESKTOP_DIR" 2>/dev/null || true
rmdir "$PREFIX/share" 2>/dev/null || true
rmdir "$BIN_DIR" 2>/dev/null || true

if command -v update-desktop-database >/dev/null 2>&1; then
  update-desktop-database "$DESKTOP_DIR" >/dev/null 2>&1 || true
fi
if command -v gtk-update-icon-cache >/dev/null 2>&1; then
  gtk-update-icon-cache -q "$PREFIX/share/icons/hicolor" >/dev/null 2>&1 || true
fi

echo "Removed ALBIS from $PREFIX"
