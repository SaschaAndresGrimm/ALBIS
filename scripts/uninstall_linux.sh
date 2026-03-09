#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: ./scripts/uninstall_linux.sh [--purge-user-data]"
}

PURGE_USER_DATA=0
if [ "${1:-}" = "--purge-user-data" ]; then
  PURGE_USER_DATA=1
  shift
elif [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  usage
  exit 0
fi
if [ "$#" -ne 0 ]; then
  usage
  exit 1
fi

PREFIX="${ALBIS_PREFIX:-$HOME/.local}"
if [ -z "$PREFIX" ] || [ "$PREFIX" = "/" ]; then
  echo "Refusing to uninstall with ALBIS_PREFIX='$PREFIX'."
  exit 1
fi

APP_DIR="$PREFIX/share/albis"
APPIMAGE_APP_DIR="$PREFIX/share/albis-appimage"
BIN_DIR="$PREFIX/bin"
DESKTOP_DIR="$PREFIX/share/applications"
ICON_DIR="$PREFIX/share/icons/hicolor/512x512/apps"

rm -rf "$APP_DIR"
rm -rf "$APPIMAGE_APP_DIR"
rm -f "$BIN_DIR/albis"
rm -f "$DESKTOP_DIR/albis.desktop"
rm -f "$ICON_DIR/albis.png"

if [ "$PURGE_USER_DATA" = "1" ]; then
  rm -rf "$HOME/ALBIS-data"
  rm -rf "$HOME/.config/albis"
fi

rmdir "$ICON_DIR" 2>/dev/null || true
rmdir "$PREFIX/share/icons/hicolor/512x512" 2>/dev/null || true
rmdir "$PREFIX/share/icons/hicolor" 2>/dev/null || true
rmdir "$PREFIX/share/icons" 2>/dev/null || true
rmdir "$DESKTOP_DIR" 2>/dev/null || true
rmdir "$PREFIX/share" 2>/dev/null || true
rmdir "$BIN_DIR" 2>/dev/null || true

if [ -d "$DESKTOP_DIR" ] && command -v update-desktop-database >/dev/null 2>&1; then
  update-desktop-database "$DESKTOP_DIR" >/dev/null 2>&1 || true
fi
if [ -d "$PREFIX/share/icons/hicolor" ] && command -v gtk-update-icon-cache >/dev/null 2>&1; then
  gtk-update-icon-cache -q "$PREFIX/share/icons/hicolor" >/dev/null 2>&1 || true
fi

if [ "$PURGE_USER_DATA" = "1" ]; then
  echo "Removed ALBIS from $PREFIX (including user data and config)."
else
  echo "Removed ALBIS from $PREFIX (user data and config kept)."
fi
