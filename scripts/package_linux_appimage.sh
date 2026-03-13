#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON_BIN="${PYTHON_BIN:-python3}"
VERSION_INFO="$("$PYTHON_BIN" scripts/version_info.py --shell)"
eval "$VERSION_INFO"

if [ ! -d "dist/ALBIS" ]; then
  echo "Missing dist/ALBIS. Run ./scripts/build_linux.sh first."
  exit 1
fi

if ! command -v appimagetool >/dev/null 2>&1; then
  echo "Missing appimagetool. Install it first, then rerun."
  exit 1
fi

APPDIR="dist/AppDir"
rm -rf "$APPDIR"
mkdir -p \
  "$APPDIR/usr/bin" \
  "$APPDIR/usr/share/applications" \
  "$APPDIR/usr/share/metainfo"

# Include the full PyInstaller one-folder payload (binary + _internal runtime).
cp -a "dist/ALBIS/." "$APPDIR/usr/bin/"

for metadata in "packaging/linux/ALBIS.desktop" "packaging/linux/ALBIS.metainfo.xml"; do
  if [ ! -f "$metadata" ]; then
    echo "Missing packaging metadata file: $metadata"
    exit 1
  fi
done

cp "packaging/linux/ALBIS.desktop" "$APPDIR/ALBIS.desktop"
cp "packaging/linux/ALBIS.desktop" "$APPDIR/usr/share/applications/ALBIS.desktop"
cp "packaging/linux/ALBIS.metainfo.xml" "$APPDIR/usr/share/metainfo/ALBIS.metainfo.xml"

cat > "$APPDIR/AppRun" <<'EOF'
#!/bin/sh
HERE="$(dirname "$(readlink -f "$0")")"
exec "$HERE/usr/bin/ALBIS" "$@"
EOF
chmod +x "$APPDIR/AppRun"

if [ -f "albis_assets/icon_512x512.png" ]; then
  ICON_SRC="albis_assets/icon_512x512.png"
  ICON_SIZE="512x512"
elif [ -f "albis_assets/icon_256x256.png" ]; then
  ICON_SRC="albis_assets/icon_256x256.png"
  ICON_SIZE="256x256"
elif [ -f "frontend/ressources/icon.png" ]; then
  ICON_SRC="frontend/ressources/icon.png"
  ICON_SIZE="1024x1024"
fi

if [ -n "${ICON_SRC:-}" ]; then
  mkdir -p "$APPDIR/usr/share/icons/hicolor/${ICON_SIZE}/apps"
  cp "$ICON_SRC" "$APPDIR/ALBIS.png"
  cp "$ICON_SRC" "$APPDIR/usr/share/icons/hicolor/${ICON_SIZE}/apps/ALBIS.png"
fi

OUT="dist/ALBIS-${VERSION}-${APPIMAGE_ARCH}.AppImage"
rm -f "$OUT"
appimagetool "$APPDIR" "$OUT"
echo "Output: $OUT"
