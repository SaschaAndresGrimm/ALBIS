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
mkdir -p "$APPDIR/usr/bin"

# Include the full PyInstaller one-folder payload (binary + _internal runtime).
cp -a "dist/ALBIS/." "$APPDIR/usr/bin/"

cat > "$APPDIR/ALBIS.desktop" <<'EOF'
[Desktop Entry]
Type=Application
Name=ALBIS
Exec=ALBIS
Icon=ALBIS
Categories=Science;
Terminal=false
EOF

cat > "$APPDIR/AppRun" <<'EOF'
#!/bin/sh
HERE="$(dirname "$(readlink -f "$0")")"
exec "$HERE/usr/bin/ALBIS" "$@"
EOF
chmod +x "$APPDIR/AppRun"

if [ -f "albis_assets/icon_512x512.png" ]; then
  cp "albis_assets/icon_512x512.png" "$APPDIR/ALBIS.png"
elif [ -f "albis_assets/icon_256x256.png" ]; then
  cp "albis_assets/icon_256x256.png" "$APPDIR/ALBIS.png"
elif [ -f "frontend/ressources/icon.png" ]; then
  cp "frontend/ressources/icon.png" "$APPDIR/ALBIS.png"
fi

OUT="dist/ALBIS-${TARGET}-${TAG}.AppImage"
rm -f "$OUT"
appimagetool "$APPDIR" "$OUT"
echo "Output: $OUT"
