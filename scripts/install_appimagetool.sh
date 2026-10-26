#!/usr/bin/env bash
set -euo pipefail

APPIMAGE_TOOL_VERSION="${APPIMAGE_TOOL_VERSION:-13}"
# appimagetool embeds the AppImage runtime of its own architecture, so the tool
# for the build machine is also the one that makes an AppImage for it.
APPIMAGE_TOOL_ARCH="${APPIMAGE_TOOL_ARCH:-$(uname -m)}"
case "$APPIMAGE_TOOL_ARCH" in
  x86_64) default_sha="df3baf5ca5facbecfc2f3fa6713c29ab9cefa8fd8c1eac5d283b79cab33e4acb" ;;
  aarch64) default_sha="334e77beb67fc1e71856c29d5f3f324ca77b0fde7a840fdd14bd3b88c25c341f" ;;
  *) echo "No pinned appimagetool build for $APPIMAGE_TOOL_ARCH" >&2; exit 1 ;;
esac
APPIMAGE_TOOL_ASSET="${APPIMAGE_TOOL_ASSET:-obsolete-appimagetool-${APPIMAGE_TOOL_ARCH}.AppImage}"
APPIMAGE_TOOL_SHA256="${APPIMAGE_TOOL_SHA256:-$default_sha}"
APPIMAGE_TOOL_DEST="${1:-/usr/local/bin/appimagetool.AppImage}"
APPIMAGE_WRAPPER_DEST="${2:-/usr/local/bin/appimagetool}"

tmp_file="$(mktemp)"
trap 'rm -f "$tmp_file"' EXIT

url="https://github.com/AppImage/AppImageKit/releases/download/${APPIMAGE_TOOL_VERSION}/${APPIMAGE_TOOL_ASSET}"
curl -fsSL "$url" -o "$tmp_file"

actual_sha="$(sha256sum "$tmp_file" | awk '{print $1}')"
if [ "$actual_sha" != "$APPIMAGE_TOOL_SHA256" ]; then
  echo "appimagetool checksum mismatch: expected $APPIMAGE_TOOL_SHA256 got $actual_sha"
  exit 1
fi

install -m 0755 "$tmp_file" "$APPIMAGE_TOOL_DEST"
cat >"$APPIMAGE_WRAPPER_DEST" <<EOF
#!/usr/bin/env bash
APPIMAGE_EXTRACT_AND_RUN=1 "$APPIMAGE_TOOL_DEST" "\$@"
EOF
chmod +x "$APPIMAGE_WRAPPER_DEST"

echo "Installed appimagetool ${APPIMAGE_TOOL_VERSION} to ${APPIMAGE_TOOL_DEST}"
