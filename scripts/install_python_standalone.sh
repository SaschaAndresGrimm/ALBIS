#!/usr/bin/env bash
# Install a pinned python-build-standalone CPython for the Linux and macOS
# release builds. PyInstaller copies this interpreter, and the libraries it
# carries, into every bundle, so what it carries is what ALBIS ships.
#
# Linux: the artifacts are built inside a manylinux_2_28 container so that they
# need nothing newer than glibc 2.28 (RHEL/Rocky/AlmaLinux 8). The interpreters
# that image ships under /opt/python are statically linked and carry no
# libpython3.x.so, which PyInstaller needs to bundle; actions/setup-python's
# builds target Ubuntu and do not run there at all. python-build-standalone
# ships a shared libpython and targets glibc 2.17, so it fits both needs.
#
# macOS: actions/setup-python's builds carry OpenSSL from the 3.0 line and
# older expat and SQLite; the SBOM scan of the 0.46 build found a Critical in
# that OpenSSL. python-build-standalone tracks the current releases, and using
# it on both platforms means one pin to move. Its macOS builds need 11.0
# (arm64) and 10.15 (x86_64), below the floor the README states.
#
# The release is the newest that was at least two weeks old when pinned.
#
# Usage: install_python_standalone.sh [DEST]
#   DEST  install prefix (default: /opt/albis-python); the interpreter lands at
#         DEST/bin/python3
set -euo pipefail

PBS_RELEASE="${PBS_RELEASE:-20261026}"
PBS_PYTHON_VERSION="${PBS_PYTHON_VERSION:-3.13.18}"
# The build runs natively on each architecture, so the interpreter matches the
# machine it is installed on.
PBS_ARCH="${PBS_ARCH:-$(uname -m)}"
[ "$PBS_ARCH" = "arm64" ] && PBS_ARCH=aarch64
case "$(uname -s)" in
  Linux) PBS_PLATFORM=unknown-linux-gnu LIBPYTHON="libpython${PBS_PYTHON_VERSION%.*}.so.1.0" ;;
  Darwin) PBS_PLATFORM=apple-darwin LIBPYTHON="libpython${PBS_PYTHON_VERSION%.*}.dylib" ;;
  *) echo "No pinned python-build-standalone build for $(uname -s)" >&2; exit 1 ;;
esac
# From the release's SHA256SUMS.
case "${PBS_ARCH}-${PBS_PLATFORM}" in
  x86_64-unknown-linux-gnu) default_sha="126343c54daff4068efc1ac269518d0cc202d57857794948103dd1454e8881be" ;;
  aarch64-unknown-linux-gnu) default_sha="b48451865bde9a39e2f3e71d7fdcbf1f32615f50f24d78dc50d04ede6501cf81" ;;
  x86_64-apple-darwin) default_sha="708be9e7df297f68bdae589ea9e62a621db5abe7708f79e851c41410347452d1" ;;
  aarch64-apple-darwin) default_sha="e5af2fde6a937a0e0c795a0c595c4efb70578d57463a2657c819992b9b9acbac" ;;
  *) echo "No pinned python-build-standalone build for ${PBS_ARCH}-${PBS_PLATFORM}" >&2; exit 1 ;;
esac
PBS_ASSET="${PBS_ASSET:-cpython-${PBS_PYTHON_VERSION}+${PBS_RELEASE}-${PBS_ARCH}-${PBS_PLATFORM}-install_only.tar.gz}"
PBS_SHA256="${PBS_SHA256:-$default_sha}"
DEST="${1:-/opt/albis-python}"

tmp_file="$(mktemp)"
trap 'rm -f "$tmp_file"' EXIT

# `+` in the asset name must be percent-encoded in the download URL.
url="https://github.com/astral-sh/python-build-standalone/releases/download/${PBS_RELEASE}/${PBS_ASSET//+/%2B}"
curl -fsSL --retry 5 --retry-connrefused "$url" -o "$tmp_file"

if command -v sha256sum >/dev/null; then
  actual_sha="$(sha256sum "$tmp_file" | awk '{print $1}')"
else
  actual_sha="$(shasum -a 256 "$tmp_file" | awk '{print $1}')"
fi
if [ "$actual_sha" != "$PBS_SHA256" ]; then
  echo "python-build-standalone checksum mismatch: expected $PBS_SHA256 got $actual_sha"
  exit 1
fi

rm -rf "$DEST"
mkdir -p "$DEST"
# The archive holds a single top-level `python/` directory.
tar -xzf "$tmp_file" -C "$DEST" --strip-components=1

if [ ! -f "$DEST/lib/$LIBPYTHON" ]; then
  echo "Installed interpreter has no shared libpython ($LIBPYTHON); PyInstaller cannot bundle it."
  exit 1
fi

"$DEST/bin/python3" --version
echo "Installed python-build-standalone ${PBS_PYTHON_VERSION} (${PBS_RELEASE}) to ${DEST}"
