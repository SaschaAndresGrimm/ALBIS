#!/usr/bin/env bash
# Install a pinned python-build-standalone CPython for the Linux, macOS and
# Windows release builds. PyInstaller copies this interpreter, and the libraries it
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
# it everywhere means one pin to move. Its macOS builds need 11.0 (arm64) and
# 10.15 (x86_64), below the floor the README states.
#
# Windows: setup-python's build carried an older SQLite and expat than the
# other platforms; same reasoning. Run it from Git Bash (`shell: bash`).
#
# Releases are normally pinned once at least two weeks old. 20261110 was pinned
# at eight days, for expat 2.10.0, which fixes advisories the release scan
# reports; its checksums come from the release's asset digests, as it has no
# SHA256SUMS file.
#
# Usage: install_python_standalone.sh [DEST]
#   DEST  install prefix (default: /opt/albis-python); the interpreter lands at
#         DEST/bin/python3, or DEST/python.exe on Windows
set -euo pipefail

PBS_RELEASE="${PBS_RELEASE:-20261110}"
PBS_PYTHON_VERSION="${PBS_PYTHON_VERSION:-3.13.18}"
# The build runs natively on each architecture, so the interpreter matches the
# machine it is installed on.
PBS_ARCH="${PBS_ARCH:-$(uname -m)}"
[ "$PBS_ARCH" = "arm64" ] && PBS_ARCH=aarch64
short="${PBS_PYTHON_VERSION%.*}"
case "$(uname -s)" in
  Linux) PBS_PLATFORM=unknown-linux-gnu LIBPYTHON="lib/libpython${short}.so.1.0" PYTHON_EXE="bin/python3" ;;
  Darwin) PBS_PLATFORM=apple-darwin LIBPYTHON="lib/libpython${short}.dylib" PYTHON_EXE="bin/python3" ;;
  MINGW* | MSYS* | CYGWIN*) PBS_PLATFORM=pc-windows-msvc LIBPYTHON="python${short//./}.dll" PYTHON_EXE="python.exe" ;;
  *) echo "No pinned python-build-standalone build for $(uname -s)" >&2; exit 1 ;;
esac
# sha256 digests of the release assets.
case "${PBS_ARCH}-${PBS_PLATFORM}" in
  x86_64-unknown-linux-gnu) default_sha="50c774f6674740739f791c05a9177ed03e2fe02abebda9bca3512eb7309b37cc" ;;
  aarch64-unknown-linux-gnu) default_sha="d25a65036d5e149db902fa52ad65878d0c00e9301360ec197832f99063087e9e" ;;
  x86_64-apple-darwin) default_sha="a46bcdeecd28ba253522b321dcb3806da1b0ff581479fc1d27e9613e3ac44764" ;;
  aarch64-apple-darwin) default_sha="1afd3983237c2a10277345f235be730e9cb49a123e0574ed252dbf3a841a24e4" ;;
  x86_64-pc-windows-msvc) default_sha="7e93813bd362c6413f30788b248590e2f169df2d17dcac81aafbcc7a6eeb542c" ;;
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

if [ ! -f "$DEST/$LIBPYTHON" ]; then
  echo "Installed interpreter has no shared libpython ($LIBPYTHON); PyInstaller cannot bundle it."
  exit 1
fi

"$DEST/$PYTHON_EXE" --version
echo "Installed python-build-standalone ${PBS_PYTHON_VERSION} (${PBS_RELEASE}) to ${DEST}"
