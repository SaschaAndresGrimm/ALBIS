#!/usr/bin/env bash
# Remove debug information from the shared libraries in a PyInstaller bundle.
#
# Usage: scripts/strip_linux_bundle.sh dist/ALBIS
#
# On Linux, the standalone Python and several wheels ship their libraries with
# full DWARF debug info: libpython alone is 140 MB of which about 6 MB is code,
# and the HDF5 filters another 180 MB. Windows keeps that in separate .pdb
# files and never bundles it, which is why the Linux download was three times
# the size of the others.
#
# ALBIS.spec keeps PyInstaller's own strip off on Linux, because stripping the
# OpenBLAS that numpy vendors broke its page alignment once ("ELF load command
# address/offset not page-aligned" on glibc 2.35). This step is narrower:
#   - --strip-debug only, so the symbol tables stay and nothing is relinked;
#   - nothing under a wheel's `<name>.libs/` folder, where auditwheel puts the
#     vendored libraries (OpenBLAS among them); those come stripped already;
#   - only shared libraries, never the ALBIS executable, whose PyInstaller
#     archive is appended to the ELF file and would be cut off.
# The smoke tests then import numpy from the stripped bundle on Rocky 8 and 9
# and on Ubuntu 22.04 and 24.04.
set -euo pipefail

BUNDLE="${1:?usage: $0 <bundle dir, e.g. dist/ALBIS>}"
INTERNAL="$BUNDLE/_internal"
if [ ! -d "$INTERNAL" ]; then
  echo "No $INTERNAL: not a PyInstaller onedir bundle" >&2
  exit 1
fi

before="$(du -sm "$BUNDLE" | cut -f1)"
count=0
while IFS= read -r -d '' lib; do
  strip --strip-debug --preserve-dates "$lib"
  count=$((count + 1))
done < <(find "$INTERNAL" -type f \( -name '*.so' -o -name '*.so.*' \) \
  -not -path '*.libs/*' -print0)
after="$(du -sm "$BUNDLE" | cut -f1)"

echo "Stripped debug info from $count libraries: ${before} MB -> ${after} MB"
