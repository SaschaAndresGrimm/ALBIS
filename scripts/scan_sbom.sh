#!/usr/bin/env bash
# Scan bills of materials for known vulnerabilities with grype.
#
#   scripts/scan_sbom.sh [--fail-on SEVERITY] SBOM.cdx.json...
#
# pip-audit checks the pins; it cannot see the interpreter or the C libraries a
# bundle carries. The per-platform SBOMs (scripts/generate_sbom.py) list both,
# with package URLs for the Python packages and CPEs for the native libraries,
# and grype matches on both. The release scans the SBOMs it is about to
# publish; the weekly Security workflow scans those of the latest release, since
# a vulnerability found after release is the one nobody re-checks.
#
# grype is pinned and its archive checked against the checksum recorded here.
# Its vulnerability database is downloaded on each run, which is the point.
# Findings that were reviewed and do not apply go in .grype.yaml, each with the
# reason as a comment.
set -euo pipefail

GRYPE_VERSION="0.121.2"

# From grype_${GRYPE_VERSION}_checksums.txt. A case, not an associative array:
# macOS ships bash 3.2.
grype_sha256() {
  case "$1" in
    linux_amd64) echo 9c8965ced9df657e5e851c14021bae838802f711edd7a48f9bc3c923a8d28f12 ;;
    linux_arm64) echo d7d2e160255b0f087280feabf6f213cf4c45d475690f3f1fe5dc336eb1037bf0 ;;
    darwin_amd64) echo 1a748df28f72e841dea5c6a19c736b61a79c3e81dc376e0c18e1ab0d274cfe08 ;;
    darwin_arm64) echo 3633c113d81ec1460b0520e23633d72a25caae62875fd48a8b0e4f22dcc7c704 ;;
  esac
}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail_on=""
if [ "${1:-}" = "--fail-on" ]; then
  fail_on="$2"
  shift 2
fi
if [ "$#" -eq 0 ]; then
  echo "usage: $0 [--fail-on SEVERITY] SBOM.cdx.json..." >&2
  exit 2
fi

# Sets GRYPE. Not called as $(install_grype): errexit does not reach into a
# command substitution in bash 3.2, so a failed download would carry on.
install_grype() {
  local os arch platform dir archive sum
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "$(uname -m)" in
    x86_64 | amd64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) echo "no grype build pinned for $(uname -m)" >&2; exit 1 ;;
  esac
  platform="${os}_${arch}"
  sum="$(grype_sha256 "$platform")"
  [ -n "$sum" ] || { echo "no grype checksum pinned for ${platform}" >&2; exit 1; }

  dir="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/albis-grype-${GRYPE_VERSION}"
  GRYPE="$dir/grype"
  if [ -x "$GRYPE" ]; then
    return
  fi
  mkdir -p "$dir"
  archive="$dir/grype.tar.gz"
  curl -fsSL --retry 5 --retry-connrefused -o "$archive" \
    "https://github.com/anchore/grype/releases/download/v${GRYPE_VERSION}/grype_${GRYPE_VERSION}_${platform}.tar.gz"
  if command -v sha256sum >/dev/null; then
    echo "${sum}  ${archive}" | sha256sum -c - >&2
  else
    echo "${sum}  ${archive}" | shasum -a 256 -c - >&2
  fi
  tar -xzf "$archive" -C "$dir" grype
  rm -f "$archive"
}

install_grype
export GRYPE_CHECK_FOR_APP_UPDATE=false

status=0
summary="${GITHUB_STEP_SUMMARY:-/dev/null}"
echo "## Vulnerability scan of the bills of materials (grype ${GRYPE_VERSION})" >>"$summary"
for sbom in "$@"; do
  echo "== $(basename "$sbom")"
  args=(--config "$ROOT/.grype.yaml" --output table)
  [ -n "$fail_on" ] && args+=(--fail-on "$fail_on")
  set +e
  report="$("$GRYPE" "sbom:${sbom}" "${args[@]}" 2>&1)"
  code=$?
  set -e
  echo "$report"
  {
    echo
    echo "### $(basename "$sbom")"
    echo
    echo '```'
    echo "$report"
    echo '```'
  } >>"$summary"
  if [ "$code" -ne 0 ]; then
    status=1
  fi
done

if [ "$status" -ne 0 ]; then
  echo "Findings at or above '${fail_on}' severity. Fix them, or record why they do not apply in .grype.yaml." >&2
fi
exit "$status"
