#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

SCRIPT_NAME="bootstrap_macos_signing_ci"

log() {
  echo "[$SCRIPT_NAME] $*"
}

warn() {
  echo "[$SCRIPT_NAME] Warning: $*" >&2
}

die() {
  echo "[$SCRIPT_NAME] Error: $*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: ./scripts/bootstrap_macos_signing_ci.sh [options]

Detect the local macOS signing certificate, validate it, infer the Developer ID
identity, and upload the required GitHub Actions secrets for this repository.

Options:
  --repo <owner/repo>                  Override the GitHub repository.
  --cert-path <path>                   Override the local .p12/.pfx path.
  --cert-password <password>           Override the .p12 import password.
  --signing-identity <identity>        Override the imported signing identity.
  --apple-id <apple-id>                Optional notarization Apple ID.
  --apple-team-id <team-id>            Optional notarization team ID.
  --apple-app-specific-password <pw>   Optional notarization app-specific password.
  --skip-notarization                  Configure signing only.
  --no-history                         Do not read shell history for missing values.
  --dry-run                            Validate and print what would be changed.
  -h, --help                           Show this help.

Resolution order for missing values:
  1. Explicit command-line option
  2. Current environment variable
  3. Latest assignment found in ~/.zsh_history or ~/.bash_history

The script prefers ~/Documents/albis-dev-id.p12 when present.
EOF
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "Missing required command: $1"
}

history_value() {
  local var_name="$1"

  python3 - "$var_name" "$HOME/.zsh_history" "$HOME/.bash_history" <<'PY'
import os
import re
import sys

var_name = sys.argv[1]
paths = sys.argv[2:]
pattern = re.compile(r"^\s*(?:export\s+)?%s=(.*?)(?:\\)?\s*$" % re.escape(var_name))
last_value = ""

for path in paths:
    if not os.path.isfile(path):
        continue
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                match = pattern.match(line.rstrip("\n"))
                if not match:
                    continue
                value = match.group(1)
                if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                    value = value[1:-1]
                last_value = value
    except OSError:
        continue

sys.stdout.write(last_value)
PY
}

resolve_repo() {
  local explicit_repo="$1"
  local remote_url

  if [ -n "$explicit_repo" ]; then
    printf '%s\n' "$explicit_repo"
    return 0
  fi

  remote_url="$(git config --get remote.origin.url || true)"
  [ -n "$remote_url" ] || die "Could not determine GitHub repository from remote.origin.url"

  python3 - "$remote_url" <<'PY'
import re
import sys

remote = sys.argv[1].strip()
match = re.search(r"github\.com[:/]([^/]+/[^/]+?)(?:\.git)?$", remote)
if not match:
    raise SystemExit(1)
print(match.group(1))
PY
}

resolve_cert_path() {
  local explicit_path="$1"

  if [ -n "$explicit_path" ]; then
    printf '%s\n' "$explicit_path"
    return 0
  fi

  python3 - <<'PY'
from pathlib import Path

preferred = Path.home() / "Documents" / "albis-dev-id.p12"
if preferred.is_file():
    print(preferred)
    raise SystemExit(0)

documents = Path.home() / "Documents"
paths = sorted(documents.glob("**/*.p12")) + sorted(documents.glob("**/*.pfx"))
for path in paths:
    if path.is_file():
        print(path)
        raise SystemExit(0)

raise SystemExit(1)
PY
}

resolve_value() {
  local explicit_value="$1"
  local env_name="$2"
  local allow_history="$3"
  local value

  if [ -n "$explicit_value" ]; then
    printf '%s\n' "$explicit_value"
    return 0
  fi

  value="${!env_name:-}"
  if [ -n "$value" ]; then
    printf '%s\n' "$value"
    return 0
  fi

  if [ "$allow_history" = "1" ]; then
    value="$(history_value "$env_name")"
    if [ -n "$value" ]; then
      printf '%s\n' "$value"
      return 0
    fi
  fi

  printf '\n'
}

set_secret() {
  local repo="$1"
  local secret_name="$2"
  local secret_value="$3"
  local dry_run="$4"

  if [ "$dry_run" = "1" ]; then
    log "Dry run: would set secret $secret_name on $repo"
    return 0
  fi

  printf '%s' "$secret_value" | gh secret set "$secret_name" --repo "$repo" >/dev/null
  log "Set secret $secret_name on $repo"
}

base64_file() {
  local file_path="$1"
  base64 < "$file_path" | tr -d '\n'
}

detect_signing_identity() (
  set -euo pipefail

  local cert_path="$1"
  local cert_password="$2"
  local requested_identity="${3:-}"
  local temp_dir keychain_path keychain_password identity

  temp_dir="$(mktemp -d)"
  keychain_path="$temp_dir/bootstrap-signing.keychain-db"
  keychain_password="$(uuidgen 2>/dev/null || echo bootstrap-signing-keychain)"

  cleanup() {
    security delete-keychain "$keychain_path" >/dev/null 2>&1 || true
    rm -rf "$temp_dir"
  }
  trap cleanup EXIT

  security create-keychain -p "$keychain_password" "$keychain_path" >/dev/null
  security set-keychain-settings -lut 21600 "$keychain_path" >/dev/null
  security unlock-keychain -p "$keychain_password" "$keychain_path" >/dev/null
  if ! security import "$cert_path" \
    -k "$keychain_path" \
    -P "$cert_password" \
    -T /usr/bin/security \
    -T /usr/bin/codesign >/dev/null 2>&1; then
    exit 1
  fi
  if ! security set-key-partition-list -S apple-tool:,apple:,codesign: -s -k "$keychain_password" "$keychain_path" >/dev/null 2>&1; then
    exit 1
  fi

  if [ -n "$requested_identity" ]; then
    if ! security find-identity -v -p codesigning "$keychain_path" \
      | awk -F'"' -v requested="$requested_identity" '
          index($0, requested) && $2 ~ /^Developer ID Application:/ { found = 1 }
          $2 == requested && $2 ~ /^Developer ID Application:/ { found = 1 }
          END { exit(found ? 0 : 1) }
        '; then
      exit 1
    fi
    printf '%s\n' "$requested_identity"
    exit 0
  fi

  identity="$(security find-identity -v -p codesigning "$keychain_path" | awk -F'"' '/"Developer ID Application:/ { print $2; exit }')"
  [ -n "$identity" ] || exit 1
  printf '%s\n' "$identity"
)

REPO=""
CERT_PATH=""
CERT_PASSWORD=""
SIGNING_IDENTITY=""
APPLE_ID_VALUE=""
APPLE_TEAM_ID_VALUE=""
APPLE_APP_SPECIFIC_PASSWORD_VALUE=""
ALLOW_HISTORY=1
SKIP_NOTARIZATION=0
DRY_RUN=0

while [ "$#" -gt 0 ]; do
  case "$1" in
    --repo)
      [ "$#" -ge 2 ] || die "Missing value for --repo"
      REPO="$2"
      shift 2
      ;;
    --cert-path)
      [ "$#" -ge 2 ] || die "Missing value for --cert-path"
      CERT_PATH="$2"
      shift 2
      ;;
    --cert-password)
      [ "$#" -ge 2 ] || die "Missing value for --cert-password"
      CERT_PASSWORD="$2"
      shift 2
      ;;
    --signing-identity)
      [ "$#" -ge 2 ] || die "Missing value for --signing-identity"
      SIGNING_IDENTITY="$2"
      shift 2
      ;;
    --apple-id)
      [ "$#" -ge 2 ] || die "Missing value for --apple-id"
      APPLE_ID_VALUE="$2"
      shift 2
      ;;
    --apple-team-id)
      [ "$#" -ge 2 ] || die "Missing value for --apple-team-id"
      APPLE_TEAM_ID_VALUE="$2"
      shift 2
      ;;
    --apple-app-specific-password)
      [ "$#" -ge 2 ] || die "Missing value for --apple-app-specific-password"
      APPLE_APP_SPECIFIC_PASSWORD_VALUE="$2"
      shift 2
      ;;
    --skip-notarization)
      SKIP_NOTARIZATION=1
      shift
      ;;
    --no-history)
      ALLOW_HISTORY=0
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown option: $1"
      ;;
  esac
done

require_command gh
require_command git
require_command python3
require_command security
require_command base64

REPO="$(resolve_repo "$REPO")"
gh auth status >/dev/null 2>&1 || die "GitHub CLI is not authenticated"
gh repo view "$REPO" >/dev/null 2>&1 || die "Cannot access GitHub repository $REPO with gh"

CERT_PATH="$(resolve_cert_path "$CERT_PATH")"
[ -n "$CERT_PATH" ] || die "Could not locate a local .p12/.pfx file"
[ -f "$CERT_PATH" ] || die "Certificate file not found: $CERT_PATH"

CERT_PASSWORD="$(resolve_value "$CERT_PASSWORD" MACOS_SIGN_CERT_PASSWORD "$ALLOW_HISTORY")"
[ -n "$CERT_PASSWORD" ] || die "Could not resolve MACOS_SIGN_CERT_PASSWORD. Pass --cert-password or export MACOS_SIGN_CERT_PASSWORD first."

SIGNING_IDENTITY="$(resolve_value "$SIGNING_IDENTITY" MACOS_SIGNING_IDENTITY "$ALLOW_HISTORY")"

log "Validating $CERT_PATH and importing it into a temporary keychain"
if ! SIGNING_IDENTITY="$(detect_signing_identity "$CERT_PATH" "$CERT_PASSWORD" "$SIGNING_IDENTITY")"; then
  die "Failed to import $CERT_PATH with the resolved certificate password, or it does not contain a Developer ID Application identity"
fi
log "Using signing identity: $SIGNING_IDENTITY"

CERT_B64="$(base64_file "$CERT_PATH")"
[ -n "$CERT_B64" ] || die "Failed to base64-encode $CERT_PATH"

set_secret "$REPO" MACOS_SIGN_CERT_B64 "$CERT_B64" "$DRY_RUN"
set_secret "$REPO" MACOS_SIGN_CERT_PASSWORD "$CERT_PASSWORD" "$DRY_RUN"
set_secret "$REPO" MACOS_SIGNING_IDENTITY "$SIGNING_IDENTITY" "$DRY_RUN"

if [ "$SKIP_NOTARIZATION" = "1" ]; then
  log "Skipping notarization secret setup by request"
  exit 0
fi

APPLE_ID_VALUE="$(resolve_value "$APPLE_ID_VALUE" APPLE_ID "$ALLOW_HISTORY")"
APPLE_TEAM_ID_VALUE="$(resolve_value "$APPLE_TEAM_ID_VALUE" APPLE_TEAM_ID "$ALLOW_HISTORY")"
APPLE_APP_SPECIFIC_PASSWORD_VALUE="$(resolve_value "$APPLE_APP_SPECIFIC_PASSWORD_VALUE" APPLE_APP_SPECIFIC_PASSWORD "$ALLOW_HISTORY")"

notary_count=0
[ -n "$APPLE_ID_VALUE" ] && notary_count=$((notary_count + 1))
[ -n "$APPLE_TEAM_ID_VALUE" ] && notary_count=$((notary_count + 1))
[ -n "$APPLE_APP_SPECIFIC_PASSWORD_VALUE" ] && notary_count=$((notary_count + 1))

if [ "$notary_count" -eq 0 ]; then
  log "No notarization credentials found. GitHub CI will sign macOS artifacts but skip notarization."
  exit 0
fi

if [ "$notary_count" -lt 3 ]; then
  warn "Notarization credentials are incomplete. GitHub CI will sign macOS artifacts but skip notarization."
  exit 0
fi

set_secret "$REPO" APPLE_ID "$APPLE_ID_VALUE" "$DRY_RUN"
set_secret "$REPO" APPLE_TEAM_ID "$APPLE_TEAM_ID_VALUE" "$DRY_RUN"
set_secret "$REPO" APPLE_APP_SPECIFIC_PASSWORD "$APPLE_APP_SPECIFIC_PASSWORD_VALUE" "$DRY_RUN"
log "Configured signing and notarization secrets for $REPO"
