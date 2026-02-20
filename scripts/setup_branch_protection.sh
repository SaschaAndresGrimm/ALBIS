#!/usr/bin/env bash
set -euo pipefail

BRANCH="${1:-main}"

if ! command -v gh >/dev/null 2>&1; then
  echo "error: GitHub CLI (gh) is required."
  exit 1
fi

if ! gh auth status >/dev/null 2>&1; then
  echo "error: gh is not authenticated. Run: gh auth login"
  exit 1
fi

REPO="${GITHUB_REPOSITORY:-}"
if [[ -z "${REPO}" ]]; then
  remote_url="$(git remote get-url origin 2>/dev/null || true)"
  if [[ "${remote_url}" =~ github\.com[:/]([^/]+)/([^/.]+)(\.git)?$ ]]; then
    REPO="${BASH_REMATCH[1]}/${BASH_REMATCH[2]}"
  fi
fi

if [[ -z "${REPO}" ]]; then
  echo "error: Could not determine repository. Set GITHUB_REPOSITORY=owner/repo."
  exit 1
fi

echo "Applying branch protection for ${REPO}:${BRANCH}"

gh api \
  --method PUT \
  -H "Accept: application/vnd.github+json" \
  "/repos/${REPO}/branches/${BRANCH}/protection" \
  --input - <<'JSON'
{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "Python (ubuntu-latest, py3.10)",
      "Python (macos-latest, py3.10)",
      "Python (windows-latest, py3.10)",
      "Frontend Lint"
    ]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": true,
    "required_approving_review_count": 1
  },
  "required_conversation_resolution": true,
  "restrictions": null
}
JSON

echo "Branch protection configured."
