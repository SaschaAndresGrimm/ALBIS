---
name: release
description: Cut and publish an ALBIS release - bump the version, date the changelog, write release notes, tag, and follow the GitHub release build until it is published. Run only when the user asks for a release.
disable-model-invocation: true
argument-hint: "X.Y.Z [release name]"
---

# Releasing ALBIS

Policy lives in `docs/RELEASE_CHECKLIST.md` and wins over anything here. This is
how to carry it out without rediscovering it. A release is outward-facing: it
publishes binaries and moves "Latest", so it happens only on the user's request.

## Ground rules

- Commit and tag as the configured git user, with no co-author trailer: that is
  the repo's convention for release commits.
- Version: `Added` or `Changed` under `[Unreleased]` means a minor release,
  `Fixed` alone a patch. If the user gave none, propose one and confirm.
- Never delete or move a pushed tag to retry. Re-run failed jobs instead.

## Shell pitfalls on this machine (zsh)

- Quote anything with `?` or `*` that is not a glob: `gh api "repos/...?ref=x"`.
- Never `echo =====`: zsh expands `=word` to a command path and fails.
- `git rev-parse origin/main` can fail as ambiguous; use `refs/remotes/origin/main`.
- Python: `.venv/bin/python`, `.venv/bin/pytest`. Node: prefix
  `PATH=/opt/homebrew/opt/node@22/bin:$PATH` (anaconda shadows both).

## 1. Pre-flight - all must hold

```bash
git fetch -q origin
git status --short                                   # empty, or only the user's intended changes
[ "$(git rev-parse refs/remotes/origin/main)" = "$(git rev-parse HEAD)" ] && echo in-sync
gh run list --commit "$(git rev-parse HEAD)" --workflow ci.yml --json conclusion --jq '.[0].conclusion'   # success
gh api repos/SaschaAndresGrimm/ALBIS/dependabot/alerts --jq '[.[]|select(.state=="open")]|length'   # 0
.venv/bin/python scripts/sync_licence_table.py --check
```

If CI is still running, wait for it. If an alert is open or the licence table
is stale, stop and report; do not release around it. Read the `[Unreleased]`
section of `CHANGELOG.md`: it becomes the release's record and must read as such.

## 2. Bump

```bash
.venv/bin/python scripts/bump_version.py X.Y.Z --dry-run   # review the diff
.venv/bin/python scripts/bump_version.py X.Y.Z
```

It sets `VERSION`, `package.json`, both versions in `package-lock.json`,
`pyproject.toml`, `CITATION.cff` (version and `date-released`), moves
`[Unreleased]` into a dated section and adds the compare link. It refuses an
empty `[Unreleased]`, a version that is not newer, and any file whose layout
drifted; fix the cause, do not edit around it.

## 3. Release notes - `release-notes/vX.Y.Z.md`

The GitHub release body. Read the latest file in `release-notes/` for tone,
then write:

- one opening line saying what the release is for (bold the release name if
  the user gave one);
- `### Added` / `### Changed` / `### Fixed` with short, user-facing bullets,
  each with a bold lead. Taken from the changelog section, but without internals:
  no module names, tests or CI mechanics;
- `---`, then one paragraph on who should update, and where to find anything
  that moved.

## 4. Verify, commit, tag

```bash
.venv/bin/pytest -q -p no:cacheprovider tests/test_version_consistency.py tests/test_documentation_consistency.py tests/test_bump_version.py
git add VERSION package.json package-lock.json pyproject.toml CITATION.cff CHANGELOG.md release-notes/vX.Y.Z.md
git commit -m "release: X.Y.Z"
git push -q origin main
git tag -a vX.Y.Z -m "ALBIS X.Y.Z"
git push -q origin vX.Y.Z
```

Run the full suites first if anything besides the release files changed.

## 5. Follow the build

The tag starts `.github/workflows/release.yml`: verify, Linux/Windows/macOS
builds, the Linux smoke tests (Rocky 8 and 9, Ubuntu 22.04 and 24.04), then
publish. About 20 minutes. Watch it in the background:

```bash
id=$(gh run list --workflow release.yml --limit 3 --json databaseId,headBranch --jq '[.[]|select(.headBranch=="vX.Y.Z")][0].databaseId')
gh run watch "$id" --exit-status --interval 30 >/dev/null 2>&1; gh run view "$id" --json jobs --jq '.jobs[]|"\(.name): \(.conclusion)"'
```

When it succeeds, confirm with `gh release view vX.Y.Z`: not a draft, marked
Latest, 15 assets (Linux tarball, AppImage and bundle with `.sig` each, macOS
arm64 and x64 `.dmg` and `.zip`, Windows setup `.exe` and `.zip`, SBOM,
`SHA256SUMS.txt` and its `.sig`).

A release name goes on afterwards; the workflow sets none:
`gh release edit vX.Y.Z --title "vX.Y.Z — Name"`.

## Known failures

- **"Publish GitHub Release: skipped"** - a build failed and nothing was
  published; "Latest" is unchanged. Find the failed step:
  `gh run view "$id" --json jobs --jq '.jobs[]|select(.conclusion=="failure")|.name'`
  and its log via `gh api --allow-escape-sequences repos/SaschaAndresGrimm/ALBIS/actions/jobs/<job>/logs`.
- **macOS notarization `HTTP status code: 403. A required agreement is missing or
  has expired`** - Apple published a new Developer Program License Agreement.
  Only the team's Account Holder can accept it, at
  https://developer.apple.com/account. Ask the user to, then
  `gh run rerun "$id" --failed`. A second 403 minutes after accepting is
  Apple's propagation delay: re-run the failed job again. Publish runs by itself
  once every build passes; the tag does not change.
- **A Linux smoke test fails** - a real regression on that distribution. Do not
  publish around it; report it.

## Report

Version, release commit, release URL, asset count, and anything that needed a
re-run and why.
