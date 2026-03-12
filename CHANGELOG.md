# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.8.12] - 2026-03-12

### Added

- Dedicated Docker CI/release workflow (`.github/workflows/docker.yml`) with:
  - PR/branch validation build for `linux/amd64`.
  - Container smoke checks for `/api/health` and `/api/files` against mounted test data.
  - Tag-triggered GHCR publish pipeline for multi-arch images (`linux/amd64`, `linux/arm64`).
  - Release-blocking Trivy vulnerability gate for `HIGH,CRITICAL` findings.
- Docker documentation updates covering GHCR pull/run usage and published image tags.

### Fixed

- Docker backend-log UX now works in browser:
  - Help -> backend log keeps desktop open behavior when available.
  - Container/headless environments now fall back to opening `/api/log-file` in a new tab.

### Changed

- Docker image startup now runs via `uvicorn backend.app:app`, fixing package import issues in containers.
- Docker build dependencies now include native compiler toolchain support required for `dectris-compression` builds on ARM.

### Notes

- This release is called: "Container? Consider it contained."
- This release is called: "From 'it builds on my machine' to 'it builds in every machine-shaped box.'"

## [0.8.10] - 2026-03-10

### Added

- `VERSION` as repository version source of truth.
- `albis.config.schema.json` for configuration schema validation in tooling.
- `albis.config.example.jsonc` as commented configuration template.
- `docs/configuration.md` with config lookup/normalization behavior and field reference.
- Release and security automation via GitHub workflows.
- Dependabot configuration for Python, npm, and GitHub Actions.
- `docs/RELEASE_CHECKLIST.md` with release dry-run and tagging steps.
- `docs/API_CONTRACTS.md` with binary-header semantics, status-code behavior, and integration guidance.
- API contract tests for strict request-body validation.
- Extended OpenAPI contract tests for binary payload/header endpoints and remote meta conflict response.

### Changed

- Backend runtime version now loads from `VERSION` via `backend/version.py`.
- Build metadata helper (`scripts/version_info.py`) now reads from `VERSION`.
- Frontend UI version display no longer hardcodes a static release value.
- Config normalization now rejects unknown sections/keys and invalid section value types.
- `system`, `analysis`, `stream`, `files`, `hdf5`, and `frames` routes now use explicit Pydantic request/response models.
- Binary download endpoints now publish explicit OpenAPI payload/header contracts (`octet-stream`, CSV, and metadata headers).
- Backend path-resolution and route workflows now include focused docstrings for maintainability and contributor onboarding.
- CI and release verification now enforce backend coverage with `pytest-cov` and `--cov-fail-under=20`.
- `docs/CODE_MAP.md` now reflects modular route/service architecture and contribution touchpoints.

## [0.8.9] - 2026-03-09

### Added

- Linux release assets now include `install_linux_appimage.sh` and `uninstall_linux.sh` so AppImage distribution is self-contained.

### Changed

- Windows installer now defaults to per-user install (`%LOCALAPPDATA%\Programs\ALBIS`) and runs without admin elevation.
- Linux AppImage packaging now includes the full PyInstaller runtime payload (including `_internal`), fixing missing `libpython3.10.so.1.0` at runtime.
- Artifact/release workflows now build and validate both Linux `.tar.gz` + `.AppImage` and Windows `.zip` + installer `.exe`.
- Release publish workflow now performs stronger asset checks and multi-step retry backoff for transient GitHub rate limits.
- Distribution variants now ship as architecture-explicit assets (`linux-x64`, `windows-x64`, `macos-arm64`, `macos-x64`) with stable target naming.
- Release and artifacts workflows now publish both installer and portable variants per supported OS.
- Packaging pipeline now includes per-artifact integrity checks, cross-platform packaged-binary smoke tests, and optional signing/notarization hooks.

### Notes

- This release is called: "No more scavenger hunts for missing runtime files."
- This release is called: "Now with fewer mysteries and more binaries."

## [0.8.2] - 2026-02-24

### Added

- UI facelift baseline for the `0.8` milestone.

### Changed

- Backend/frontend architecture and tests expanded as part of the `0.7` to `0.8` refactoring track.

[Unreleased]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.8.12...HEAD
[0.8.12]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.12
[0.8.11]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.11
[0.8.10]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.10
[0.8.9]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.9
[0.8.2]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.2
