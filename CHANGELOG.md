# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.1] - 2026-03-19

### Added

- Added geometry-aware resolution rings for Dectris Pilatus 12M CBF images, using DIALS `imported.expt` detector geometry for the C-shaped vacuum detector layout.
- Added installer-path smoke coverage for Windows installers and mounted macOS DMGs in artifact/release workflows.
- Added pinned AppImage installation tooling with checksum verification for Linux packaging workflows.

### Changed

- Pilatus 12M geometry mode now supports manual beam-center and detector-distance adjustments, and default rings now use `1.0`, `3.67`, and `11.01 Å`.
- Raised enforced backend coverage gates from `20%` to `50%` across CI, artifact validation, release verification, and contributor docs.
- Pinned backend runtime dependencies and PyInstaller build inputs for more reproducible source, packaging, and Docker builds.
- Refactored the Docker image to a pinned multi-stage build that runs as a dedicated non-root user.
- Updated README, release docs, and the power-user guide to position ALBIS `1.0` as a local-first desktop app for localhost and trusted lab/LAN use.
- Tag releases now require Linux signing credentials, while Windows code signing and macOS signing/notarization remain optional unless configured.

### Fixed

- Fixed FileWriter2 pixel mask loading.
- Fixed Docker runtime file access when mounted data is read-only.

### Security

- Security reporting now points to GitHub private vulnerability reporting instead of public issues, with supported-version and response-target guidance.

### Notes

- This release is called: "Bent detector, straight rings."
- This release is called: "Curved hardware, less curved release math."

## [0.9.0] - 2026-03-13

### Added

- Added a translation review workflow with glossary, allowlist, technical-domain reporting, and CI visibility for locale QA.
- Added committed desktop icon assets for Linux, Windows, and macOS packaging, including prebuilt `.ico` and `.icns` bundles.

### Changed

- Refined technical terminology and UI wording across German, Swiss German, Swedish, Danish, French, Spanish, Italian, Portuguese, Japanese, and Simplified Chinese locales.
- Updated desktop packaging and runtime asset wiring to use the new icon set consistently across app builds and frontend-served assets.

### Fixed

- macOS x64 release packaging now retries DMG creation when `hdiutil` hits a transient `Resource busy` failure on GitHub runners.

### Notes

- This release is called: "Icons aligned, translations sharpened."
- This release is called: "From workflow dry-run to release-ready."

## [0.8.17] - 2026-03-13

### Added

- Frontend locale coverage now includes `en`, `zh-CN`, `ja`, `fr`, `es`, `it`, `pt`, `rm`, `de`, `sv`, `da`, `mi`, and `gsw`.
- Added first-pass UI translations for French, Spanish, Italian, Portuguese, Romansh, German, Swedish, Danish, Māori, and Swiss German.

### Changed

- Swiss German (`gsw`) copy now reads more consistently in Basel-style wording across the splash screen, menus, and series workflow.
- German locale wording now uses informal phrasing throughout the UI.

### Fixed

- Startup language selection now respects persisted backend preferences instead of sticking to an inferred browser fallback.
- Language switching now refreshes playback controls, splash-screen status text, and dynamic file/dataset placeholders immediately.
- Added locale integrity coverage for key parity, placeholder-token parity, and expanded alias normalization across all shipped locales.

### Notes

- This release is called: "One beamline, thirteen tongues."
- This release is called: "Kia ora, Grüezi, bonjour, and please mind the diffraction peaks."

## [0.8.16] - 2026-03-13

### Added

- Multilingual UI support across key viewer surfaces with `en`, `zh-CN`, and `ja` locales.
- Expanded localization for menus, tooltips, inspector/data panels, playback dropdowns, ROI overlays, and cursor readouts.

### Fixed

- Language switching now refreshes dynamic ROI labels immediately (no follow-up click required).
- Locale test fixture path resolution is now CI-safe across environments.
- Locale-specific CJK font fallback order now prefers Japanese glyph forms under `ja`.

### Notes

- This release is called: "Konnichiwa, Ni Hao, and welcome to ALBIS."
- This release is called: "One viewer, three languages, zero passport control."

## [0.8.15] - 2026-03-12

### Fixed

- Docker GHCR publish no longer fails on Trivy setup/runtime drift.
- Trivy release gate now ignores unfixed vulnerabilities while still failing on fixable `HIGH/CRITICAL` findings.
- Docker image build now upgrades `setuptools`, `wheel`, and `jaraco.context` to patched versions required by the Trivy gate.

### Notes

- This release is called: "Now scanning, still shipping."
- This release is called: "Red gate, green build."

## [0.8.14] - 2026-03-12

### Fixed

- Docker release publishing no longer fails in the Trivy setup phase due to stale Trivy binary resolution.
- Updated Docker publish scanning to use current `aquasecurity/trivy-action` with `version: latest`.

### Notes

- This release is called: "Scan me maybe."
- This release is called: "No more tripping before the Trivy starts."

## [0.8.13] - 2026-03-12

### Fixed

- Docker drag-and-drop now works even when data is mounted read-only (`:ro`).
- Upload handling now falls back to a writable container temp path (`/tmp/albis-uploads`) when `/app/data` cannot be written.
- Added regression coverage for read-only upload targets to prevent future Docker regressions.

### Notes

- This release is called: "Drop it like it's docked."
- This release is called: "Read-only mount, write-capable mood."

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

[Unreleased]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.9.1...HEAD
[0.9.1]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.1
[0.9.0]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.9.0
[0.8.17]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.17
[0.8.16]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.16
[0.8.15]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.15
[0.8.14]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.14
[0.8.13]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.13
[0.8.12]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.12
[0.8.11]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.11
[0.8.10]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.10
[0.8.9]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.9
[0.8.2]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.2
