# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

## [0.8.2] - 2026-02-24

### Added

- UI facelift baseline for the `0.8` milestone.

### Changed

- Backend/frontend architecture and tests expanded as part of the `0.7` to `0.8` refactoring track.

[Unreleased]: https://github.com/SaschaAndresGrimm/ALBIS/compare/v0.8.2...HEAD
[0.8.2]: https://github.com/SaschaAndresGrimm/ALBIS/releases/tag/v0.8.2
