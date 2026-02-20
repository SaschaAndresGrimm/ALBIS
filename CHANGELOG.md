# Changelog

All notable changes to ALBIS are documented in this file.

## [0.6.1] - 2026-02-18

### Added
- GitHub Actions CI for Python (Linux/macOS/Windows) and frontend lint.
- Developer tooling baseline: Ruff, Black, ESLint, pre-commit config.
- API smoke tests for health and remote stream endpoints.
- OSS collaboration templates: issue forms, PR template, `CODEOWNERS`.
- Branch protection setup script: `scripts/setup_branch_protection.sh`.

### Changed
- Pinned Black version in dev requirements for deterministic formatting in CI.
- Expanded contributor documentation for local quality checks.

## [0.6.0] - 2026-02-17

### Added
- Remote Stream API (`/api/remote/v1/*`) for frame + metadata ingest.
- Support for metadata-driven overlays from remote producers.

