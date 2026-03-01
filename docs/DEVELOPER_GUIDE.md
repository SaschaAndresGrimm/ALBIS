# Developer Guide

This guide is for developers who want to build, test, and contribute to ALBIS.
For general contribution guidelines, see `CONTRIBUTING.md`.

## Run (backend + frontend)

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
python backend/app.py
```

Open `http://localhost:8000` (ALBIS).

## Developer Quality Gates

Install dev tooling:

```bash
pip install -r requirements-dev.txt
npm ci
```

Run local checks:

```bash
ruff check backend tests scripts test_scripts
black --check tests scripts test_scripts
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 pytest --cov=backend --cov-report=term-missing --cov-report=xml --cov-fail-under=20
npm run lint:js
```

Optional pre-commit hooks:

```bash
pre-commit install
pre-commit run --all-files
```

CI runs on GitHub Actions.

## Architecture

ALBIS uses a server-client architecture:

- Backend server (FastAPI + Python):
  Loads detector/image data, handles monitor streams, computes metadata/analysis, and exposes REST endpoints.
- Frontend client (browser UI):
  Runs in the browser, renders images/overlays, and interacts with the backend over HTTP.
- Local deployment model:
  The backend typically runs on the same machine as the user, and the UI connects to `http://localhost:<port>`.

Detailed implementation notes:

- System architecture and data flows: `ARCHITECTURE.md`
- Backend/frontend code navigation map: `CODE_MAP.md`

## Packaging (PyInstaller)

ALBIS can be bundled into a **platform‑native app** (no Python required) using PyInstaller.

### Build (macOS)

```bash
./scripts/build_mac.sh
```

This produces versioned artifacts in `dist/`, e.g.:
- `ALBIS-macos-<os_version>-v<version>-<commit>.zip`
- `ALBIS-macos-<os_version>-v<version>-<commit>.dmg`

`build_mac.sh` also attempts to create a macOS `.app` bundle with icon support (from `frontend/ressources/icon.png`).
DMG images include an `Applications` shortcut for drag-and-drop installation.

### Build (Linux)

```bash
./scripts/build_linux.sh
```

Example output:
- `ALBIS-linux-<distro_version>-v<version>-<commit>.tar.gz`

Optional local desktop integration (user scope):

```bash
./scripts/install_linux.sh
```

This installs ALBIS under `~/.local` (launcher + desktop entry + icon).

To remove it again:

```bash
./scripts/uninstall_linux.sh
```

### Build (Windows)

```powershell
.\scripts\build_windows.ps1
```

Example output:
- `ALBIS-windows-<os_version>-v<version>-<commit>.zip`
- Inno Setup installer (via `.\scripts\package_windows_innosetup.ps1`):
  `ALBIS-Setup-windows-<os_version>-v<version>-<commit>.exe`

The Inno installer creates Start Menu entries for:
- `ALBIS`
- `Open Logs`
- `Open Data Folder`
- `Edit Config`

### Output

The unpacked app payload is created under `dist/ALBIS/` (and on macOS additionally `dist/ALBIS.app`).
Use `albis.config.json` to change data path, host/port, logging, and launcher behavior.

## Versioning and Releases

- Repository release version source of truth: `VERSION`
- Build metadata helper: `scripts/version_info.py`
- Human-readable release history: `CHANGELOG.md`
- Tag format for releases: `v<version>` (for example `v1.0.0`)
- Release execution checklist (including workflow dry-run): `RELEASE_CHECKLIST.md`
