# ALBIS (ALBIS WEB VIEW)

ALBIS is an **ALBULA‑style**, browser‑based image viewer for diffraction data and large HDF5 stacks. It is platform‑independent, free, and open source.

It targets modern **DECTRIS** detectors (SELUN, EIGER2, PILATUS4) and supports **filewriter1** and **filewriter2** layouts, including multi‑threshold (multi‑channel) data.

Image sources can be:
- Files on disk (`.h5/.hdf5` stacks) and common detector image formats (`.tif/.tiff`, `.cbf/.cbf.gz`, `.edf`). Numbered series are treated like stacks.
- The detector **SIMPLON monitor** stream for live viewing.

ALBIS includes quick statistics tools, an HDF5 dataset inspector, and many small workflow optimizations.

Project note: this is a private vibe‑coding project for fun and educational purposes.

- Contributions welcome: see `CONTRIBUTING.md`.
- Security: see `SECURITY.md`.
- Developer architecture docs: `docs/ARCHITECTURE.md` and `docs/CODE_MAP.md`.

![ALBIS screenshot](frontend/ressources/albis.png)

## Highlights

- ALBULA‑style UI with fast navigation and contrast control.
- Full support for DECTRIS filewriter1 and filewriter2 (multi‑threshold data with selector).
- Live SIMPLON monitor mode with mask prefetch.
- ROI tools (line, box, circle, annulus) with statistics and plots.
- Pixel mask support (gaps and defective pixels).
- WebGL2 rendering with CPU fallback.
- spotfinding & resolution rings overlay

## Run (backend + frontend)

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
python backend/app.py
```

Open `http://localhost:8000` (ALBIS).

## Run Modes

- Python/source mode:
  Run directly from this repository with `python backend/app.py` (or `python albis_launcher.py`).
- Standalone mode:
  Use packaged artifacts created by the build scripts (no Python installation required on target machines).

## Architecture

ALBIS uses a server-client architecture:

- Backend server (FastAPI + Python):
  Loads detector/image data, handles monitor streams, computes metadata/analysis, and exposes REST endpoints.
- Frontend client (browser UI):
  Runs in the browser, renders images/overlays, and interacts with the backend over HTTP.
- Local deployment model:
  The backend typically runs on the same machine as the user, and the UI connects to `http://localhost:<port>`.

Detailed implementation notes:

- System architecture and data flows: `docs/ARCHITECTURE.md`
- Backend/frontend code navigation map: `docs/CODE_MAP.md`

## Configuration (`albis.config.json`)

ALBIS runtime settings are configured via `albis.config.json` (project root by default).

Example:

```json
{
  "server": {
    "host": "0.0.0.0",
    "port": 8000,
    "reload": false
  },
  "data": {
    "root": "/path/to/data",
    "allow_abs_paths": true,
    "scan_cache_sec": 2.0,
    "max_scan_depth": 3,
    "max_upload_mb": 0
  },
  "logging": {
    "level": "INFO",
    "dir": ""
  },
  "launcher": {
    "port": 0,
    "startup_timeout_sec": 5.0,
    "open_browser": true
  }
}
```

Notes:
- `data.root = ""` defaults to project root for source runs and `~/ALBIS-data` for packaged runs.
- `server.host = "0.0.0.0"` enables LAN access (`http://<your-ip>:8000`).
- `logging.dir = ""` writes logs to `<data.root>/logs/albis.log`.
- Packaged installs auto-create a default user config at `~/.config/albis/config.json` on first run (if no config is found).

## Logging

Log level and log directory are configured in `albis.config.json` under `logging.level` and `logging.dir`.

Frontend warnings/errors are forwarded to the backend log via `/api/client-log`.

## Packaging (PyInstaller)

ALBIS can be bundled into a **platform‑native app** (no Python required) using PyInstaller.

### Build (macOS)

```bash
./scripts/build_mac.sh
```

This produces versioned artifacts in `dist/`, e.g.:
- `ALBIS-macos-<os_version>-v0.4-<commit>.zip`
- `ALBIS-macos-<os_version>-v0.4-<commit>.dmg`

`build_mac.sh` also attempts to create a macOS `.app` bundle with icon support (from `frontend/ressources/icon.png`).
DMG images include an `Applications` shortcut for drag-and-drop installation.

### Build (Linux)

```bash
./scripts/build_linux.sh
```

Example output:
- `ALBIS-linux-<distro_version>-v0.4-<commit>.tar.gz`

### Build (Windows)

```powershell
.\scripts\build_windows.ps1
```

Example output:
- `ALBIS-windows-<os_version>-v0.4-<commit>.zip`
- Inno Setup installer (via `.\scripts\package_windows_innosetup.ps1`):
  `ALBIS-Setup-windows-<os_version>-v0.4-<commit>.exe`

### Output

The unpacked app payload is created under `dist/ALBIS/` (and on macOS additionally `dist/ALBIS.app`).
Use `albis.config.json` to change data path, host/port, logging, and launcher behavior.

## Keyboard Shortcuts

- `⌘O` Open
- `⌘W` Close File
- `⌘S` Save As
- `⌘E` Export
- `F1` Documentation
- `Tab` Play/Pause
- `←`/`→` Previous/Next frame
- `↑`/`↓` Jump by Step setting (or threshold change when multi‑threshold is active)

## Roadmap — Next Milestones

1. Detector control and status.
2. Installer for non‑Python users.
3. ...? =)

## Notes
- current focus of the development is server and client running on the same machine.

## Acknowledgements and Contributions
This projects stand on the shoulder of a giant: ALBULA. Thanks a lot to Volker Pilipp for creating such an intuitive image viewer which set for many the benchmark.
