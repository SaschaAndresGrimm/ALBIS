# ALBIS (ALBIS WEB VIEW)

ALBIS is an **ALBULA‑style**, browser‑based image viewer for diffraction data and large HDF5 stacks. It is platform‑independent, free, and open source.

It targets modern **DECTRIS** detectors (SELUN, EIGER2, PILATUS4) and supports **filewriter1** and **filewriter2** layouts, including multi‑threshold (multi‑channel) data.

Image sources can be:
- Files on disk (`.h5/.hdf5`, `.tif/.tiff`, `.cbf`).
- The detector **SIMPLON monitor** stream for live viewing.

ALBIS includes quick statistics tools, an HDF5 dataset inspector, and many small workflow optimizations.

Project note: this is a private vibe‑coding project for fun and educational purposes.

- Contributions welcome: see `CONTRIBUTING.md`.
- Security: see `SECURITY.md`.

## Highlights

- ALBULA‑style UI with fast navigation and contrast control.
- Full support for DECTRIS filewriter1 and filewriter2 (multi‑threshold data with selector).
- Live SIMPLON monitor mode with mask prefetch.
- ROI tools (line, box, circle, annulus) with statistics and plots.
- Pixel mask support (gaps and defective pixels).
- WebGL2 rendering with CPU fallback.

## Run (backend + frontend)

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
python backend/app.py
```

Open `http://localhost:8000` (ALBIS).

To allow LAN access:

```bash
ALBIS_HOST=0.0.0.0 python backend/app.py
```

Then use `http://<your-ip>:8000` from another device.

## Data Location

By default the backend scans the project root (recursively) for `*.h5` / `*.hdf5`.
Override with:

```bash
VIEWER_DATA_DIR=/path/to/data python backend/app.py
```

Absolute folder watching is enabled by default for Auto Load. You can disable it with:

```bash
VIEWER_ALLOW_ABS=0 python backend/app.py
```

For large directory trees, you can tune scanning:

```bash
# cache directory scans (seconds)
ALBIS_SCAN_CACHE_SEC=2

# limit recursive scan depth (-1 = unlimited)
ALBIS_MAX_SCAN_DEPTH=3
```

## Logging

Logs are written to `<VIEWER_DATA_DIR>/logs/albis.log` by default.
When using the launcher, `VIEWER_DATA_DIR` defaults to `~/ALBIS-data`.

You can configure:

```bash
# log level: DEBUG, INFO, WARNING, ERROR
ALBIS_LOG_LEVEL=INFO

# log directory
ALBIS_LOG_DIR=/path/to/logs
```

Frontend warnings/errors are forwarded to the backend log via `/api/client-log`.

## Packaging (PyInstaller)

ALBIS can be bundled into a **platform‑native app** (no Python required) using PyInstaller.

### Build (macOS)

```bash
./scripts/build_mac.sh
```

### Build (Linux)

```bash
./scripts/build_linux.sh
```

### Build (Windows)

```powershell
.\scripts\build_windows.ps1
```

### Output

The packaged app is created under `dist/ALBIS/`.

By default, the launcher uses `~/ALBIS-data` as a writable data directory.
Override it with:

```bash
ALBIS_DATA_DIR=/path/to/data ./dist/ALBIS/ALBIS
```

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

1. Resolution rings.
2. Spot finding.
3. Config file.
4. Detector control and status.
5. Installer for non‑Python users.
6. Make it mobile friendly

## Notes
- WebGL texture size limits may apply for very large frames.
