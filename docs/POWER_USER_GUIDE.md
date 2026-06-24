# Power User Guide

This guide is for users who want to run the ALBIS server manually, use the Remote Stream API, or configure advanced settings.

## Run Modes

- **Python/source mode**:
  Run directly from this repository with `python backend/app.py` (or `python albis_launcher.py`).

## Run (backend + frontend)

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
python backend/app.py
```

By default `server.port` is `0`, so ALBIS auto-selects a free port at startup.
For `python backend/app.py`, read the startup URL printed by Uvicorn (for example `http://127.0.0.1:51243`) and open that URL.
For `python albis_launcher.py` or packaged app runs, ALBIS opens the browser automatically.

- **Standalone mode**:
  Use packaged artifacts created by the build scripts (no Python installation required on target machines).
- **Docker mode**:
  Build and run ALBIS inside a container (`Dockerfile` provided). See [Running via Docker](#running-via-docker) for details.

## Configuration

ALBIS runtime settings are configured via `albis.config.json` (project root by default).
JSON does not support comments, so a commented template is provided at `albis.config.example.jsonc`.
The machine-readable schema is `albis.config.schema.json`.

Config lookup order:

1. `<cwd>/albis.config.json`
2. `<app-dir>/albis.config.json` (packaged/frozen mode)
3. `<repo-root>/albis.config.json`
4. `~/.config/albis/config.json`

In packaged mode, if no config exists, ALBIS writes defaults to `~/.config/albis/config.json`.

## Data Export

Use **File -> Convert Dataset...** or `Ctrl+Shift+X` (`Cmd+Shift+X` on macOS) to batch-export the currently selected dataset or image series to TIFF or CBF.
For HDF5 inputs, select the dataset first, then choose all frames, the current frame, or a frame range.

Exported detector frames are written as signed integer images.
ALBIS follows the common detector convention where module-gap pixels are `-1` and bad or saturated pixels are `-2`.

TIFF exports include Dectris-style header metadata in private TIFF tag `0xC7F8` when matching metadata is present in the source image or HDF5 master file.
CBF exports use a miniCBF-style `_array_data.header_contents` block with the available detector, pixel-size, exposure, energy/wavelength, distance, beam-center, and rotation metadata.

### Rules

- Unknown top-level sections are rejected.
- Unknown keys inside known sections are rejected.
- Values are normalized and clamped where applicable (for example port range and UI limits).
- Missing fields use defaults.

### Settings Reference

#### `server`

- `host` (`string`, default `127.0.0.1`): Set to `"0.0.0.0"` to enable LAN access.
- `port` (`integer`, default `0`, clamped `0..65535`): Single port used by backend + launcher. `0` means auto-select a free port at startup.
- `reload` (`boolean`, default `false`)

#### `launcher`

- `startup_timeout_sec` (`number`, default `5.0`, minimum `0.1`)
- `open_browser` (`boolean`, default `true`)
- `debug_macos_events` (`boolean`, default `false`): Enables verbose macOS Dock/app event traces in launcher log.

#### `data`

- `root` (`string`, default `""`): Defaults to project root for source runs and `~/ALBIS-data` for packaged runs.
- `allow_abs_paths` (`boolean`, default `true`)
- `scan_cache_sec` (`number`, default `2.0`, minimum `0.0`)
- `max_scan_depth` (`integer`, default `-1`, minimum `-1`)
- `max_upload_mb` (`integer`, default `0`, minimum `0`)

#### `logging`

- `level` (`DEBUG|INFO|WARNING|ERROR|CRITICAL`, default `INFO`)
- `dir` (`string`, default `""`):
  - source mode default: `<data.root>/logs`
  - packaged/standalone default: `~/.config/albis/logs`

#### `ui`

- `tool_hints` (`boolean`, default `false`)
- `pixel_label_min_cell_px` (`integer`, default `18`, clamped `8..64`)
- `pixel_label_max_labels` (`integer`, default `4000`, clamped `100..100000`)
- `pixel_label_format` (`auto|integer|scientific`, default `auto`)
- `pixel_label_show_during_drag` (`boolean`, default `false`)

## Logging

Log level and log directory are configured in `albis.config.json` under `logging.level` and `logging.dir`.
ALBIS writes:

- Backend log: `<resolved log dir>/albis.log`
- Launcher log: `<resolved log dir>/launcher.log` (automatic rotation at ~1 MiB to `launcher.log.1`)

When `logging.dir` is empty:

- source mode uses `<data.root>/logs`
- packaged/standalone mode uses `~/.config/albis/logs`

Launcher host/port status is persisted in `~/.config/albis/server.json`.

Frontend warnings/errors are forwarded to the backend log via `/api/client-log`.

## Running via Docker

ALBIS includes a `Dockerfile` for containerized deployments. The container binds to `0.0.0.0` internally so the host can publish the port, but the supported `1.0` posture is still local-first and trusted-network use only.
Public internet exposure is **not** a supported deployment mode for `1.0`.

### Build a Local Image (development/local changes)

```bash
docker build -t albis:latest .
```

Local `docker build` remains fully supported and is recommended when you want to test uncommitted or branch-local changes.

### Pull a Published GHCR Image

Published container images are available at:

- `ghcr.io/<owner>/albis`

For this repository, `<owner>` is the lowercase GitHub owner name (currently `saschaandresgrimm`).

Examples:

```bash
docker pull ghcr.io/saschaandresgrimm/albis:latest
docker pull ghcr.io/saschaandresgrimm/albis:v1.0.0
```

### Published Tags and Architectures

Release publishing provides these tags:

- `vX.Y.Z`
- `vX.Y`
- `vX`
- `latest`
- `sha-<shortsha>`

Published images are multi-arch:

- `linux/amd64`
- `linux/arm64`

### Run the Container

To use ALBIS properly in a container, you should mount your data directory so the server can see your HDF5 or image files. Optionally, you can also mount a custom `albis.config.json` and a `logs/` directory.

Example run command (localhost publish):

```bash
docker run -d \
  --name albis \
  -p 127.0.0.1:8000:8000 \
  -v /path/to/your/data:/app/data:ro \
  ghcr.io/saschaandresgrimm/albis:latest
```

If you built locally instead of pulling from GHCR, replace the image reference with `albis:latest`.

*Note: In the example above, `/path/to/your/data` is mounted into the container at `/app/data` as read-only (`:ro`). In the default ALBIS configuration (`albis.config.json`), the `data.root` is already set to `./data`, so ALBIS will immediately see your mounted files.*

If you intentionally expose the container on a trusted LAN, do it behind your own network controls and treat it as a lab-managed deployment rather than a public service.

When ALBIS is opened through a localhost backend, drag-and-drop file uploads are disabled to avoid copying detector data into a second location. Use **File -> Open...** or the browser panel to open files directly from the configured data path instead. Non-local browser sessions can still use drag-and-drop as an upload workflow, but read-only data mounts should be opened from their mounted path rather than uploaded.

## Remote Stream API

ALBIS can ingest externally generated frames and metadata when Data Source is set to `Remote Stream`.

### Endpoints

- `POST /api/remote/v1/frame`
  - Query:
    - `source_id` (optional, default `default`)
    - `seq` (optional sequence number)
  - Multipart fields:
    - `image` (required): raw frame bytes or encoded image bytes
    - `meta` (optional): JSON string
- `GET /api/remote/v1/latest`
  - Query:
    - `source_id`
    - `after_seq` (optional; returns `204` if no new frame)
  - Returns frame bytes and `X-Remote-*` headers
- `GET /api/remote/v1/meta`
  - Query:
    - `source_id`
    - `seq` (optional)
  - Returns parsed metadata including `peak_sets`

### Supported `meta` keys

- Decode settings:
  - `format`: `raw`, `tiff`, `cbf`, `cbf.gz`, `edf`
  - `dtype` and `shape` (required for `raw`)
- Display:
  - `display_name`, `series_number`, `image_number`, `image_datetime`
- Resolution ring parameters:
  - `distance_mm`, `pixel_size_um`, `energy_ev`, `wavelength_a`
  - `beam_center_x`, `beam_center_y` or `resolution.beam_center_px: [x, y]`
- Overlay peak lists:
  - `peak_sets`: list of `{name, color, points}` where points are `[x, y]` or `[x, y, intensity]`

### Minimal sender example

The example below targets port `8000`. Note that the default `server.port` is `0`
(auto-select), so a source-mode server picks a random free port at startup. Either set
`server.port` to a fixed value in `albis.config.json`, or read the port ALBIS prints at
startup and use that here.

```python
import json
import requests
import numpy as np

PORT = 8000  # match albis.config.json server.port, or the port printed at startup
SOURCE_ID = "default"

frame = (np.random.rand(512, 512) * 1000).astype("<u2")
meta = {
    "format": "raw",
    "dtype": "<u2",
    "shape": [512, 512],
    "display_name": "Remote demo frame",
    "series_number": 1,
    "image_number": 42,
    "resolution": {
        "distance_mm": 150.0,
        "pixel_size_um": 75.0,
        "energy_ev": 12000.0,
        "beam_center_px": [256, 256]
    },
    "peak_sets": [
        {"name": "predicted", "color": "#00ff88", "points": [[240, 250], [270, 265]]}
    ]
}

requests.post(
    f"http://127.0.0.1:{PORT}/api/remote/v1/frame?source_id={SOURCE_ID}",
    data={"meta": json.dumps(meta)},
    files={"image": ("frame.raw", frame.tobytes(), "application/octet-stream")},
    timeout=5,
).raise_for_status()
```

### Quick local smoke test

`test_scripts/stream_ingest.py` posts one synthetic frame to the backend:

```bash
python test_scripts/stream_ingest.py
```

Important: the script `source_id` must match the UI `Remote Stream` source id (default `default`).

## JUNGFRAUJOCH Preview

`JUNGFRAUJOCH Preview` mode subscribes to a JUNGFRAUJOCH ZeroMQ preview PUB stream, decodes the
CBOR image messages, maps `spots` to peak overlays, and exposes the result through the same
Remote Stream endpoints (`/api/remote/v1/latest`, `/api/remote/v1/meta`) under the configured
`source_id`.

You can select this mode from the UI (Data tab), or drive the backend subscription directly:

### Endpoints

- `POST /api/jfjoch/preview/start` — start or reconfigure the subscription
  - JSON body:
    - `endpoint` (required): ZeroMQ PUB endpoint, e.g. `tcp://host:5555`
    - `source_id` (optional, default `jungfraujoch`)
    - `topic` (optional): SUB topic filter
    - `channel` (optional): preview channel selector
- `POST /api/jfjoch/preview/stop` — stop the active subscription worker
- `GET /api/jfjoch/preview/status` — current worker state and ingest counters

Once started, poll the Remote Stream endpoints with the same `source_id` to read frames,
metadata, and reflection overlays.

## Handoff API

ALBIS can accept external handoff jobs (for example from upstream processing orchestration) and auto-open resulting files in the frontend.

### Endpoints

- `POST /api/handoff/v1/jobs`
  - JSON body:
    - `manifest_path` (required): path to a manifest `.json` readable by the backend host
  - Returns `HandoffJobResponse` with parsed `open_path`, `dataset`, and `run_id`
- `GET /api/handoff/v1/jobs/latest`
  - Query:
    - `after_id` (optional, default `0`)
  - Returns `200` with newest job where `id > after_id`
  - Returns `204` when no newer job exists

### Manifest parsing notes

- ALBIS reads `run_id` and `outputs` from the provided JSON file.
- Preferred target is the first output with `kind == "master_h5"`.
- If no `master_h5` exists, ALBIS falls back to the first output entry.
- Dataset defaults to `/entry/data/data` when omitted for `master_h5`.

### Queue behavior

- Jobs are stored in an in-memory bounded queue (max `1024`).
- When full, oldest jobs are evicted.
- Job IDs stay monotonic.
