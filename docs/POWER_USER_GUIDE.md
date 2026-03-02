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

Open `http://localhost:8000` (ALBIS).


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

### Rules

- Unknown top-level sections are rejected.
- Unknown keys inside known sections are rejected.
- Values are normalized and clamped where applicable (for example port range and UI limits).
- Missing fields use defaults.

### Settings Reference

#### `server`
- `host` (`string`, default `127.0.0.1`): Set to `"0.0.0.0"` to enable LAN access.
- `port` (`integer`, default `8000`, clamped `0..65535`): Single port used by backend + launcher.
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
- `dir` (`string`, default `""`): Writes logs to `<data.root>/logs/albis.log` when empty.

#### `ui`
- `tool_hints` (`boolean`, default `false`)
- `pixel_label_min_cell_px` (`integer`, default `18`, clamped `8..64`)
- `pixel_label_max_labels` (`integer`, default `4000`, clamped `100..100000`)
- `pixel_label_format` (`auto|integer|scientific`, default `auto`)
- `pixel_label_show_during_drag` (`boolean`, default `false`)

## Logging

Log level and log directory are configured in `albis.config.json` under `logging.level` and `logging.dir`.
Launcher startup logs are also written to `~/.config/albis/launcher.log` (with automatic rotation at ~1 MiB to `launcher.log.1`).

Frontend warnings/errors are forwarded to the backend log via `/api/client-log`.

## Running via Docker

ALBIS includes a `Dockerfile` for containerized deployments. By default, the image exposes port `8000` and forces the server to bind to `0.0.0.0` so it is accessible from the host network.

### Build the Image

```bash
docker build -t albis:latest .
```

### Run the Container

To use ALBIS properly in a container, you should mount your data directory so the server can see your HDF5 or image files. Optionally, you can also mount a custom `albis.config.json` and a `logs/` directory.

Example run command:

```bash
docker run -d \
  --name albis \
  -p 8000:8000 \
  -v /path/to/your/data:/app/data:ro \
  albis:latest
```

*Note: In the example above, `/path/to/your/data` is mounted into the container at `/app/data` as read-only (`:ro`). In the default ALBIS configuration (`albis.config.json`), the `data.root` is already set to `./data`, so ALBIS will immediately see your mounted files.*

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

```python
import json
import requests
import numpy as np

PORT = 8000
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
