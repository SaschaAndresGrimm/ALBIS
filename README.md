# ALBIS (ALBIS WEB VIEW)

ALBIS (ALBIS WEB VIEW) is a local, **ALBULA‑style** web viewer for large HDF5 stacks and diffraction data.

## Run (backend + frontend)

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r backend/requirements.txt
python backend/app.py
```

Open `http://localhost:8000` (ALBIS).  
To access from another device on the same network, use `http://<your-mac-ip>:8000`.

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

## Current Features

- **File handling**
  - File → Open: system file picker uploads `.h5/.hdf5` if not already in the data directory.
  - If the selected file already exists under the data directory, it is opened **in place** (no copy).
  - File → Close: clears current dataset.
  - Export PNG: saves the current frame.
- **Auto Load**
  - Watch folder: continuously loads the newest `.h5/.hdf5`, `.tif/.tiff`, or `.cbf` file.
  - File‑type filters for watch mode (HDF5 / TIFF / CBF).
  - Optional filename pattern (glob) filter for watch mode.
  - Browse… opens a native folder picker (macOS) to select absolute paths.
  - SIMPLON monitor: pulls the latest monitor image (TIFF) from the detector API with a live badge.
  - SIMPLON monitor fetches the detector pixel mask at start and applies it to monitor frames.
- **Dataset + frame navigation**
  - Dataset selection, frame slider, frame step, play/pause with FPS.
  - Toolbar buttons for previous/next/play.
- **Viewer**
  - Fit‑to‑window on first load and on demand.
  - Mouse‑wheel zoom (cursor‑centered), slider zoom, double‑click zoom.
  - Drag to pan.
  - Cursor overlay with **0‑indexed X/Y** and pixel value.
  - Pixel value overlay at high zoom (toggleable).
- **Overview panel**
  - Live overview image with viewport rectangle.
  - Drag to pan; drag handles to zoom. Alt/Option resizes around center.
- **Histogram**
  - Full‑frame histogram (excludes negative values and dtype saturation max).
  - Log X / Log Y toggles.
  - Auto‑contrast using log‑percentile stretch (0.1% → 99.9%).
  - Draggable background/foreground markers with tooltips.
- **ROI tools**
  - Line, box, circle, and annulus ROIs (right‑drag on the image).
  - Stats: min, max, mean, sum, std, pixel count.
  - Plots: line profile, X/Y projections, radial profile.
  - Axis labels, ticks, and hover readout values.
- **Masking**
  - Optional detector pixel mask from master files.
  - Gaps (bit 0) render as value 0; defective pixels (bits 1‑4) render blue.
- **Color maps**
  - Heat (default), HDR, Grey, Viridis, Magma, Inferno, Cividis, Turbo.
- **Rendering**
  - WebGL2 LUT pipeline with CPU fallback.

## Keyboard Shortcuts

- `⌘O` Open
- `⌘W` Close File
- `⌘S` Save As (Export PNG)
- `⌘E` Export
- `F1` Documentation

## API Endpoints

- `GET /api/files`
- `GET /api/folders`
- `GET /api/datasets?file=...`
- `GET /api/metadata?file=...&dataset=...`
- `GET /api/frame?file=...&dataset=...&index=...`
- `GET /api/preview?file=...&dataset=...&index=...`
- `GET /api/mask?file=...`
- `GET /api/autoload/latest?folder=...&pattern=...&exts=...`
- `GET /api/image?file=...`
- `GET /api/simplon/monitor?url=...`
- `GET /api/simplon/mask?url=...`
- `POST /api/simplon/mode?url=...&mode=enabled|disabled`
- `GET /api/choose-folder`
- `POST /api/upload`

## Notes

- Dataset `/entry/data/data` is auto‑preferred if present.
- External links in master files are supported if targets are inside the data directory.
- `hdf5plugin` is loaded to support compressed datasets.
- The main viewer and tools sidebar scroll independently.
- WebGL texture size limits may apply for very large frames.

## Next Milestones

1. Export to TIFF/PNG with metadata sidecar, batch export.
2. WebGL tiling for >16 MP frames.
3. Peak finder / spot tracking utilities.
