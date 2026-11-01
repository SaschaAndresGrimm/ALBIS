"""MYTHEN(2) strip-detector acquisitions read as a (frames, channels) image."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

from ..image_formats import _image_ext_name, _normalize_image_array
from .image_values import _first_number

# ---------------------------------------------------------------------------
# MYTHEN(2) strip-detector acquisitions
#
# A MYTHEN acquisition is a folder holding one XML ``.cfg`` descriptor plus one
# ``FrameNNNN.dat`` file per exposure. Each ``.dat`` is ASCII "<channel> <count>"
# pairs describing a single 1D strip readout. We assemble the whole acquisition
# into a 2D array of shape (frames, channels) so it renders through the standard
# image pipeline as an intensity map (x = channel, y = frame, color = counts).
# ---------------------------------------------------------------------------

_MYTHEN_FRAME_RE = re.compile(r"(\d+)\.dat$", re.IGNORECASE)

# folder key -> (signature, array, metadata)
_mythen_cache: dict[str, tuple[tuple[int, float], np.ndarray, dict[str, Any]]] = {}


def _mythen_frame_files(folder: Path) -> list[Path]:
    """Return the acquisition's frame files sorted by their numeric index."""
    indexed: list[tuple[int, Path]] = []
    try:
        entries = list(folder.iterdir())
    except OSError:
        return []
    for entry in entries:
        if not entry.is_file():
            continue
        match = _MYTHEN_FRAME_RE.search(entry.name)
        if not match:
            continue
        indexed.append((int(match.group(1)), entry))
    indexed.sort(key=lambda item: item[0])
    return [path for _, path in indexed]


def _resolve_mythen_acquisition(path: Path) -> tuple[Path, Path | None]:
    """Resolve a ``.cfg`` or ``.dat`` path to (folder, cfg_path)."""
    ext = _image_ext_name(path.name)
    folder = path.parent
    if ext == ".cfg":
        return folder, path
    cfgs = sorted(folder.glob("*.cfg"))
    return folder, (cfgs[0] if cfgs else None)


def _parse_mythen_cfg(cfg_path: Path | None) -> dict[str, Any]:
    """Extract detector/acquisition metadata from a MYTHEN ``.cfg`` file."""
    meta: dict[str, Any] = {"detector": "MYTHEN"}
    if cfg_path is None or not cfg_path.is_file():
        return meta
    import xml.etree.ElementTree as ET

    try:
        root = ET.parse(cfg_path).getroot()
    except (ET.ParseError, OSError):
        return meta

    def _text(tag: str) -> str | None:
        el = root.find(tag)
        if el is None or el.text is None:
            return None
        text = el.text.strip()
        return text or None

    channels = _first_number(_text("channels"))
    if channels is not None:
        meta["channels"] = int(channels)
    frames = _first_number(_text("frames"))
    if frames is not None:
        meta["frames_declared"] = int(frames)
    energy_kev = _first_number(_text("energy"))
    if energy_kev is not None:
        meta["energy_ev"] = float(energy_kev) * 1000.0
    exposure_ms = _first_number(_text("exposureTime"))
    if exposure_ms is not None:
        meta["exposure_time_s"] = float(exposure_ms) / 1000.0
    period_ms = _first_number(_text("exposurePeriod"))
    if period_ms is not None:
        meta["exposure_period_s"] = float(period_ms) / 1000.0
    system_number = _text("systemNumber")
    if system_number:
        meta["system_number"] = system_number
    server_version = _text("serverVersion")
    if server_version:
        meta["server_version"] = server_version

    bad_channels: list[int] = []
    bad_root = root.find("badChannels")
    if bad_root is not None:
        for bad in bad_root.findall("bad"):
            value = _first_number(bad.text)
            if value is not None:
                bad_channels.append(int(value))
    if bad_channels:
        meta["bad_channels"] = sorted(set(bad_channels))

    module = root.find("modules/module")
    if module is not None:

        def _mod_number(tag: str) -> float | None:
            el = module.find(tag)
            if el is None:
                return None
            return _first_number(el.text)

        serial_el = module.find("serialNumber")
        if serial_el is not None and serial_el.text:
            meta["module_serial"] = serial_el.text.strip()
        material_el = module.find("material")
        if material_el is not None and material_el.text:
            meta["material"] = material_el.text.strip()
        threshold_kev = _mod_number("threshold")
        if threshold_kev is not None:
            meta["threshold_ev"] = float(threshold_kev) * 1000.0
        thickness_um = _mod_number("thickness")
        if thickness_um is not None:
            meta["sensor_thickness_um"] = float(thickness_um)
    return meta


def _is_channel_index_column(values: np.ndarray) -> bool:
    """Report whether a flat token run is "<channel> <count>" pairs.

    Checked by shape rather than guessed: the first column of a pair layout is
    the channel index, so it ascends one at a time from 0 (or from 1). A counts
    column does not, except by coincidence on a perfectly linear ramp.
    """
    if values.size < 4 or values.size % 2:
        return False
    index = values.reshape(-1, 2)[:, 0]
    expected = np.arange(index.size, dtype=index.dtype)
    return bool(np.array_equal(index, expected) or np.array_equal(index, expected + 1))


def _mythen_counts_column(values: np.ndarray, n_channels: int | None) -> np.ndarray:
    """Pick the counts out of a MYTHEN frame, whichever layout it uses.

    A frame is either a single counts column or "<channel> <count>" pairs.
    Deciding that on the parity of the token count cannot tell them apart:
    every real MYTHEN module has an even channel count (1280 per module), so a
    counts-only frame always had an even token count, always took the pair
    branch, and lost half its channels -- the survivors being the odd-index
    counts, zero-padded back to full width so the shape still looked right.
    The counts-only fallback the old comment described could never be reached.

    Decide on the channel count from the `.cfg` when it is known, and on the
    shape of the first column when it is not.
    """
    if n_channels:
        if values.size == n_channels:
            return values
        if values.size == 2 * n_channels:
            return values.reshape(-1, 2)[:, 1]
        # Neither layout fits the declared width. Fall through to the shape
        # check; the caller pads or truncates whatever comes back.
    if _is_channel_index_column(values):
        return values.reshape(-1, 2)[:, 1]
    return values


def _read_mythen_dat(path: Path, n_channels: int | None) -> np.ndarray:
    """Parse one ``FrameNNNN.dat`` file into a 1D intensity vector (counts only)."""
    try:
        text = path.read_text()
    except OSError as exc:
        raise HTTPException(
            status_code=422, detail=f"Cannot read MYTHEN frame {path.name}"
        ) from exc
    tokens = text.split()
    if not tokens:
        return np.zeros(int(n_channels or 0), dtype=np.int64)
    try:
        values = np.array(tokens, dtype=np.int64)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Malformed MYTHEN frame {path.name}") from exc
    counts = _mythen_counts_column(values, n_channels)
    if n_channels and counts.size != n_channels:
        fixed = np.zeros(int(n_channels), dtype=np.int64)
        limit = min(int(n_channels), counts.size)
        fixed[:limit] = counts[:limit]
        counts = fixed
    return counts


def _read_mythen_acquisition(path: Path) -> tuple[np.ndarray, dict[str, Any]]:
    """Assemble a MYTHEN acquisition into a (frames, channels) intensity map."""
    folder, cfg_path = _resolve_mythen_acquisition(path)
    frame_files = _mythen_frame_files(folder)
    if not frame_files:
        raise HTTPException(
            status_code=422,
            detail="No MYTHEN frame (.dat) files found next to this acquisition",
        )

    try:
        latest_mtime = max(fp.stat().st_mtime for fp in frame_files)
    except OSError:
        latest_mtime = 0.0
    signature = (len(frame_files), latest_mtime)
    cache_key = str(folder.resolve())
    cached = _mythen_cache.get(cache_key)
    if cached is not None and cached[0] == signature:
        return cached[1], cached[2]

    meta = _parse_mythen_cfg(cfg_path)
    n_channels = meta.get("channels")
    rows = [_read_mythen_dat(fp, n_channels) for fp in frame_files]
    width = n_channels or max((row.size for row in rows), default=0)
    matrix = np.zeros((len(rows), int(width)), dtype=np.int64)
    for i, row in enumerate(rows):
        limit = min(int(width), row.size)
        matrix[i, :limit] = row[:limit]
    arr = _normalize_image_array(matrix)

    meta["channels"] = int(width)
    meta["frames_read"] = len(rows)
    if cfg_path is not None:
        meta["config_file"] = cfg_path.name

    _mythen_cache[cache_key] = (signature, arr, meta)
    return arr, meta


def _mythen_header_text(path: Path) -> str:
    """Human-readable header summary for a MYTHEN acquisition."""
    _, cfg_path = _resolve_mythen_acquisition(path)
    meta = _parse_mythen_cfg(cfg_path)
    frame_files = _mythen_frame_files(path.parent)
    lines: list[str] = ["# MYTHEN acquisition"]
    if meta.get("module_serial"):
        lines.append(f"# Module S/N {meta['module_serial']}")
    if meta.get("system_number"):
        lines.append(f"# System {meta['system_number']}")
    if meta.get("channels"):
        lines.append(f"# Channels {int(meta['channels'])}")
    lines.append(f"# Frames {len(frame_files)}")
    if meta.get("exposure_time_s") is not None:
        lines.append(f"# Exposure_time {meta['exposure_time_s']:.6g} s")
    if meta.get("energy_ev") is not None:
        lines.append(f"# Incident_energy {meta['energy_ev']:.6g} eV")
    if meta.get("threshold_ev") is not None:
        lines.append(f"# Threshold_energy {meta['threshold_ev']:.6g} eV")
    if meta.get("material"):
        lines.append(f"# Sensor {meta['material']}")
    if meta.get("bad_channels"):
        bad = meta["bad_channels"]
        preview = ", ".join(str(c) for c in bad[:16]) + (" …" if len(bad) > 16 else "")
        lines.append(f"# Bad_channels ({len(bad)}): {preview}")
    if cfg_path is not None:
        try:
            lines.append("")
            lines.append(cfg_path.read_text().strip())
        except OSError:
            pass
    return "\n".join(lines)
