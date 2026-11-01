"""Multi-panel detector geometry for an image: DIALS `.expt` files, geometry
embedded in an ALBIS HDF5, and the PILATUS 12M blueprint."""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any

import numpy as np

from ..detector_profiles import DLS_I23_P12M_DETECTOR, dls_i23_pilatus_12m_geometry
from ..image_formats import _image_ext_name, _pilatus_header_text
from .image_values import _as_int, _first_number


def _pilatus_is_12m_header_text(text: str) -> bool:
    upper = str(text or "").upper()
    return "PILATUS 12M" in upper and "S/N 120-0100" in upper


def _coerce_float_vector(value: Any, size: int) -> list[float] | None:
    if not isinstance(value, list | tuple) or len(value) != size:
        return None
    out: list[float] = []
    for item in value:
        number = _first_number(item)
        if number is None or not math.isfinite(number):
            return None
        out.append(float(number))
    return out


def _coerce_int_vector(value: Any, size: int) -> list[int] | None:
    if not isinstance(value, list | tuple) or len(value) != size:
        return None
    out: list[int] = []
    for item in value:
        number = _as_int(item)
        if number is None:
            return None
        out.append(int(number))
    return out


def _normalize_geometry_panel(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    name = str(payload.get("name") or "").strip()
    origin = _coerce_float_vector(payload.get("origin"), 3)
    fast_axis = _coerce_float_vector(payload.get("fast_axis"), 3)
    slow_axis = _coerce_float_vector(payload.get("slow_axis"), 3)
    pixel_size = _coerce_float_vector(payload.get("pixel_size"), 2)
    image_size = _coerce_int_vector(payload.get("image_size"), 2)
    raw_offset = _coerce_int_vector(payload.get("raw_image_offset"), 2)
    if not (
        name and origin and fast_axis and slow_axis and pixel_size and image_size and raw_offset
    ):
        return None
    if any(v <= 0 for v in pixel_size) or any(v <= 0 for v in image_size):
        return None
    return {
        "name": name,
        "origin_mm": origin,
        "fast_axis": fast_axis,
        "slow_axis": slow_axis,
        "pixel_size_mm": pixel_size,
        "image_size_px": image_size,
        "raw_offset_px": raw_offset,
    }


def _expt_frame(node: Any) -> tuple[np.ndarray, np.ndarray, np.ndarray] | None:
    if not isinstance(node, dict):
        return None
    fast = _coerce_float_vector(node.get("fast_axis"), 3)
    slow = _coerce_float_vector(node.get("slow_axis"), 3)
    origin = _coerce_float_vector(node.get("origin"), 3)
    if not (fast and slow and origin):
        return None
    return np.asarray(fast), np.asarray(slow), np.asarray(origin)


def _compose_expt_frame(
    parent: tuple[np.ndarray, np.ndarray, np.ndarray],
    local: tuple[np.ndarray, np.ndarray, np.ndarray],
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """A child's frame in its parent's coordinates, as dxtbx composes them."""
    p_fast, p_slow, p_origin = parent
    p_normal = np.cross(p_fast, p_slow)
    basis = np.stack([p_fast, p_slow, p_normal])  # rows: parent axes

    def to_parent(vector: np.ndarray) -> np.ndarray:
        return vector @ basis

    l_fast, l_slow, l_origin = local
    return to_parent(l_fast), to_parent(l_slow), p_origin + to_parent(l_origin)


def _expt_lab_frames(detector: dict[str, Any], panels: list[Any]) -> dict[int, tuple]:
    """Each panel's frame in the lab, composed through the detector hierarchy.

    dxtbx serialises a panel's frame relative to the group it sits in; the
    hierarchy's root and groups carry the rest. A panel the hierarchy does not
    reach keeps its own frame.
    """
    frames: dict[int, tuple] = {}
    identity = (np.array([1.0, 0, 0]), np.array([0, 1.0, 0]), np.zeros(3))

    def walk(node: Any, parent: tuple) -> None:
        if not isinstance(node, dict):
            return
        index = node.get("panel")
        if isinstance(index, int) and 0 <= index < len(panels):
            local = _expt_frame(panels[index])
            if local is not None:
                frames[index] = _compose_expt_frame(parent, local)
            return
        group = _expt_frame(node)
        here = _compose_expt_frame(parent, group) if group is not None else parent
        for child in node.get("children") or []:
            walk(child, here)

    walk(detector.get("hierarchy"), identity)
    return frames


def _rotation_to_albis(beam_direction: list[float]) -> np.ndarray:
    """Rotate the lab so the X-rays travel along +z, as ALBIS's rings assume.

    dxtbx's beam `direction` points from the sample back to the source, so the
    X-rays travel along its negative. Any rotation that takes that onto +z
    gives the same scattering angles; this is the shortest one.
    """
    travel = -np.asarray(beam_direction, dtype=float)
    travel /= np.linalg.norm(travel)
    target = np.array([0.0, 0.0, 1.0])
    cos_angle = float(np.clip(travel @ target, -1.0, 1.0))
    if cos_angle > 1 - 1e-12:
        return np.eye(3)
    if cos_angle < -1 + 1e-12:
        return np.diag([-1.0, 1.0, -1.0])  # half a turn about y
    axis = np.cross(travel, target)
    axis /= np.linalg.norm(axis)
    k = np.array([[0, -axis[2], axis[1]], [axis[2], 0, -axis[0]], [-axis[1], axis[0], 0]])
    sin_angle = np.sqrt(1 - cos_angle**2)
    return np.eye(3) + sin_angle * k + (1 - cos_angle) * (k @ k)


def _load_dials_expt_geometry(path: Path) -> list[dict[str, Any]]:
    """The first detector's panels, in ALBIS's frame: sample at the origin,
    X-rays travelling along +z.

    A DIALS file states a beam and a hierarchy, and needs both: its panel
    frames are relative to their group, and its lab has the X-rays travelling
    along -z. Read as they stood, a flat detector's panel sat behind the
    sample and its rings came out wrong (d 0.84 A where dxtbx gives 1.92 A).
    So the frames are composed through the hierarchy and turned to face the
    beam. A file without a beam -- written for ALBIS, not by DIALS -- is taken
    to be in ALBIS's frame already, as before.
    """
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return []
    detectors = payload.get("detector")
    if not isinstance(detectors, list) or not detectors:
        return []
    detector = detectors[0]
    if not isinstance(detector, dict):
        return []
    panels = detector.get("panels")
    if not isinstance(panels, list):
        return []

    beams = payload.get("beam")
    direction = None
    if isinstance(beams, list) and beams and isinstance(beams[0], dict):
        direction = _coerce_float_vector(beams[0].get("direction"), 3)
    if direction is not None and np.linalg.norm(direction) > 0:
        rotation = _rotation_to_albis(direction)
        lab = _expt_lab_frames(detector, panels)
        oriented: list[Any] = []
        for index, panel in enumerate(panels):
            if index in lab and isinstance(panel, dict):
                fast, slow, origin = (rotation @ vector for vector in lab[index])
                panel = {
                    **panel,
                    "fast_axis": fast.tolist(),
                    "slow_axis": slow.tolist(),
                    "origin": origin.tolist(),
                }
            oriented.append(panel)
        panels = oriented

    normalized = []
    for panel in panels:
        item = _normalize_geometry_panel(panel)
        if item is not None:
            normalized.append(item)
    return normalized


def _resolve_pilatus_12m_geometry_file(image_path: Path) -> Path | None:
    candidates = [
        image_path.parent / "imported.expt",
        image_path.parent / "P12M_geometry" / "imported.expt",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    return None


def _read_hdf5_embedded_geometry(path: Path) -> dict[str, Any] | None:
    try:
        import h5py  # type: ignore[import-not-found]
    except Exception:
        return None
    try:
        with h5py.File(path, "r") as h5:
            if "/entry/albis/geometry/json" not in h5:
                return None
            dataset = h5["/entry/albis/geometry/json"]
            try:
                raw = dataset[()]
            except Exception:
                return None
    except Exception:
        return None
    if isinstance(raw, np.ndarray):
        if raw.size != 1:
            return None
        raw = raw.reshape(-1)[0]
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="ignore")
    elif isinstance(raw, np.bytes_):
        text = raw.tobytes().decode("utf-8", errors="ignore")
    elif hasattr(raw, "item"):
        item = raw.item()
        text = item.decode("utf-8", errors="ignore") if isinstance(item, bytes) else str(item)
    else:
        text = str(raw)
    try:
        payload = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    panels = []
    for panel in payload.get("panels", []):
        if not isinstance(panel, dict):
            return None
        item = {
            "name": str(panel.get("name") or "").strip(),
            "origin_mm": _coerce_float_vector(panel.get("origin_mm"), 3),
            "fast_axis": _coerce_float_vector(panel.get("fast_axis"), 3),
            "slow_axis": _coerce_float_vector(panel.get("slow_axis"), 3),
            "pixel_size_mm": _coerce_float_vector(panel.get("pixel_size_mm"), 2),
            "image_size_px": _coerce_int_vector(panel.get("image_size_px"), 2),
            "raw_offset_px": _coerce_float_vector(panel.get("raw_offset_px"), 2),
        }
        if not (
            item["name"]
            and item["origin_mm"]
            and item["fast_axis"]
            and item["slow_axis"]
            and item["pixel_size_mm"]
            and item["image_size_px"]
            and item["raw_offset_px"]
        ):
            return None
        panels.append(item)
    if not panels:
        return None
    source = str(payload.get("source") or "").strip()
    if source.startswith("embedded HDF5 geometry"):
        source_label = source
    elif source:
        source_label = f"embedded HDF5 geometry (from {Path(source).name})"
    else:
        source_label = "embedded HDF5 geometry"
    return {
        "mode": "geometry",
        "detector": str(payload.get("detector") or ""),
        "source": source_label,
        "panels": panels,
    }


def _resolve_hdf5_source_file(path: Path) -> Path | None:
    try:
        import h5py  # type: ignore[import-not-found]
    except Exception:
        return None
    try:
        with h5py.File(path, "r") as h5:
            raw = h5.attrs.get("source_file")
    except Exception:
        return None
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", errors="ignore").strip()
    elif hasattr(raw, "item"):
        item = raw.item()
        if isinstance(item, bytes):
            text = item.decode("utf-8", errors="ignore").strip()
        else:
            text = str(item).strip()
    else:
        text = str(raw or "").strip()
    if not text:
        return None
    source_path = Path(text).expanduser()
    if not source_path.is_absolute():
        source_path = (path.parent / source_path).resolve()
    else:
        source_path = source_path.resolve()
    return source_path


def _pilatus_image_geometry_internal(
    path: Path,
    geometry_path: Path | None,
    visited: set[Path],
) -> dict[str, Any]:
    geometry = {"mode": "planar", "detector": "", "source": "", "panels": []}
    if geometry_path is not None:
        panels = _load_dials_expt_geometry(geometry_path)
        if not panels:
            return geometry
        return {
            "mode": "geometry",
            "detector": DLS_I23_P12M_DETECTOR,
            "source": str(geometry_path),
            "panels": panels,
        }
    try:
        resolved = path.resolve()
    except OSError:
        resolved = path
    if resolved in visited:
        return geometry
    visited.add(resolved)
    ext = _image_ext_name(path.name)
    if ext in {".h5", ".hdf5"}:
        embedded = _read_hdf5_embedded_geometry(path)
        if embedded:
            return embedded
        source_path = _resolve_hdf5_source_file(path)
        if source_path and source_path.exists():
            return _pilatus_image_geometry_internal(source_path, None, visited)
        return geometry
    # The DECTRIS TIFF carries the same text header as the miniCBF, in its
    # ImageDescription tag.
    if ext not in {".cbf", ".cbf.gz", ".tif", ".tiff"}:
        return geometry
    header_text = _pilatus_header_text(path)
    if not _pilatus_is_12m_header_text(header_text):
        return geometry
    # A geometry file next to the data still wins, so a site that keeps its own
    # there goes on using it. Otherwise the detector's fixed blueprint geometry.
    geometry_path = _resolve_pilatus_12m_geometry_file(path)
    panels = _load_dials_expt_geometry(geometry_path) if geometry_path is not None else []
    if not panels:
        return dls_i23_pilatus_12m_geometry()
    return {
        "mode": "geometry",
        "detector": DLS_I23_P12M_DETECTOR,
        "source": str(geometry_path),
        "panels": panels,
    }


def _pilatus_image_geometry(path: Path, geometry_path: Path | None = None) -> dict[str, Any]:
    return _pilatus_image_geometry_internal(path, geometry_path, set())
