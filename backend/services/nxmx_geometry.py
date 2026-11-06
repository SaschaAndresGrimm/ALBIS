"""Detector panels from an NXmx file's transformation chain, for a tilted detector.

NXmx places a detector by a chain of transformations: each NXdetector_module
has `module_offset`, `fast_pixel_direction` and `slow_pixel_direction`, each
`depends_on` the next -- `det_z`, `two_theta`, ... -- until `.`. Diamond's I19-2
mounts its Eiger on a 2θ arm: the file says `beam_center_x/y` (985, 1419) and
`detector_distance` 85 mm, but those are the detector's 2θ = 0 description,
and with the arm at 20° the beam meets the detector at (572, 1419). ALBIS drew
its rings around the stated centre, which is in fact at 20° 2θ, so every ring
and every d-spacing it showed for such data was wrong. dxtbx, DIALS's geometry
library, reads the same chain; tests/fixtures/dls_i19_2_two_theta_dxtbx.json
holds its answer for this file, which these panels reproduce.

The coordinates are NeXus's (McStas): the beam along +z, which is also ALBIS's
ring frame, so no change of frame is needed. Lengths come out in millimetres.

Only a detector that is not square to the beam gets panels; any other is
better served by the flat model that also lets the beam centre be edited
freely, so `read_nxmx_geometry` returns None for it and nothing changes.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np

# Below this the detector is square to the beam for any purpose rings serve.
_MIN_TILT_DEG = 0.1

_LENGTH_TO_MM = {
    "m": 1000.0,
    "meter": 1000.0,
    "metre": 1000.0,
    "cm": 10.0,
    "mm": 1.0,
    "millimeter": 1.0,
    "millimetre": 1.0,
    "um": 1e-3,
    "µm": 1e-3,
    "micron": 1e-3,
    "nm": 1e-6,
}
_ANGLE_TO_RAD = {
    "deg": math.pi / 180,
    "degree": math.pi / 180,
    "degrees": math.pi / 180,
    "rad": 1.0,
    "radian": 1.0,
    "radians": 1.0,
}
_MAX_CHAIN = 32  # a loop in `depends_on` must not hang a request


def _text(value: Any) -> str:
    if isinstance(value, np.ndarray):
        value = value.reshape(-1)[0] if value.size else ""
    if isinstance(value, bytes | np.bytes_):
        return bytes(value).decode("utf-8", "replace").strip()
    return str(value if value is not None else "").strip()


def _first(value: Any) -> float | None:
    try:
        number = float(np.asarray(value, dtype=float).reshape(-1)[0])
    except (TypeError, ValueError, IndexError):
        return None
    return number if math.isfinite(number) else None


def _vector(value: Any) -> np.ndarray | None:
    try:
        vec = np.asarray(value, dtype=float).reshape(-1)
    except (TypeError, ValueError):
        return None
    if vec.shape != (3,) or not np.all(np.isfinite(vec)):
        return None
    length = float(np.linalg.norm(vec))
    return vec / length if length > 1e-12 else None


def _rotation(axis: np.ndarray, angle: float) -> np.ndarray:
    """Right-handed rotation by `angle` about the unit `axis` (Rodrigues)."""
    x, y, z = axis
    c, s = math.cos(angle), math.sin(angle)
    t = 1.0 - c
    return np.array(
        [
            [t * x * x + c, t * x * y - s * z, t * x * z + s * y],
            [t * x * y + s * z, t * y * y + c, t * y * z - s * x],
            [t * x * z - s * y, t * y * z + s * x, t * z * z + c],
        ]
    )


def _length_mm(value: float, units: str) -> float | None:
    factor = _LENGTH_TO_MM.get(units.lower())
    return value * factor if factor is not None else None


def _transformation(dset: Any) -> tuple[np.ndarray, np.ndarray] | None:
    """One NXtransformations entry as (rotation 3x3, translation mm).

    The point maps as p' = R p + t: a rotation turns about `vector` through the
    parent frame's origin, a translation moves `value` along `vector`, and the
    fixed `offset` is added after either, in the parent frame.
    """
    kind = _text(dset.attrs.get("transformation_type")).lower()
    vector = _vector(dset.attrs.get("vector"))
    value = _first(dset[()])
    units = _text(dset.attrs.get("units"))
    if vector is None or value is None or kind not in {"rotation", "translation"}:
        return None
    offset = np.zeros(3)
    if "offset" in dset.attrs:
        raw = np.asarray(dset.attrs["offset"], dtype=float).reshape(-1)
        offset_units = _text(dset.attrs.get("offset_units")) or units
        scale = _LENGTH_TO_MM.get(offset_units.lower())
        if raw.shape != (3,) or scale is None:
            return None
        offset = raw * scale
    if kind == "rotation":
        factor = _ANGLE_TO_RAD.get(units.lower())
        if factor is None:
            return None
        return _rotation(vector, value * factor), offset
    distance = _length_mm(value, units)
    if distance is None:
        return None
    return np.eye(3), vector * distance + offset


def _resolve(h5: Any, path: str, relative_to: Any) -> Any:
    if path.startswith("/"):
        return h5.get(path)
    return relative_to.parent.get(path)


def _chain(h5: Any, start: Any) -> tuple[np.ndarray, np.ndarray] | None:
    """The composed (R, t) from `start` down to the lab, following depends_on."""
    rotation, translation = np.eye(3), np.zeros(3)
    node = start
    for _ in range(_MAX_CHAIN):
        step = _transformation(node)
        if step is None:
            return None
        r, t = step
        # Applied after what is already composed: p_lab = r (R p + T) + t.
        rotation, translation = r @ rotation, r @ translation + t
        target = _text(node.attrs.get("depends_on"))
        if target in ("", "."):
            return rotation, translation
        node = _resolve(h5, target, node)
        if node is None:
            return None
    return None


def _nonzero_offset(dset: Any) -> bool:
    if "offset" not in dset.attrs:
        return False
    try:
        return bool(np.any(np.abs(np.asarray(dset.attrs["offset"], dtype=float)) > 1e-12))
    except (TypeError, ValueError):
        return True


def _module_panel(h5: Any, module: Any, name: str) -> dict[str, Any] | None:
    fast_ds = module.get("fast_pixel_direction")
    slow_ds = module.get("slow_pixel_direction")
    offset_ds = module.get("module_offset")
    if fast_ds is None or slow_ds is None or offset_ds is None:
        return None
    # An offset on a pixel direction moves pixel (0, 0) away from module_offset,
    # and writers disagree on how. None of the files seen use one; the flat
    # model is better than a detector placed by a guess.
    if _nonzero_offset(fast_ds) or _nonzero_offset(slow_ds):
        return None
    fast_vec = _vector(fast_ds.attrs.get("vector"))
    slow_vec = _vector(slow_ds.attrs.get("vector"))
    fast_size = _length_mm(_first(fast_ds[()]) or 0.0, _text(fast_ds.attrs.get("units")))
    slow_size = _length_mm(_first(slow_ds[()]) or 0.0, _text(slow_ds.attrs.get("units")))
    # Pixel (0, 0) is where module_offset's chain puts the origin. The pixel
    # directions turn with whatever they depend on (module_offset, whose own
    # part is a translation and turns nothing) -- a direction is not moved.
    origin_chain = _chain(h5, offset_ds)
    parent = _resolve(h5, _text(fast_ds.attrs.get("depends_on")), fast_ds)
    direction_chain = _chain(h5, parent) if parent is not None else None
    if (
        origin_chain is None
        or direction_chain is None
        or fast_vec is None
        or slow_vec is None
        or not fast_size
        or not slow_size
    ):
        return None
    rotation = direction_chain[0]
    data_size = module.get("data_size")
    data_origin = module.get("data_origin")
    size = np.asarray(data_size[()] if data_size is not None else [0, 0], dtype=int).reshape(-1)
    start = np.asarray(data_origin[()] if data_origin is not None else [0, 0], dtype=int)
    start = start.reshape(-1)
    if size.shape != (2,) or start.shape != (2,) or np.any(size <= 0):
        return None
    # NeXus counts slow then fast; ALBIS's panels say fast then slow.
    return {
        "name": name,
        "origin_mm": [float(v) for v in origin_chain[1]],
        "fast_axis": [float(v) for v in rotation @ fast_vec],
        "slow_axis": [float(v) for v in rotation @ slow_vec],
        "pixel_size_mm": [float(fast_size), float(slow_size)],
        "image_size_px": [int(size[1]), int(size[0])],
        "raw_offset_px": [float(start[1]), float(start[0])],
    }


def _tilt_deg(panel: dict[str, Any]) -> float:
    normal = np.cross(panel["fast_axis"], panel["slow_axis"])
    cosine = abs(float(normal[2])) / max(float(np.linalg.norm(normal)), 1e-12)
    return math.degrees(math.acos(min(1.0, cosine)))


def _scalar_mm(h5: Any, path: str) -> float | None:
    dset = h5.get(path)
    if dset is None:
        return None
    value = _first(dset[()])
    units = _text(dset.attrs.get("units")) or "m"
    return _length_mm(value, units) if value is not None else None


def _scalar(h5: Any, path: str) -> float | None:
    dset = h5.get(path)
    return _first(dset[()]) if dset is not None else None


def _arm(h5: Any, detector: Any) -> tuple[float, list[float]] | None:
    """The detector's 2θ arm: its angle (degrees) and its axis in the lab frame.

    The rotation named `two_theta` in the detector's own chain, provided what
    it hangs from only turns -- so it pivots about the sample, as an arm does.
    Its axis is the file's `vector` turned by those parent rotations. None when
    the chain has no such rotation, so the angle can only be read, not changed.
    """
    start = detector.get("depends_on")
    if start is None:
        return None
    target = _text(start[()])
    node = _resolve(h5, target, start) if target not in ("", ".") else None
    steps: list[tuple[str, Any]] = []
    for _ in range(_MAX_CHAIN):
        if node is None:
            return None
        steps.append((node.name.rsplit("/", 1)[-1].lower(), node))
        target = _text(node.attrs.get("depends_on"))
        if target in ("", "."):
            break
        node = _resolve(h5, target, node)
    else:
        return None
    for index, (name, dset) in enumerate(steps):
        if "two_theta" not in name and "2theta" not in name:
            continue
        if _text(dset.attrs.get("transformation_type")).lower() != "rotation":
            return None
        vector = _vector(dset.attrs.get("vector"))
        value = _first(dset[()])
        factor = _ANGLE_TO_RAD.get(_text(dset.attrs.get("units")).lower())
        if vector is None or value is None or factor is None:
            return None
        parents = np.eye(3)
        for _name, parent in steps[index + 1 :]:
            step = _transformation(parent)
            if step is None or np.linalg.norm(step[1]) > 1e-9:
                return None  # a parent that moves it: no longer a pivot at the sample
            parents = step[0] @ parents
        return math.degrees(value * factor), [float(v) for v in parents @ vector]
    return None


def read_nxmx_geometry(h5: Any) -> dict[str, Any] | None:
    """The detector's panels when the file places it tilted to the beam, else None.

    Carries the pose ALBIS's Detector Geometry fields are measured against:
    the file's own `beam_center_x/y` and `detector_distance`, which for a
    detector on an arm are its 2θ = 0 values, and the direction a change of
    distance moves the detector -- along the arm, not along the beam.
    """
    detector = h5.get("/entry/instrument/detector")
    if detector is None:
        return None
    panels: list[dict[str, Any]] = []
    for key in sorted(detector.keys()):
        node = detector.get(key)
        if node is None or not hasattr(node, "keys"):
            continue
        if _text(node.attrs.get("NX_class")) != "NXdetector_module":
            continue
        panel = _module_panel(h5, node, key)
        if panel is None:
            return None  # half a detector is worse than the flat model
        panels.append(panel)
    if not panels or max(_tilt_deg(panel) for panel in panels) < _MIN_TILT_DEG:
        return None

    first = panels[0]
    normal = np.cross(first["fast_axis"], first["slow_axis"])
    normal = normal / np.linalg.norm(normal)
    if float(np.dot(normal, first["origin_mm"])) < 0:
        normal = -normal
    geometry: dict[str, Any] = {
        "mode": "geometry",
        "detector": (
            _text(detector.get("description", b"")[()]) if "description" in detector else ""
        ),
        "source": "NXmx transformations",
        "panels": panels,
        "reference_center_x_px": _scalar(h5, "/entry/instrument/detector/beam_center_x"),
        "reference_center_y_px": _scalar(h5, "/entry/instrument/detector/beam_center_y"),
        "reference_distance_mm": _scalar_mm(h5, "/entry/instrument/detector/detector_distance"),
        "distance_axis": [float(v) for v in normal],
    }
    # The arm, so the interface can show its angle and a manual override can
    # turn the detector about the file's own axis.
    arm = _arm(h5, detector)
    if arm is not None:
        geometry["two_theta_deg"], geometry["two_theta_axis"] = arm
    return geometry
