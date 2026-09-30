"""Detectors ALBIS recognises by serial number and knows the geometry of.

One detector so far: the PILATUS 12M at Diamond Light Source beamline I23,
S/N 120-0100. It is unique -- 24 rows of modules on a half cylinder around the
sample, in vacuum -- and its file header cannot describe that shape: a miniCBF
header states one distance and one beam centre, which is enough for a flat
detector and nothing like enough for this one. Until now ALBIS drew its
resolution rings only when a DIALS `imported.expt` for it sat next to the data
or was loaded by hand.

The geometry is fixed hardware, so it is built here from the same blueprint
constants as dxtbx's `FormatCBFMiniPilatusDLS12M`, which is where DIALS gets
it: `tests/test_detector_profiles.py` checks the result against an
`imported.expt` written by `dials.import` for real I23 data. The panels are in
the frame ALBIS's ring model uses (sample at the origin, beam along +z), which
is the per-panel frame dials writes into `imported.expt`.

What the header does say is where this detector sits relative to that
blueprint, and dxtbx reads it that way: `Detector_distance` is how far the
cylinder is displaced along the beam (I23 writes 0.010 m), and `Beam_xy` is
where the beam lands. ALBIS's rings take an absolute sample-to-detector
distance, so a raw 10 mm made them collapse onto the beam centre.
`ring_meta` supplies the distance they need: the blueprint's own beam-hit
distance plus the header's offset.
"""

from __future__ import annotations

import math
from functools import lru_cache
from typing import Any

DLS_I23_P12M_DETECTOR = "pilatus-12m-dls-cshape"
DLS_I23_P12M_SERIAL = "120-0100"
# Shown in the rings panel as "Auto geometry: <source>". No slash: the source
# is split on "/" for display, and summed HDF5 files record its last component.
DLS_I23_P12M_SOURCE = "DLS I23 PILATUS 12M 120-0100"

# Blueprint constants, as in dxtbx's FormatCBFMiniPilatusDLS12M.
_ROWS = 24
_ROW_SIZE_PX = (2463, 195)
_ROW_GAP_PX = 17
_PIXEL_MM = 0.172
_RADIUS_MM = 250.0
_FAST_OFFSET_MM = 184.9
_SLOW_OFFSET_MM = 16.8
_FIRST_ROW_DEG = -12.2 + 0.5 * 7.903
_ROW_STEP_DEG = 7.903 + 0.441

_INCIDENT_BEAM = (0.0, 0.0, 1.0)
_EPSILON = 1e-9


def is_dls_i23_pilatus_12m(description: Any, serial: Any) -> bool:
    """True for the I23 detector, from the parsed `Detector:` header line."""
    return "PILATUS 12M" in str(description or "").upper() and (
        str(serial or "").strip().upper() == DLS_I23_P12M_SERIAL
    )


@lru_cache(maxsize=1)
def _dls_i23_pilatus_12m_panels() -> tuple[dict[str, Any], ...]:
    panels = []
    fast = (1.0, 0.0, 0.0)
    for row in range(_ROWS):
        angle = math.radians(_FIRST_ROW_DEG + row * _ROW_STEP_DEG)
        slow = (0.0, math.sin(angle), math.cos(angle))
        normal = (0.0, -math.cos(angle), math.sin(angle))  # fast x slow
        origin = [
            _RADIUS_MM * n - _FAST_OFFSET_MM * f - _SLOW_OFFSET_MM * s
            for n, f, s in zip(normal, fast, slow, strict=True)
        ]
        panels.append(
            {
                "name": f"row-{row:02d}",
                "origin_mm": origin,
                "fast_axis": list(fast),
                "slow_axis": list(slow),
                "pixel_size_mm": [_PIXEL_MM, _PIXEL_MM],
                "image_size_px": list(_ROW_SIZE_PX),
                "raw_offset_px": [0, row * (_ROW_SIZE_PX[1] + _ROW_GAP_PX)],
            }
        )
    return tuple(panels)


def dls_i23_pilatus_12m_geometry() -> dict[str, Any]:
    """The `/api/image/geometry` payload for the I23 detector."""
    return {
        "mode": "geometry",
        "detector": DLS_I23_P12M_DETECTOR,
        "source": DLS_I23_P12M_SOURCE,
        "panels": [
            {key: list(value) if isinstance(value, list) else value for key, value in panel.items()}
            for panel in _dls_i23_pilatus_12m_panels()
        ],
    }


def _dot(a: tuple[float, ...] | list[float], b: tuple[float, ...] | list[float]) -> float:
    return a[0] * b[0] + a[1] * b[1] + a[2] * b[2]


@lru_cache(maxsize=1)
def dls_i23_pilatus_12m_beam_distance_mm() -> float:
    """How far along the beam it meets the blueprint detector, in mm.

    The same ray-panel intersection as `getGeometryReferencePose` in
    `frontend/modules/ring_geometry_utils.js`, which is the reference the ring
    model measures its distance field against, so the two agree exactly.
    """
    best: tuple[float, float] | None = None
    for panel in _dls_i23_pilatus_12m_panels():
        fast, slow, origin = panel["fast_axis"], panel["slow_axis"], panel["origin_mm"]
        normal = (
            fast[1] * slow[2] - fast[2] * slow[1],
            fast[2] * slow[0] - fast[0] * slow[2],
            fast[0] * slow[1] - fast[1] * slow[0],
        )
        denom = _dot(_INCIDENT_BEAM, normal)
        if abs(denom) <= _EPSILON:
            continue
        t = _dot(origin, normal) / denom
        if t <= 0:
            continue
        rel = [t * b - o for b, o in zip(_INCIDENT_BEAM, origin, strict=True)]
        x = _dot(rel, fast) / _PIXEL_MM - 0.5
        y = _dot(rel, slow) / _PIXEL_MM - 0.5
        width, height = panel["image_size_px"]
        dx = x - max(-0.5, min(width - 0.5, x))
        dy = y - max(-0.5, min(height - 0.5, y))
        outside = math.hypot(dx, dy)
        if best is None or (outside, t) < best:
            best = (outside, t)
    if best is None:
        raise RuntimeError("the I23 blueprint geometry does not meet the beam")
    return best[1]


def ring_meta(meta: dict[str, Any]) -> dict[str, Any]:
    """The header metadata as the resolution rings need it.

    Unchanged for every detector but the I23 PILATUS 12M, whose header
    distance is an offset from the blueprint position rather than a distance.
    Only what feeds the rings goes through this: exports write the header's
    own value back, since that is what DIALS reads.
    """
    if not is_dls_i23_pilatus_12m(
        meta.get("detector_description"), meta.get("detector_serial_number")
    ):
        return meta
    offset_mm = meta.get("distance_mm")
    if not isinstance(offset_mm, int | float) or not math.isfinite(offset_mm):
        offset_mm = 0.0
    return {**meta, "distance_mm": dls_i23_pilatus_12m_beam_distance_mm() + float(offset_mm)}
