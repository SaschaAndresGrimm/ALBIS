"""Flatfield correction for exported frames: every frame divided by a flat.

Mirrors ``frontend/modules/flatfield.js``, which corrects the frame on screen;
see there for the method. In short::

    counts:        corrected = raw / flat * mean(flat)
    transmission:  corrected = raw / flat

Flat pixels that cannot be divided by -- not finite, zero or negative (a CBF's
-1 and -2), at their type's maximum (a gap or an overflow), or below
MIN_FLAT_FRACTION of the flat's mean -- are holes: inpainting fills them when
asked, and they are written as bad pixels otherwise.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

MIN_FLAT_FRACTION = 0.05
SCALES = ("counts", "transmission")


@dataclass(frozen=True)
class Flat:
    data: np.ndarray  # float64, holes set to 1 (never divided by)
    holes: np.ndarray  # bool
    mean: float


def prepare_flat(frame: np.ndarray) -> Flat | None:
    """A flat made ready to divide by, or None when no pixel of it is usable."""
    values = np.asarray(frame)
    if values.ndim != 2:
        return None
    unusable = ~np.isfinite(values) if values.dtype.kind == "f" else np.zeros(values.shape, bool)
    if values.dtype.kind in "ui":
        unusable |= values == np.iinfo(values.dtype).max
    as_float = values.astype(np.float64)
    unusable |= ~(as_float > 0)
    usable = as_float[~unusable]
    if not usable.size:
        return None
    unusable |= as_float < usable.mean() * MIN_FLAT_FRACTION
    usable = as_float[~unusable]
    if not usable.size:
        return None
    divisor = np.where(unusable, 1.0, as_float)
    return Flat(data=divisor, holes=unusable, mean=float(usable.mean()))


def apply_flat(frame: np.ndarray, flat: Flat, scale: str = "counts") -> np.ndarray:
    """``frame`` divided by the flat (float64); the flat's holes come out NaN."""
    factor = 1.0 if scale == "transmission" else flat.mean
    corrected = np.asarray(frame, dtype=np.float64) / flat.data * factor
    corrected[flat.holes] = np.nan
    return corrected
