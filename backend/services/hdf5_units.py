"""Pure helper utilities for HDF5 scalar parsing and unit conversion."""

from __future__ import annotations

import math
from typing import Any

import numpy as np


def coerce_scalar(value: Any) -> float | None:
    """Convert an HDF5 scalar/array-like value into a plain float.

    A non-finite value is reported as absent rather than passed on. An
    uninitialised NeXus field reads as `NaN`, and a `NaN` here does not stay
    here: it survives the unit conversion, reaches the analysis payload, and is
    then serialized as the literal `NaN`, which is not valid JSON -- so a single
    unwritten field in a file turned the whole geometry response into a parse
    error in the browser. "We do not know this number" is the truth and the
    interface already handles it.
    """
    try:
        if isinstance(value, np.ndarray):
            if value.size == 0:
                return None
            value = value.reshape(-1)[0]
        if isinstance(value, np.generic):
            value = value.item()
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def get_units(obj: Any) -> str | None:
    """Best-effort extraction of `units`/`unit` attributes from a dataset-like object."""
    try:
        units = obj.attrs.get("units") or obj.attrs.get("unit")
        if isinstance(units, bytes):
            return units.decode("utf-8", "replace")
        if isinstance(units, np.ndarray) and units.size == 1:
            units = units.reshape(-1)[0]
        if isinstance(units, np.generic):
            units = units.item()
        if isinstance(units, bytes):
            return units.decode("utf-8", "replace")
        return str(units) if units is not None else None
    except Exception:
        return None


def norm_unit(unit: str | None) -> str:
    return (unit or "").strip().lower().replace("µ", "u")


def to_mm(value: float, unit: str | None) -> float:
    u = norm_unit(unit)
    if u in {"m", "meter", "metre", "meters", "metres"}:
        return value * 1000
    if u in {"cm", "centimeter", "centimetre", "centimeters", "centimetres"}:
        return value * 10
    if u in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
        return value
    if u in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
        return value / 1000
    if u in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
        return value / 1e6
    if value < 0.5:
        return value * 1000
    return value


def to_um(value: float, unit: str | None) -> float:
    u = norm_unit(unit)
    if u in {"m", "meter", "metre", "meters", "metres"}:
        return value * 1e6
    if u in {"cm", "centimeter", "centimetre", "centimeters", "centimetres"}:
        return value * 1e4
    if u in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
        return value * 1000
    if u in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
        return value
    if u in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
        return value / 1000
    if value < 1e-2:
        return value * 1e6
    if value < 1:
        return value * 1000
    return value


def to_ev(value: float, unit: str | None) -> float:
    u = norm_unit(unit)
    if u in {"kev", "kiloelectronvolt", "kiloelectronvolts"}:
        return value * 1000
    if u in {"ev", "electronvolt", "electronvolts"}:
        return value
    if value < 1000:
        return value * 1000
    return value


def wavelength_to_ev(value: float, unit: str | None) -> float | None:
    u = norm_unit(unit)
    wavelength_m = None
    if u in {"m", "meter", "metre", "meters", "metres"}:
        wavelength_m = value
    elif u in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
        wavelength_m = value * 1e-9
    elif u in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
        wavelength_m = value * 1e-6
    elif u in {"a", "ang", "angstrom", "angstroms"}:
        wavelength_m = value * 1e-10
    elif value < 1e-6:
        wavelength_m = value
    if wavelength_m is None or wavelength_m <= 0:
        return None
    return 12398.4193 / (wavelength_m * 1e10)
