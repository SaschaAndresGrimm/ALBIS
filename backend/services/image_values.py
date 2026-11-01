"""Coerce loosely typed detector metadata into plain numbers and text.

Header parsers, the DECTRIS TIFF tag and export writers all receive values that
may be a scalar, a list, a NumPy array or bytes; these helpers turn each into
the one shape the caller needs, or None.
"""

from __future__ import annotations

import math
from typing import Any

import numpy as np


def _first_number(value: Any) -> float | None:
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, list | tuple):
        if not value:
            return None
        value = value[0]
    if isinstance(value, np.generic):
        value = value.item()
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    number = _first_number(value)
    if number is None:
        return None
    try:
        return int(number)
    except (TypeError, ValueError):
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        try:
            return value.decode("ascii", errors="replace").strip("\x00")
        except Exception:
            return None
    if isinstance(value, list | tuple):
        if not value:
            return None
        return _as_str(value[0])
    return str(value)


def _as_pair(value: Any) -> tuple[float, float] | None:
    if isinstance(value, np.ndarray):
        value = value.tolist()
    if isinstance(value, list | tuple) and len(value) >= 2:
        first = _first_number(value[0])
        second = _first_number(value[1])
        if first is None or second is None:
            return None
        return float(first), float(second)
    return None


def _as_number_list(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, np.ndarray):
        value = value.reshape(-1).tolist()
    elif not isinstance(value, list | tuple):
        value = [value]
    out: list[float] = []
    for item in value:
        number = _first_number(item)
        if number is not None and math.isfinite(number):
            out.append(float(number))
    return out


def _as_int_list(value: Any) -> list[int]:
    out: list[int] = []
    for number in _as_number_list(value):
        out.append(int(round(number)))
    return out


def _export_float(metadata: dict[str, Any] | None, key: str) -> float | None:
    if not metadata:
        return None
    number = _first_number(metadata.get(key))
    if number is None or not math.isfinite(number):
        return None
    return float(number)


def _export_int(metadata: dict[str, Any] | None, key: str) -> int | None:
    number = _export_float(metadata, key)
    if number is None:
        return None
    return int(round(number))


def _export_text(metadata: dict[str, Any] | None, key: str) -> str:
    if not metadata:
        return ""
    text = _as_str(metadata.get(key)) or ""
    return " ".join(text.replace("\r", " ").replace("\n", " ").split())


def _distance_to_mm(value: float | None) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    if value <= 10:
        return value * 1000.0
    return value
