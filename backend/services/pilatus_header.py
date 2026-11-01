"""Parse the text header of a PILATUS mini-CBF (or DECTRIS TIFF description)."""

from __future__ import annotations

import math
import re
from typing import Any

from .hdf5_units import wavelength_to_ev

_PILATUS_NUM_RE = re.compile(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?")


def _parse_unit_value(text: str) -> tuple[float | None, str]:
    if not text:
        return None, ""
    match = _PILATUS_NUM_RE.search(text)
    if not match:
        return None, ""
    value = float(match.group(0))
    unit_match = re.search(rf"{re.escape(match.group(0))}\s*([A-Za-zµÅ]+)", text)
    unit = unit_match.group(1) if unit_match else ""
    return value, unit


def _convert_length(value: float | None, unit: str, target: str) -> float | None:
    if value is None or not math.isfinite(value):
        return None
    unit_l = unit.lower()
    if target == "um":
        if unit_l in {"m", "meter", "metre"}:
            return value * 1e6
        if unit_l == "mm":
            return value * 1e3
        if unit_l in {"um", "µm"}:
            return value
        if unit_l == "nm":
            return value * 1e-3
        return value
    if target == "mm":
        if unit_l in {"m", "meter", "metre"}:
            return value * 1e3
        if unit_l == "cm":
            return value * 10.0
        if unit_l == "mm":
            return value
        if unit_l in {"um", "µm"}:
            return value * 1e-3
        return value
    if target == "a":
        if unit_l in {"a", "å", "angstrom", "ang"}:
            return value
        if unit_l == "nm":
            return value * 10.0
        if unit_l in {"m", "meter", "metre"}:
            return value * 1e10
        if unit_l == "mm":
            return value * 1e7
        return value
    return value


# The bare ISO timestamp a miniCBF writes as its second line, with no key.
_PILATUS_DATETIME_RE = re.compile(r"^\d{4}-[A-Za-z0-9]{2,3}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:\.\d+)?$")


def _parse_pilatus_header_text(text: str) -> dict[str, Any]:
    """Read what a miniCBF header states about the instrument and the exposure.

    Deliberately selective. Lines describing the INSTRUMENT and the ACQUISITION
    -- detector, geometry, exposure, thresholds -- stay true of a frame ALBIS
    has re-exported, so they are worth carrying. Lines describing the state of
    the PIXEL ARRAY do not: `N_excluded_pixels`, `Excluded_pixels`,
    `Flat_field` and `Trim_file` describe corrections applied to the raw data,
    and an export has already substituted -1 for masked gaps and -2 for bad or
    saturated pixels, so repeating them would describe an array that no longer
    exists. `Image_path` is skipped for a different reason: the provenance line
    deliberately records the source file's NAME and not its directory, and
    carrying the path here would put back what that was avoiding.
    """
    meta: dict[str, Any] = {}
    if not text:
        return meta
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            line = line[1:].strip()
        lower = line.lower()
        if _PILATUS_DATETIME_RE.match(line) and "image_datetime" not in meta:
            meta["image_datetime"] = line
            continue
        if lower.startswith("detector:") and "detector_description" not in meta:
            # "Detector: PILATUS 300K, S/N 3-0118, Universite de Geneve"
            rest = line.split(":", 1)[1].strip()
            parts = [part.strip() for part in rest.split(",")]
            description = parts[0] if parts else ""
            if description:
                meta["detector_description"] = description
            # Everything after the model that is not the serial: on a PILATUS
            # this is where the instrument lives, which is real provenance and
            # is stated nowhere else in the file.
            location_parts = []
            for part in parts[1:]:
                if part.lower().startswith("s/n"):
                    serial = part[3:].strip()
                    if serial:
                        meta["detector_serial_number"] = serial
                    continue
                if part:
                    location_parts.append(part)
            if location_parts:
                meta["detector_location"] = ", ".join(location_parts)
            continue
        if "sensor" in lower and "thickness" in lower and "sensor_thickness_m" not in meta:
            # "Silicon sensor, thickness 0.000320 m"
            value, unit = _parse_unit_value(line[line.lower().index("thickness") :])
            thickness_m = _convert_length(value, unit or "m", "m")
            if thickness_m is not None:
                meta["sensor_thickness_m"] = float(thickness_m)
            continue
        if "exposure_period" in lower and "exposure_period_s" not in meta:
            value, _unit = _parse_unit_value(line)
            if value is not None:
                meta["exposure_period_s"] = float(value)
            continue
        if "exposure_time" in lower and "exposure_time_s" not in meta:
            value, _unit = _parse_unit_value(line)
            if value is not None:
                meta["exposure_time_s"] = float(value)
            continue
        if lower.startswith("tau") and "tau_s" not in meta:
            value, _unit = _parse_unit_value(line)
            if value is not None:
                meta["tau_s"] = float(value)
            continue
        if "count_cutoff" in lower and "count_cutoff" not in meta:
            value, _unit = _parse_unit_value(line)
            if value is not None:
                meta["count_cutoff"] = int(round(value))
            continue
        if "threshold_setting" in lower and "threshold_energy_ev" not in meta:
            value, unit = _parse_unit_value(line)
            if value is not None:
                meta["threshold_energy_ev"] = float(
                    value * 1000.0 if unit.lower() == "kev" else value
                )
            continue
        if "gain_setting" in lower and "gain_setting" not in meta:
            # The whole phrase, parentheses included: "high gain (vrf = -0.150)"
            # only means anything complete.
            meta["gain_setting"] = line.split(":", 1)[1].strip() if ":" in line else ""
            if not meta["gain_setting"]:
                del meta["gain_setting"]
            continue
        if "angle_increment" in lower and "angle_increment_deg" not in meta:
            value, _unit = _parse_unit_value(line)
            if value is not None:
                meta["angle_increment_deg"] = float(value)
            continue
        if "start_angle" in lower and "start_angle_deg" not in meta:
            value, _unit = _parse_unit_value(line)
            if value is not None:
                meta["start_angle_deg"] = float(value)
            continue
        if "pixel_size" in lower and "pixel_size_um" not in meta:
            value, unit = _parse_unit_value(line)
            pixel_um = _convert_length(value, unit, "um")
            if pixel_um is not None:
                meta["pixel_size_um"] = float(pixel_um)
            continue
        if "beam_xy" in lower or "beam center" in lower or "beam_center" in lower:
            nums = _PILATUS_NUM_RE.findall(line)
            if len(nums) >= 2:
                meta["beam_center_px"] = (float(nums[0]), float(nums[1]))
            continue
        if "detector_distance" in lower or "detector distance" in lower:
            value, unit = _parse_unit_value(line)
            distance_mm = _convert_length(value, unit, "mm")
            if distance_mm is not None:
                meta["distance_mm"] = float(distance_mm)
            continue
        if "wavelength" in lower and "wavelength_a" not in meta:
            value, unit = _parse_unit_value(line)
            wavelength_a = _convert_length(value, unit, "a")
            if wavelength_a is not None:
                meta["wavelength_a"] = float(wavelength_a)
            continue
        if "energy" in lower and "threshold" not in lower and "energy_ev" not in meta:
            value, unit = _parse_unit_value(line)
            if value is None:
                continue
            unit_l = unit.lower()
            if unit_l in {"kev"}:
                meta["energy_ev"] = float(value * 1000.0)
            elif unit_l in {"ev"}:
                meta["energy_ev"] = float(value)
            continue
    if "energy_ev" not in meta:
        wavelength = meta.get("wavelength_a")
        if wavelength and wavelength > 0:
            meta["energy_ev"] = wavelength_to_ev(float(wavelength), "a")
    return meta
