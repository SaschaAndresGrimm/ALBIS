"""The text header ALBIS writes into an exported CBF or TIFF.

A mini-CBF header built from the source metadata, followed by provenance lines
saying which build produced the file and what was done to the pixels.
"""

from __future__ import annotations

import math
from typing import Any

from ..build_info import ALBIS_COMMIT
from ..version import ALBIS_VERSION
from .image_values import _export_int, _export_text, _first_number


def _format_header_number(value: Any) -> str | None:
    number = _first_number(value)
    if number is None or not math.isfinite(number):
        return None
    return f"{float(number):.12g}"


def producer_string() -> str:
    """Name this build, the way a bug report or a citation needs it named."""
    return f"ALBIS {ALBIS_VERSION}" + (f" ({ALBIS_COMMIT})" if ALBIS_COMMIT else "")


def _provenance_lines(metadata: dict[str, Any] | None) -> list[str]:
    """Say who made this file, from what, and what was done to the pixels.

    An exported frame is not a copy of the detector's output. The dtype is
    widened, masked gaps become `-1` and bad or saturated pixels become `-2`.
    Those are the right conventions, but a mini-CBF header that declares
    `SLS_1.0` and lists detector, wavelength and distance -- and says nothing
    about ALBIS -- reads to XDS or DIALS as genuine detector output. For
    software that asks to be cited when it contributed to a result, derived data
    that cannot be traced back is the wrong default.
    """
    lines = [f"# Produced by {producer_string()} -- derived data, not raw detector output"]

    source = _export_text(metadata or {}, "source_name")
    if source:
        parts = [f"# Source: {source}"]
        dataset = _export_text(metadata or {}, "source_dataset")
        if dataset:
            parts.append(dataset)
        frame = _export_int(metadata or {}, "source_frame")
        if frame is not None:
            count = _export_int(metadata or {}, "source_frame_count")
            parts.append(f"frame {frame}" + (f"/{count}" if count else ""))
        threshold = _export_int(metadata or {}, "source_threshold")
        if threshold is not None:
            count = _export_int(metadata or {}, "source_threshold_count")
            parts.append(f"threshold {threshold}" + (f"/{count}" if count else ""))
        lines.append(" ".join(parts))

    combined = _export_text(metadata or {}, "combined_description")
    if combined:
        # What a combined frame is, and why the per-exposure fields are absent
        # from its header. A sum of ten one-second frames is not a one-second
        # exposure, its values can exceed the per-frame count cutoff, and it
        # spans a wedge of rotation rather than a single step -- so those lines
        # are omitted upstream rather than restated as if they still held.
        lines.append(f"# Combined: {combined}")
        lines.append(
            "# Per-frame exposure, rotation and count-cutoff values are omitted: "
            "they do not describe a combined frame"
        )

    flat = _export_text(metadata or {}, "flatfield")
    if flat:
        scale = (metadata or {}).get("flatfield_scale")
        scale_text = "transmission, 0 to 1" if scale == "transmission" else "counts, times its mean"
        lines.append(f"# Flatfield: divided by {flat} ({scale_text}) by ALBIS")
    # An unsigned export has no -1 or -2: every pixel without a value holds
    # the type's highest value, and counts that did not fit were clipped.
    marker = _export_int(metadata or {}, "marker_value")
    data_type = _export_text(metadata or {}, "data_type")
    if marker is not None and data_type:
        bits = "".join(ch for ch in data_type if ch.isdigit())
        clipped = _export_int(metadata or {}, "clipped_pixels") or 0
        lines.append(
            f"# Data type: unsigned {bits}-bit, chosen in ALBIS; "
            f"counts outside 0 to {marker - 1} clipped ({clipped} pixels)"
        )
    inpainted = (metadata or {}).get("inpainted")
    if inpainted in {"defects", "saturated"}:
        # Values that were not measured: say so, and which.
        filled = (
            "gaps, defective and saturated pixels"
            if inpainted == "saturated"
            else "gaps and defective pixels"
        )
        remaining = "bad = -2" if inpainted == "saturated" else "bad or saturated = -2"
        if marker is not None:
            remaining = (
                f"bad = {marker}" if inpainted == "saturated" else f"bad or saturated = {marker}"
            )
        margin = _export_int(metadata or {}, "inpainted_margin") or 0
        if margin > 0:
            filled += f" and {margin} px around them"
        method = (metadata or {}).get("inpainted_method")
        if method == "fixed":
            value = (metadata or {}).get("inpainted_value", 0)
            how = f"set to {value:g} by ALBIS"
        elif method == "edges":
            how = "inpainted by ALBIS (interpolated along the edges crossing them)"
        elif method == "nearest":
            how = "inpainted by ALBIS (each the value of its nearest valid neighbours)"
        else:
            how = "inpainted by ALBIS (interpolated from their neighbours)"
        if (metadata or {}).get("inpainted_noise"):
            # Invented: say so, so the noise is not taken for measured.
            how += ", with Poisson noise added"
        lines.append(f"# Pixel substitutions: {filled} {how}; {remaining}")
    elif marker is not None:
        lines.append(f"# Pixel substitutions: masked gaps, bad or saturated = {marker}")
    else:
        lines.append("# Pixel substitutions: masked gaps = -1, bad or saturated = -2")
    return lines


def _active_threshold_ev(metadata: dict[str, Any]) -> float | None:
    """The threshold energy of the frame being written, in eV.

    A source can carry one threshold or a list of them. With a list, the frame
    belongs to exactly one channel, and `source_threshold` (1-based, the same
    number the provenance line reports) says which -- writing the wrong one, or
    all of them, would mislabel the frame.
    """
    energies = metadata.get("threshold_energies_ev")
    if isinstance(energies, list | tuple) and energies:
        index = _export_int(metadata, "source_threshold")
        if index is not None and 1 <= index <= len(energies):
            return _first_number(energies[index - 1])
        if len(energies) == 1:
            return _first_number(energies[0])
        return None
    return _first_number(metadata.get("threshold_energy_ev"))


def _mini_cbf_header_text(metadata: dict[str, Any] | None) -> str:
    lines: list[str] = []
    if not metadata:
        return "\n".join(_provenance_lines(metadata))
    detector = _export_text(metadata, "detector_description")
    serial = _export_text(metadata, "detector_serial_number")
    location = _export_text(metadata, "detector_location")
    if detector or serial:
        line = f"# Detector: {detector or 'unknown'}"
        if serial:
            line += f", S/N {serial}"
        if location:
            line += f", {location}"
        lines.append(line)

    timestamp = _export_text(metadata, "image_datetime")
    if timestamp:
        # A miniCBF puts the acquisition time on its own keyless line, right
        # after the detector, and readers expect it there.
        lines.append(f"# {timestamp}")

    pixel_x = _format_header_number(metadata.get("pixel_size_x_m"))
    pixel_y = _format_header_number(metadata.get("pixel_size_y_m"))
    if pixel_x and pixel_y:
        lines.append(f"# Pixel_size {pixel_x} m x {pixel_y} m")
    thickness = _format_header_number(metadata.get("sensor_thickness_m"))
    if thickness:
        lines.append(f"# Silicon sensor, thickness {thickness} m")
    exposure = _format_header_number(metadata.get("exposure_time_s"))
    if exposure:
        lines.append(f"# Exposure_time {exposure} s")
    period = _format_header_number(metadata.get("exposure_period_s"))
    if period:
        lines.append(f"# Exposure_period {period} s")
    tau = _format_header_number(metadata.get("tau_s"))
    if tau:
        lines.append(f"# Tau = {tau} s")
    cutoff = _export_int(metadata, "count_cutoff")
    if cutoff is not None:
        lines.append(f"# Count_cutoff {cutoff} counts")
    threshold = _active_threshold_ev(metadata)
    if threshold is not None:
        # The threshold this frame was counted at. Carried all along for TIFF
        # and HDF5 sources and never written, which on a multi-threshold
        # detector loses the one number that says which channel this is.
        lines.append(f"# Threshold_setting: {threshold:.12g} eV")
    gain = _export_text(metadata, "gain_setting")
    if gain:
        lines.append(f"# Gain_setting: {gain}")
    wavelength = _format_header_number(metadata.get("wavelength_a"))
    if wavelength:
        lines.append(f"# Wavelength {wavelength} A")
    energy = _format_header_number(metadata.get("incident_energy_ev"))
    if energy:
        lines.append(f"# Incident_energy {energy} eV")
    distance = _format_header_number(metadata.get("detector_distance_m"))
    if distance:
        lines.append(f"# Detector_distance {distance} m")
    beam_x = _format_header_number(metadata.get("beam_center_x_px"))
    beam_y = _format_header_number(metadata.get("beam_center_y_px"))
    if beam_x and beam_y:
        lines.append(f"# Beam_xy ({beam_x}, {beam_y}) pixels")
    start = _format_header_number(metadata.get("start_angle_deg"))
    if start:
        lines.append(f"# Start_angle {start} deg.")
    increment = _format_header_number(metadata.get("angle_increment_deg"))
    if increment:
        lines.append(f"# Angle_increment {increment} deg.")
    provenance = _provenance_lines(metadata)
    if not lines:
        # Nothing was parsed from the source, so keep the source's own header
        # rather than replacing it with silence. Provenance is appended to
        # whichever of the two bases we ended up with.
        #
        # Re-exporting a file ALBIS wrote carries that file's provenance here,
        # which is worth keeping -- it is the chain back to the original -- but
        # not worth repeating verbatim, so lines the new block already says are
        # dropped rather than stacked up on every round trip.
        source_header = str(metadata.get("source_header_text") or "").strip()
        carried = source_header.splitlines() if source_header else []
        lines = [line for line in carried if line.strip() not in {p.strip() for p in provenance}]
    return "\n".join(lines + provenance)
