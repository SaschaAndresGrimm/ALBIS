"""What a miniCBF header keeps when ALBIS re-exports the frame.

The exported header used to state five things -- pixel size, beam centre,
distance, wavelength, energy -- because those were the only five
`_parse_pilatus_header_text` could read. Everything else a PILATUS writes was
dropped on conversion: detector model and serial, the timestamp, sensor
thickness, exposure time and period, tau, count cutoff, the threshold setting
and the gain. `Threshold_setting` was the worst of them, being the number that
says which channel a multi-threshold frame belongs to.

These tests run against testdata/in16c_010001.cbf, a real PILATUS 300K frame,
rather than a hand-written header: a parser tested only against the strings
its author imagined is tested against their imagination.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from backend.image_formats import (
    _mini_cbf_header_text,
    _parse_pilatus_header_text,
    _pilatus_header_text,
)

FIXTURE = Path(__file__).resolve().parents[1] / "testdata" / "in16c_010001.cbf"


@pytest.fixture(scope="module")
def source_header() -> str:
    return _pilatus_header_text(FIXTURE)


@pytest.fixture(scope="module")
def parsed(source_header: str) -> dict:
    return _parse_pilatus_header_text(source_header)


def test_the_fixture_still_has_the_header_these_tests_describe(source_header: str) -> None:
    """Guards the guard: a re-saved fixture would make the rest vacuous."""
    for expected in (
        "PILATUS 300K",
        "Threshold_setting: 4024 eV",
        "Gain_setting: high gain",
        "Tau = 383.8e-09 s",
        "Count_cutoff 1302749 counts",
    ):
        assert expected in source_header


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("detector_description", "PILATUS 300K"),
        ("detector_serial_number", "3-0118"),
        ("detector_location", "Universite de Geneve"),
        ("image_datetime", "2011-Nov-01T17:59:04.733"),
        ("pixel_size_um", 172.0),
        ("sensor_thickness_m", 0.000320),
        ("exposure_time_s", 1.0),
        ("exposure_period_s", 1.005),
        ("tau_s", 383.8e-09),
        ("count_cutoff", 1302749),
        ("threshold_energy_ev", 4024.0),
        ("gain_setting", "high gain (vrf = -0.150)"),
        ("wavelength_a", 1.542),
        ("distance_mm", 40.0),
        ("start_angle_deg", 0.0),
        ("angle_increment_deg", 0.1),
    ],
)
def test_every_instrument_field_is_read(parsed: dict, key: str, value: object) -> None:
    assert key in parsed, f"{key} is no longer read from the header"
    if isinstance(value, float):
        assert parsed[key] == pytest.approx(value)
    else:
        assert parsed[key] == value


def test_the_beam_centre_is_read_as_a_pair(parsed: dict) -> None:
    assert tuple(parsed["beam_center_px"]) == (244.0, 308.0)


def test_energy_is_derived_when_only_a_wavelength_is_stated(parsed: dict) -> None:
    # The header states no incident energy, so it comes from Bragg's constant.
    assert parsed["energy_ev"] == pytest.approx(12398.4193 / 1.542, rel=1e-9)


@pytest.mark.parametrize(
    "field",
    ["N_excluded_pixels", "Excluded_pixels", "Flat_field", "Trim_file", "Image_path"],
)
def test_pixel_array_and_path_fields_are_deliberately_not_read(
    source_header: str, parsed: dict, field: str
) -> None:
    """These describe the raw array's corrections, or someone's directory.

    An export substitutes -1 for masked gaps and -2 for bad or saturated
    pixels, so a carried `N_excluded_pixels` would describe an array that no
    longer exists. `Image_path` is skipped because the provenance line
    deliberately records the source file's name and not its directory.
    """
    assert field in source_header  # it is there to be read, and is not read
    haystack = " ".join(str(v).lower() for v in parsed.values())
    assert field.lower().rstrip("s") not in haystack


def _exported_header(parsed: dict, **extra: object) -> str:
    meta: dict = {
        "source_name": FIXTURE.name,
        "detector_description": parsed.get("detector_description"),
        "detector_serial_number": parsed.get("detector_serial_number"),
        "detector_location": parsed.get("detector_location"),
        "image_datetime": parsed.get("image_datetime"),
        "pixel_size_x_m": parsed["pixel_size_um"] / 1e6,
        "pixel_size_y_m": parsed["pixel_size_um"] / 1e6,
        "sensor_thickness_m": parsed.get("sensor_thickness_m"),
        "exposure_time_s": parsed.get("exposure_time_s"),
        "exposure_period_s": parsed.get("exposure_period_s"),
        "tau_s": parsed.get("tau_s"),
        "count_cutoff": parsed.get("count_cutoff"),
        "threshold_energy_ev": parsed.get("threshold_energy_ev"),
        "gain_setting": parsed.get("gain_setting"),
        "wavelength_a": parsed.get("wavelength_a"),
        "detector_distance_m": parsed["distance_mm"] / 1000.0,
        "beam_center_x_px": parsed["beam_center_px"][0],
        "beam_center_y_px": parsed["beam_center_px"][1],
        "start_angle_deg": parsed.get("start_angle_deg"),
        "angle_increment_deg": parsed.get("angle_increment_deg"),
    }
    meta.update(extra)
    return _mini_cbf_header_text(meta)


def test_the_exported_header_states_what_the_original_stated(parsed: dict) -> None:
    header = _exported_header(parsed)
    for expected in (
        "# Detector: PILATUS 300K, S/N 3-0118, Universite de Geneve",
        "# 2011-Nov-01T17:59:04.733",
        "# Silicon sensor, thickness 0.00032 m",
        "# Exposure_time 1 s",
        "# Exposure_period 1.005 s",
        "# Tau = 3.838e-07 s",
        "# Count_cutoff 1302749 counts",
        "# Threshold_setting: 4024 eV",
        "# Gain_setting: high gain (vrf = -0.150)",
        "# Wavelength 1.542 A",
        "# Detector_distance 0.04 m",
        "# Beam_xy (244, 308) pixels",
        "# Start_angle 0 deg.",
        "# Angle_increment 0.1 deg.",
    ):
        assert expected in header, f"missing from the exported header: {expected}"


def test_the_exported_header_still_says_it_is_derived(parsed: dict) -> None:
    header = _exported_header(parsed)
    assert "derived data, not raw detector output" in header
    assert "masked gaps = -1" in header
    assert f"# Source: {FIXTURE.name}" in header


def test_a_zero_start_angle_is_written_rather_than_dropped(parsed: dict) -> None:
    # 0 deg is a real value and the commonest one; a truthiness test would lose it.
    assert parsed["start_angle_deg"] == 0.0
    assert "# Start_angle 0 deg." in _exported_header(parsed)


class TestThresholdSelection:
    """Which threshold a multi-threshold frame reports."""

    def test_a_single_energy_is_written(self, parsed: dict) -> None:
        header = _exported_header(parsed, threshold_energies_ev=[7000.0])
        assert "# Threshold_setting: 7000 eV" in header

    def test_the_frame_s_own_channel_is_chosen_from_several(self, parsed: dict) -> None:
        header = _exported_header(
            parsed, threshold_energies_ev=[4000.0, 9000.0], source_threshold=2
        )
        assert "# Threshold_setting: 9000 eV" in header
        assert "4000" not in header.split("Threshold_setting")[1].splitlines()[0]

    def test_nothing_is_written_when_the_channel_is_unknown(self, parsed: dict) -> None:
        # Guessing between two thresholds would mislabel the frame; better to
        # say nothing than to name the wrong channel.
        header = _exported_header(parsed, threshold_energies_ev=[4000.0, 9000.0])
        assert "Threshold_setting" not in header

    def test_an_out_of_range_channel_names_none(self, parsed: dict) -> None:
        header = _exported_header(
            parsed, threshold_energies_ev=[4000.0, 9000.0], source_threshold=7
        )
        assert "Threshold_setting" not in header


def test_the_header_is_a_valid_mini_cbf_comment_block(parsed: dict) -> None:
    """Every line is a comment; a bare line would corrupt the CBF section."""
    for line in _exported_header(parsed).splitlines():
        if line.strip():
            assert re.match(r"^#", line.strip()), f"not a comment line: {line!r}"
