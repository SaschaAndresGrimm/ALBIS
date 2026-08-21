"""Cover the unit conversions detector geometry is read through.

Every resolution ring, every d-spacing and every ROI in physical units comes out
of these functions. They matter more than their size suggests for a second
reason: a NeXus file is not obliged to carry a `units` attribute, and plenty do
not, so each conversion also has a fallback that guesses from the magnitude. A
wrong guess does not fail -- it silently mis-scales the geometry, which is the
worst kind of wrong for a measurement.
"""

from __future__ import annotations

import numpy as np
import pytest

from backend.services.hdf5_units import (
    coerce_scalar,
    get_units,
    norm_unit,
    to_ev,
    to_mm,
    to_um,
    wavelength_to_ev,
)


class _Obj:
    def __init__(self, attrs):
        self.attrs = attrs


# --------------------------------------------------------------------------
# Reading the value and its unit out of HDF5
# --------------------------------------------------------------------------


def test_coerce_scalar_handles_numpy_values() -> None:
    assert coerce_scalar(np.array([3.5])) == pytest.approx(3.5)
    assert coerce_scalar(np.array([], dtype=np.float32)) is None
    assert coerce_scalar(np.float32(2.0)) == pytest.approx(2.0)


@pytest.mark.parametrize(
    "value",
    [None, "not a number", b"nope", [], {}, float("nan"), float("inf")],
)
def test_coerce_scalar_rejects_what_is_not_a_finite_number(value) -> None:
    """A NaN distance is worse than a missing one: it propagates into the geometry."""
    assert coerce_scalar(value) is None


def test_coerce_scalar_accepts_plain_numbers_and_numeric_text() -> None:
    assert coerce_scalar(7) == pytest.approx(7.0)
    assert coerce_scalar("7.5") == pytest.approx(7.5)


def test_get_units_reads_bytes_and_scalar_arrays() -> None:
    assert get_units(_Obj({"units": b"mm"})) == "mm"
    assert get_units(_Obj({"unit": np.array(["um"])})) == "um"


def test_get_units_prefers_units_over_unit() -> None:
    assert get_units(_Obj({"units": "mm", "unit": "m"})) == "mm"


def test_get_units_returns_nothing_when_the_file_says_nothing() -> None:
    assert get_units(_Obj({})) is None
    assert get_units(object()) is None


def test_norm_unit_folds_case_whitespace_and_micro_sign() -> None:
    assert norm_unit("  MM ") == "mm"
    assert norm_unit("µm") == "um"
    assert norm_unit("μm") == "um" or norm_unit("μm") == "μm"
    assert norm_unit(None) == ""


# --------------------------------------------------------------------------
# Declared units
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,unit,expected",
    [
        (0.12, "m", 120.0),
        (0.12, "metre", 120.0),
        (0.12, "meters", 120.0),
        (12.0, "cm", 120.0),
        (120.0, "mm", 120.0),
        (120.0, "millimetres", 120.0),
        (120000.0, "um", 120.0),
        (120000.0, "µm", 120.0),
        (1.2e8, "nm", 120.0),
    ],
)
def test_to_mm_converts_every_declared_length_unit(value, unit, expected) -> None:
    assert to_mm(value, unit) == pytest.approx(expected)


@pytest.mark.parametrize(
    "value,unit,expected",
    [
        (7.5e-5, "m", 75.0),
        (7.5e-3, "cm", 75.0),
        (0.075, "mm", 75.0),
        (75.0, "um", 75.0),
        (75.0, "micrometer", 75.0),
        (75000.0, "nm", 75.0),
    ],
)
def test_to_um_converts_every_declared_length_unit(value, unit, expected) -> None:
    assert to_um(value, unit) == pytest.approx(expected)


@pytest.mark.parametrize(
    "value,unit,expected",
    [
        (12.0, "keV", 12000.0),
        (12.0, "kiloelectronvolts", 12000.0),
        (12000.0, "eV", 12000.0),
        (12000.0, "electronvolt", 12000.0),
    ],
)
def test_to_ev_converts_every_declared_energy_unit(value, unit, expected) -> None:
    assert to_ev(value, unit) == pytest.approx(expected)


# --------------------------------------------------------------------------
# Undeclared units: the magnitude heuristics
# --------------------------------------------------------------------------


def test_a_detector_distance_in_metres_is_recognised_without_a_unit() -> None:
    """0.15 is 150 mm, not 0.15 mm: no detector sits 150 um from the sample."""
    assert to_mm(0.15, None) == pytest.approx(150.0)


def test_a_detector_distance_already_in_mm_is_left_alone() -> None:
    assert to_mm(150.0, None) == pytest.approx(150.0)
    assert to_mm(0.6, None) == pytest.approx(0.6)


def test_a_pixel_size_in_metres_is_recognised_without_a_unit() -> None:
    """75e-6 m is the EIGER pixel; 7.5e-5 must not be read as 0.000075 um."""
    assert to_um(75e-6, None) == pytest.approx(75.0)


def test_a_pixel_size_in_mm_is_recognised_without_a_unit() -> None:
    assert to_um(0.075, None) == pytest.approx(75.0)


def test_a_pixel_size_already_in_um_is_left_alone() -> None:
    assert to_um(75.0, None) == pytest.approx(75.0)


def test_a_beam_energy_in_kev_is_recognised_without_a_unit() -> None:
    assert to_ev(12.4, None) == pytest.approx(12400.0)


def test_a_beam_energy_already_in_ev_is_left_alone() -> None:
    assert to_ev(12400.0, None) == pytest.approx(12400.0)


def test_an_unknown_unit_falls_back_to_the_magnitude_heuristic() -> None:
    """A unit string nobody recognises must not be trusted as if it were mm."""
    assert to_mm(0.15, "furlong") == pytest.approx(150.0)
    assert to_ev(12.4, "banana") == pytest.approx(12400.0)


# --------------------------------------------------------------------------
# Wavelength and energy
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,unit",
    [
        (1.0, "angstrom"),
        (1.0, "A"),
        (1.0, "ang"),
        (0.1, "nm"),
        (1e-4, "um"),
        (1e-10, "m"),
    ],
)
def test_one_angstrom_is_the_same_energy_however_it_is_spelled(value, unit) -> None:
    assert wavelength_to_ev(value, unit) == pytest.approx(12398.4193)


def test_a_wavelength_in_metres_is_recognised_without_a_unit() -> None:
    assert wavelength_to_ev(1e-10, None) == pytest.approx(12398.4193)


def test_wavelength_and_energy_agree_on_a_real_beamline_value() -> None:
    """1.0332 A is 12 keV, the number a user would recognise on a beamline."""
    assert wavelength_to_ev(1.0332, "angstrom") == pytest.approx(12000.0, rel=1e-3)


@pytest.mark.parametrize("value", [0.0, -1.0])
def test_a_nonphysical_wavelength_has_no_energy(value: float) -> None:
    """Better no geometry than geometry computed from a division by zero."""
    assert wavelength_to_ev(value, "angstrom") is None


def test_a_wavelength_with_an_unusable_unit_and_magnitude_has_no_energy() -> None:
    assert wavelength_to_ev(1.0, "parsec") is None
