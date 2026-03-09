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


def test_coerce_scalar_handles_numpy_values() -> None:
    assert coerce_scalar(np.array([3.5])) == pytest.approx(3.5)
    assert coerce_scalar(np.array([], dtype=np.float32)) is None
    assert coerce_scalar(np.float32(2.0)) == pytest.approx(2.0)


def test_get_units_reads_bytes_and_scalar_arrays() -> None:
    assert get_units(_Obj({"units": b"mm"})) == "mm"
    assert get_units(_Obj({"unit": np.array(["um"])})) == "um"


def test_unit_normalization_and_conversions() -> None:
    assert norm_unit("µm") == "um"
    assert to_mm(0.12, "m") == pytest.approx(120.0)
    assert to_um(0.1, "mm") == pytest.approx(100.0)
    assert to_ev(12.0, "keV") == pytest.approx(12000.0)


def test_wavelength_to_ev_angstrom() -> None:
    assert wavelength_to_ev(1.0, "angstrom") == pytest.approx(12398.4193)
