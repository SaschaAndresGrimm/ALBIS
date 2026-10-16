"""Flatfield correction for exported frames: every frame divided by a flat."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pytest

from backend.services.flatfield import apply_flat, prepare_flat

CASES = json.loads(
    (Path(__file__).parent / "fixtures" / "inpainting_cases.json").read_text(encoding="utf-8")
)["flatfield"]


@pytest.mark.parametrize("case", CASES, ids=[case["name"] for case in CASES])
def test_divides_like_the_viewer(case: dict) -> None:
    shape = (case["height"], case["width"])
    flat = prepare_flat(np.array(case["flat"], dtype=np.uint32).reshape(shape))
    assert flat is not None
    assert flat.holes.astype(int).ravel().tolist() == case["holes"]
    out = apply_flat(np.array(case["raw"], dtype=np.uint32).reshape(shape), flat, case["scale"])
    expected = np.array([np.nan if v is None else v for v in case["expected"]]).reshape(shape)
    np.testing.assert_allclose(out, expected, rtol=1e-5, equal_nan=True)


def test_counts_keep_the_scale_and_transmission_runs_to_one() -> None:
    flat = prepare_flat(np.array([[500.0, 1000.0], [1500.0, 1000.0]]))
    raw = np.array([[500, 500], [1500, 1000]], dtype=np.uint16)
    assert apply_flat(raw, flat, "counts").tolist() == [[1000.0, 500.0], [1000.0, 1000.0]]
    assert apply_flat(raw, flat, "transmission").tolist() == [[1.0, 0.5], [1.0, 1.0]]


def test_a_flat_with_nothing_usable_is_none() -> None:
    assert prepare_flat(np.zeros((3, 3), dtype=np.uint16)) is None
    assert prepare_flat(np.zeros(4)) is None
