"""Inpainting for exported frames: gaps and defective pixels filled from their neighbours."""

from __future__ import annotations

import json
import time
from pathlib import Path

import numpy as np
import pytest

from backend.services.inpainting import grow_holes, inpaint

CASES = json.loads(
    (Path(__file__).parent / "fixtures" / "inpainting_cases.json").read_text(encoding="utf-8")
)["cases"]


@pytest.mark.parametrize("case", CASES, ids=[case["name"] for case in CASES])
def test_fills_the_shared_cases_like_the_viewer(case: dict) -> None:
    shape = (case["height"], case["width"])
    frame = np.array(case["data"], dtype=np.float64).reshape(shape)
    holes = np.array(case["holes"], dtype=bool).reshape(shape)
    expected = np.array(case["expected"]).reshape(shape)
    method = case.get("method", "interpolate")
    np.testing.assert_allclose(inpaint(frame, holes, method), expected, rtol=1e-5, atol=1e-4)


def test_a_dead_pixel_takes_its_neighbours_and_the_rest_stays() -> None:
    frame = np.full((5, 5), 7.0)
    frame[2, 2] = 1000.0
    holes = np.zeros((5, 5), dtype=bool)
    holes[2, 2] = True
    out = inpaint(frame, holes)
    assert out[2, 2] == pytest.approx(7.0)
    assert np.array_equal(np.delete(out.ravel(), 12), np.delete(frame.ravel(), 12))
    assert frame[2, 2] == 1000.0  # the input is not changed


def test_a_gradient_carries_straight_through_a_gap() -> None:
    frame = np.tile(np.arange(12, dtype=np.float64) * 5.0, (4, 1))
    holes = np.zeros(frame.shape, dtype=bool)
    holes[:, 4:8] = True
    np.testing.assert_allclose(inpaint(frame, holes), frame)


def test_where_two_full_length_gaps_cross_it_still_fills() -> None:
    frame = np.full((20, 20), 3.0)
    holes = np.zeros(frame.shape, dtype=bool)
    holes[9:12, :] = True
    holes[:, 9:12] = True
    out = inpaint(frame, holes)
    np.testing.assert_allclose(out, 3.0)


def test_nothing_to_fill_returns_the_frame() -> None:
    frame = np.arange(6, dtype=np.uint32).reshape(2, 3)
    out = inpaint(frame, np.zeros(frame.shape, dtype=bool))
    assert out.dtype == np.float64 and np.array_equal(out, frame)


def test_fast_enough_for_a_4m_frame() -> None:
    # An EIGER2 4M-sized frame with module gaps: well under a second.
    frame = np.random.default_rng(0).poisson(50, (2162, 2068)).astype(np.uint32)
    holes = np.zeros(frame.shape, dtype=bool)
    for y in range(514, 2162, 551):
        holes[y : y + 37, :] = True
    for x in range(1030, 2068, 1040):
        holes[:, x : x + 10] = True
    start = time.perf_counter()
    out = inpaint(frame, holes)
    elapsed = time.perf_counter() - start
    assert not np.isnan(out).any()
    assert 30 < out[holes].mean() < 70
    assert elapsed < 5.0


GROWTH = json.loads(
    (Path(__file__).parent / "fixtures" / "inpainting_cases.json").read_text(encoding="utf-8")
)["growth"]


@pytest.mark.parametrize("case", GROWTH, ids=[case["name"] for case in GROWTH])
def test_grows_the_holes_like_the_viewer(case: dict) -> None:
    shape = (case["height"], case["width"])
    holes = np.array(case["holes"], dtype=bool).reshape(shape)
    expected = np.array(case["expected"], dtype=bool).reshape(shape)
    assert np.array_equal(grow_holes(holes, case["margin"]), expected)


def test_a_margin_is_a_square_and_capped() -> None:
    holes = np.zeros((15, 15), dtype=bool)
    holes[7, 7] = True
    assert grow_holes(holes, 1).sum() == 9
    assert grow_holes(holes, 99).sum() == 11 * 11  # at most 5 px
    assert not grow_holes(holes, 0)[7, 8]


def test_the_side_median_is_np_nanmedian_of_the_valid_samples() -> None:
    from backend.services.inpainting import SIDE_SAMPLES, _side_medians

    rng = np.random.default_rng(5)
    for count in range(SIDE_SAMPLES + 1):
        # One line per case: the gap edge at 0, then `count` valid pixels.
        line = rng.integers(0, 1000, 12).astype(float)
        holes = np.zeros(12, dtype=bool)
        holes[count] = True  # the first hole ends the samples
        value, at = _side_medians(
            line,
            holes,
            np.zeros(1, dtype=np.int64),
            1,
            np.zeros(1, dtype=np.int64),
            np.ones(1, dtype=bool),
            np.ones(1, dtype=np.int64),
            12,
        )
        expected = np.median(line[:count]) if count else 0.0
        assert value[0] == expected
        assert at[0] == (max(count, 1) - 1) / 2.0


def test_an_unknown_method_is_refused() -> None:
    with pytest.raises(ValueError):
        inpaint(np.ones((3, 3)), np.eye(3, dtype=bool), "smudge")


def test_nearest_takes_the_nearer_side_and_never_blends() -> None:
    frame = np.array([[1.0, 0, 0, 0, 0, 9]])
    holes = np.array([[False, True, True, True, True, False]])
    # The first two holes are nearer the left, the last two the right.
    assert inpaint(frame, holes, "nearest").tolist() == [[1, 1, 1, 9, 9, 9]]
