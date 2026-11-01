"""The pure helpers series jobs inherit, tested without running a job.

They were only reached through whole jobs before they moved into
`series_reduction.py` and `series_output_files.py`; these pin the edge cases a
job rarely hits.
"""

from __future__ import annotations

import numpy as np

from backend.services.series_output_files import SeriesOutputFilesMixin
from backend.services.series_reduction import SeriesReductionMixin


def test_cast_clips_to_the_target_range_instead_of_wrapping() -> None:
    # Without the clip, a negative value cast to uint16 wraps to a huge count.
    out = SeriesReductionMixin._cast_result_to_dtype(
        np.array([-3.0, 1.4, 70000.0]), np.dtype(np.uint16)
    )
    assert out.dtype == np.uint16
    assert out.tolist() == [0, 1, 65535]


def test_cast_to_float_keeps_values() -> None:
    out = SeriesReductionMixin._cast_result_to_dtype(np.array([1.5, 2.5]), np.dtype(np.float32))
    assert out.dtype == np.float32
    assert out.tolist() == [1.5, 2.5]


def test_result_range_ignores_non_finite_and_empty_chunks() -> None:
    update = SeriesReductionMixin._update_result_range
    assert update(np.array([]), None, None) == (None, None)
    assert update(np.array([np.nan, np.inf]), 1.0, 2.0) == (1.0, 2.0)
    assert update(np.array([np.nan, -5.0, 9.0]), 0.0, 4.0) == (-5.0, 9.0)


def test_median_of_integral_values_stays_integer() -> None:
    picked = SeriesReductionMixin._select_output_dtype(
        np.dtype(np.uint16), "median", False, 3, 0.0, 100.0, False
    )
    assert picked == np.dtype(np.uint16)
    fractional = SeriesReductionMixin._select_output_dtype(
        np.dtype(np.uint16), "median", False, 3, 0.0, 100.0, True
    )
    assert fractional == np.dtype(np.float32)


def test_frames_label_for_the_whole_series_and_for_a_chunk() -> None:
    label = SeriesReductionMixin._frames_output_label
    assert label(
        operation="sum",
        group_total=1,
        chunk_idx=0,
        start_idx=0,
        end_idx=9,
        count=10,
        frame_count=10,
        frame_step=1,
    ) == ("_all", "sum of 10 frames")
    tag, what = label(
        operation="mean",
        group_total=2,
        chunk_idx=1,
        start_idx=5,
        end_idx=9,
        count=3,
        frame_count=10,
        frame_step=2,
    )
    assert tag == "_chunk0002_f000006-000010_every002"
    assert what == "mean of 3 frames 6-10, every 2th frame"


def test_operation_base_name_replaces_an_existing_operation_suffix() -> None:
    name = SeriesOutputFilesMixin._operation_base_name
    assert name("scan_series_sum", "median") == "scan_series_median"
    assert name("scan_mean", "sum") == "scan_sum"
    assert name("scan", "bogus") == "scan_sum"
    assert name("", "mean") == "series_mean"


def test_geometry_payload_is_rejected_unless_every_panel_is_complete() -> None:
    normalize = SeriesOutputFilesMixin._normalize_geometry_payload
    panel = {
        "name": "p0",
        "origin_mm": [0, 0, 100],
        "fast_axis": [1, 0, 0],
        "slow_axis": [0, 1, 0],
        "pixel_size_mm": [0.075, 0.075],
        "image_size_px": [1028, 1062],
        "raw_offset_px": [0, 0],
    }
    good = normalize({"mode": "geometry", "panels": [panel]})
    assert good is not None and good["panels"][0]["image_size_px"] == [1028, 1062]
    assert SeriesOutputFilesMixin._geometry_pixel_size_um(good) == 75.0
    assert normalize({"mode": "geometry", "panels": [{**panel, "pixel_size_mm": [0, 1]}]}) is None
    assert normalize({"mode": "simple", "panels": [panel]}) is None
