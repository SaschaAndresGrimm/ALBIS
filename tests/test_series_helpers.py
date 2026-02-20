from __future__ import annotations

import numpy as np
import pytest
from fastapi import HTTPException

from backend.services.series_ops import iter_sum_groups, mask_flag_value, mask_slices


def test_iter_sum_groups_chunks() -> None:
    groups = iter_sum_groups(
        frame_count=10, mode="chunks", step=4, range_start_1=None, range_end_1=None
    )
    assert len(groups) == 3
    assert groups[0]["indices"] == [0, 1, 2, 3]
    assert groups[1]["indices"] == [4, 5, 6, 7]
    assert groups[2]["indices"] == [8, 9]


def test_iter_sum_groups_nth() -> None:
    groups = iter_sum_groups(
        frame_count=10, mode="nth", step=3, range_start_1=None, range_end_1=None
    )
    assert len(groups) == 1
    assert groups[0]["indices"] == [0, 3, 6, 9]


def test_iter_sum_groups_range() -> None:
    groups = iter_sum_groups(frame_count=20, mode="range", step=5, range_start_1=3, range_end_1=14)
    assert len(groups) == 3
    assert groups[0]["indices"] == [2, 3, 4, 5, 6]
    assert groups[1]["indices"] == [7, 8, 9, 10, 11]
    assert groups[2]["indices"] == [12, 13]


def test_iter_sum_groups_invalid_range() -> None:
    with pytest.raises(HTTPException):
        iter_sum_groups(frame_count=10, mode="range", step=2, range_start_1=8, range_end_1=2)


def test_mask_helpers() -> None:
    mask = np.array([[0, 1, 2, 4, 8, 16]], dtype=np.uint32)
    gap, bad, any_mask = mask_slices(mask)
    assert gap.tolist() == [[False, True, False, False, False, False]]
    assert bad.tolist() == [[False, False, True, True, True, True]]
    assert any_mask.tolist() == [[False, True, True, True, True, True]]

    assert np.isnan(mask_flag_value(np.dtype(np.float32)))
    assert mask_flag_value(np.dtype(np.uint16)) == float(np.iinfo(np.uint16).max)
    assert mask_flag_value(np.dtype(np.int16)) == float(np.iinfo(np.int16).min)
