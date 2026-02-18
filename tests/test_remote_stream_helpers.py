from __future__ import annotations

import json

import numpy as np
import pytest
from fastapi import HTTPException

from backend.services.remote_stream import (
    remote_extract_metadata,
    remote_parse_meta,
    remote_safe_source_id,
    remote_snapshot,
    remote_store_frame,
)


def test_remote_safe_source_id_defaults_and_validation() -> None:
    assert remote_safe_source_id(None) == "default"
    assert remote_safe_source_id("abc-123") == "abc-123"
    with pytest.raises(HTTPException):
        remote_safe_source_id("bad id with spaces")


def test_remote_parse_meta() -> None:
    payload = remote_parse_meta('{"a": 1}')
    assert payload == {"a": 1}
    with pytest.raises(HTTPException):
        remote_parse_meta("not-json")
    with pytest.raises(HTTPException):
        remote_parse_meta(json.dumps([1, 2, 3]))


def test_remote_extract_metadata_resolution_and_peaksets() -> None:
    meta = {
        "display_name": "test",
        "series_number": 3,
        "image_number": 7,
        "image_datetime": "2026-01-01T00:00:00Z",
        "resolution": {
            "distance_mm": 120.0,
            "pixel_size_um": 75.0,
            "energy_ev": 12000.0,
            "beam_center_px": [10.0, 20.0],
        },
        "peak_sets": [{"name": "s1", "color": "00ff88", "points": [[1, 2, 3.0], [4, 5]]}],
    }
    out = remote_extract_metadata(meta)
    assert out["display_name"] == "test"
    assert out["series_number"] == 3
    assert out["image_number"] == 7
    assert out["resolution"]["distance_mm"] == 120.0
    assert out["resolution"]["beam_center_px"] == [10.0, 20.0]
    assert len(out["peak_sets"]) == 1
    assert out["peak_sets"][0]["color"] == "#00ff88"


def test_remote_store_and_snapshot_roundtrip() -> None:
    frame = np.arange(6, dtype=np.uint16).reshape(2, 3)
    seq = remote_store_frame(
        source_id="pytest-src", frame=frame, meta={"display_name": "f"}, seq=None
    )
    snap = remote_snapshot("pytest-src")
    assert snap is not None
    assert snap["seq"] == seq
    assert snap["dtype"] == frame.dtype.str
    assert tuple(snap["shape"]) == (2, 3)
    returned = np.frombuffer(snap["bytes"], dtype=np.dtype(frame.dtype.str)).reshape(2, 3)
    np.testing.assert_array_equal(returned, frame)
