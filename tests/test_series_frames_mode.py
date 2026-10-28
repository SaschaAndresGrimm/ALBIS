"""Mode "frames": which frames and how many images, as two separate choices.

The Combine Frames section sends only this mode. Each of the older modes is one
setting of it, and it also combines a range with every Nth frame, which none of
them could.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.app import app
from backend.services.series_ops import iter_sum_groups, mask_flag_value, mask_slices
from backend.services.series_summing import SeriesSummingDeps, SeriesSummingService


def _indices(frame_count: int, mode: str, step: int, start=None, end=None, **kw) -> list[list[int]]:
    return [g["indices"] for g in iter_sum_groups(frame_count, mode, step, start, end, **kw)]


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (("all", 4, None, None), {"start": None, "end": None}),
        (("chunks", 4, None, None), {"start": None, "end": None, "group_size": 4}),
        (("nth", 3, None, None), {"start": None, "end": None, "frame_step": 3}),
        (("range", 5, 3, 14), {"start": 3, "end": 14, "group_size": 5}),
    ],
)
def test_each_older_mode_is_one_setting_of_frames(old, new) -> None:
    mode, step, start, end = old
    expected = _indices(20, mode, step, start, end)
    assert _indices(20, "frames", 1, **new) == expected


def test_a_range_with_every_nth_frame_into_one_image() -> None:
    assert _indices(20, "frames", 1, 3, 14, frame_step=4) == [[2, 6, 10]]


def test_groups_count_the_selected_frames_not_the_span() -> None:
    assert _indices(10, "frames", 1, frame_step=2, group_size=2) == [[0, 2], [4, 6], [8]]


def test_a_range_past_the_end_is_clipped_and_a_reversed_one_refused() -> None:
    assert _indices(5, "frames", 1, 4, 99) == [[3, 4]]
    with pytest.raises(HTTPException):
        iter_sum_groups(10, "frames", 1, 8, 2)


def _service(
    tmp_path: Path,
    frame_total: int,
    written: list[tuple[Path, np.ndarray, Any]],
    frames: dict[Path, np.ndarray] | None = None,
):
    files = [tmp_path / f"img_{i:04d}.tiff" for i in range(1, frame_total + 1)]
    if frames is None:
        frames = {p: np.full((2, 2), i, dtype=np.int32) for i, p in enumerate(files, start=1)}
    else:
        frames = dict(zip(files, frames.values(), strict=True))

    def _unsupported(*_a, **_k):
        raise AssertionError("unsupported dependency was called in this test")

    deps = SeriesSummingDeps(
        data_dir=tmp_path,
        get_allow_abs_paths=lambda: True,
        is_within=lambda p, root: p.resolve().is_relative_to(root.resolve()),
        logger=logging.getLogger("albis.test"),
        ensure_hdf5_stack=lambda: None,
        get_h5py=lambda: None,
        resolve_image_file=Path,
        image_ext_name=lambda name: ".tiff",
        resolve_series_files=lambda _src: (list(files), 0),
        read_tiff=lambda path, index=0: np.asarray(frames[path]),
        read_cbf=_unsupported,
        read_cbf_gz=_unsupported,
        read_edf=_unsupported,
        write_tiff=lambda path, arr, meta=None: written.append((path, np.asarray(arr), meta)),
        iter_sum_groups=iter_sum_groups,
        mask_flag_value=mask_flag_value,
        mask_slices=mask_slices,
        resolve_dataset_view=_unsupported,
        extract_frame=_unsupported,
        find_pixel_mask=lambda *_a, **_k: None,
        combined_frame_metadata=lambda _src, _ds, description: {"description": description},
    )
    return SeriesSummingService(deps), files


def _run(service: SeriesSummingService, first: Path, tmp_path: Path, **kw) -> dict[str, Any]:
    job_id = service.start_job(
        file=str(first),
        dataset="",
        mode="frames",
        step=10,
        operation=kw.pop("operation", "sum"),
        output_path=str(tmp_path / "out"),
        output_format="tiff",
        apply_mask=kw.pop("apply_mask", False),
        range_start=kw.pop("range_start", None),
        range_end=kw.pop("range_end", None),
        **kw,
    )
    deadline = time.time() + 5
    while time.time() < deadline:
        job = service.get_job(job_id)
        if job and job["status"] in {"done", "error", "cancelled"}:
            return job
        time.sleep(0.02)
    raise AssertionError("job did not finish")


def test_whole_series_into_one_image_keeps_integer_counts(tmp_path: Path) -> None:
    written: list = []
    service, files = _service(tmp_path, 6, written)
    job = _run(service, files[0], tmp_path)
    assert job["status"] == "done", job
    [(path, arr, meta)] = written
    np.testing.assert_array_equal(arr, np.full((2, 2), 21))
    assert np.issubdtype(arr.dtype, np.integer)
    assert "_all_" in path.name
    assert meta["description"] == "sum of 6 frames"


def test_a_range_with_every_nth_frame_names_and_describes_its_frames(tmp_path: Path) -> None:
    written: list = []
    service, files = _service(tmp_path, 10, written)
    job = _run(service, files[0], tmp_path, range_start=2, range_end=9, frame_step=3)
    assert job["status"] == "done", job
    [(path, arr, meta)] = written
    np.testing.assert_array_equal(arr, np.full((2, 2), 2 + 5 + 8))
    assert "_f000002-000008_every003_" in path.name
    assert meta["description"] == "sum of 3 frames 2-8, every 3th frame"


def test_groups_write_one_numbered_image_each(tmp_path: Path) -> None:
    written: list = []
    service, files = _service(tmp_path, 5, written)
    job = _run(service, files[0], tmp_path, group_size=2, operation="mean")
    assert job["status"] == "done", job
    names = [p.name for p, _a, _m in written]
    assert [n.split("_mean")[1][:22] for n in names] == [
        "_chunk0001_f000001-000",
        "_chunk0002_f000003-000",
        "_chunk0003_f000005-000",
    ]
    np.testing.assert_allclose(written[0][1], np.full((2, 2), 1.5))


@pytest.mark.parametrize(
    ("field", "value", "detail"),
    [("frame_step", 0, "Frame step"), ("group_size", 0, "Group size"), ("mode", "bogus", "mode")],
)
def test_the_api_refuses_a_bad_frames_request(field, value, detail) -> None:
    body = {"file": "x.tiff", "mode": "frames", field: value}
    response = TestClient(app).post("/api/analysis/series-sum/start", json=body)
    assert response.status_code == 400
    assert detail.lower() in response.json()["detail"].lower()


def test_normalization_is_still_accepted_but_logged_as_deprecated(caplog) -> None:
    with caplog.at_level(logging.WARNING, logger="albis"):
        response = TestClient(app).post(
            "/api/analysis/series-sum/start",
            json={
                "file": "missing.tiff",
                "mode": "all",
                "normalize_method": "scalar",
                "normalize_scalar": 2,
            },
        )
    assert response.status_code == 200
    assert any("normalize_scalar is deprecated" in r.getMessage() for r in caplog.records)


def _uint16_frames_with_a_gap(count: int) -> dict[Path, np.ndarray]:
    """Unsigned frames mark a gap with the type's maximum, as a detector TIFF does."""
    frames = {}
    for i in range(count):
        frame = np.full((2, 2), 10 + i, dtype=np.uint16)
        frame[0, 1] = np.iinfo(np.uint16).max
        frames[Path(f"f{i}")] = frame
    return frames


def test_an_unsigned_gap_is_masked_in_a_mean_as_minus_one(tmp_path: Path) -> None:
    written: list = []
    service, files = _service(tmp_path, 4, written, _uint16_frames_with_a_gap(4))
    job = _run(service, files[0], tmp_path, operation="mean", apply_mask=True)
    assert job["status"] == "done", job
    [(_path, arr, _meta)] = written
    # Float output: the viewer reads -1 as a gap and inpaints it.
    assert arr.dtype.kind == "f"
    assert arr[0, 1] == -1.0
    assert arr[0, 0] == pytest.approx(11.5)


def test_an_unsigned_gap_stays_the_maximum_in_a_sum_not_a_count(tmp_path: Path) -> None:
    written: list = []
    service, files = _service(tmp_path, 4, written, _uint16_frames_with_a_gap(4))
    job = _run(service, files[0], tmp_path, operation="sum", apply_mask=True)
    assert job["status"] == "done", job
    [(_path, arr, _meta)] = written
    assert arr.dtype.kind == "u"
    assert arr[0, 1] == np.iinfo(arr.dtype).max
    assert arr[0, 0] == 10 + 11 + 12 + 13


def test_without_apply_mask_the_maximum_is_combined_as_recorded(tmp_path: Path) -> None:
    written: list = []
    service, files = _service(tmp_path, 4, written, _uint16_frames_with_a_gap(4))
    job = _run(service, files[0], tmp_path, operation="mean", apply_mask=False)
    assert job["status"] == "done", job
    [(_path, arr, _meta)] = written
    assert arr[0, 1] == pytest.approx(65535.0)
