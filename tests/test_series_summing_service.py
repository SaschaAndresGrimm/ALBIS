from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from fastapi import HTTPException

from backend.services.series_summing import SeriesSummingDeps, SeriesSummingService
from backend.services.series_ops import iter_sum_groups, mask_flag_value, mask_slices


def _wait_for_job(
    service: SeriesSummingService, job_id: str, timeout_s: float = 5.0
) -> dict[str, Any]:
    deadline = time.time() + timeout_s
    last: dict[str, Any] | None = None
    while time.time() < deadline:
        last = service.get_job(job_id)
        if last and last.get("status") in {"done", "error", "cancelled"}:
            return last
        time.sleep(0.02)
    raise AssertionError(f"job did not finish in time: {last}")


def _make_deps(
    tmp_path: Path,
    *,
    resolve_image_file,
    resolve_series_files,
    read_tiff,
    write_tiff,
) -> SeriesSummingDeps:
    def _unsupported(*_args, **_kwargs):
        raise AssertionError("unsupported dependency was called in this test")

    return SeriesSummingDeps(
        data_dir=tmp_path,
        get_allow_abs_paths=lambda: True,
        is_within=lambda p, root: p.resolve().is_relative_to(root.resolve()),
        logger=type("L", (), {"exception": lambda *_args, **_kwargs: None})(),
        ensure_hdf5_stack=lambda: None,
        get_h5py=lambda: None,
        resolve_image_file=resolve_image_file,
        image_ext_name=lambda name: ".tiff" if name.lower().endswith((".tif", ".tiff")) else "",
        resolve_series_files=resolve_series_files,
        read_tiff=read_tiff,
        read_cbf=_unsupported,
        read_cbf_gz=_unsupported,
        read_edf=_unsupported,
        write_tiff=write_tiff,
        iter_sum_groups=iter_sum_groups,
        mask_flag_value=mask_flag_value,
        mask_slices=mask_slices,
        resolve_dataset_view=_unsupported,
        extract_frame=_unsupported,
        find_pixel_mask=lambda *_args, **_kwargs: None,
    )


def test_series_summing_service_job_lifecycle_done(tmp_path: Path) -> None:
    series_files = [
        tmp_path / "img_0001.tiff",
        tmp_path / "img_0002.tiff",
        tmp_path / "img_0003.tiff",
    ]
    frames = {
        series_files[0]: np.array([[1, 2], [3, 4]], dtype=np.int32),
        series_files[1]: np.array([[5, 6], [7, 8]], dtype=np.int32),
        series_files[2]: np.array([[9, 10], [11, 12]], dtype=np.int32),
    }
    written: list[tuple[Path, np.ndarray]] = []

    def resolve_image_file(name: str) -> Path:
        return Path(name)

    def resolve_series_files(_source: Path) -> tuple[list[Path], int]:
        return list(series_files), 0

    def read_tiff(path: Path, index: int) -> np.ndarray:
        assert index == 0
        return np.asarray(frames[path])

    def write_tiff(path: Path, arr: np.ndarray) -> None:
        written.append((Path(path), np.asarray(arr)))

    service = SeriesSummingService(
        _make_deps(
            tmp_path,
            resolve_image_file=resolve_image_file,
            resolve_series_files=resolve_series_files,
            read_tiff=read_tiff,
            write_tiff=write_tiff,
        )
    )
    job_id = service.start_job(
        file=str(series_files[0]),
        dataset="",
        mode="all",
        step=1,
        operation="sum",
        normalize_frame=None,
        range_start=None,
        range_end=None,
        output_path=str(tmp_path / "series_out"),
        output_format="tiff",
        apply_mask=False,
    )
    job = _wait_for_job(service, job_id)

    assert job["status"] == "done"
    assert float(job["progress"]) == pytest.approx(1.0)
    assert len(job["outputs"]) == 1
    assert len(written) == 1
    _path, out = written[0]
    np.testing.assert_array_equal(out, np.array([[15, 18], [21, 24]], dtype=np.int32))


def test_series_summing_service_job_lifecycle_error(tmp_path: Path) -> None:
    def resolve_image_file(_name: str) -> Path:
        raise HTTPException(status_code=404, detail="File not found")

    service = SeriesSummingService(
        _make_deps(
            tmp_path,
            resolve_image_file=resolve_image_file,
            resolve_series_files=lambda _source: ([], 0),
            read_tiff=lambda _path, _index: np.zeros((2, 2), dtype=np.int32),
            write_tiff=lambda _path, _arr: None,
        )
    )
    job_id = service.start_job(
        file="missing.tiff",
        dataset="",
        mode="all",
        step=1,
        operation="sum",
        normalize_frame=None,
        range_start=None,
        range_end=None,
        output_path=str(tmp_path / "series_out"),
        output_format="tiff",
        apply_mask=False,
    )
    job = _wait_for_job(service, job_id)

    assert job["status"] == "error"
    assert float(job["progress"]) == pytest.approx(1.0)
    assert "Failed: File not found" in str(job["message"])
    assert "File not found" in str(job["error"])


def test_series_summing_service_median_preserves_integer_dtype_when_integral(
    tmp_path: Path,
) -> None:
    series_files = [
        tmp_path / "img_0001.tiff",
        tmp_path / "img_0002.tiff",
        tmp_path / "img_0003.tiff",
    ]
    frames = {
        series_files[0]: np.array([[1, 2], [3, 4]], dtype=np.int16),
        series_files[1]: np.array([[5, 6], [7, 8]], dtype=np.int16),
        series_files[2]: np.array([[9, 10], [11, 12]], dtype=np.int16),
    }
    written: list[tuple[Path, np.ndarray]] = []

    def resolve_image_file(name: str) -> Path:
        return Path(name)

    def resolve_series_files(_source: Path) -> tuple[list[Path], int]:
        return list(series_files), 0

    def read_tiff(path: Path, index: int) -> np.ndarray:
        assert index == 0
        return np.asarray(frames[path])

    def write_tiff(path: Path, arr: np.ndarray) -> None:
        written.append((Path(path), np.asarray(arr)))

    service = SeriesSummingService(
        _make_deps(
            tmp_path,
            resolve_image_file=resolve_image_file,
            resolve_series_files=resolve_series_files,
            read_tiff=read_tiff,
            write_tiff=write_tiff,
        )
    )
    job_id = service.start_job(
        file=str(series_files[0]),
        dataset="",
        mode="all",
        step=1,
        operation="median",
        normalize_frame=None,
        range_start=None,
        range_end=None,
        output_path=str(tmp_path / "median_out"),
        output_format="tiff",
        apply_mask=False,
    )
    job = _wait_for_job(service, job_id)

    assert job["status"] == "done"
    assert len(written) == 1
    _path, out = written[0]
    assert out.dtype == np.int16
    np.testing.assert_array_equal(out, np.array([[5, 6], [7, 8]], dtype=np.int16))


def test_series_summing_service_cancel_job(tmp_path: Path) -> None:
    series_files = [tmp_path / f"img_{idx:04d}.tiff" for idx in range(1, 81)]
    frame = np.array([[1, 2], [3, 4]], dtype=np.int32)

    def resolve_image_file(name: str) -> Path:
        return Path(name)

    def resolve_series_files(_source: Path) -> tuple[list[Path], int]:
        return list(series_files), 0

    def read_tiff(_path: Path, index: int) -> np.ndarray:
        assert index == 0
        time.sleep(0.01)
        return np.asarray(frame)

    written: list[Path] = []

    def write_tiff(path: Path, _arr: np.ndarray) -> None:
        written.append(Path(path))

    service = SeriesSummingService(
        _make_deps(
            tmp_path,
            resolve_image_file=resolve_image_file,
            resolve_series_files=resolve_series_files,
            read_tiff=read_tiff,
            write_tiff=write_tiff,
        )
    )
    job_id = service.start_job(
        file=str(series_files[0]),
        dataset="",
        mode="all",
        step=1,
        operation="sum",
        normalize_frame=None,
        range_start=None,
        range_end=None,
        output_path=str(tmp_path / "cancel_out"),
        output_format="tiff",
        apply_mask=False,
    )

    deadline = time.time() + 2.0
    while time.time() < deadline:
        job = service.get_job(job_id) or {}
        if job.get("status") == "running":
            break
        time.sleep(0.01)

    assert service.cancel_job(job_id) is True
    job = _wait_for_job(service, job_id, timeout_s=5.0)
    assert job["status"] == "cancelled"
    assert float(job["progress"]) == pytest.approx(1.0)
    assert written == []
