from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import numpy as np
import pytest
from fastapi import HTTPException

from backend.services.series_ops import iter_sum_groups, mask_flag_value, mask_slices
from backend.services.series_summing import SeriesSummingDeps, SeriesSummingService


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


def test_series_summing_service_nth_mode_sum(tmp_path: Path) -> None:
    series_files = [tmp_path / f"img_{idx:04d}.tiff" for idx in range(1, 6)]
    frames = {
        path: np.full((2, 2), idx, dtype=np.int16) for idx, path in enumerate(series_files, start=1)
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
        mode="nth",
        step=2,
        operation="sum",
        normalize_frame=None,
        range_start=None,
        range_end=None,
        output_path=str(tmp_path / "nth_out"),
        output_format="tiff",
        apply_mask=False,
    )
    job = _wait_for_job(service, job_id)
    assert job["status"] == "done"
    assert len(written) == 1
    _path, out = written[0]
    # nth with step=2 over 5 frames selects indices 0,2,4 => values 1,3,5
    np.testing.assert_array_equal(out, np.full((2, 2), 9, dtype=np.int16))


def test_series_summing_service_range_mode_emits_multiple_chunks(tmp_path: Path) -> None:
    series_files = [tmp_path / f"img_{idx:04d}.tiff" for idx in range(1, 7)]
    frames = {
        path: np.full((2, 2), idx, dtype=np.int16) for idx, path in enumerate(series_files, start=1)
    }
    written: list[np.ndarray] = []

    def resolve_image_file(name: str) -> Path:
        return Path(name)

    def resolve_series_files(_source: Path) -> tuple[list[Path], int]:
        return list(series_files), 0

    def read_tiff(path: Path, index: int) -> np.ndarray:
        assert index == 0
        return np.asarray(frames[path])

    def write_tiff(_path: Path, arr: np.ndarray) -> None:
        written.append(np.asarray(arr))

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
        mode="range",
        step=2,
        operation="sum",
        normalize_frame=None,
        range_start=2,
        range_end=5,
        output_path=str(tmp_path / "range_out"),
        output_format="tiff",
        apply_mask=False,
    )
    job = _wait_for_job(service, job_id)
    assert job["status"] == "done"
    # range 2..5 with chunk step=2 => [2,3] and [4,5]
    assert len(written) == 2
    np.testing.assert_array_equal(written[0], np.full((2, 2), 5, dtype=np.int16))
    np.testing.assert_array_equal(written[1], np.full((2, 2), 9, dtype=np.int16))


def test_series_summing_service_normalize_and_mask_non_h5(tmp_path: Path) -> None:
    series_files = [tmp_path / "img_0001.tiff", tmp_path / "img_0002.tiff"]
    frames = {
        series_files[0]: np.array([[2, 4], [6, 8]], dtype=np.int16),
        # -1 => gap pixel, -2 => defective pixel
        series_files[1]: np.array([[4, -1], [8, -2]], dtype=np.int16),
    }
    written: list[np.ndarray] = []

    def resolve_image_file(name: str) -> Path:
        return Path(name)

    def resolve_series_files(_source: Path) -> tuple[list[Path], int]:
        return list(series_files), 0

    def read_tiff(path: Path, index: int) -> np.ndarray:
        assert index == 0
        return np.asarray(frames[path])

    def write_tiff(_path: Path, arr: np.ndarray) -> None:
        written.append(np.asarray(arr))

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
        operation="mean",
        normalize_frame=1,
        range_start=None,
        range_end=None,
        output_path=str(tmp_path / "norm_mask_out"),
        output_format="tiff",
        apply_mask=True,
    )
    job = _wait_for_job(service, job_id)
    assert job["status"] == "done"
    assert len(written) == 1
    out = written[0]
    assert out.dtype == np.float32
    assert out[0, 0] == pytest.approx(1.5, abs=1e-4)
    assert out[1, 0] == pytest.approx((1.0 + (8 / 6)) / 2, abs=1e-4)
    assert out[0, 1] == pytest.approx(-1.0)
    assert out[1, 1] == pytest.approx(-2.0)


def test_series_summing_service_normalize_scalar_non_h5(tmp_path: Path) -> None:
    series_files = [tmp_path / "img_0001.tiff", tmp_path / "img_0002.tiff"]
    frames = {
        series_files[0]: np.array([[2, 4], [6, 8]], dtype=np.int16),
        series_files[1]: np.array([[4, 8], [10, 12]], dtype=np.int16),
    }
    written: list[np.ndarray] = []

    def resolve_image_file(name: str) -> Path:
        return Path(name)

    def resolve_series_files(_source: Path) -> tuple[list[Path], int]:
        return list(series_files), 0

    def read_tiff(path: Path, index: int) -> np.ndarray:
        assert index == 0
        return np.asarray(frames[path])

    def write_tiff(_path: Path, arr: np.ndarray) -> None:
        written.append(np.asarray(arr))

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
        normalize_method="scalar",
        normalize_frame=None,
        normalize_scalar=2.0,
        normalize_image=None,
        range_start=None,
        range_end=None,
        output_path=str(tmp_path / "norm_scalar_out"),
        output_format="tiff",
        apply_mask=False,
    )
    job = _wait_for_job(service, job_id)

    assert job["status"] == "done"
    assert len(written) == 1
    out = written[0]
    assert out.dtype == np.float32
    np.testing.assert_allclose(out, np.array([[3.0, 6.0], [8.0, 10.0]], dtype=np.float32))


def test_series_summing_service_normalize_image_masks_invalid_ref_non_h5(tmp_path: Path) -> None:
    series_files = [tmp_path / "img_0001.tiff", tmp_path / "img_0002.tiff"]
    norm_file = tmp_path / "norm_ref.tiff"
    frames = {
        series_files[0]: np.array([[2, 4], [6, 8]], dtype=np.int16),
        series_files[1]: np.array([[4, 8], [12, 16]], dtype=np.int16),
        norm_file: np.array([[2.0, 0.0], [np.nan, 4.0]], dtype=np.float32),
    }
    written: list[np.ndarray] = []

    def resolve_image_file(name: str) -> Path:
        return Path(name)

    def resolve_series_files(_source: Path) -> tuple[list[Path], int]:
        return list(series_files), 0

    def read_tiff(path: Path, index: int) -> np.ndarray:
        assert index == 0
        return np.asarray(frames[path])

    def write_tiff(_path: Path, arr: np.ndarray) -> None:
        written.append(np.asarray(arr))

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
        operation="mean",
        normalize_method="image",
        normalize_frame=None,
        normalize_scalar=None,
        normalize_image=str(norm_file),
        range_start=None,
        range_end=None,
        output_path=str(tmp_path / "norm_image_out"),
        output_format="tiff",
        apply_mask=True,
    )
    job = _wait_for_job(service, job_id)

    assert job["status"] == "done"
    assert len(written) == 1
    out = written[0]
    assert out.dtype == np.float32
    assert out[0, 0] == pytest.approx(1.5, abs=1e-4)
    assert out[1, 1] == pytest.approx(3.0, abs=1e-4)
    assert out[0, 1] == pytest.approx(-2.0, abs=1e-4)
    assert out[1, 0] == pytest.approx(-2.0, abs=1e-4)


def test_series_summing_service_tiff_filename_reflects_operation(tmp_path: Path) -> None:
    series_files = [tmp_path / "img_0001.tiff", tmp_path / "img_0002.tiff"]
    frames = {
        series_files[0]: np.array([[1, 2], [3, 4]], dtype=np.int16),
        series_files[1]: np.array([[5, 6], [7, 8]], dtype=np.int16),
    }
    written_paths: list[Path] = []

    def resolve_image_file(name: str) -> Path:
        return Path(name)

    def resolve_series_files(_source: Path) -> tuple[list[Path], int]:
        return list(series_files), 0

    def read_tiff(path: Path, index: int) -> np.ndarray:
        assert index == 0
        return np.asarray(frames[path])

    def write_tiff(path: Path, _arr: np.ndarray) -> None:
        written_paths.append(Path(path))

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
        operation="mean",
        normalize_method="none",
        normalize_frame=None,
        normalize_scalar=None,
        normalize_image=None,
        range_start=None,
        range_end=None,
        output_path=str(tmp_path / "series_sum"),
        output_format="tiff",
        apply_mask=False,
    )
    job = _wait_for_job(service, job_id)

    assert job["status"] == "done"
    assert len(written_paths) == 1
    assert "_mean_" in written_paths[0].name
