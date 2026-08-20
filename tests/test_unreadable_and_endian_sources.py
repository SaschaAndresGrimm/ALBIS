"""Regression cover for two whole-file failure modes that reached users as 500s.

Both were invisible to the rest of the suite because every fixture it builds is
little-endian and intact -- the two properties that keep these paths quiet.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient

from backend.app import app

client = TestClient(app)


def _write_stack(path: Path, data: np.ndarray, dataset: str = "entry/data/data") -> Path:
    import h5py

    with h5py.File(path, "w") as h5:
        h5.create_dataset(dataset, data=data)
    return path


def _truncate(src: Path, fraction: float = 0.4) -> Path:
    """Copy `src` cut short, standing in for a file the filewriter is still writing."""
    dst = src.with_name(f"truncated_{src.name}")
    raw = src.read_bytes()
    dst.write_bytes(raw[: int(len(raw) * fraction)])
    return dst


# --------------------------------------------------------------------------
# Big-endian sources
#
# `ndarray.newbyteorder` was removed in NumPy 2.0, so the swap these paths rely
# on raised AttributeError for exactly the inputs that need swapping.
# --------------------------------------------------------------------------


@pytest.mark.parametrize("dtype", [">u2", ">u4", ">i4", ">f4", ">f8"])
def test_frame_endpoint_serves_big_endian_datasets_as_little_endian(
    tmp_path: Path, dtype: str
) -> None:
    expected = (np.arange(2 * 4 * 4) % 97).astype(dtype).reshape(2, 4, 4)
    path = _write_stack(tmp_path / f"be_{dtype[1:]}.h5", expected)

    response = client.get(
        "/api/frame",
        params={"file": str(path), "dataset": "/entry/data/data", "index": 1},
    )

    assert response.status_code == 200
    assert response.headers["X-Dtype"].startswith("<")
    shape = [int(part) for part in response.headers["X-Shape"].split(",")]
    served = np.frombuffer(response.content, dtype=response.headers["X-Dtype"]).reshape(shape)
    # The bytes must be relabeled *and* reordered: a swap without the relabel,
    # or a relabel without the swap, both yield the wrong numbers here.
    assert np.array_equal(served, expected[1])


def test_mask_endpoint_serves_big_endian_pixel_mask(tmp_path: Path) -> None:
    import h5py

    expected = (np.arange(16) % 3).astype(">u4").reshape(4, 4)
    path = tmp_path / "mask.h5"
    with h5py.File(path, "w") as h5:
        h5.create_dataset("entry/instrument/detector/detectorSpecific/pixel_mask", data=expected)

    response = client.get("/api/mask", params={"file": str(path)})

    assert response.status_code == 200
    served = np.frombuffer(response.content, dtype=response.headers["X-Dtype"]).reshape(4, 4)
    assert np.array_equal(served, expected)


def test_remote_ingest_accepts_big_endian_raw_frames() -> None:
    expected = np.arange(16, dtype=">u2").reshape(4, 4)

    ingest = client.post(
        "/api/remote/v1/frame",
        params={"source_id": "endian-test"},
        data={"meta": '{"format": "raw", "dtype": ">u2", "shape": [4, 4]}'},
        files={"image": ("frame.raw", expected.tobytes(), "application/octet-stream")},
    )
    assert ingest.status_code == 200

    latest = client.get("/api/remote/v1/latest", params={"source_id": "endian-test"})
    assert latest.status_code == 200
    served = np.frombuffer(latest.content, dtype=latest.headers["X-Dtype"]).reshape(4, 4)
    assert np.array_equal(served, expected)


def test_little_endian_sources_are_passed_through_untouched(tmp_path: Path) -> None:
    expected = np.arange(16, dtype="<u4").reshape(1, 4, 4)
    path = _write_stack(tmp_path / "le.h5", expected)

    response = client.get("/api/frame", params={"file": str(path), "dataset": "/entry/data/data"})

    assert response.status_code == 200
    served = np.frombuffer(response.content, dtype=response.headers["X-Dtype"]).reshape(4, 4)
    assert np.array_equal(served, expected[0])


# --------------------------------------------------------------------------
# Unreadable files
#
# A viewer is pointed at whatever is on disk, which at a beamline routinely
# includes a file the filewriter has not finished writing. That is a property
# of the file, so it is reported as 422 -- never as a 500, and never as a
# silently zero-filled frame.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "endpoint,extra_params",
    [
        ("/api/datasets", {}),
        ("/api/hdf5/tree", {"path": "/"}),
        ("/api/metadata", {"dataset": "/entry/data/data"}),
        ("/api/frame", {"dataset": "/entry/data/data"}),
        ("/api/mask", {}),
    ],
)
def test_truncated_hdf5_reports_422(
    tmp_path: Path, endpoint: str, extra_params: dict[str, str]
) -> None:
    intact = _write_stack(
        tmp_path / "series.h5", np.random.default_rng(0).integers(0, 500, (20, 64, 64))
    )
    truncated = _truncate(intact)

    response = client.get(endpoint, params={"file": str(truncated), **extra_params})

    assert response.status_code == 422
    assert truncated.name in response.json()["detail"]


def test_truncated_tiff_reports_422(tmp_path: Path) -> None:
    import tifffile

    intact = tmp_path / "frame.tif"
    tifffile.imwrite(intact, np.random.default_rng(0).integers(0, 5000, (256, 256)).astype("<u2"))

    response = client.get("/api/image", params={"file": str(_truncate(intact))})

    assert response.status_code == 422


@pytest.mark.parametrize("suffix", [".cbf", ".edf"])
def test_truncated_fabio_image_reports_422(tmp_path: Path, suffix: str) -> None:
    """A short EDF is the subtle one: fabio zero-fills the missing tail.

    It decodes into a correctly shaped array and reports the shortfall only by
    logging, so without the probe in `_decode_fabio_data` half the frame would
    render as genuine zero counts.
    """
    import fabio

    data = np.random.default_rng(0).integers(0, 5000, (128, 128)).astype(np.int32)
    intact = tmp_path / f"frame{suffix}"
    writer = fabio.cbfimage.CbfImage if suffix == ".cbf" else fabio.edfimage.EdfImage
    writer(data=data).write(str(intact))

    response = client.get("/api/image", params={"file": str(_truncate(intact))})

    assert response.status_code == 422
    assert "may be incomplete or corrupt" in response.json()["detail"]


@pytest.mark.parametrize("suffix", [".cbf", ".edf", ".tif"])
def test_intact_images_still_decode(tmp_path: Path, suffix: str) -> None:
    """The guards must not cost the happy path."""
    import fabio
    import tifffile

    expected = np.random.default_rng(0).integers(0, 5000, (64, 64)).astype(np.int32)
    path = tmp_path / f"frame{suffix}"
    if suffix == ".tif":
        tifffile.imwrite(path, expected)
    else:
        writer = fabio.cbfimage.CbfImage if suffix == ".cbf" else fabio.edfimage.EdfImage
        writer(data=expected).write(str(path))

    response = client.get("/api/image", params={"file": str(path)})

    assert response.status_code == 200
    served = np.frombuffer(response.content, dtype=response.headers["X-Dtype"]).reshape(64, 64)
    assert np.array_equal(served, expected)
