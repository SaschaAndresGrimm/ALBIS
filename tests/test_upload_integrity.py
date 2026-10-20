"""An upload that stops short is discarded, and is not reopened on retry.

A detector file chosen in the browser while it is still being written or
copied arrives cut off. Kept on the server under its real name, HDF5 cannot
open it, and the upload flow used to reopen that broken copy on every retry
because the name already existed.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import h5py
import numpy as np
from fastapi.testclient import TestClient

from backend.app import app
from backend.routes.files import _hdf5_declared_size


def _hdf5_bytes(tmp: Path, *, userblock: int = 0, libver: str = "earliest") -> bytes:
    path = tmp / "source.h5"
    kwargs = {"userblock_size": userblock} if userblock else {}
    kwargs["libver"] = libver
    with h5py.File(path, "w", **kwargs) as h5:
        h5.create_dataset("entry/data/data", data=np.arange(200_000, dtype=np.uint32))
    return path.read_bytes()


def test_superblock_size_matches_a_complete_file() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        # superblock v0 and v3, with and without a user block in front
        for libver in ("earliest", "latest"):
            for userblock in (0, 512, 2048):
                data = _hdf5_bytes(tmp_path, userblock=userblock, libver=libver)
                target = tmp_path / f"copy_{libver}_{userblock}.h5"
                target.write_bytes(data)
                assert _hdf5_declared_size(target) == len(data)
                target.write_bytes(data[:-1])
                assert _hdf5_declared_size(target) == len(data)


def test_superblock_size_is_none_for_other_files() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        target = Path(tmp) / "frame.tif"
        target.write_bytes(b"II*\x00" + b"\x00" * 5000)
        assert _hdf5_declared_size(target) is None


def test_a_cut_off_hdf5_upload_is_rejected_and_discarded() -> None:
    client = TestClient(app)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp).resolve()
        data = _hdf5_bytes(root)
        partial = data[: len(data) // 2]

        response = client.post(
            "/api/upload",
            params={"folder": str(root)},
            files={"file": ("scan_data_000001.h5", partial, "application/x-hdf5")},
        )

        assert response.status_code == 422
        detail = response.json()["detail"]
        assert f"received {len(partial)} of {len(data)} bytes" in detail
        assert not (root / "scan_data_000001.h5").exists()


def test_a_body_shorter_than_the_browser_said_is_rejected() -> None:
    client = TestClient(app)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp).resolve()
        response = client.post(
            "/api/upload",
            params={"folder": str(root), "size": 100},
            files={"file": ("frame_00001.tif", b"II*\x00" + b"\x00" * 40, "image/tiff")},
        )

        assert response.status_code == 422
        assert "received 44 of 100 bytes" in response.json()["detail"]
        assert not (root / "frame_00001.tif").exists()


def test_a_complete_upload_is_kept() -> None:
    client = TestClient(app)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp).resolve()
        data = _hdf5_bytes(root)

        response = client.post(
            "/api/upload",
            params={"folder": str(root), "size": len(data)},
            files={"file": ("scan_data_000001.h5", data, "application/x-hdf5")},
        )

        assert response.status_code == 200
        assert (root / "scan_data_000001.h5").read_bytes() == data


def test_existing_copy_is_reused_only_when_complete_and_same_size() -> None:
    client = TestClient(app)
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp).resolve()
        data = _hdf5_bytes(root)
        name = "scan_data_000001.h5"

        def existing(size: int) -> str | None:
            response = client.get(
                "/api/upload/existing",
                params={"name": name, "size": size, "folder": str(root)},
            )
            assert response.status_code == 200
            return response.json()["path"]

        assert existing(len(data)) is None  # nothing there yet

        (root / name).write_bytes(data[: len(data) // 2])
        # A cut-off copy from an earlier attempt: neither the browser's size
        # nor the file's own header lets it stand in for the real thing.
        assert existing(len(data)) is None
        assert existing(len(data) // 2) is None

        (root / name).write_bytes(data)
        assert existing(len(data)) == str(root / name)
