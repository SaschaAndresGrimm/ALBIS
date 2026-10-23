"""Pixel masks (beta): load, save, and read or replace a detector's mask."""

from __future__ import annotations

import base64
import io
import json
import time
from collections.abc import Iterator
from pathlib import Path

import h5py
import numpy as np
import pytest
import tifffile
from fastapi.testclient import TestClient

from backend.app import app, runtime_state
from backend.config import normalize_config
from backend.services import pixel_mask
from tests.fake_simplon import FakeDCUServer


@pytest.fixture()
def dcu() -> Iterator[FakeDCUServer]:
    with FakeDCUServer(init_delay=0.05, max_series_s=0.6) as server:
        yield server


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> TestClient:
    monkeypatch.setattr(runtime_state, "pixel_mask", True)
    monkeypatch.setattr(runtime_state, "detector_control", True)
    monkeypatch.setattr(runtime_state, "data_dir", tmp_path)
    return TestClient(app, base_url="http://127.0.0.1")


def _initialize(client: TestClient, url: str) -> None:
    client.post(
        "/api/detector/command", json={"url": url, "subsystem": "detector", "command": "initialize"}
    )
    deadline = time.time() + 5
    while time.time() < deadline:
        job = client.get("/api/detector/status", params={"url": url}).json()["command"]
        if job and not job["running"]:
            return
        time.sleep(0.02)
    raise AssertionError("initialize did not finish")


def _mask(shape: tuple[int, int] = (256, 256)) -> np.ndarray:
    mask = np.zeros(shape, dtype="<u4")
    mask[0, :] = 1  # a gap row
    mask[1, 2] = 2  # dead
    mask[2, 3] = 8  # hot
    mask[-1, -1] = 0x80000000  # a high bit, which must survive every trip
    return mask


def _decode(response) -> np.ndarray:
    assert response.status_code == 200, response.text
    shape = tuple(int(n) for n in response.headers["X-Shape"].split(","))
    return np.frombuffer(response.content, dtype=response.headers["X-Dtype"]).reshape(shape)


def _form(meta: dict, mask: np.ndarray) -> dict:
    return {
        "data": {"meta": json.dumps(meta)},
        "files": {"pixels": ("mask.bin", mask.astype("<u4").tobytes(), "application/octet-stream")},
    }


def test_the_setting_is_off_unless_switched_on() -> None:
    assert normalize_config({})["ui"]["pixel_mask"] is False
    assert normalize_config({"ui": {"pixel_mask": "true"}})["ui"]["pixel_mask"] is True


def test_everything_is_off_until_switched_on(
    monkeypatch: pytest.MonkeyPatch, dcu: FakeDCUServer, tmp_path: Path
) -> None:
    monkeypatch.setattr(runtime_state, "pixel_mask", False)
    monkeypatch.setattr(runtime_state, "detector_control", True)
    client = TestClient(app, base_url="http://127.0.0.1")
    tiff = tmp_path / "m.tiff"
    tifffile.imwrite(tiff, _mask())
    meta = {"url": dcu.url, "shape": [256, 256], "format": "tiff"}
    for response in (
        client.get("/api/pixel-mask/file", params={"file": str(tiff)}),
        client.post("/api/pixel-mask/encode", **_form(meta, _mask())),
        client.get("/api/pixel-mask/detector", params={"url": dcu.url}),
        client.post("/api/pixel-mask/detector", **_form(meta, _mask())),
    ):
        assert response.status_code == 404
        assert "switched off" in response.json()["detail"]
    assert dcu.dcu.requests == []


def test_the_detector_routes_also_need_detector_control(
    monkeypatch: pytest.MonkeyPatch, dcu: FakeDCUServer
) -> None:
    monkeypatch.setattr(runtime_state, "pixel_mask", True)
    monkeypatch.setattr(runtime_state, "detector_control", False)
    client = TestClient(app, base_url="http://127.0.0.1")
    meta = {"url": dcu.url, "shape": [256, 256]}
    for response in (
        client.get("/api/pixel-mask/detector", params={"url": dcu.url}),
        client.post("/api/pixel-mask/detector", **_form(meta, _mask())),
    ):
        assert response.status_code == 404
        assert "Detector control" in response.json()["detail"]
    assert dcu.dcu.requests == []


@pytest.mark.parametrize("fmt", ["tiff", "h5"])
def test_a_saved_mask_loads_back_bit_for_bit(client: TestClient, tmp_path: Path, fmt: str) -> None:
    mask = _mask((40, 30))
    saved = client.post(
        "/api/pixel-mask/encode",
        **_form({"format": fmt, "shape": [40, 30], "name": "my mask/../x"}, mask),
    )
    assert saved.status_code == 200, saved.text
    disposition = saved.headers["Content-Disposition"]
    assert disposition.startswith("attachment;")
    # The name is kept to safe characters: no folder can be named through it.
    assert "/" not in disposition.split("filename=")[1]
    assert disposition.endswith(f'.{fmt}"')

    path = tmp_path / f"saved.{fmt}"
    path.write_bytes(saved.content)
    if fmt == "h5":
        with h5py.File(path, "r") as h5:
            assert np.array_equal(h5[pixel_mask.HDF5_MASK_PATH][()], mask)
    else:
        assert np.array_equal(tifffile.imread(path), mask)

    loaded = client.get("/api/pixel-mask/file", params={"file": str(path)})
    assert np.array_equal(_decode(loaded), mask)
    assert loaded.headers["X-Dtype"] == "<u4"


def test_a_mask_is_read_from_where_a_detector_file_keeps_it(
    client: TestClient, tmp_path: Path
) -> None:
    mask = _mask((20, 10)).astype("<u4")
    path = tmp_path / "series_master.h5"
    with h5py.File(path, "w") as h5:
        h5["/entry/instrument/detector/detectorSpecific/pixel_mask"] = mask
    loaded = client.get("/api/pixel-mask/file", params={"file": str(path)})
    assert np.array_equal(_decode(loaded), mask)
    assert loaded.headers["X-Mask-Path"].endswith("detectorSpecific/pixel_mask")


def test_a_master_with_a_mask_per_threshold_gives_one(client: TestClient, tmp_path: Path) -> None:
    """An EIGER2 or PILATUS4 with several thresholds writes no single mask."""
    path = tmp_path / "series_160_master.h5"
    masks = [np.full((6, 4), n + 1, dtype="<u4") for n in range(4)]
    with h5py.File(path, "w") as h5:
        for n, mask in enumerate(masks):
            h5[f"/entry/instrument/detector/threshold_{n + 1}_channel/pixel_mask"] = mask
        h5["/entry/instrument/detector/pixel_mask_applied"] = np.int8(1)

    first = client.get("/api/pixel-mask/file", params={"file": str(path)})
    assert np.array_equal(_decode(first), masks[0])
    assert first.headers["X-Mask-Path"].endswith("threshold_1_channel/pixel_mask")

    third = client.get("/api/pixel-mask/file", params={"file": str(path), "threshold": 2})
    assert np.array_equal(_decode(third), masks[2])
    assert third.headers["X-Mask-Path"].endswith("threshold_3_channel/pixel_mask")

    # A threshold the file does not have: the first one's, not nothing.
    absent = client.get("/api/pixel-mask/file", params={"file": str(path), "threshold": 7})
    assert np.array_equal(_decode(absent), masks[0])


def test_files_that_are_not_masks_are_refused(client: TestClient, tmp_path: Path) -> None:
    no_mask = tmp_path / "no_mask.h5"
    with h5py.File(no_mask, "w") as h5:
        h5["/entry/data/data"] = np.zeros((2, 4, 4), dtype="u2")
    assert client.get("/api/pixel-mask/file", params={"file": str(no_mask)}).status_code == 404

    floats = tmp_path / "floats.tiff"
    tifffile.imwrite(floats, np.full((4, 4), 0.5, dtype="f4"))
    assert client.get("/api/pixel-mask/file", params={"file": str(floats)}).status_code == 400

    wrong_size = client.post(
        "/api/pixel-mask/encode", **_form({"format": "tiff", "shape": [5, 5]}, _mask((4, 4)))
    )
    assert wrong_size.status_code == 400
    wrong_format = client.post(
        "/api/pixel-mask/encode", **_form({"format": "png", "shape": [4, 4]}, _mask((4, 4)))
    )
    assert wrong_format.status_code == 400


def test_the_detectors_mask_is_read(client: TestClient, dcu: FakeDCUServer) -> None:
    _initialize(client, dcu.url)
    response = client.get("/api/pixel-mask/detector", params={"url": dcu.url})
    mask = _decode(response)
    assert mask.shape == (256, 256)
    assert response.headers["X-Mask-Path"] == "pixel_mask"
    assert mask[128, 0] == 1 and mask[0, 40] == 2


def test_sending_a_mask_backs_up_the_old_one_and_confirms_the_new(
    client: TestClient, dcu: FakeDCUServer, tmp_path: Path
) -> None:
    _initialize(client, dcu.url)
    before = dcu.dcu.pixel_mask()
    old = np.frombuffer(base64.b64decode(before["value"]["data"]), dtype="<u4").reshape(256, 256)
    mask = _mask()

    sent = client.post(
        "/api/pixel-mask/detector",
        **_form({"url": dcu.url, "shape": [256, 256], "serial": "E-32-0123/x"}, mask),
    )
    assert sent.status_code == 200, sent.text
    result = sent.json()
    assert result["verified"] is True
    assert result["key"] == "pixel_mask"
    assert result["masked"] == int(np.count_nonzero(mask))
    assert result["previous_masked"] == int(np.count_nonzero(old))
    assert np.array_equal(dcu.dcu.mask, mask)

    # The detector's previous mask, in the data root, under its serial.
    backup = tmp_path / result["backup"]
    assert result["backup"].startswith("masks/backups/detector_E-32-0123-x_")
    assert np.array_equal(tifffile.imread(backup), old)
    # ... and it loads like any mask file, to send back.
    restored = client.get("/api/pixel-mask/file", params={"file": str(backup)})
    assert np.array_equal(_decode(restored), old)


def test_a_mask_of_another_size_is_not_sent(
    client: TestClient, dcu: FakeDCUServer, tmp_path: Path
) -> None:
    _initialize(client, dcu.url)
    sent = client.post(
        "/api/pixel-mask/detector",
        **_form({"url": dcu.url, "shape": [128, 256]}, _mask((128, 256))),
    )
    assert sent.status_code == 400
    assert "256 x 256" in sent.json()["detail"]
    assert dcu.dcu.mask is not None and dcu.dcu.mask.shape == (256, 256)
    assert not (tmp_path / "masks").exists()
    assert not any(method == "PUT" and "pixel_mask" in path for method, path in dcu.dcu.requests)


def test_no_mask_is_sent_while_the_detector_is_busy(client: TestClient, dcu: FakeDCUServer) -> None:
    _initialize(client, dcu.url)
    dcu.dcu.state = "acquire"
    sent = client.post(
        "/api/pixel-mask/detector", **_form({"url": dcu.url, "shape": [256, 256]}, _mask())
    )
    assert sent.status_code == 409
    assert not any(method == "PUT" and "pixel_mask" in path for method, path in dcu.dcu.requests)


def test_an_uninitialized_detector_has_no_mask_to_read(
    client: TestClient, dcu: FakeDCUServer
) -> None:
    response = client.get("/api/pixel-mask/detector", params={"url": dcu.url})
    assert response.status_code == 404


def test_the_array_format_round_trips() -> None:
    mask = _mask((3, 5))
    assert np.array_equal(pixel_mask.decode_darray(pixel_mask.encode_darray(mask)), mask)


def test_a_tiff_written_here_is_readable_without_albis() -> None:
    mask = _mask((6, 4))
    assert np.array_equal(tifffile.imread(io.BytesIO(pixel_mask.encode_tiff(mask))), mask)
