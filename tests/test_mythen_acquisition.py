from __future__ import annotations

import numpy as np
from fastapi.testclient import TestClient

from backend.app import app
from backend.image_formats import _mythen_header_text, _read_mythen_acquisition

CFG_XML = """<config>
  <version>3.0</version>
  <frames>4</frames>
  <exposureTime units="ms">100</exposureTime>
  <exposurePeriod units="ms">100.089</exposurePeriod>
  <energy units="keV">8.1</energy>
  <channels>6</channels>
  <systemNumber>M-1451</systemNumber>
  <badChannels>
    <bad>0</bad>
    <bad>5</bad>
  </badChannels>
  <modules>
    <module>
      <serialNumber>SN2409</serialNumber>
      <threshold units="keV">6.4</threshold>
      <material>Si</material>
      <thickness units="um">1000</thickness>
    </module>
  </modules>
</config>
"""


def _write_acquisition(folder, n_frames: int = 4, n_channels: int = 6):
    """Create a minimal MYTHEN acquisition and return the expected count matrix."""
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "Acq.cfg").write_text(CFG_XML)
    expected = np.zeros((n_frames, n_channels), dtype=np.int64)
    for f in range(n_frames):
        lines = []
        for c in range(n_channels):
            count = f * 10 + c  # deterministic, unique per (frame, channel)
            expected[f, c] = count
            lines.append(f"{c} {count}")
        (folder / f"Frame{f + 1:04d}.dat").write_text("\n".join(lines) + "\n")
    return expected


def test_read_mythen_acquisition_builds_frames_by_channels_matrix(tmp_path):
    folder = tmp_path / "Acquisition0001"
    expected = _write_acquisition(folder)

    arr, meta = _read_mythen_acquisition(folder / "Acq.cfg")

    assert arr.shape == (4, 6)  # (frames, channels)
    assert np.array_equal(arr, expected)
    assert meta["channels"] == 6
    assert meta["frames_read"] == 4
    assert meta["frames_declared"] == 4
    assert meta["energy_ev"] == 8100.0
    assert meta["exposure_time_s"] == 0.1
    assert meta["threshold_ev"] == 6400.0
    assert meta["bad_channels"] == [0, 5]
    assert meta["module_serial"] == "SN2409"
    assert meta["material"] == "Si"


def test_read_mythen_acquisition_accepts_dat_path(tmp_path):
    folder = tmp_path / "Acquisition0001"
    expected = _write_acquisition(folder)

    via_dat, _ = _read_mythen_acquisition(folder / "Frame0002.dat")

    assert np.array_equal(via_dat, expected)


def test_read_mythen_acquisition_without_frames_raises(tmp_path):
    folder = tmp_path / "Empty"
    folder.mkdir()
    (folder / "Acq.cfg").write_text(CFG_XML)

    import pytest
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as exc:
        _read_mythen_acquisition(folder / "Acq.cfg")
    assert exc.value.status_code == 422


def test_mythen_header_text_summarizes_config(tmp_path):
    folder = tmp_path / "Acquisition0001"
    _write_acquisition(folder)

    header = _mythen_header_text(folder / "Acq.cfg")

    assert "MYTHEN acquisition" in header
    assert "Module S/N SN2409" in header
    assert "Frames 4" in header
    assert "Bad_channels (2)" in header


def test_image_endpoint_serves_mythen_acquisition(tmp_path):
    folder = tmp_path / "Acquisition0001"
    expected = _write_acquisition(folder)
    client = TestClient(app)

    response = client.get("/api/image", params={"file": str(folder / "Acq.cfg")})

    assert response.status_code == 200
    dtype = np.dtype(response.headers["X-Dtype"])
    shape = tuple(int(x) for x in response.headers["X-Shape"].split(","))
    arr = np.frombuffer(response.content, dtype=dtype).reshape(shape)
    assert shape == (4, 6)
    assert np.array_equal(arr.astype(np.int64), expected)
    assert response.headers.get("X-Image-Energy-Ev") == "8100.0"
    assert response.headers.get("X-Image-Bad-Channels") == "0,5"


def test_image_header_endpoint_returns_mythen_summary(tmp_path):
    folder = tmp_path / "Acquisition0001"
    _write_acquisition(folder)
    client = TestClient(app)

    response = client.get("/api/image/header", params={"file": str(folder / "Acq.cfg")})

    assert response.status_code == 200
    assert "MYTHEN acquisition" in response.json()["header"]
