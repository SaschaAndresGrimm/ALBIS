from __future__ import annotations

import errno
import json
import tempfile
import uuid
from pathlib import Path

import numpy as np
from fastapi.testclient import TestClient

from backend.app import ALBIS_VERSION, LOG_PATH, app


def test_health_endpoint() -> None:
    client = TestClient(app)
    response = client.get("/api/health")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["version"] == ALBIS_VERSION


def test_open_log_endpoint_returns_backend_log_path() -> None:
    client = TestClient(app)
    response = client.post("/api/open-log")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert LOG_PATH is not None
    returned_path = Path(payload["path"]).resolve()
    assert returned_path == LOG_PATH.resolve()
    assert returned_path.exists()
    assert returned_path.name == "albis.log"


def test_upload_falls_back_when_target_is_read_only(monkeypatch) -> None:
    client = TestClient(app)
    file_name = f"pytest-upload-{uuid.uuid4().hex[:8]}.tif"
    payload = b"albis-upload-payload"

    with tempfile.TemporaryDirectory() as temp_root:
        upload_root = Path(temp_root).resolve()
        primary_dest = (upload_root / file_name).resolve()
        real_path_open = Path.open

        def patched_open(self, mode="r", *args, **kwargs):
            if self.resolve() == primary_dest and "w" in mode:
                raise OSError(errno.EROFS, "Read-only file system")
            return real_path_open(self, mode, *args, **kwargs)

        monkeypatch.setattr(Path, "open", patched_open)

        response = client.post(
            "/api/upload",
            params={"folder": str(upload_root)},
            files={"file": (file_name, payload, "image/tiff")},
        )

    assert response.status_code == 200
    body = response.json()
    resolved = Path(body["path"]).resolve()
    assert resolved.is_absolute()
    assert resolved.name == file_name
    assert resolved.exists()
    assert resolved.read_bytes() == payload
    resolved.unlink(missing_ok=True)


def test_remote_latest_returns_204_for_missing_source() -> None:
    client = TestClient(app)
    response = client.get(
        "/api/remote/v1/latest", params={"source_id": f"missing-{uuid.uuid4().hex}"}
    )
    assert response.status_code == 204


def test_remote_stream_roundtrip_raw_frame() -> None:
    client = TestClient(app)
    source_id = f"pytest-{uuid.uuid4().hex[:8]}"
    frame = (np.arange(24, dtype=np.uint16) * 3).reshape(4, 6)
    meta = {
        "format": "raw",
        "dtype": "<u2",
        "shape": [4, 6],
        "display_name": "Pytest remote frame",
        "series_number": 7,
        "image_number": 11,
        "image_datetime": "2026-02-17T08:00:00Z",
        "resolution": {
            "distance_mm": 120.0,
            "pixel_size_um": 75.0,
            "energy_ev": 12000.0,
            "beam_center_px": [3.0, 2.0],
        },
        "peak_sets": [
            {"name": "set-a", "color": "#00ff88", "points": [[1, 2, 10.0], [3, 1, 8.0]]},
        ],
    }
    upload = client.post(
        "/api/remote/v1/frame",
        params={"source_id": source_id},
        data={"meta": json.dumps(meta)},
        files={"image": ("frame.raw", frame.tobytes(order="C"), "application/octet-stream")},
    )
    assert upload.status_code == 200
    assert upload.json()["source_id"] == source_id

    latest = client.get("/api/remote/v1/latest", params={"source_id": source_id})
    assert latest.status_code == 200
    assert latest.headers["x-dtype"] == "<u2"
    assert latest.headers["x-shape"] == "4,6"
    assert latest.headers["x-remote-source"] == source_id
    assert latest.headers["x-remote-display"] == "Pytest remote frame"
    assert latest.headers["x-remote-peaksets"] == "1"
    returned = np.frombuffer(latest.content, dtype=np.dtype("<u2")).reshape(4, 6)
    np.testing.assert_array_equal(returned, frame)

    # Same sequence should now return 204 when asked after current seq.
    seq = int(latest.headers["x-remote-seq"])
    same = client.get("/api/remote/v1/latest", params={"source_id": source_id, "after_seq": seq})
    assert same.status_code == 204

    meta_response = client.get("/api/remote/v1/meta", params={"source_id": source_id, "seq": seq})
    assert meta_response.status_code == 200
    meta_payload = meta_response.json()
    assert meta_payload["series_number"] == 7
    assert meta_payload["image_number"] == 11
    assert len(meta_payload["peak_sets"]) == 1
