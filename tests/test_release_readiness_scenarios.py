from __future__ import annotations

import gzip
import importlib.util
import json
import sys
import tempfile
from pathlib import Path

import h5py
import numpy as np
import pytest
import tifffile
from fabio.cbfimage import CbfImage
from fabio.edfimage import EdfImage
from fastapi.testclient import TestClient

import backend.app as backend_app_module
from backend.app import app

HEADER_TEXT = """# Pixel_size 172e-6 m x 172e-6 m
# Wavelength 1.0 A
# Detector_distance 0.2 m
# Beam_xy (123.4, 234.5) pixels
"""


def _decode_frame(response, dtype: np.dtype, shape: tuple[int, ...]) -> np.ndarray:
    return np.frombuffer(response.content, dtype=dtype).reshape(shape)


def _write_tiff(path: Path, arr: np.ndarray) -> bytes:
    tifffile.imwrite(path, arr, description=HEADER_TEXT)
    return path.read_bytes()


def _write_cbf(path: Path, arr: np.ndarray) -> bytes:
    image = CbfImage(data=arr)
    image.header = {"_array_data.header_contents": HEADER_TEXT}
    image.write(str(path))
    return path.read_bytes()


def _write_edf(path: Path, arr: np.ndarray) -> bytes:
    image = EdfImage(data=arr)
    image.header = {"_array_data.header_contents": HEADER_TEXT}
    image.write(str(path))
    return path.read_bytes()


def test_image_route_roundtrip_and_metadata_across_supported_formats(tmp_path: Path) -> None:
    client = TestClient(app)
    arr = (np.arange(12, dtype=np.uint16) * 2).reshape(3, 4)

    tiff_path = tmp_path / "series_0001.tiff"
    cbf_path = tmp_path / "series_0001.cbf"
    cbf_gz_path = tmp_path / "series_0001.cbf.gz"
    cbf_gz_sibling = tmp_path / "series_0002.cbf.gz"
    edf_path = tmp_path / "series_0001.edf"

    _write_tiff(tiff_path, arr)
    cbf_bytes = _write_cbf(cbf_path, arr)
    with gzip.open(cbf_gz_path, "wb") as handle:
        handle.write(cbf_bytes)
    with gzip.open(cbf_gz_sibling, "wb") as handle:
        handle.write(cbf_bytes)
    _write_edf(edf_path, arr)

    for path in (tiff_path, cbf_path, cbf_gz_path, edf_path):
        image_response = client.get("/api/image", params={"file": str(path)})
        assert image_response.status_code == 200
        assert image_response.headers["x-shape"] == "3,4"
        assert float(image_response.headers["x-image-detectordistance-mm"]) == 200.0
        assert float(image_response.headers["x-image-wavelength-a"]) == 1.0
        assert float(image_response.headers["x-image-energy-ev"]) == 12398.4193
        returned = _decode_frame(image_response, np.dtype("<u2"), (3, 4))
        np.testing.assert_array_equal(returned, arr)

        header_response = client.get("/api/image/header", params={"file": str(path)})
        assert header_response.status_code == 200
        assert "Pixel_size" in header_response.json()["header"]

    series_response = client.get("/api/series", params={"file": str(cbf_gz_sibling)})
    assert series_response.status_code == 200
    series_payload = series_response.json()
    assert series_payload["series"] is True
    assert series_payload["index"] == 1
    assert len(series_payload["files"]) == 2


def test_hdf5_routes_cover_linked_stack_traversal_and_frames(tmp_path: Path) -> None:
    client = TestClient(app)
    segment_a = tmp_path / "segment_a.h5"
    segment_b = tmp_path / "segment_b.h5"
    main_file = tmp_path / "linked_stack.h5"

    frames_a = np.arange(12, dtype=np.uint16).reshape(2, 2, 3)
    frames_b = (np.arange(6, dtype=np.uint16) + 100).reshape(1, 2, 3)
    mask = np.array([[1, 0, 1], [0, 1, 0]], dtype=np.uint32)

    with h5py.File(segment_a, "w") as h5:
        h5.create_dataset("/entry/data/data", data=frames_a)

    with h5py.File(segment_b, "w") as h5:
        h5.create_dataset("/entry/data/data", data=frames_b)

    with h5py.File(main_file, "w") as h5:
        entry = h5.require_group("entry")
        data_group = entry.require_group("data")
        data_group["data_000001"] = h5py.ExternalLink(str(segment_a), "/entry/data/data")
        data_group["data_000002"] = h5py.ExternalLink(str(segment_b), "/entry/data/data")
        summary = entry.require_group("summary")
        grid = summary.create_dataset("grid", data=np.arange(25, dtype=np.int16).reshape(5, 5))
        grid.attrs["units"] = "adu"
        detector = entry.require_group("instrument").require_group("detector")
        detector.create_dataset("pixel_mask", data=mask)

    datasets = client.get("/api/datasets", params={"file": str(main_file)})
    assert datasets.status_code == 200
    linked = {item["path"]: item for item in datasets.json()["datasets"]}
    assert "/entry/data" in linked
    assert linked["/entry/data"]["linked_stack"] is True
    assert linked["/entry/data"]["shape"] == [3, 2, 3]
    assert len(linked["/entry/data"]["members"]) == 2

    metadata = client.get(
        "/api/metadata", params={"file": str(main_file), "dataset": "/entry/data"}
    )
    assert metadata.status_code == 200
    metadata_payload = metadata.json()
    assert metadata_payload["linked_stack"] is True
    assert metadata_payload["shape"] == [3, 2, 3]

    frame = client.get(
        "/api/frame",
        params={"file": str(main_file), "dataset": "/entry/data", "index": 2},
    )
    assert frame.status_code == 200
    returned = _decode_frame(frame, np.dtype("<u2"), (2, 3))
    np.testing.assert_array_equal(returned, frames_b[0])

    mask_response = client.get("/api/mask", params={"file": str(main_file)})
    assert mask_response.status_code == 200
    assert mask_response.headers["x-mask-path"] == "/entry/instrument/detector/pixel_mask"
    np.testing.assert_array_equal(_decode_frame(mask_response, np.dtype("<u4"), (2, 3)), mask)

    tree = client.get("/api/hdf5/tree", params={"file": str(main_file), "path": "/entry"})
    assert tree.status_code == 200
    child_names = {child["name"] for child in tree.json()["children"]}
    assert {"data", "instrument", "summary"}.issubset(child_names)

    node = client.get(
        "/api/hdf5/node", params={"file": str(main_file), "path": "/entry/summary/grid"}
    )
    assert node.status_code == 200
    assert node.json()["dtype"] == "int16"

    value = client.get(
        "/api/hdf5/value",
        params={"file": str(main_file), "path": "/entry/summary/grid", "max_cells": 16},
    )
    assert value.status_code == 200
    assert value.json()["truncated"] is True

    search = client.get(
        "/api/hdf5/search",
        params={"file": str(main_file), "query": "grid"},
    )
    assert search.status_code == 200
    assert search.json()["matches"][0]["path"] == "/entry/summary/grid"

    missing = client.get("/api/hdf5/tree", params={"file": str(main_file), "path": "/missing"})
    assert missing.status_code == 404


def test_mask_route_falls_back_to_single_threshold_filewriter2_mask(tmp_path: Path) -> None:
    client = TestClient(app)
    main_file = tmp_path / "filewriter2_single_threshold.h5"
    frames = np.arange(6, dtype=np.uint32).reshape(1, 1, 2, 3)
    mask = np.array([[0, 1, 0], [2, 0, 4]], dtype=np.uint32)

    with h5py.File(main_file, "w") as h5:
        entry = h5.require_group("entry")
        entry.require_group("data").create_dataset("data", data=frames)
        threshold_group = (
            entry.require_group("instrument")
            .require_group("detector")
            .require_group("threshold_1_channel")
        )
        threshold_group.create_dataset("pixel_mask", data=mask)

    metadata = client.get(
        "/api/metadata", params={"file": str(main_file), "dataset": "/entry/data/data"}
    )
    assert metadata.status_code == 200
    assert metadata.json()["shape"] == [1, 1, 2, 3]

    mask_response = client.get("/api/mask", params={"file": str(main_file)})
    assert mask_response.status_code == 200
    assert (
        mask_response.headers["x-mask-path"]
        == "/entry/instrument/detector/threshold_1_channel/pixel_mask"
    )
    np.testing.assert_array_equal(_decode_frame(mask_response, np.dtype("<u4"), (2, 3)), mask)


def test_remote_stream_roundtrip_for_encoded_payloads(tmp_path: Path) -> None:
    client = TestClient(app)
    arr = (np.arange(12, dtype=np.uint16) + 10).reshape(3, 4)

    builders = {
        "tiff": ("frame.tiff", lambda path: _write_tiff(path, arr)),
        "edf": ("frame.edf", lambda path: _write_edf(path, arr)),
        "cbf.gz": (
            "frame.cbf.gz",
            lambda path: gzip.compress(_write_cbf(path.with_suffix(""), arr)),
        ),
    }

    for fmt, (filename, builder) in builders.items():
        source_id = f"encoded-{fmt.replace('.', '-')}"
        payload_path = tmp_path / filename
        payload = builder(payload_path)
        upload = client.post(
            "/api/remote/v1/frame",
            params={"source_id": source_id, "seq": 7},
            data={"meta": json.dumps({"format": fmt, "display_name": f"Encoded {fmt}"})},
            files={"image": (filename, payload, "application/octet-stream")},
        )
        assert upload.status_code == 200

        latest = client.get("/api/remote/v1/latest", params={"source_id": source_id})
        assert latest.status_code == 200
        assert latest.headers["x-remote-seq"] == "7"
        assert latest.headers["x-remote-display"] == f"Encoded {fmt}"
        np.testing.assert_array_equal(_decode_frame(latest, np.dtype("<u2"), (3, 4)), arr)

        same = client.get("/api/remote/v1/latest", params={"source_id": source_id, "after_seq": 7})
        assert same.status_code == 204

        conflict = client.get("/api/remote/v1/meta", params={"source_id": source_id, "seq": 6})
        assert conflict.status_code == 409
        assert conflict.json()["current_seq"] == 7


def test_upload_browse_autoload_and_series_flow(tmp_path: Path, monkeypatch) -> None:
    client = TestClient(app)
    upload_root = tmp_path / "uploads"
    upload_root.mkdir()

    arr = np.arange(6, dtype=np.uint16).reshape(2, 3)
    upload_path = upload_root / "series_0001.tiff"
    upload_bytes = _write_tiff(upload_path, arr)
    upload_path.unlink()

    upload_response = client.post(
        "/api/upload",
        params={"folder": str(upload_root)},
        files={"file": ("series_0001.tiff", upload_bytes, "image/tiff")},
    )
    assert upload_response.status_code == 200
    uploaded_target = Path(upload_response.json()["path"]).resolve()
    assert uploaded_target.exists()

    sibling = upload_root / "series_0002.tiff"
    _write_tiff(sibling, arr + 1)

    browse = client.get("/api/browse", params={"path": str(upload_root)})
    assert browse.status_code == 200
    assert browse.json()["files"] == ["series_0001.tiff", "series_0002.tiff"]

    files = client.get("/api/files", params={"folder": str(upload_root)})
    assert files.status_code == 200
    assert str(uploaded_target) in files.json()["files"]

    autoload = client.get("/api/autoload/latest", params={"folder": str(upload_root)})
    assert autoload.status_code == 200
    assert autoload.json()["absolute"] is True
    assert Path(autoload.json()["file"]).is_absolute()

    image = client.get("/api/image", params={"file": str(uploaded_target)})
    assert image.status_code == 200
    np.testing.assert_array_equal(_decode_frame(image, np.dtype("<u2"), (2, 3)), arr)

    series = client.get("/api/series", params={"file": str(uploaded_target)})
    assert series.status_code == 200
    assert series.json()["series"] is True
    assert len(series.json()["files"]) == 2

    unsupported = client.post(
        "/api/upload",
        params={"folder": str(upload_root)},
        files={"file": ("notes.txt", b"not-an-image", "text/plain")},
    )
    assert unsupported.status_code == 400

    monkeypatch.setattr(backend_app_module.runtime_state, "max_upload_bytes", len(upload_bytes) - 1)
    too_large = client.post(
        "/api/upload",
        params={"folder": str(upload_root)},
        files={"file": ("too-large.tiff", upload_bytes, "image/tiff")},
    )
    assert too_large.status_code == 413


def _load_smoke_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "smoke_packaged_binary.py"
    spec = importlib.util.spec_from_file_location("smoke_packaged_binary", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_smoke_harness_rejects_a_build_without_zstd() -> None:
    """A PyInstaller build that drops the zstandard extension still starts and
    serves frames, just uncompressed. Nothing else would notice, so this guard is
    the only thing standing between a packaging slip and a silent remote
    performance regression — it has to actually fail."""
    module = _load_smoke_module()

    module._assert_zstd_bundled({"compression_encodings": ["zstd", "gzip"]})

    for degraded in ({"compression_encodings": ["gzip"]}, {"compression_encodings": []}, {}):
        with pytest.raises(RuntimeError, match="zstd"):
            module._assert_zstd_bundled(degraded)


def test_packaged_binary_smoke_harness_with_dummy_server(tmp_path: Path) -> None:
    module = _load_smoke_module()

    debug_log = Path(tempfile.gettempdir()) / "albis_dummy_server.log"
    debug_log.unlink(missing_ok=True)

    server_source = """\
import json
import os
import signal
import socket
import tempfile
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# HTTPServer.server_bind() calls socket.getfqdn(host), a reverse-DNS lookup that
# can block for many seconds on macOS CI runners, delaying listen() so the
# harness times out connecting. The resolved name is unused here, so stub it.
socket.getfqdn = lambda *_args: "localhost"

_DEBUG_LOG = Path(r"__DEBUG_LOG__")


def _main():
    config = json.loads(Path("albis.config.json").read_text(encoding="utf-8"))
    port = int(config["server"]["port"])

    # Move out of the launch cwd so the harness can delete its temp dir even if
    # a wrapper process (Windows .bat) leaves this server briefly orphaned.
    os.chdir(tempfile.gettempdir())

    def _watchdog():
        # Exit even if no stop signal is delivered (e.g. when launched through a
        # Windows .bat wrapper that is terminated without us).
        time.sleep(30)
        os._exit(0)

    threading.Thread(target=_watchdog, daemon=True).start()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/api/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(
                    b"{\\"status\\": \\"ok\\", \\"version\\": \\"9.9.9\\","
                    b" \\"compression_encodings\\": [\\"zstd\\", \\"gzip\\"]}"
                )
                return
            if self.path == "/":
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"<!doctype html><html><body>dummy</body></html>")
                return
            if self.path == "/app.js":
                self.send_response(200)
                self.send_header("Content-Type", "application/javascript; charset=utf-8")
                self.end_headers()
                self.wfile.write(b"console.log('dummy albis');")
                return
            # The harness decodes a frame of each format it stages, so the dummy
            # has to answer those too -- otherwise this test would stop covering
            # the decode assertions the moment they were added.
            if self.path.startswith("/api/image?"):
                body = bytes(4 * 4 * 4)
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("X-Shape", "4,4")
                self.send_header("X-Dtype", "<u4")
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path.startswith("/api/datasets?"):
                self.send_response(200)
                self.end_headers()
                payload = {"datasets": [{"path": "/entry/data/data"}]}
                self.wfile.write(json.dumps(payload).encode())
                return
            if self.path.startswith("/api/frame?"):
                # Frame N filled with N, matching the committed fixture.
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("X-Shape", "4,4")
                self.send_header("X-Dtype", "<u4")
                self.end_headers()
                self.wfile.write((2).to_bytes(4, "little") * 16)
                return
            self.send_response(404)
            self.end_headers()

        def log_message(self, *args, **kwargs):
            return

    def _stop(*_args):
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.serve_forever()


try:
    _main()
except SystemExit:
    raise
except BaseException:
    _DEBUG_LOG.write_text(traceback.format_exc(), encoding="utf-8")
    raise
""".replace(
        "__DEBUG_LOG__", str(debug_log)
    )

    # smoke_packaged_binary launches the binary directly (no interpreter prefix),
    # so the stand-in must be natively executable on each OS. sys.executable avoids
    # depending on a "python3" being resolvable on PATH (notably the macOS system
    # python stub), and the Windows .bat wrapper is launchable where a POSIX
    # shebang script is not.
    if sys.platform.startswith("win"):
        server_py = tmp_path / "dummy_albis_server.py"
        server_py.write_text(server_source, encoding="utf-8")
        server = tmp_path / "dummy_albis.bat"
        server.write_text(
            f'@echo off\r\n"{sys.executable}" "{server_py}" %*\r\n',
            encoding="utf-8",
        )
    else:
        server = tmp_path / "dummy_albis"
        server.write_text(f"#!{sys.executable}\n{server_source}", encoding="utf-8")
        server.chmod(0o755)

    try:
        module.run_smoke(
            server,
            startup_timeout_sec=15.0,
            stop_timeout_sec=2.0,
            expected_version="9.9.9",
        )
    finally:
        if debug_log.exists():
            print("dummy smoke server crash log:\n" + debug_log.read_text(encoding="utf-8"))
            debug_log.unlink(missing_ok=True)


def test_native_pickers_return_conflict_when_unavailable(monkeypatch) -> None:
    client = TestClient(app)

    def _raise_no_display(*_args, **_kwargs) -> None:
        raise RuntimeError("No graphical display available")

    monkeypatch.setattr("backend.routes.files._choose_file", _raise_no_display)
    monkeypatch.setattr("backend.routes.files._choose_folder", _raise_no_display)

    choose_file = client.get("/api/choose-file")
    assert choose_file.status_code == 409
    assert choose_file.json()["detail"] == "File picker unavailable: No graphical display available"

    choose_folder = client.get("/api/choose-folder")
    assert choose_folder.status_code == 409
    assert (
        choose_folder.json()["detail"]
        == "Folder picker unavailable: No graphical display available"
    )
