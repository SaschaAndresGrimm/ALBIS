#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


# The backend is always reached over loopback, so a proxy must never be
# consulted. A dedicated no-proxy opener also avoids the slow system proxy
# auto-detection urllib performs on macOS, which can stall localhost requests.
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _wait_for_health(url: str, timeout_sec: float) -> dict[str, object]:
    deadline = time.monotonic() + timeout_sec
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with _OPENER.open(url, timeout=1.5) as response:
                if response.status == 200:
                    payload = json.load(response)
                    return payload if isinstance(payload, dict) else {}
        except (json.JSONDecodeError, urllib.error.URLError, TimeoutError, OSError) as exc:
            last_error = exc
        time.sleep(0.2)
    detail = f" last error: {last_error}" if last_error is not None else ""
    raise TimeoutError(f"Timed out waiting for {url}.{detail}")


def _assert_http_asset(
    url: str, *, expected_content_type_substring: str, timeout_sec: float
) -> None:
    with _OPENER.open(url, timeout=timeout_sec) as response:
        if response.status != 200:
            raise RuntimeError(f"Unexpected HTTP status {response.status} for {url}")
        content_type = response.headers.get("Content-Type", "")
        if expected_content_type_substring not in content_type:
            raise RuntimeError(
                f"Unexpected Content-Type for {url}: {content_type!r};"
                f" expected substring {expected_content_type_substring!r}"
            )
        if not response.read(256):
            raise RuntimeError(f"Empty response body for {url}")


def _assert_zstd_bundled(payload: dict[str, object]) -> None:
    """Fail if the packaged build cannot produce zstd responses.

    zstd needs a native extension. If PyInstaller misses it the app still starts
    and remote clients silently fall back to gzip, which is a real regression that
    no functional test would notice — so assert it explicitly here.
    """
    encodings = payload.get("compression_encodings")
    if not isinstance(encodings, list) or "zstd" not in encodings:
        raise RuntimeError(
            "Packaged backend cannot produce zstd responses; the zstandard "
            f"extension is likely not bundled. /api/health reported: {encodings!r}"
        )


def _terminate_process(proc: subprocess.Popen[str], timeout_sec: float) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout_sec)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=timeout_sec)


def _read_repo_version() -> str | None:
    version_file = Path(__file__).resolve().parents[1] / "VERSION"
    try:
        version = version_file.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return version or None


def _assert_health_version(payload: dict[str, object], expected_version: str) -> None:
    actual_version = str(payload.get("version") or "")
    if actual_version != expected_version:
        raise RuntimeError(
            "Unexpected packaged backend version from /api/health: "
            f"{actual_version or '<missing>'!r}; expected {expected_version!r}"
        )


# Detector files the packaged build must be able to decode. Serving the UI shell
# proves the process starts; it says nothing about whether the image stack --
# fabio's format plugins, tifffile, and the native hdf5plugin filter libraries --
# survived being frozen. Those are the parts a PyInstaller build actually loses,
# and losing them breaks the first thing a user does.
REPO_TESTDATA = Path(__file__).resolve().parents[1] / "testdata"
DECODE_FIXTURES = ("in16c_010001.cbf", "monitor.tiff", "compressed_stack.h5")


def _stage_decode_fixtures(data_root: Path) -> list[str]:
    """Copy the decode fixtures into the server's data root, if present."""
    data_root.mkdir(parents=True, exist_ok=True)
    staged: list[str] = []
    for name in DECODE_FIXTURES:
        source = REPO_TESTDATA / name
        if not source.exists():
            continue
        shutil.copy2(source, data_root / name)
        staged.append(name)
    return staged


def _get(url: str, timeout_sec: float):
    try:
        response = _OPENER.open(url, timeout=timeout_sec)
    except urllib.error.HTTPError as exc:
        # Surface the backend's own explanation. A bare "HTTP 500" in a release
        # log says nothing; the detail names the format that failed to decode.
        detail = ""
        try:
            body = json.loads(exc.read().decode("utf-8", "replace"))
            detail = str(body.get("detail") or "")
        except Exception:
            pass
        raise RuntimeError(
            f"{url} failed with HTTP {exc.code}"
            + (f": {detail}" if detail else "")
            + " -- the packaged build cannot decode this format."
            " Check that the image stack (fabio plugins, tifffile, hdf5plugin"
            " filter libraries) was collected into the bundle."
        ) from exc
    if response.status != 200:
        raise RuntimeError(f"Unexpected HTTP status {response.status} for {url}")
    return response


def _assert_decodes_image(base_url: str, name: str, timeout_sec: float) -> None:
    """Decode a non-HDF image and check the payload matches its own headers."""
    url = f"{base_url}/api/image?file={urllib.parse.quote(name)}"
    with _get(url, timeout_sec) as response:
        shape = response.headers.get("X-Shape", "")
        dtype = response.headers.get("X-Dtype", "")
        body = response.read()
    dims = [int(part) for part in shape.split(",") if part.strip()]
    if len(dims) != 2 or not dtype:
        raise RuntimeError(f"{name}: unusable headers (X-Shape={shape!r} X-Dtype={dtype!r})")
    itemsize = int(dtype[-1])
    expected = dims[0] * dims[1] * itemsize
    if len(body) != expected:
        raise RuntimeError(
            f"{name}: decoded {len(body)} bytes, expected {expected} for {shape} {dtype}"
        )


def _assert_decodes_compressed_hdf5(base_url: str, name: str, timeout_sec: float) -> None:
    """Read a frame from a compression-filtered HDF5 and verify its contents.

    This is the check that fails when hdf5plugin's native filter libraries are
    missing from the bundle: the file opens and its datasets list, and only the
    actual frame read fails.
    """
    quoted = urllib.parse.quote(name)
    with _get(f"{base_url}/api/datasets?file={quoted}", timeout_sec) as response:
        datasets = json.load(response).get("datasets") or []
    if not datasets:
        raise RuntimeError(f"{name}: no image datasets discovered")

    dataset = str(datasets[0].get("path") or "")
    index = 2
    url = f"{base_url}/api/frame?file={quoted}&dataset={urllib.parse.quote(dataset)}&index={index}"
    with _get(url, timeout_sec) as response:
        dtype = response.headers.get("X-Dtype", "")
        body = response.read()
    if not body:
        raise RuntimeError(f"{name}: empty frame payload for {dataset}")
    # The fixture fills frame N with the value N (see
    # scripts/make_compressed_hdf5_fixture.py), so a decode that silently
    # returned zeros or the wrong frame is caught rather than passing as "200".
    first_pixel = int.from_bytes(body[:4], "little", signed=False)
    if first_pixel != index:
        raise RuntimeError(
            f"{name}: frame {index} first pixel was {first_pixel}, expected {index}"
            f" (dtype={dtype!r}) -- decompression produced the wrong data"
        )


def run_smoke(
    binary_path: Path,
    startup_timeout_sec: float,
    stop_timeout_sec: float,
    expected_version: str | None = None,
) -> None:
    if not binary_path.exists():
        raise FileNotFoundError(f"Binary not found: {binary_path}")

    port = _find_free_port()
    with tempfile.TemporaryDirectory(prefix="albis-smoke-") as tmp_dir:
        root = Path(tmp_dir)
        config_path = root / "albis.config.json"
        config = {
            "server": {
                "host": "127.0.0.1",
                "port": port,
                "reload": False,
            },
            "launcher": {
                "startup_timeout_sec": 2.0,
                "open_browser": False,
                "debug_macos_events": False,
            },
            "data": {
                "root": str((root / "data").resolve()),
                "allow_abs_paths": True,
            },
            "logging": {
                "level": "INFO",
                "dir": str((root / "logs").resolve()),
            },
        }
        config_path.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")

        staged_fixtures = _stage_decode_fixtures(root / "data")

        child_env = os.environ.copy()
        child_env["HOME"] = str(root)
        if sys.platform.startswith("win"):
            child_env["USERPROFILE"] = str(root)

        proc = subprocess.Popen(
            [str(binary_path)],
            cwd=root,
            env=child_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            if proc.poll() is not None:
                raise RuntimeError(f"Process exited early with code {proc.returncode}.")
            health_payload = _wait_for_health(
                f"http://127.0.0.1:{port}/api/health", startup_timeout_sec
            )
            if expected_version:
                _assert_health_version(health_payload, expected_version)
            _assert_zstd_bundled(health_payload)
            _assert_http_asset(
                f"http://127.0.0.1:{port}/",
                expected_content_type_substring="text/html",
                timeout_sec=1.5,
            )
            _assert_http_asset(
                f"http://127.0.0.1:{port}/app.js",
                expected_content_type_substring="javascript",
                timeout_sec=1.5,
            )

            base_url = f"http://127.0.0.1:{port}"
            for name in staged_fixtures:
                if name.endswith((".h5", ".hdf5")):
                    _assert_decodes_compressed_hdf5(base_url, name, timeout_sec=20.0)
                else:
                    _assert_decodes_image(base_url, name, timeout_sec=20.0)
            missing = sorted(set(DECODE_FIXTURES) - set(staged_fixtures))
            if missing:
                # Never let absent fixtures read as a pass: the decode path would
                # simply go unchecked.
                raise RuntimeError(
                    f"Decode fixtures missing from {REPO_TESTDATA}: {', '.join(missing)}"
                )
        finally:
            _terminate_process(proc, stop_timeout_sec)


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test a packaged ALBIS binary.")
    parser.add_argument("--binary", required=True, help="Path to packaged ALBIS executable")
    parser.add_argument("--startup-timeout", type=float, default=30.0)
    parser.add_argument("--stop-timeout", type=float, default=10.0)
    parser.add_argument(
        "--expected-version",
        default=None,
        help="Expected /api/health version. Defaults to VERSION when available.",
    )
    parser.add_argument(
        "--skip-version-check",
        action="store_true",
        help="Do not assert the packaged /api/health version.",
    )
    args = parser.parse_args()

    binary_path = Path(args.binary).expanduser().resolve()
    expected_version = (
        None if args.skip_version_check else args.expected_version or _read_repo_version()
    )
    run_smoke(
        binary_path,
        startup_timeout_sec=args.startup_timeout,
        stop_timeout_sec=args.stop_timeout,
        expected_version=expected_version,
    )
    print(f"Smoke test passed for {binary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
