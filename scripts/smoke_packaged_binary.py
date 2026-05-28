#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _wait_for_health(url: str, timeout_sec: float) -> dict[str, object]:
    deadline = time.monotonic() + timeout_sec
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.5) as response:
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
    with urllib.request.urlopen(url, timeout=timeout_sec) as response:
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
