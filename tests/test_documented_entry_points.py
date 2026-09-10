"""The run commands the documentation promises must actually start ALBIS.

`python backend/app.py` was the first executable line of CONTRIBUTING.md's
quick start and appeared in six places across three documents. It had been
broken for roughly seven releases: running a module of a package as a script
leaves it with no parent package, so `from .build_info import ...` fails before
anything is served. Nothing that ships was affected -- the launcher, Docker and
the PyInstaller bundle all import `backend` as a package -- which is exactly
why nothing caught it. Every other test imports the app object directly and so
never exercises an entry point.

These tests launch the documented commands as real subprocesses and wait for
`/api/health`, which is the only way this class of breakage is visible.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
STARTUP_TIMEOUT_S = 60.0


def _free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _health(port: int) -> dict | None:
    try:
        with urllib.request.urlopen(  # noqa: S310 - fixed loopback URL
            f"http://127.0.0.1:{port}/api/health", timeout=2
        ) as response:
            if response.status != 200:
                return None
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError, ValueError, json.JSONDecodeError):
        return None


@contextmanager
def _serving(command: list[str], port: int) -> Iterator[dict]:
    """Run `command`, yield the /api/health payload, then shut it down."""
    env = dict(
        os.environ,
        ALBIS_SERVER_HOST="127.0.0.1",
        ALBIS_SERVER_PORT=str(port),
        # A browser opening on a CI runner would be at best pointless.
        ALBIS_UI_OPEN_BROWSER="false",
        PYTHONUNBUFFERED="1",
    )
    process = subprocess.Popen(
        command,
        cwd=REPO_ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        deadline = time.monotonic() + STARTUP_TIMEOUT_S
        payload = None
        while time.monotonic() < deadline:
            if process.poll() is not None:
                output = process.communicate()[0]
                pytest.fail(
                    f"{' '.join(command)} exited with {process.returncode} before serving:\n{output}"
                )
            payload = _health(port)
            if payload is not None:
                break
            time.sleep(0.25)
        if payload is None:
            process.terminate()
            output = process.communicate(timeout=15)[0]
            pytest.fail(
                f"{' '.join(command)} never answered /api/health "
                f"within {STARTUP_TIMEOUT_S:.0f}s:\n{output}"
            )
        yield payload
    finally:
        process.terminate()
        try:
            process.communicate(timeout=15)
        except subprocess.TimeoutExpired:  # pragma: no cover - defensive
            process.kill()
            process.communicate()


@pytest.mark.parametrize(
    ("command", "documented_in"),
    [
        (
            [sys.executable, "albis_launcher.py"],
            "CONTRIBUTING.md, DEVELOPER_GUIDE.md and POWER_USER_GUIDE.md quick starts",
        ),
        (
            [sys.executable, "-m", "uvicorn", "backend.app:app"],
            "the backend-only alternative, and Dockerfile's CMD",
        ),
        (
            [sys.executable, "-m", "backend.app"],
            "DEVELOPER_GUIDE.md's auto-reload note",
        ),
    ],
    ids=["albis_launcher.py", "uvicorn backend.app:app", "-m backend.app"],
)
def test_a_documented_command_serves_the_health_endpoint(
    command: list[str], documented_in: str
) -> None:
    port = _free_port()
    if command[-1] == "backend.app:app":
        command = [*command, "--host", "127.0.0.1", "--port", str(port)]

    with _serving(command, port) as payload:
        assert payload.get("status"), f"empty health payload ({documented_in})"
        assert payload.get("version"), f"no version reported ({documented_in})"


def test_running_the_package_module_as_a_script_fails_with_one_clear_error() -> None:
    """`python backend/app.py` cannot work, and should say so in one line.

    Pinned rather than merely deleted from the docs: it is the command anyone
    who read an older copy of the guide will still type. Before the dual-import
    scheme was removed it produced a cascade of chained ImportErrors that read
    like a broken project rather than a wrong command.
    """
    result = subprocess.run(
        [sys.executable, "backend/app.py"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )

    assert result.returncode != 0
    assert "attempted relative import with no known parent package" in result.stderr
    # One traceback, not a chain of them.
    assert result.stderr.count("During handling of the above exception") == 0
