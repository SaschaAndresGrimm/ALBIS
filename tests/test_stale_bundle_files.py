"""An upgrade over an older install must not run on the old version's files.

The Windows installer copied the new bundle over the old one and left the
rest in place. 0.48.0 dropped httptools from the bundle, but an upgrade from
an older ALBIS kept its `_internal/httptools` folder. Python imports such a
folder as an empty namespace package, uvicorn's "auto" took that for
httptools, and every connection failed: the browser opened on a server that
answered nothing, and with no console on Windows nothing said why.
"""

from __future__ import annotations

import logging
import re
import subprocess
import sys
import textwrap
from logging.handlers import RotatingFileHandler
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
ISS = REPO_ROOT / "scripts" / "installer_windows.iss"


def test_uvicorn_ignores_a_stale_httptools_folder() -> None:
    # An empty module in sys.modules is what a leftover folder imports as.
    script = textwrap.dedent("""
        import sys, types
        sys.modules["httptools"] = types.ModuleType("httptools")
        import uvicorn
        from backend.launcher_server import UVICORN_PROTOCOLS

        async def app(scope, receive, send):
            pass

        config = uvicorn.Config(app, log_config=None, **UVICORN_PROTOCOLS)
        config.load()
        print(config.http_protocol_class.__module__)
        """)
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    assert result.stdout.strip() == "uvicorn.protocols.http.h11_impl", result.stderr


def test_the_launcher_starts_uvicorn_with_the_named_protocols() -> None:
    source = (REPO_ROOT / "albis_launcher.py").read_text(encoding="utf-8")
    config_call = re.search(r"uvicorn\.Config\((.*?)\n    \)", source, re.S)
    assert config_call is not None
    assert "**UVICORN_PROTOCOLS" in config_call.group(1)


def test_each_install_starts_from_an_empty_internal_folder() -> None:
    text = ISS.read_text(encoding="utf-8")
    section = re.search(r"^\[InstallDelete\]\s*\n(.*?)(?=^\[)", text, re.S | re.M)
    assert section is not None, "installer has no [InstallDelete] section"
    entries = [line.strip() for line in section.group(1).splitlines()]
    assert 'Type: filesandordirs; Name: "{app}\\_internal"' in entries
    # Only the bundle's own folder: the user may have picked a folder that
    # holds other things, and {app} itself must never be emptied.
    assert not any(re.search(r'Name: "\{app\}\\?\*?"', line) for line in entries)


def test_uvicorn_and_asyncio_errors_reach_the_log_file(tmp_path) -> None:
    from backend.log_format import log_server_errors_to_files

    source = logging.getLogger("albis.test_stale_bundle")
    handler = RotatingFileHandler(tmp_path / "albis.log", encoding="utf-8")
    source.addHandler(handler)
    added = []
    try:
        log_server_errors_to_files(source.name)
        log_server_errors_to_files(source.name)  # twice: still one handler
        for name in ("uvicorn", "asyncio"):
            target = logging.getLogger(name)
            assert target.handlers.count(handler) == 1
            added.append(target)
        logging.getLogger("asyncio").error("Exception in callback")
        logging.getLogger("uvicorn.error").error("Exception in ASGI application")
        handler.flush()
    finally:
        for target in added:
            target.removeHandler(handler)
        source.removeHandler(handler)
        handler.close()
    written = (tmp_path / "albis.log").read_text(encoding="utf-8")
    assert "Exception in callback" in written
    assert "Exception in ASGI application" in written
