"""The background import of the image libraries at startup.

Without it the first file a user opened waited a second or two for h5py,
hdf5plugin and fabio to import, with nothing else going on.
"""

from __future__ import annotations

import sys
import threading

from fastapi.testclient import TestClient

import backend.app as app_module
from backend.services import library_warmup


def test_every_library_on_the_list_imports() -> None:
    """A typo in the list would make the warm-up quietly import nothing useful."""
    assert library_warmup.warm_up() == list(library_warmup.LIBRARIES)


def test_a_module_that_fails_to_import_is_skipped_and_the_rest_still_load(monkeypatch) -> None:
    """One missing optional reader must not stop the others being warmed up."""
    monkeypatch.delitem(sys.modules, "colorsys", raising=False)

    imported = library_warmup.warm_up(["albis_no_such_module", "colorsys"])

    assert imported == ["colorsys"]
    assert "colorsys" in sys.modules


def test_the_warm_up_runs_on_a_daemon_thread() -> None:
    """A daemon thread cannot hold up shutdown if an import hangs."""
    thread = library_warmup.start_warm_up(["colorsys"])
    thread.join(timeout=10)

    assert thread.daemon
    assert not thread.is_alive()


def test_starting_the_server_does_not_start_the_warm_up(monkeypatch) -> None:
    """Started with the server, the imports stalled the page itself.

    Under Rosetta, 0.42.3's release check timed out fetching the start page
    while h5py's libraries loaded.
    """
    ran = threading.Event()
    # The thread's target, so a start by any route -- an import of
    # start_warm_up by name included -- is seen.
    monkeypatch.setattr(library_warmup, "warm_up", lambda *_args: ran.set())
    monkeypatch.setattr(library_warmup, "_started", False)
    monkeypatch.setattr(app_module, "_startup_banner_logged", False)

    with TestClient(app_module.app, base_url="http://127.0.0.1") as client:
        client.get("/api/health")
        client.get("/")

    assert not ran.wait(timeout=0.5)


def test_the_file_list_starts_the_warm_up_once(monkeypatch) -> None:
    """The interface's last request before it waits for a file starts it."""
    calls = []
    monkeypatch.setattr(library_warmup, "start_warm_up", lambda: calls.append(1))
    monkeypatch.setattr(library_warmup, "_started", False)

    client = TestClient(app_module.app, base_url="http://127.0.0.1")
    assert client.get("/api/files").status_code == 200
    client.get("/api/files")

    assert calls == [1]
