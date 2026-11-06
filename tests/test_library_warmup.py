"""The background import of the image libraries at startup.

Without it the first file a user opened waited a second or two for h5py,
hdf5plugin and fabio to import, with nothing else going on.
"""

from __future__ import annotations

import sys

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


def test_startup_starts_the_warm_up_once(monkeypatch) -> None:
    """The server starts the warm-up itself; nothing else would."""
    calls = []
    monkeypatch.setattr(app_module, "start_warm_up", lambda: calls.append(1))
    monkeypatch.setattr(app_module, "_startup_banner_logged", False)

    with TestClient(app_module.app, base_url="http://127.0.0.1"):
        pass
    with TestClient(app_module.app, base_url="http://127.0.0.1"):
        pass

    assert calls == [1]
