"""Import the image-reading libraries in the background once the server is up.

h5py with its compression plugins, and fabio, take a second or two to import.
ALBIS imports them only when a file first needs them, so the first file a user
opened waited for them on top of its own reading, and the next one did not.
Importing them on a background thread does that work while the user is still
choosing a file. A request that needs a library before the thread has got to
it is no worse off: Python's import lock makes it wait for the import already
under way rather than start another.

The thread starts when the interface first asks for the file list, the last
thing it loads before it waits for a file, and not when the server starts.
Python holds its interpreter lock while it loads a native library, so the
imports stall every request meanwhile. That is unnoticeable on a native build,
but the Intel build on Apple Silicon translates h5py's libraries under Rosetta
on first launch, and starting at server start left the interface itself
waiting: 0.42.3's release check timed out fetching the start page.
"""

from __future__ import annotations

import importlib
import logging
import threading
import time
from collections.abc import Iterable

_log = logging.getLogger("albis.warmup")
_started = False
_start_lock = threading.Lock()

# hdf5plugin first: it registers the compression filters that h5py needs for
# detector data. The fabio readers and tifffile are the ones image_formats
# loads on first use.
LIBRARIES = (
    "hdf5plugin",
    "h5py",
    "fabio.cbfimage",
    "fabio.edfimage",
    "fabio.tifimage",
    "tifffile",
)


def warm_up(modules: Iterable[str] = LIBRARIES) -> list[str]:
    """Import each module and return those that imported.

    A module that fails is skipped and logged: the request that really needs
    it will raise the same error where the user can see it.
    """
    started = time.perf_counter()
    imported = []
    for name in modules:
        try:
            importlib.import_module(name)
        except Exception as exc:  # noqa: BLE001 - any failure here is only a missed head start
            _log.info("Warm-up could not import %s: %s", name, exc)
            continue
        imported.append(name)
    _log.info(
        "Warm-up imported %s in %.2fs",
        ", ".join(imported) or "nothing",
        time.perf_counter() - started,
    )
    return imported


def start_warm_up(modules: Iterable[str] = LIBRARIES) -> threading.Thread:
    """Run warm_up on a daemon thread, so it never holds up startup or shutdown."""
    thread = threading.Thread(
        target=warm_up, args=(tuple(modules),), name="albis-warm-up", daemon=True
    )
    thread.start()
    return thread


def start_warm_up_once() -> threading.Thread | None:
    """Start the warm-up the first time this is called; later calls do nothing."""
    global _started
    with _start_lock:
        if _started:
            return None
        _started = True
    return start_warm_up()
