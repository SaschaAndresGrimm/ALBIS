"""Import the image-reading libraries in the background once the server is up.

h5py with its compression plugins, and fabio, take a second or two to import.
ALBIS imports them only when a file first needs them, so the first file a user
opened waited for them on top of its own reading, and the next one did not.
Importing them on a background thread at startup does that work while the user
is still choosing a file. A request that needs a library before the thread has
got to it is no worse off: Python's import lock makes it wait for the import
already under way rather than start another.
"""

from __future__ import annotations

import importlib
import logging
import threading
import time
from collections.abc import Iterable

_log = logging.getLogger("albis.warmup")

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
