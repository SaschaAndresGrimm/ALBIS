"""How ALBIS asks itself to stop.

Applying an update means replacing the files this process is running from, and
that cannot finish while the process holds them: on Windows the installer needs
ALBIS gone before it can write, and on Linux a relaunch has to wait for the
port to be released. So the update flow needs a way to end the process
*cleanly* -- letting the JUNGFRAUJOCH bridge and the log handlers shut down --
rather than calling `os._exit` and hoping nothing was mid-write.

The server object lives in `albis_launcher.py`, which the backend does not
import. So the launcher registers a callback here at start, and the backend
asks for shutdown through it. Two consequences worth stating:

* A source run (`uvicorn backend.app:app`) registers nothing, so the request
  fails and the caller reports that ALBIS could not close itself. That is
  correct: applying an update is only offered to packaged builds, which are
  always started by the launcher.
* The request is deferred by a moment. The caller is an HTTP handler, and a
  response cannot be delivered by a process that has already exited.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from logging import Logger

# Long enough for the HTTP response to reach the browser and the interface to
# render "ALBIS is closing", short enough that the window does not sit there
# looking like nothing happened.
SHUTDOWN_DELAY_SECONDS = 1.5


class ShutdownController:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._hook: Callable[[], None] | None = None
        self._timer: threading.Timer | None = None

    def register(self, hook: Callable[[], None]) -> None:
        with self._lock:
            self._hook = hook

    def is_available(self) -> bool:
        """Whether anything is listening. False for a source run."""
        with self._lock:
            return self._hook is not None

    def request(self, logger: Logger, reason: str) -> bool:
        """Ask the process to stop shortly. Returns False if nothing can.

        Idempotent by construction: a second request while one is pending
        replaces the pending timer rather than queueing another shutdown.
        """
        with self._lock:
            hook = self._hook
            if hook is None:
                return False
            if self._timer is not None:
                self._timer.cancel()
            logger.info("Shutdown requested (%s)", reason)

            def _fire() -> None:
                try:
                    hook()
                except Exception:
                    logger.exception("Shutdown hook failed")

            timer = threading.Timer(SHUTDOWN_DELAY_SECONDS, _fire)
            timer.name = "albis-shutdown"
            timer.daemon = True
            self._timer = timer
            timer.start()
            return True


shutdown_controller = ShutdownController()
