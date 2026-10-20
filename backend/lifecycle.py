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


# ---- Stopping when nobody uses it -------------------------------------------

# A browser slows the polling of a hidden tab to about once a minute; this
# often a check comes round, which is ample for a limit in minutes.
IDLE_CHECK_SECONDS = 30.0


class IdleWatch:
    """Stops ALBIS once no tab has been in touch for a while.

    On Windows and Linux there is no Dock to quit from, and a server nobody
    looks at keeps its memory and holds its HDF5 files locked -- on Windows
    they cannot be moved or deleted while it runs. Every open ALBIS tab asks
    the server something about once a second (less often when hidden), so a
    long silence means every tab is gone. Started by the launcher, for a server
    on this computer only: a shared server or a container is idle by nature.
    """

    def __init__(self, clock: Callable[[], float] | None = None) -> None:
        import time as _time

        self._clock = clock or _time.monotonic
        self._lock = threading.Lock()
        self._last = self._clock()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def touch(self) -> None:
        """A tab was in touch."""
        with self._lock:
            self._last = self._clock()

    def idle_seconds(self) -> float:
        with self._lock:
            return self._clock() - self._last

    def check(
        self,
        minutes: float,
        busy: Callable[[], list[str]],
        stop: Callable[[], None],
        logger: Logger,
    ) -> bool:
        """Stop now if idle for `minutes` with nothing running. True if it did."""
        if minutes <= 0 or self.idle_seconds() < minutes * 60:
            return False
        if busy():
            # A conversion still writing its files: not idle, whatever the tabs.
            return False
        logger.info("No ALBIS tab for %.0f minutes: stopping", minutes)
        stop()
        return True

    def start(
        self,
        minutes: Callable[[], float],
        busy: Callable[[], list[str]],
        stop: Callable[[], None],
        logger: Logger,
        interval: float = IDLE_CHECK_SECONDS,
    ) -> None:
        if self._thread is not None:
            return
        self.touch()

        def _run() -> None:
            while not self._stop.wait(interval):
                try:
                    if self.check(minutes(), busy, stop, logger):
                        return
                except Exception:
                    logger.exception("Idle check failed")

        self._thread = threading.Thread(target=_run, name="albis-idle-watch", daemon=True)
        self._thread.start()

    def cancel(self) -> None:
        self._stop.set()


idle_watch = IdleWatch()
