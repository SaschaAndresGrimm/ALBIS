"""TTL cache for directory scans, with one scan per key at a time.

Two callers asking for the same listing at the same moment used to run the walk
twice. That was invisible while only the root file and folder lists were cached
and a person clicked them, and it stops being invisible once live autoload polls
a scan every second: a walk that takes longer than the poll interval is started
again before the first one has finished, and each one holds a threadpool worker
for as long as it runs. Enough of those and nothing else gets served.

So the loader is single-flighted. Whoever arrives first runs the scan; everyone
else waits for it and gets that result, rather than starting their own.

Values are cached as handed over and returned unchanged, so they must be
immutable -- `ScanResult` is a frozen dataclass holding a tuple for exactly this
reason. Returning a shared mutable list would let one caller edit another's
answer, and the copy that would prevent it is the thing this cache exists to
avoid paying for repeatedly.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

# Autoload keys carry the folder, extensions and pattern being watched, so a
# session that walks around the filesystem would otherwise grow this map without
# bound. The least recently refreshed entries go first.
MAX_ENTRIES = 64


@dataclass
class _Entry:
    value: Any = None
    refreshed_at: float | None = None
    load_lock: threading.Lock = field(default_factory=threading.Lock)


class ScanCacheService:
    """Cache scan results per key for `ttl` seconds, one loader run at a time."""

    def __init__(self, max_entries: int = MAX_ENTRIES) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, _Entry] = {}
        self._max_entries = max(1, int(max_entries))

    def get(self, key: str, ttl: float, loader: Callable[[], Any]) -> Any:
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                self._prune_locked()
                entry = _Entry()
                self._entries[key] = entry
            if ttl > 0 and entry.refreshed_at is not None and now - entry.refreshed_at < ttl:
                return entry.value

        with entry.load_lock:
            # Someone may have refreshed this entry while we waited for the
            # lock. Their timestamp is then newer than the clock reading we took
            # on the way in, which is how that is noticed without reading the
            # clock a second time.
            with self._lock:
                if ttl > 0 and entry.refreshed_at is not None and entry.refreshed_at >= now:
                    return entry.value
            value = loader()
            refreshed_at = time.monotonic()
            with self._lock:
                entry.value = value
                entry.refreshed_at = refreshed_at
            return value

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()

    def _prune_locked(self) -> None:
        """Make room for one new key by dropping the stalest finished entries.

        An entry with no timestamp has a loader running against it right now and
        is never evicted: whoever is waiting on its lock would come back to an
        entry no longer in the map and start a second scan of the same
        directory, which is the thing this class exists to prevent.
        """
        while len(self._entries) >= self._max_entries:
            finished = [
                (entry.refreshed_at, key)
                for key, entry in self._entries.items()
                if entry.refreshed_at is not None and not entry.load_lock.locked()
            ]
            if not finished:
                return
            self._entries.pop(min(finished)[1], None)
