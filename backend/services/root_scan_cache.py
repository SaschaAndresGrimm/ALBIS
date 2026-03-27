"""Thread-safe TTL caches for root file and folder scans."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable


class RootScanCacheService:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._root_files: dict[str, float | list[str]] = {"ts": 0.0, "items": []}
        self._root_folders: dict[str, float | list[str]] = {"ts": 0.0, "items": []}

    def get_root_files(self, cache_ttl: float, loader: Callable[[], list[str]]) -> list[str]:
        return self._get_cached_items(self._root_files, cache_ttl, loader)

    def get_root_folders(self, cache_ttl: float, loader: Callable[[], list[str]]) -> list[str]:
        return self._get_cached_items(self._root_folders, cache_ttl, loader)

    def clear(self) -> None:
        with self._lock:
            self._root_files = {"ts": 0.0, "items": []}
            self._root_folders = {"ts": 0.0, "items": []}

    def _get_cached_items(
        self,
        entry: dict[str, float | list[str]],
        cache_ttl: float,
        loader: Callable[[], list[str]],
    ) -> list[str]:
        now = time.monotonic()
        with self._lock:
            if cache_ttl > 0 and now - float(entry["ts"]) < cache_ttl:
                return list(entry["items"])
        items = list(loader())
        refreshed_at = time.monotonic()
        with self._lock:
            entry["ts"] = refreshed_at
            entry["items"] = list(items)
            return list(entry["items"])
