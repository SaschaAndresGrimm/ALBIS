"""Cover the scan cache, including the part that stops polls from stacking up.

Autoload polls a directory scan about once a second. If the scan takes longer
than the interval, the next poll used to start its own walk while the first was
still running, each holding a threadpool worker -- so a slow filesystem turned a
viewer left open into a server that answered nothing.
"""

from __future__ import annotations

import threading

from backend.services import scan_cache as scan_cache_module
from backend.services.scan_cache import ScanCacheService


def test_first_read_loads_even_with_a_low_monotonic_clock(monkeypatch) -> None:
    """An empty entry is a miss, whatever the clock happens to read."""
    cache = ScanCacheService()
    calls = {"n": 0}
    monotonic_values = iter((10.0, 10.1))

    def load() -> list[str]:
        calls["n"] += 1
        return ["boot/a.h5"]

    monkeypatch.setattr(scan_cache_module.time, "monotonic", lambda: next(monotonic_values))

    assert cache.get("files", 60.0, load) == ["boot/a.h5"]
    assert calls["n"] == 1


def test_hits_within_the_ttl() -> None:
    cache = ScanCacheService()
    calls = {"n": 0}

    def load() -> list[str]:
        calls["n"] += 1
        return ["a.h5", "b.h5"]

    assert cache.get("files", 60.0, load) == ["a.h5", "b.h5"]
    assert cache.get("files", 60.0, load) == ["a.h5", "b.h5"]
    assert calls["n"] == 1


def test_a_zero_ttl_always_reloads() -> None:
    cache = ScanCacheService()
    versions = iter((["a.h5"], ["b.h5"]))

    assert cache.get("files", 0.0, lambda: next(versions)) == ["a.h5"]
    assert cache.get("files", 0.0, lambda: next(versions)) == ["b.h5"]


def test_keys_are_independent() -> None:
    cache = ScanCacheService()

    assert cache.get("files", 60.0, lambda: ["scan/a.h5"]) == ["scan/a.h5"]
    assert cache.get("folders", 60.0, lambda: ["scan"]) == ["scan"]
    assert cache.get("files", 60.0, lambda: ["ignored"]) == ["scan/a.h5"]


def test_clear_drops_everything() -> None:
    cache = ScanCacheService()
    cache.get("files", 60.0, lambda: ["a.h5"])

    cache.clear()

    assert cache.get("files", 60.0, lambda: ["b.h5"]) == ["b.h5"]


def test_concurrent_callers_run_the_scan_once() -> None:
    """The single-flight property: this is what keeps polls from stacking."""
    cache = ScanCacheService()
    calls = {"n": 0}
    started = threading.Event()
    release = threading.Event()

    def slow_load() -> list[str]:
        calls["n"] += 1
        started.set()
        release.wait(timeout=10)
        return ["a.h5"]

    results: list[list[str]] = []

    def worker() -> None:
        results.append(cache.get("files", 60.0, slow_load))

    first = threading.Thread(target=worker)
    first.start()
    assert started.wait(timeout=10), "the first loader never started"

    others = [threading.Thread(target=worker) for _ in range(4)]
    for thread in others:
        thread.start()
    # Give the waiters time to queue on the load lock rather than racing past it.
    for thread in others:
        thread.join(timeout=0.2)

    release.set()
    first.join(timeout=10)
    for thread in others:
        thread.join(timeout=10)

    assert calls["n"] == 1
    assert results == [["a.h5"]] * 5


def test_entries_are_pruned_so_a_walked_filesystem_cannot_grow_it() -> None:
    """Autoload keys carry the folder being watched, so keys are unbounded input."""
    cache = ScanCacheService(max_entries=4)

    for index in range(12):
        cache.get(f"latest:/data/run_{index}", 60.0, lambda: ["frame.cbf"])

    assert len(cache._entries) <= 4


def test_pruning_keeps_the_most_recent_entries(monkeypatch) -> None:
    """The clock is driven by hand here, and not for tidiness.

    `time.monotonic()` on Windows is coarse enough that three consecutive cache
    writes can share a timestamp, at which point "the oldest" is a tie and the
    eviction order is arbitrary -- which is fine for a cache and useless for a
    test. Advancing the clock a second per read states the intent instead of
    depending on how fast the runner is.
    """
    ticks = iter(float(second) for second in range(1, 100))
    monkeypatch.setattr(scan_cache_module.time, "monotonic", lambda: next(ticks))

    cache = ScanCacheService(max_entries=2)
    calls = {"n": 0}

    def load() -> list[str]:
        calls["n"] += 1
        return ["x"]

    cache.get("old", 600.0, load)
    cache.get("newer", 600.0, load)
    cache.get("newest", 600.0, load)  # evicts "old"

    assert calls["n"] == 3
    cache.get("newest", 600.0, load)
    assert calls["n"] == 3, "the newest entry should still be cached"
    cache.get("old", 600.0, load)
    assert calls["n"] == 4, "the oldest entry should have been evicted"


def test_the_cached_object_is_handed_back_unchanged() -> None:
    """Values are immutable by contract, so no copy is made or needed."""
    cache = ScanCacheService()
    value = ("a.h5", "b.h5")

    first = cache.get("files", 60.0, lambda: value)
    second = cache.get("files", 60.0, lambda: ("ignored",))

    assert first is value
    assert second is value
