from __future__ import annotations

from backend.services import root_scan_cache as root_scan_cache_module
from backend.services.root_scan_cache import RootScanCacheService


def test_root_scan_cache_first_read_ignores_empty_cache_even_with_low_monotonic(
    monkeypatch,
) -> None:
    cache = RootScanCacheService()
    calls = {"files": 0}
    monotonic_values = iter((10.0, 10.1))

    def load_files() -> list[str]:
        calls["files"] += 1
        return ["boot/a.h5"]

    monkeypatch.setattr(root_scan_cache_module.time, "monotonic", lambda: next(monotonic_values))

    first = cache.get_root_files(60.0, load_files)

    assert first == ["boot/a.h5"]
    assert calls["files"] == 1


def test_root_scan_cache_hits_within_ttl() -> None:
    cache = RootScanCacheService()
    calls = {"files": 0}

    def load_files() -> list[str]:
        calls["files"] += 1
        return ["a.h5", "b.h5"]

    first = cache.get_root_files(60.0, load_files)
    second = cache.get_root_files(60.0, load_files)

    assert first == ["a.h5", "b.h5"]
    assert second == ["a.h5", "b.h5"]
    assert calls["files"] == 1


def test_root_scan_cache_refreshes_after_ttl_expiry() -> None:
    cache = RootScanCacheService()
    versions = iter((["a.h5"], ["b.h5"]))

    first = cache.get_root_files(0.0, lambda: next(versions))
    second = cache.get_root_files(0.0, lambda: next(versions))

    assert first == ["a.h5"]
    assert second == ["b.h5"]


def test_root_scan_cache_keeps_file_and_folder_entries_independent() -> None:
    cache = RootScanCacheService()

    files = cache.get_root_files(60.0, lambda: ["scan/a.h5"])
    folders = cache.get_root_folders(60.0, lambda: ["scan"])

    assert files == ["scan/a.h5"]
    assert folders == ["scan"]


def test_root_scan_cache_returns_copies() -> None:
    cache = RootScanCacheService()

    files = cache.get_root_files(60.0, lambda: ["a.h5"])
    folders = cache.get_root_folders(60.0, lambda: ["folder"])
    files.append("b.h5")
    folders.append("other")

    assert cache.get_root_files(60.0, lambda: ["ignored"]) == ["a.h5"]
    assert cache.get_root_folders(60.0, lambda: ["ignored"]) == ["folder"]
