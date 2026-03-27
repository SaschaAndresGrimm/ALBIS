"""Thread-safe in-memory handoff queue helpers."""

from __future__ import annotations

import threading


class HandoffQueueService:
    def __init__(self, max_jobs: int = 1024) -> None:
        self._jobs: list[dict[str, str | int]] = []
        self._next_id = 1
        self._max_jobs = max(1, int(max_jobs))
        self._lock = threading.Lock()

    def queue_job(self, payload: dict[str, str]) -> dict[str, str | int]:
        with self._lock:
            item: dict[str, str | int] = {
                "id": int(self._next_id),
                "manifest_path": str(payload.get("manifest_path") or ""),
                "open_path": str(payload.get("open_path") or ""),
                "dataset": str(payload.get("dataset") or ""),
                "run_id": str(payload.get("run_id") or ""),
            }
            self._jobs.append(dict(item))
            overflow = len(self._jobs) - self._max_jobs
            if overflow > 0:
                del self._jobs[:overflow]
            self._next_id += 1
            return dict(item)

    def latest_job(self, after_id: int) -> dict[str, str | int] | None:
        with self._lock:
            for job in reversed(self._jobs):
                if int(job["id"]) > int(after_id):
                    return dict(job)
        return None

    def snapshot(self) -> list[dict[str, str | int]]:
        with self._lock:
            return [dict(job) for job in self._jobs]

    def clear(self) -> None:
        with self._lock:
            self._jobs = []
            self._next_id = 1

    def set_max_jobs(self, max_jobs: int) -> None:
        with self._lock:
            self._max_jobs = max(1, int(max_jobs))
            overflow = len(self._jobs) - self._max_jobs
            if overflow > 0:
                del self._jobs[:overflow]
