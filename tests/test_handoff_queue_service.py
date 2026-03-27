from __future__ import annotations

from backend.services.handoff_queue import HandoffQueueService


def test_handoff_queue_assigns_ids_and_returns_latest_match() -> None:
    queue = HandoffQueueService(max_jobs=4)

    first = queue.queue_job({"manifest_path": "a.json", "open_path": "a.h5"})
    second = queue.queue_job({"manifest_path": "b.json", "open_path": "b.h5"})

    assert first["id"] == 1
    assert second["id"] == 2
    assert queue.latest_job(0) == second
    assert queue.latest_job(1) == second
    assert queue.latest_job(2) is None


def test_handoff_queue_evicts_oldest_jobs_at_capacity() -> None:
    queue = HandoffQueueService(max_jobs=3)

    for idx in range(5):
        queue.queue_job({"manifest_path": f"{idx}.json", "run_id": f"run-{idx}"})

    snapshot = queue.snapshot()
    assert [int(item["id"]) for item in snapshot] == [3, 4, 5]


def test_handoff_queue_snapshot_and_latest_return_copies() -> None:
    queue = HandoffQueueService(max_jobs=2)
    queue.queue_job({"manifest_path": "orig.json", "dataset": "/entry/data/data"})

    latest = queue.latest_job(0)
    assert latest is not None
    latest["dataset"] = "/mutated"
    snapshot = queue.snapshot()
    snapshot[0]["manifest_path"] = "mutated.json"

    persisted = queue.latest_job(0)
    assert persisted is not None
    assert persisted["dataset"] == "/entry/data/data"
    assert persisted["manifest_path"] == "orig.json"
