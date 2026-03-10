from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import backend.app as backend_app_module
from backend.app import app


@pytest.fixture(autouse=True)
def _isolate_handoff_state():
    original_jobs = list(backend_app_module._handoff_jobs)
    original_next_id = int(backend_app_module._handoff_next_id)
    original_queue_max = int(backend_app_module._handoff_queue_max)
    backend_app_module._handoff_jobs = []
    backend_app_module._handoff_next_id = 1
    try:
        yield
    finally:
        backend_app_module._handoff_jobs = original_jobs
        backend_app_module._handoff_next_id = original_next_id
        backend_app_module._handoff_queue_max = original_queue_max


def _write_manifest(
    path: Path,
    *,
    run_id: str,
    open_path: str,
    dataset: str = "/entry/data/data",
) -> None:
    payload = {
        "run_id": run_id,
        "outputs": [
            {
                "kind": "master_h5",
                "path": open_path,
                "dataset": dataset,
            }
        ],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_handoff_create_and_latest_roundtrip(tmp_path: Path) -> None:
    client = TestClient(app)
    manifest = tmp_path / "handoff.json"
    _write_manifest(
        manifest,
        run_id="run-42",
        open_path="/tmp/run-42_master.h5",
        dataset="/entry/data/data_000001",
    )

    created = client.post("/api/handoff/v1/jobs", json={"manifest_path": str(manifest)})
    assert created.status_code == 200
    created_payload = created.json()
    created_id = int(created_payload["id"])
    assert created_payload["run_id"] == "run-42"
    assert created_payload["open_path"] == "/tmp/run-42_master.h5"
    assert created_payload["dataset"] == "/entry/data/data_000001"

    latest = client.get("/api/handoff/v1/jobs/latest", params={"after_id": created_id - 1})
    assert latest.status_code == 200
    latest_payload = latest.json()
    assert int(latest_payload["id"]) == created_id
    assert latest_payload["run_id"] == "run-42"
    assert latest_payload["open_path"] == "/tmp/run-42_master.h5"
    assert latest_payload["dataset"] == "/entry/data/data_000001"

    no_newer = client.get("/api/handoff/v1/jobs/latest", params={"after_id": created_id})
    assert no_newer.status_code == 204


def test_handoff_latest_returns_204_when_no_matching_job() -> None:
    client = TestClient(app)
    response = client.get("/api/handoff/v1/jobs/latest", params={"after_id": 10**9})
    assert response.status_code == 204


def test_handoff_queue_cap_evicts_oldest_jobs(tmp_path: Path) -> None:
    client = TestClient(app)
    backend_app_module._handoff_queue_max = 3

    for idx in range(1, 6):
        manifest = tmp_path / f"handoff-{idx}.json"
        _write_manifest(
            manifest,
            run_id=f"run-{idx}",
            open_path=f"/tmp/run-{idx}_master.h5",
        )
        created = client.post("/api/handoff/v1/jobs", json={"manifest_path": str(manifest)})
        assert created.status_code == 200

    ids = [int(item["id"]) for item in backend_app_module._handoff_jobs]
    assert ids == [3, 4, 5]
    assert len(backend_app_module._handoff_jobs) == 3

    latest = client.get("/api/handoff/v1/jobs/latest", params={"after_id": 0})
    assert latest.status_code == 200
    assert int(latest.json()["id"]) == 5
