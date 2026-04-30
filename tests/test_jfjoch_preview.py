from __future__ import annotations

import uuid
from types import SimpleNamespace

import cbor2
import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend.services.jungfraujoch_preview import (
    _decode_multi_dim_array,
    _decode_typed_array,
    jfjoch_peak_sets_from_spots,
)


def _decode_error_type() -> type[Exception]:
    return getattr(cbor2, "CBORDecodeValueError", cbor2.CBORDecodeError)


def test_jfjoch_peak_sets_from_spots_splits_indexed_and_unindexed() -> None:
    spots = [
        {"x": 10.0, "y": 20.0, "I": 33.0, "indexed": True},
        {"x": 11.0, "y": 21.0, "I": 12.0, "indexed": False},
        {"x": 12.0, "y": 22.0, "indexed": False},
    ]
    peak_sets = jfjoch_peak_sets_from_spots(spots)
    assert len(peak_sets) == 2
    assert peak_sets[0]["style"] == "jfjoch-indexed"
    assert peak_sets[1]["style"] == "jfjoch-unindexed"
    assert len(peak_sets[0]["points"]) == 1
    assert len(peak_sets[1]["points"]) == 2


def test_jfjoch_preview_control_endpoints_lifecycle() -> None:
    client = TestClient(app)
    client.post("/api/jfjoch/preview/stop")

    source_id = f"jfjoch-{uuid.uuid4().hex[:8]}"
    start = client.post(
        "/api/jfjoch/preview/start",
        json={
            "endpoint": "tcp://127.0.0.1:31999",
            "source_id": source_id,
            "topic": "",
            "channel": "",
        },
    )
    assert start.status_code == 200
    start_payload = start.json()
    assert start_payload["status"] == "ok"
    assert start_payload["source_id"] == source_id

    status = client.get("/api/jfjoch/preview/status")
    assert status.status_code == 200
    status_payload = status.json()
    assert status_payload["source_id"] == source_id
    assert isinstance(status_payload["running"], bool)
    assert status_payload["endpoint"] == "tcp://127.0.0.1:31999"

    stop = client.post("/api/jfjoch/preview/stop")
    assert stop.status_code == 200
    stop_payload = stop.json()
    assert stop_payload["status"] == "ok"
    assert stop_payload["running"] is False


def test_jfjoch_preview_start_rejects_invalid_source_id() -> None:
    client = TestClient(app)
    response = client.post(
        "/api/jfjoch/preview/start",
        json={"endpoint": "tcp://127.0.0.1:31999", "source_id": "invalid source"},
    )
    assert response.status_code == 400


def test_jfjoch_typed_array_rejects_non_bytes_payload() -> None:
    with pytest.raises(_decode_error_type(), match="expected bytes payload"):
        _decode_typed_array(SimpleNamespace(value="bad-payload"), "u1")


def test_jfjoch_multi_dim_array_rejects_invalid_dimensions() -> None:
    tag = SimpleNamespace(value=(["bad-dimension"], [1, 2, 3]))
    with pytest.raises(_decode_error_type(), match="invalid multidim dimensions"):
        _decode_multi_dim_array(tag, column_major=False)
