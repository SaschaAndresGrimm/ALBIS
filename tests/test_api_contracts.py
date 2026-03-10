from __future__ import annotations

from fastapi.testclient import TestClient

from backend.app import app


def test_settings_request_rejects_unknown_top_level_fields() -> None:
    client = TestClient(app)
    response = client.post("/api/settings", json={"config": {}, "unexpected": True})
    assert response.status_code == 422


def test_client_log_request_rejects_unknown_fields() -> None:
    client = TestClient(app)
    response = client.post("/api/client-log", json={"message": "hello", "unexpected": "x"})
    assert response.status_code == 422


def test_handoff_job_request_rejects_unknown_fields() -> None:
    client = TestClient(app)
    response = client.post(
        "/api/handoff/v1/jobs",
        json={"manifest_path": "manifest.json", "unexpected": True},
    )
    assert response.status_code == 422


def test_series_sum_start_request_rejects_unknown_fields() -> None:
    client = TestClient(app)
    response = client.post(
        "/api/analysis/series-sum/start",
        json={
            "file": "dataset.h5",
            "dataset": "/entry/data/data",
            "operation": "sum",
            "unexpected": 1,
        },
    )
    assert response.status_code == 422


def test_openapi_uses_typed_models_for_files_hdf5_and_frames() -> None:
    client = TestClient(app)
    spec = client.get("/openapi.json").json()

    files_ref = spec["paths"]["/api/files"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]["$ref"]
    tree_ref = spec["paths"]["/api/hdf5/tree"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]["$ref"]
    metadata_ref = spec["paths"]["/api/metadata"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]["$ref"]

    assert files_ref.endswith("/FilesListResponse")
    assert tree_ref.endswith("/HDF5TreeResponse")
    assert metadata_ref.endswith("/FrameMetadataResponse")


def test_openapi_documents_binary_payload_headers() -> None:
    client = TestClient(app)
    spec = client.get("/openapi.json").json()

    endpoint_contracts = [
        ("/api/frame", {"X-Dtype", "X-Shape", "X-Frame"}, False),
        ("/api/mask", {"X-Dtype", "X-Shape", "X-Mask-Path"}, False),
        ("/api/image", {"X-Dtype", "X-Shape", "X-Frame"}, False),
        ("/api/simplon/monitor", {"X-Dtype", "X-Shape", "X-Frame"}, True),
        ("/api/simplon/mask", {"X-Dtype", "X-Shape"}, True),
        (
            "/api/remote/v1/latest",
            {"X-Dtype", "X-Shape", "X-Frame", "X-Remote-Source", "X-Remote-Seq"},
            True,
        ),
    ]
    for path, expected_headers, expect_no_content in endpoint_contracts:
        responses = spec["paths"][path]["get"]["responses"]
        payload_200 = responses["200"]
        schema = payload_200["content"]["application/octet-stream"]["schema"]
        assert schema["type"] == "string"
        assert schema["format"] == "binary"
        for header in expected_headers:
            assert header in payload_200["headers"]
        if expect_no_content:
            assert "204" in responses

    csv_response = spec["paths"]["/api/hdf5/csv"]["get"]["responses"]["200"]
    assert "text/csv" in csv_response["content"]
    assert "Content-Disposition" in csv_response["headers"]


def test_openapi_documents_remote_meta_conflict_contract() -> None:
    client = TestClient(app)
    spec = client.get("/openapi.json").json()

    responses = spec["paths"]["/api/remote/v1/meta"]["get"]["responses"]
    ok_ref = responses["200"]["content"]["application/json"]["schema"]["$ref"]
    conflict_ref = responses["409"]["content"]["application/json"]["schema"]["$ref"]
    assert ok_ref.endswith("/RemoteMetaResponse")
    assert conflict_ref.endswith("/RemoteMetaConflictResponse")
