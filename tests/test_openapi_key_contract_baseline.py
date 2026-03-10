from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from backend.app import app

BASELINE_PATH = Path(__file__).resolve().parent / "fixtures" / "openapi_key_contract_baseline.json"

KEY_ENDPOINTS: dict[str, str] = {
    "/api/files": "get",
    "/api/frame": "get",
    "/api/mask": "get",
    "/api/image": "get",
    "/api/simplon/monitor": "get",
    "/api/simplon/mask": "get",
    "/api/handoff/v1/jobs": "post",
    "/api/handoff/v1/jobs/latest": "get",
    "/api/remote/v1/latest": "get",
    "/api/remote/v1/meta": "get",
    "/api/analysis/series-sum/start": "post",
    "/api/analysis/series-sum/status": "get",
    "/api/hdf5/tree": "get",
}


def _extract_key_contract(spec: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for path, method in KEY_ENDPOINTS.items():
        operation = spec["paths"][path][method]
        entry: dict[str, Any] = {
            "parameters": [
                {
                    "name": p.get("name"),
                    "in": p.get("in"),
                    "required": bool(p.get("required")),
                    "schema": p.get("schema") or {},
                }
                for p in (operation.get("parameters") or [])
            ]
        }
        if "requestBody" in operation:
            entry["requestBody"] = operation["requestBody"]
        responses: dict[str, Any] = {}
        for status, info in sorted(operation.get("responses", {}).items(), key=lambda kv: kv[0]):
            content = info.get("content") or {}
            headers = info.get("headers") or {}
            responses[status] = {
                "description": info.get("description"),
                "content_types": sorted(content.keys()),
                "header_names": sorted(headers.keys()),
            }
        entry["responses"] = responses
        out[f"{method.upper()} {path}"] = entry
    return out


def test_openapi_key_contract_matches_baseline_snapshot() -> None:
    baseline = json.loads(BASELINE_PATH.read_text(encoding="utf-8"))
    current = _extract_key_contract(TestClient(app).get("/openapi.json").json())
    assert current == baseline
