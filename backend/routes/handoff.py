from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

try:
    from ..api_models import HandoffJobCreateRequest, HandoffJobResponse
except ImportError:  # pragma: no cover
    from api_models import HandoffJobCreateRequest, HandoffJobResponse  # type: ignore[no-redef]


@dataclass(frozen=True)
class HandoffRouteDeps:
    logger: any
    queue_job: Callable[[dict[str, str]], dict[str, str | int]]
    latest_job: Callable[[int], dict[str, str | int] | None]


def _extract_open_target(path: Path) -> tuple[str, str, str]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return "", "", ""
    if not isinstance(payload, dict):
        return "", "", ""
    run_id = str(payload.get("run_id") or "")
    outputs = payload.get("outputs")
    if not isinstance(outputs, list):
        return "", "", run_id
    for item in outputs:
        if not isinstance(item, dict):
            continue
        if str(item.get("kind") or "") != "master_h5":
            continue
        open_path = str(item.get("path") or "")
        dataset = str(item.get("dataset") or "/entry/data/data")
        return open_path, dataset, run_id
    if outputs:
        first = outputs[0]
        if isinstance(first, dict):
            return str(first.get("path") or ""), str(first.get("dataset") or ""), run_id
    return "", "", run_id


def register_handoff_routes(app: FastAPI, deps: HandoffRouteDeps) -> None:
    @app.post("/api/handoff/v1/jobs", response_model=HandoffJobResponse)
    def create_handoff_job(payload: HandoffJobCreateRequest) -> HandoffJobResponse:
        manifest_path = str(payload.manifest_path or "").strip()
        if not manifest_path:
            raise HTTPException(status_code=400, detail="Missing manifest_path")
        path = Path(manifest_path).expanduser().resolve()
        if not path.exists() or not path.is_file():
            raise HTTPException(status_code=404, detail="Manifest file not found")
        if path.suffix.lower() != ".json":
            raise HTTPException(status_code=400, detail="Manifest must be a .json file")
        open_path, dataset, run_id = _extract_open_target(path)
        job = deps.queue_job(
            {
                "manifest_path": str(path),
                "open_path": open_path,
                "dataset": dataset,
                "run_id": run_id,
            }
        )
        deps.logger.info(
            "Handoff job queued: id=%s manifest=%s open=%s",
            job["id"],
            job["manifest_path"],
            job.get("open_path") or "",
        )
        return HandoffJobResponse(
            id=int(job["id"]),
            manifest_path=str(job["manifest_path"]),
            open_path=str(job.get("open_path") or ""),
            dataset=str(job.get("dataset") or ""),
            run_id=str(job.get("run_id") or ""),
        )

    @app.get("/api/handoff/v1/jobs/latest", response_model=HandoffJobResponse)
    def latest_handoff_job(after_id: int = Query(0, ge=0)) -> HandoffJobResponse:
        job = deps.latest_job(after_id)
        if job is None:
            raise HTTPException(status_code=204)
        return HandoffJobResponse(
            id=int(job["id"]),
            manifest_path=str(job["manifest_path"]),
            open_path=str(job.get("open_path") or ""),
            dataset=str(job.get("dataset") or ""),
            run_id=str(job.get("run_id") or ""),
        )
