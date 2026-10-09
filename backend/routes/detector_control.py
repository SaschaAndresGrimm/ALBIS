"""Detector control routes (beta): a guarded proxy to one SIMPLON detector.

Everything here answers 404 unless `ui.detector_control` is switched on, so a
default installation exposes no way to drive a detector. When it is on, writes
and commands are PUT/POST and therefore pass the request guard's cross-site
check; the detector address is validated the same way as the monitor's.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

from ..api_models import DetectorCommandRequest, DetectorConfigRequest, DetectorSeriesFetchRequest
from ..services import simplon_control

OFF_MESSAGE = (
    "Detector control is switched off. Turn it on under Settings -> Viewer -> "
    "Beta: detector control."
)


@dataclass(frozen=True)
class DetectorControlRouteDeps:
    logger: Any
    # Read per request: the setting can be changed while ALBIS runs.
    enabled: Callable[[], bool]
    runner: simplon_control.CommandRunner
    # Where a series copied from the detector goes: inside the data root, so
    # it opens like any file there.
    data_dir: Callable[[], Path]
    invalidate_scans: Callable[[], None]


def register_detector_control_routes(app: FastAPI, deps: DetectorControlRouteDeps) -> None:
    def require_enabled() -> None:
        if not deps.enabled():
            raise HTTPException(status_code=404, detail=OFF_MESSAGE)

    @app.get("/api/detector/describe")
    def detector_describe(
        url: str = Query(..., min_length=1), version: str = Query("1.8.0")
    ) -> dict[str, Any]:
        """Every documented parameter the detector answers for, with its limits."""
        require_enabled()
        return simplon_control.describe(url, version)

    @app.get("/api/detector/status")
    def detector_status(
        url: str = Query(..., min_length=1), version: str = Query("1.8.0")
    ) -> dict[str, Any]:
        """Detector and data-interface state, plus the command in progress, if any."""
        require_enabled()
        payload = simplon_control.status(url, version)
        payload["command"] = deps.runner.current(url)
        return payload

    @app.put("/api/detector/config")
    def detector_config(payload: DetectorConfigRequest) -> dict[str, Any]:
        """Write one parameter and return what the detector changed alongside."""
        require_enabled()
        result = simplon_control.set_config(
            payload.url, payload.version, payload.subsystem, payload.key, payload.value
        )
        deps.logger.info(
            "Detector control: %s/%s set (changed: %s)",
            payload.subsystem,
            payload.key,
            ", ".join(result["changed"]) or "-",
        )
        return result

    @app.post("/api/detector/command")
    def detector_command(payload: DetectorCommandRequest) -> dict[str, Any]:
        """Start one command; follow it through /api/detector/status."""
        require_enabled()
        job = deps.runner.start(
            payload.url, payload.version, payload.subsystem, payload.command, payload.value
        )
        deps.logger.info("Detector control: %s/%s sent", payload.subsystem, payload.command)
        return job

    @app.get("/api/detector/files")
    def detector_files(
        url: str = Query(..., min_length=1), version: str = Query("1.8.0")
    ) -> dict[str, Any]:
        """The files the detector's file writer holds, with sizes where known."""
        require_enabled()
        return {"files": simplon_control.list_files(url, version)}

    @app.post("/api/detector/series/fetch")
    def detector_series_fetch(payload: DetectorSeriesFetchRequest) -> dict[str, Any]:
        """Copy one series from the detector into the data root, to open in ALBIS."""
        require_enabled()
        root = deps.data_dir().resolve()
        written = simplon_control.fetch_series(payload.url, payload.version, payload.prefix, root)
        deps.invalidate_scans()
        deps.logger.info(
            "Detector control: series %s copied (%d files)", payload.prefix, len(written)
        )
        return {
            "path": written[0].relative_to(root).as_posix(),
            "files": [path.name for path in written],
            "bytes": sum(path.stat().st_size for path in written),
        }

    @app.get("/api/detector/files/download")
    def detector_file_download(
        url: str = Query(..., min_length=1), name: str = Query(..., min_length=1)
    ) -> StreamingResponse:
        """One file from the detector, passed through as it downloads."""
        require_enabled()
        body, headers = simplon_control.open_download(url, name)
        return StreamingResponse(body, media_type="application/x-hdf5", headers=headers)
