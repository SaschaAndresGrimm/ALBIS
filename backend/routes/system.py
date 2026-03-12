from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

try:
    from ..api_models import (
        ClientLogRequest,
        HealthResponse,
        PathStatusResponse,
        SettingsPayloadResponse,
        SettingsSaveRequest,
        StatusResponse,
    )
    from ..services.os_actions import open_in_system
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from api_models import (  # type: ignore[no-redef]
        ClientLogRequest,
        HealthResponse,
        PathStatusResponse,
        SettingsPayloadResponse,
        SettingsSaveRequest,
        StatusResponse,
    )
    from services.os_actions import open_in_system  # type: ignore[no-redef]


@dataclass(frozen=True)
class SystemRouteDeps:
    version: str
    logger: Any
    default_config: dict[str, Any]
    config_path: Path
    settings_payload: Callable[[], dict[str, Any]]
    normalize_config: Callable[[dict[str, Any]], dict[str, Any]]
    save_config: Callable[[dict[str, Any], Path], None]
    apply_runtime_config: Callable[[dict[str, Any]], None]
    get_log_path: Callable[[], Path | None]


def register_system_routes(app: FastAPI, deps: SystemRouteDeps) -> None:
    def _ensure_log_file() -> Path:
        log_path = deps.get_log_path()
        if log_path is None:
            raise HTTPException(status_code=500, detail="Log file unavailable")
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.touch(exist_ok=True)
        except OSError as exc:
            raise HTTPException(status_code=500, detail="Failed to access log file") from exc
        return log_path

    @app.get("/api/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse(status="ok", version=deps.version)

    @app.get("/api/settings", response_model=SettingsPayloadResponse)
    def get_settings() -> SettingsPayloadResponse:
        return SettingsPayloadResponse(**deps.settings_payload())

    @app.post("/api/settings", response_model=SettingsPayloadResponse)
    def save_settings(payload: SettingsSaveRequest) -> SettingsPayloadResponse:
        raw = payload.config

        try:
            normalized = deps.normalize_config(raw)
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid config: {exc}") from exc

        try:
            deps.save_config(normalized, deps.config_path)
        except OSError as exc:
            raise HTTPException(status_code=500, detail="Failed to save config") from exc

        deps.apply_runtime_config(normalized)
        deps.logger.info("Config updated via UI: %s", deps.config_path)
        return SettingsPayloadResponse(**deps.settings_payload())

    @app.post("/api/client-log", response_model=StatusResponse)
    def client_log(payload: ClientLogRequest) -> StatusResponse:
        try:
            level = str(payload.level).lower()
            message = str(payload.message).strip()
            context = payload.context
            meta = {
                "url": payload.url,
                "userAgent": payload.userAgent,
                "extra": payload.extra,
            }
            if not message:
                return StatusResponse(status="ignored")
            if len(message) > 2000:
                message = message[:2000] + "…"
            if isinstance(context, str) and len(context) > 4000:
                context = context[:4000] + "…"
            try:
                meta_json = json.dumps(meta, default=str)
            except Exception:
                meta_json = "{}"
            if isinstance(context, dict | list):
                try:
                    context = json.dumps(context, default=str)
                except Exception:
                    context = str(context)
            level_map = {
                "debug": 10,
                "info": 20,
                "warning": 30,
                "error": 40,
                "critical": 50,
            }
            log_level = level_map.get(level, 20)
            if context:
                deps.logger.log(log_level, "CLIENT %s | %s | %s", message, context, meta_json)
            else:
                deps.logger.log(log_level, "CLIENT %s | %s", message, meta_json)
            return StatusResponse(status="ok")
        except Exception as exc:
            deps.logger.exception("Failed to record client log: %s", exc)
            raise HTTPException(status_code=400, detail="Invalid log payload") from exc

    @app.post("/api/open-log", response_model=PathStatusResponse)
    def open_log() -> PathStatusResponse:
        log_path = _ensure_log_file()
        opened = False

        try:
            opened = open_in_system(log_path)
        except Exception:
            opened = False

        return PathStatusResponse(status="ok", path=str(log_path), opened=opened)

    @app.get("/api/log-file")
    def log_file() -> FileResponse:
        log_path = _ensure_log_file()
        return FileResponse(
            path=log_path,
            media_type="text/plain; charset=utf-8",
            filename=log_path.name,
        )
