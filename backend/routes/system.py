from __future__ import annotations

import json
import os
import platform
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from fastapi import Body, FastAPI, HTTPException


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
    data_dir: Path
    get_allow_abs_paths: Callable[[], bool]
    is_within: Callable[[Path, Path], bool]


def register_system_routes(app: FastAPI, deps: SystemRouteDeps) -> None:
    @app.get("/api/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "version": deps.version}

    @app.get("/api/settings")
    def get_settings() -> dict[str, Any]:
        return deps.settings_payload()

    @app.post("/api/settings")
    def save_settings(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        raw = payload.get("config") if isinstance(payload, dict) else None
        if not isinstance(raw, dict):
            raise HTTPException(status_code=400, detail="Missing config payload")

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
        return deps.settings_payload()

    @app.post("/api/client-log")
    def client_log(payload: dict[str, Any] = Body(...)) -> dict[str, str]:
        try:
            level = str(payload.get("level", "info")).lower()
            message = str(payload.get("message", "")).strip()
            context = payload.get("context")
            meta = {
                "url": payload.get("url"),
                "userAgent": payload.get("userAgent"),
                "extra": payload.get("extra"),
            }
            if not message:
                return {"status": "ignored"}
            if len(message) > 2000:
                message = message[:2000] + "…"
            if isinstance(context, str) and len(context) > 4000:
                context = context[:4000] + "…"
            try:
                meta_json = json.dumps(meta, default=str)
            except Exception:
                meta_json = "{}"
            if isinstance(context, (dict, list)):
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
            return {"status": "ok"}
        except Exception as exc:
            deps.logger.exception("Failed to record client log: %s", exc)
            raise HTTPException(status_code=400, detail="Invalid log payload")

    @app.post("/api/open-log")
    def open_log() -> dict[str, str]:
        log_path = deps.get_log_path()
        if log_path is None:
            raise HTTPException(status_code=500, detail="Log file unavailable")
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.touch(exist_ok=True)
        except OSError as exc:
            raise HTTPException(status_code=500, detail="Failed to access log file") from exc

        system = platform.system()
        try:
            if system == "Windows":
                os.startfile(str(log_path))  # type: ignore[attr-defined]
            elif system == "Darwin":
                subprocess.run(["open", str(log_path)], check=False)
            else:
                subprocess.run(["xdg-open", str(log_path)], check=False)
        except Exception as exc:
            raise HTTPException(status_code=500, detail="Failed to open log file") from exc
        return {"status": "ok", "path": str(log_path)}

    @app.post("/api/open-path")
    def open_path(payload: dict[str, Any] = Body(...)) -> dict[str, str]:
        raw = str(payload.get("path", "")).strip()
        if not raw:
            raise HTTPException(status_code=400, detail="Missing path")

        path = Path(raw).expanduser()
        if not path.is_absolute():
            path = (deps.data_dir / path).resolve()
        else:
            path = path.resolve()

        if not path.exists():
            raise HTTPException(status_code=404, detail="Path does not exist")

        allowed_root = deps.data_dir.resolve()
        if not deps.get_allow_abs_paths() and not deps.is_within(path, allowed_root):
            raise HTTPException(status_code=400, detail="Path is outside data directory")

        system = platform.system()
        try:
            if system == "Windows":
                os.startfile(str(path))  # type: ignore[attr-defined]
            elif system == "Darwin":
                subprocess.run(["open", str(path)], check=False)
            else:
                subprocess.run(["xdg-open", str(path)], check=False)
        except Exception as exc:
            raise HTTPException(status_code=500, detail="Failed to open path") from exc
        return {"status": "ok", "path": str(path)}
