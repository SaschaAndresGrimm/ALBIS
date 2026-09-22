from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse

from ..api_models import (
    ClientLogRequest,
    HealthResponse,
    LogTailResponse,
    PathStatusResponse,
    SettingsPayloadResponse,
    SettingsSaveRequest,
    StatusResponse,
    UpdateApplyStartRequest,
    UpdateApplyStatusResponse,
    UpdateCheckResponse,
    UpdateDownloadStartRequest,
    UpdateDownloadStatusResponse,
)
from ..response_compression import available_encodings
from ..services.log_tail import read_log_tail
from ..services.os_actions import open_in_system
from ..services.update_apply import ApplyRefusedError, UpdateApplyService
from ..services.update_download import (
    DownloadRefusedError,
    UpdateDownloadService,
)


@dataclass(frozen=True)
class SystemRouteDeps:
    version: str
    commit: str
    logger: Any
    default_config: dict[str, Any]
    config_path: Path
    settings_payload: Callable[[], dict[str, Any]]
    normalize_config: Callable[[dict[str, Any]], dict[str, Any]]
    save_config: Callable[[dict[str, Any], Path], None]
    apply_runtime_config: Callable[[dict[str, Any]], None]
    get_log_path: Callable[[], Path | None]
    check_update: Callable[[], UpdateCheckResponse]
    # Read per request rather than captured: the setting can be changed in the
    # interface while ALBIS runs, and the release check's answer is cached for
    # five minutes, so baking the policy into that answer would keep offering
    # a download after it had been switched off.
    allow_update_download: Callable[[], bool]
    update_download: UpdateDownloadService
    allow_update_apply: Callable[[], bool]
    update_apply: UpdateApplyService
    install_kind: str
    # What the backend itself has running, which the interface cannot see. Its
    # own jobs count as live work exactly as a live watch does.
    backend_busy: Callable[[], list[str]]


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
        return HealthResponse(
            status="ok",
            version=deps.version,
            commit=deps.commit,
            compression_encodings=list(available_encodings()),
        )

    @app.get("/api/update-check", response_model=UpdateCheckResponse)
    def update_check() -> UpdateCheckResponse:
        response = deps.check_update()
        # There is nothing to download when no asset matched this install --
        # Docker and source checkouts update by command -- so the capability is
        # reported as the conjunction rather than as the setting alone.
        supported = bool(response.download_url) and deps.allow_update_download()
        # "Could this build ever apply an update", not "is one ready": the
        # interface needs to know whether to show the step at all, and the
        # readiness of a particular download is the apply status endpoint's
        # answer.
        apply_supported = deps.update_apply.refusal_code(
            install_kind=deps.install_kind,
            allowed=deps.allow_update_apply(),
        ) not in (
            "disabled",
            "unsupported_install",
            "shutdown_unavailable",
            "target_unknown",
        )
        return response.model_copy(
            update={"download_supported": supported, "apply_supported": apply_supported}
        )

    def _download_status() -> UpdateDownloadStatusResponse:
        return UpdateDownloadStatusResponse(**deps.update_download.status())

    @app.post("/api/update-download/start", response_model=UpdateDownloadStatusResponse)
    def update_download_start(
        payload: UpdateDownloadStartRequest,
    ) -> UpdateDownloadStatusResponse:
        if not deps.allow_update_download():
            raise HTTPException(status_code=403, detail="In-app update downloads are disabled")
        try:
            deps.update_download.start(url=payload.url, name=payload.name)
        except DownloadRefusedError as exc:
            # 409 for "one at a time", 400 for a request that would never be
            # acted on whenever it arrived.
            status_code = 409 if "in progress" in str(exc) else 400
            raise HTTPException(status_code=status_code, detail=str(exc)) from exc
        return _download_status()

    @app.get("/api/update-download/status", response_model=UpdateDownloadStatusResponse)
    def update_download_status() -> UpdateDownloadStatusResponse:
        return _download_status()

    @app.post("/api/update-download/cancel", response_model=UpdateDownloadStatusResponse)
    def update_download_cancel() -> UpdateDownloadStatusResponse:
        deps.update_download.cancel()
        return _download_status()

    @app.post("/api/update-download/reset", response_model=UpdateDownloadStatusResponse)
    def update_download_reset() -> UpdateDownloadStatusResponse:
        deps.update_download.reset()
        return _download_status()

    def _apply_status(busy: list[str] | None = None) -> UpdateApplyStatusResponse:
        payload = deps.update_apply.status()
        refusal = deps.update_apply.refusal_code(
            install_kind=deps.install_kind,
            allowed=deps.allow_update_apply(),
            busy=list(busy or []) + deps.backend_busy(),
        )
        return UpdateApplyStatusResponse(**payload, refusal=refusal or "")

    @app.get("/api/update-apply/status", response_model=UpdateApplyStatusResponse)
    def update_apply_status() -> UpdateApplyStatusResponse:
        return _apply_status()

    @app.post("/api/update-apply/start", response_model=UpdateApplyStatusResponse)
    def update_apply_start(payload: UpdateApplyStartRequest) -> UpdateApplyStatusResponse:
        """Apply a verified download, then close ALBIS.

        The response is sent before the process stops: the shutdown is deferred
        by a moment precisely so the interface can be told what is about to
        happen.
        """
        busy = [str(entry) for entry in payload.busy if str(entry).strip()]
        busy.extend(deps.backend_busy())
        try:
            deps.update_apply.apply(
                install_kind=deps.install_kind,
                allowed=deps.allow_update_apply(),
                busy=busy,
            )
        except ApplyRefusedError as exc:
            raise HTTPException(status_code=409, detail=exc.code) from exc
        return _apply_status(busy)

    @app.post("/api/update-download/reveal", response_model=PathStatusResponse)
    def update_download_reveal() -> PathStatusResponse:
        """Show the downloaded file in the platform file manager.

        The containing folder, never the file itself: opening a `.exe` is
        running it, and ALBIS does not launch installers. The user applies the
        update. The path comes from the service's own record of what it wrote,
        so no client-supplied path is ever opened.
        """
        ready = deps.update_download.ready_path()
        if ready is None:
            raise HTTPException(status_code=404, detail="No verified download available")
        try:
            opened = open_in_system(ready.parent)
        except Exception:
            opened = False
        return PathStatusResponse(status="ok", path=str(ready), opened=opened)

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

    @app.get("/api/log-tail", response_model=LogTailResponse)
    def log_tail(lines: int = 500) -> LogTailResponse:
        log_path = _ensure_log_file()
        try:
            payload = read_log_tail(log_path, lines)
        except OSError as exc:
            raise HTTPException(status_code=500, detail="Failed to read log file") from exc
        return LogTailResponse(
            path=payload.path,
            text=payload.text,
            requested_lines=payload.requested_lines,
            returned_lines=payload.returned_lines,
            truncated=payload.truncated,
            size_bytes=payload.size_bytes,
            modified_at=payload.modified_at,
        )

    @app.get("/api/log-file")
    def log_file() -> FileResponse:
        log_path = _ensure_log_file()
        return FileResponse(
            path=log_path,
            media_type="text/plain; charset=utf-8",
            filename=log_path.name,
        )
