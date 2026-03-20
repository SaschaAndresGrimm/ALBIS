from __future__ import annotations

import errno
import os
import platform
import re
import subprocess
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import Response

try:
    from ..api_models import (
        AutoloadLatestResponse,
        BrowseResponse,
        FilesListResponse,
        FoldersListResponse,
        PathSelectionResponse,
        SeriesInfoResponse,
        UploadResponse,
    )
    from ..services.os_actions import (
        choose_file as _choose_file,
    )
    from ..services.os_actions import (
        choose_folder as _choose_folder,
    )
    from ..services.os_actions import (
        is_applescript_cancel as _is_applescript_cancel,
    )
except ImportError:  # pragma: no cover - supports `python backend/app.py`
    from api_models import (  # type: ignore[no-redef]
        AutoloadLatestResponse,
        BrowseResponse,
        FilesListResponse,
        FoldersListResponse,
        PathSelectionResponse,
        SeriesInfoResponse,
        UploadResponse,
    )
    from services.os_actions import (  # type: ignore[no-redef]
        choose_file as _choose_file,
    )
    from services.os_actions import (
        choose_folder as _choose_folder,
    )
    from services.os_actions import (
        is_applescript_cancel as _is_applescript_cancel,
    )


@dataclass(frozen=True)
class FileRouteDeps:
    data_dir: Path
    autoload_exts: set[str]
    logger: Any
    get_allow_abs_paths: Callable[[], bool]
    get_scan_cache_sec: Callable[[], float]
    get_max_upload_bytes: Callable[[], int]
    resolve_dir: Callable[[str | None], Path]
    resolve_image_file: Callable[[str], Path]
    is_within: Callable[[Path, Path], bool]
    parse_ext_filter: Callable[[str | None], set[str]]
    latest_image_file: Callable[[Path, set[str], str | None], Path | None]
    safe_rel_path: Callable[[str], Path]
    scan_files: Callable[[Path], list[str]]
    scan_folders: Callable[[Path], list[str]]
    image_ext_name: Callable[[str], str]
    split_series_name: Callable[[str], tuple[str, int, str] | None]
    strip_image_ext: Callable[[str, str], str]


def _upload_fallback_root() -> Path:
    """Writable fallback location used when configured upload root is read-only."""
    return (Path(tempfile.gettempdir()) / "albis-uploads").resolve()


def _is_readonly_upload_error(exc: OSError) -> bool:
    return exc.errno in {errno.EROFS, errno.EACCES, errno.EPERM}


def _cleanup_partial_upload(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except OSError:
        pass


def _picker_unavailable_status(exc: RuntimeError) -> int:
    """Map missing native picker capabilities to a handled client-visible response."""
    text = str(exc).strip().lower()
    if not text:
        return 409
    unavailable_markers = (
        "no graphical display available",
        "no supported linux file dialog found",
        "tk folder picker unavailable",
        "tk file picker unavailable",
    )
    if any(marker in text for marker in unavailable_markers):
        return 409
    return 500


def _stream_upload_to_path(
    file: UploadFile, dest: Path, get_max_upload_bytes: Callable[[], int], chunk_size: int
) -> int:
    written = 0
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as fh:
        while True:
            chunk = file.file.read(chunk_size)
            if not chunk:
                break
            written += len(chunk)
            max_upload_bytes = get_max_upload_bytes()
            if max_upload_bytes and written > max_upload_bytes:
                raise HTTPException(status_code=413, detail="Upload too large")
            fh.write(chunk)
    return written


def _prefix_paths(root: Path, data_dir: Path, items: list[str]) -> list[str]:
    """Prefix scanned file names with selected subfolder when needed."""
    root = root.resolve()
    data_root = data_dir.resolve()
    try:
        rel_root = root.relative_to(data_root)
        prefix = rel_root.as_posix()
    except ValueError:
        return [str((root / Path(item)).resolve()) for item in items]
    if prefix in ("", "."):
        return items
    return [f"{prefix}/{item}" for item in items]


def _parse_requested_picker_exts(
    exts: str | None, autoload_exts: set[str]
) -> tuple[set[str], bool]:
    if not exts:
        return set(autoload_exts), False
    allowed: set[str] = set()
    allow_expt = False
    for raw in exts.split(","):
        token = raw.strip().lower()
        if not token:
            continue
        if not token.startswith("."):
            token = f".{token}"
        if token == ".expt":
            allow_expt = True
            continue
        if token == ".cbf":
            allowed.add(".cbf.gz")
        if token in autoload_exts:
            allowed.add(token)
    return allowed, allow_expt


def register_file_routes(app: FastAPI, deps: FileRouteDeps) -> None:
    files_cache: dict[str, Any] = {"ts": 0.0, "items": []}
    folders_cache: dict[str, Any] = {"ts": 0.0, "items": []}

    @app.get("/api/files", response_model=FilesListResponse)
    def files(folder: str | None = Query(None)) -> FilesListResponse:
        """List discoverable image files from data root or a selected subfolder."""
        trimmed = (folder or "").strip()
        use_cache = trimmed in ("", ".", "./")
        cache_sec = deps.get_scan_cache_sec()
        if use_cache:
            now = time.monotonic()
            if cache_sec > 0 and now - float(files_cache["ts"]) < cache_sec:
                return FilesListResponse(files=list(files_cache["items"]))
            items = deps.scan_files(deps.data_dir)
            files_cache["ts"] = now
            files_cache["items"] = items
            return FilesListResponse(files=items)
        root = deps.resolve_dir(trimmed)
        items = deps.scan_files(root)
        return FilesListResponse(files=_prefix_paths(root, deps.data_dir, items))

    @app.get("/api/series", response_model=SeriesInfoResponse)
    def series(file: str = Query(...)) -> SeriesInfoResponse:
        """Resolve neighboring image files that belong to the same numbered series."""
        path = deps.resolve_image_file(file)
        ext = deps.image_ext_name(path.name)
        if ext in {".h5", ".hdf5"}:
            return SeriesInfoResponse(files=[file], index=0, series=False)
        parts = deps.split_series_name(path.name)
        if not parts:
            return SeriesInfoResponse(files=[file], index=0, series=False)
        prefix, _digits, suffix = parts
        entries: list[tuple[int, Path]] = []
        try:
            with os.scandir(path.parent) as it:
                for entry in it:
                    if not entry.is_file(follow_symlinks=False):
                        continue
                    name = entry.name
                    if deps.image_ext_name(name) != ext:
                        continue
                    stem = deps.strip_image_ext(name, ext)
                    match = re.match(rf"{re.escape(prefix)}(\d+){re.escape(suffix)}$", stem)
                    if not match:
                        continue
                    try:
                        idx = int(match.group(1))
                    except ValueError:
                        continue
                    entries.append((idx, Path(entry.path)))
        except OSError:
            return SeriesInfoResponse(files=[file], index=0, series=False)
        if not entries:
            return SeriesInfoResponse(files=[file], index=0, series=False)
        entries.sort(key=lambda item: item[0])
        paths = [p for _, p in entries]
        is_abs = Path(file).is_absolute()
        if is_abs:
            files = [str(p) for p in paths]
            target = str(path)
        else:
            root = deps.data_dir.resolve()
            files = []
            target = None
            for p in paths:
                try:
                    rel = p.resolve().relative_to(root)
                except ValueError:
                    continue
                rel_str = str(rel).replace(os.sep, "/")
                files.append(rel_str)
                if p.resolve() == path.resolve():
                    target = rel_str
            if target is None:
                target = file
        try:
            index = files.index(target)
        except ValueError:
            index = 0
        return SeriesInfoResponse(files=files, index=index, series=len(files) > 1)

    @app.get("/api/folders", response_model=FoldersListResponse)
    def folders() -> FoldersListResponse:
        """List cached folder paths under the configured data directory."""
        now = time.monotonic()
        cache_sec = deps.get_scan_cache_sec()
        if cache_sec > 0 and now - float(folders_cache["ts"]) < cache_sec:
            return FoldersListResponse(folders=list(folders_cache["items"]))
        items = deps.scan_folders(deps.data_dir)
        folders_cache["ts"] = now
        folders_cache["items"] = items
        return FoldersListResponse(folders=items)

    @app.get("/api/choose-folder", response_model=PathSelectionResponse)
    def choose_folder() -> Response:
        """Show a native folder chooser and return the selected absolute path."""
        if not deps.get_allow_abs_paths():
            raise HTTPException(status_code=403, detail="Absolute paths are disabled")
        system = platform.system()
        deps.logger.debug("Folder picker requested (os=%s)", system)
        try:
            path = _choose_folder()
        except subprocess.CalledProcessError as exc:
            if _is_applescript_cancel(exc.stderr):
                return Response(status_code=204)
            raise HTTPException(status_code=500, detail="Folder picker failed") from exc
        except RuntimeError as exc:
            deps.logger.warning("Folder picker failed (os=%s): %s", system, exc)
            raise HTTPException(
                status_code=_picker_unavailable_status(exc),
                detail=f"Folder picker unavailable: {exc}",
            ) from exc

        if not path:
            return Response(status_code=204)
        deps.logger.info("Folder picker selected: %s", path)
        return PathSelectionResponse(path=path)

    @app.get("/api/choose-file", response_model=PathSelectionResponse)
    def choose_file(exts: str | None = Query(None)) -> Response:
        """Show a native file chooser and return a validated absolute file path."""
        if not deps.get_allow_abs_paths():
            raise HTTPException(status_code=403, detail="Absolute paths are disabled")
        system = platform.system()
        deps.logger.debug("File picker requested (os=%s)", system)
        allowed, allow_expt = _parse_requested_picker_exts(exts, deps.autoload_exts)
        picker_exts = sorted(allowed)
        if allow_expt:
            picker_exts.append(".expt")
        prompt = "Select geometry file" if allow_expt and not allowed else "Select image file"
        try:
            path = _choose_file(exts=picker_exts, prompt=prompt)
        except subprocess.CalledProcessError as exc:
            if _is_applescript_cancel(exc.stderr):
                return Response(status_code=204)
            raise HTTPException(status_code=500, detail="File picker failed") from exc
        except RuntimeError as exc:
            deps.logger.warning("File picker failed (os=%s): %s", system, exc)
            raise HTTPException(
                status_code=_picker_unavailable_status(exc),
                detail=f"File picker unavailable: {exc}",
            ) from exc

        if not path:
            return Response(status_code=204)
        picked = Path(path).expanduser().resolve()
        picked_ext = deps.image_ext_name(picked.name)
        if picked_ext not in allowed and not (allow_expt and picked.suffix.lower() == ".expt"):
            raise HTTPException(status_code=400, detail="Unsupported file type")
        if not picked.exists():
            raise HTTPException(status_code=404, detail="File not found")
        deps.logger.info("File picker selected: %s", picked)
        return PathSelectionResponse(path=str(picked))

    @app.get("/api/browse", response_model=BrowseResponse)
    def browse(path: str | None = Query(None), exts: str | None = Query(None)) -> BrowseResponse:
        """List folders and image files in a directory for web-based file browser."""
        try:
            target_dir = deps.resolve_dir(path)
        except HTTPException as exc:
            if exc.status_code == 404:
                target_dir = deps.data_dir.resolve()
            else:
                raise

        allowed, allow_expt = _parse_requested_picker_exts(exts, deps.autoload_exts)

        dirs: set[str] = set()
        try:
            with os.scandir(target_dir) as it:
                for entry in it:
                    if entry.name.startswith("."):
                        continue
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            dirs.add(entry.name)
                    except OSError:
                        continue
        except OSError:
            pass

        files: set[str] = set()
        try:
            with os.scandir(target_dir) as it:
                for entry in it:
                    if entry.name.startswith("."):
                        continue
                    try:
                        if entry.is_file(follow_symlinks=False):
                            ext = deps.image_ext_name(entry.name)
                            if ext in allowed or (
                                allow_expt and Path(entry.name).suffix.lower() == ".expt"
                            ):
                                files.add(entry.name)
                    except OSError:
                        continue
        except OSError:
            pass

        data_root = deps.data_dir.resolve()
        try:
            rel_path = target_dir.relative_to(data_root)
            current_path_display = rel_path.as_posix() if rel_path != Path(".") else ""
        except ValueError:
            current_path_display = str(target_dir) if deps.get_allow_abs_paths() else ""

        can_go_up = target_dir.resolve() != data_root.resolve()

        return BrowseResponse(
            folders=sorted(dirs),
            files=sorted(files),
            currentPath=current_path_display,
            root=str(data_root),
            canGoUp=can_go_up,
            allowAbsolutePaths=deps.get_allow_abs_paths(),
        )

    @app.get("/api/autoload/latest", response_model=AutoloadLatestResponse)
    def autoload_latest(
        folder: str | None = Query(None),
        exts: str | None = Query(None),
        pattern: str | None = Query(None),
    ) -> Response:
        """Return metadata for the most recently modified matching image file."""
        root = deps.resolve_dir(folder)
        allowed = deps.parse_ext_filter(exts)
        latest = deps.latest_image_file(root, allowed, pattern)
        if not latest:
            deps.logger.debug(
                "Autoload scan: no file found (folder=%s pattern=%s)", root, pattern or ""
            )
            return Response(status_code=204)
        try:
            rel = latest.resolve().relative_to(deps.data_dir.resolve()).as_posix()
            absolute = False
            file_label = rel
        except ValueError:
            if not deps.get_allow_abs_paths():
                raise HTTPException(status_code=400, detail="Invalid file location") from None
            absolute = True
            file_label = str(latest.resolve())
        deps.logger.debug("Autoload scan: latest=%s absolute=%s", file_label, absolute)
        return AutoloadLatestResponse(
            file=file_label,
            ext=deps.image_ext_name(latest.name),
            mtime=latest.stat().st_mtime,
            absolute=absolute,
        )

    @app.post("/api/upload", response_model=UploadResponse)
    async def upload(
        file: UploadFile = File(...), folder: str | None = Query(None)
    ) -> UploadResponse:
        """Stream an uploaded detector file into the selected data directory."""
        if not file.filename:
            raise HTTPException(status_code=400, detail="Missing filename")
        safe_path = deps.safe_rel_path(Path(file.filename).name)
        safe = safe_path.as_posix()
        ext = deps.image_ext_name(safe)
        if ext not in deps.autoload_exts:
            raise HTTPException(status_code=400, detail="Unsupported file type")
        root = deps.resolve_dir(folder) if folder else deps.data_dir.resolve()
        dest = (root / safe).resolve()
        if not deps.is_within(dest, root):
            raise HTTPException(status_code=400, detail="Invalid file name")

        deps.logger.info("Upload start: %s -> %s", safe, dest)
        chunk_size = 1024 * 1024 * 4
        resolved_dest = dest
        try:
            written = _stream_upload_to_path(file, dest, deps.get_max_upload_bytes, chunk_size)
        except HTTPException:
            _cleanup_partial_upload(dest)
            raise
        except OSError as exc:
            _cleanup_partial_upload(dest)
            if not (_is_readonly_upload_error(exc) and deps.get_allow_abs_paths()):
                raise
            try:
                file.file.seek(0)
            except OSError:
                raise HTTPException(status_code=500, detail="Upload failed") from exc

            fallback_root = _upload_fallback_root()
            fallback_dest = (fallback_root / safe).resolve()
            if not deps.is_within(fallback_dest, fallback_root):
                raise HTTPException(status_code=400, detail="Invalid file name") from None
            deps.logger.warning(
                "Upload target is read-only (%s). Falling back to %s",
                dest.parent,
                fallback_root,
            )
            try:
                written = _stream_upload_to_path(
                    file, fallback_dest, deps.get_max_upload_bytes, chunk_size
                )
            except HTTPException:
                _cleanup_partial_upload(fallback_dest)
                raise
            except OSError as fallback_exc:
                _cleanup_partial_upload(fallback_dest)
                raise HTTPException(status_code=500, detail="Upload failed") from fallback_exc
            resolved_dest = fallback_dest
        deps.logger.info("Upload complete: %s (%d bytes)", resolved_dest, written)
        try:
            resolved_rel = resolved_dest.relative_to(deps.data_dir.resolve()).as_posix()
            open_path = resolved_rel
        except ValueError:
            open_path = str(resolved_dest)
        return UploadResponse(filename=safe, path=open_path)
