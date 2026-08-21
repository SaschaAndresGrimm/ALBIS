from __future__ import annotations

import errno
import os
import platform
import re
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, Response

try:
    from ..api_models import (
        AutoloadLatestResponse,
        BrowseFileItem,
        BrowseResponse,
        FilesListResponse,
        FoldersListResponse,
        PathSelectionResponse,
        SeriesInfoResponse,
        UploadResponse,
    )
    from ..services.directory_scan import LatestFileResult, ScanResult
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
        BrowseFileItem,
        BrowseResponse,
        FilesListResponse,
        FoldersListResponse,
        PathSelectionResponse,
        SeriesInfoResponse,
        UploadResponse,
    )
    from services.directory_scan import (  # type: ignore[no-redef]
        LatestFileResult,
        ScanResult,
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
    get_max_scan_entries: Callable[[], int]
    get_max_upload_bytes: Callable[[], int]
    resolve_dir: Callable[[str | None], Path]
    resolve_image_file: Callable[[str], Path]
    is_within: Callable[[Path, Path], bool]
    parse_ext_filter: Callable[[str | None], set[str]]
    latest_image_file: Callable[[Path, set[str], str | None], LatestFileResult]
    get_cached_scan: Callable[[str, float, Callable[[], Any]], Any]
    invalidate_scans: Callable[[], None]
    safe_rel_path: Callable[[str], Path]
    scan_files: Callable[[Path], ScanResult]
    scan_folders: Callable[[Path], ScanResult]
    image_ext_name: Callable[[str], str]
    split_series_name: Callable[[str], tuple[str, int, str] | None]
    strip_image_ext: Callable[[str, str], str]


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


_BROWSE_SERIES_EXTS = {".tif", ".tiff", ".cbf", ".cbf.gz", ".edf"}
_HDF_SERIES_EXTS = {".h5", ".hdf5"}
_NATURAL_SPLIT_RE = re.compile(r"(\d+)")
_HDF_MASTER_RE = re.compile(r"^(?P<prefix>.+?)_master\.(?:h5|hdf5)$", re.IGNORECASE)
_HDF_DATA_RE = re.compile(r"^(?P<prefix>.+?)_data_(?P<index>\d+)\.(?:h5|hdf5)$", re.IGNORECASE)


def _split_hdf_series(name: str) -> tuple[str, str, int] | None:
    """Classify a DECTRIS-style HDF5 file as a member of a master/data series.

    Returns ``(kind, prefix, index)`` where ``kind`` is ``"master"`` or
    ``"data"``, or ``None`` when the file does not follow the
    ``PREFIX_master.h5`` / ``PREFIX_data_NNNNNN.h5`` convention (e.g. standalone
    or summed HDF5 files, which must never be collapsed into a series).
    """
    master = _HDF_MASTER_RE.match(name)
    if master:
        return ("master", master.group("prefix").casefold(), 0)
    data = _HDF_DATA_RE.match(name)
    if data:
        return ("data", data.group("prefix").casefold(), int(data.group("index")))
    return None


def _natural_sort_key(value: str) -> tuple[tuple[int, Any], ...]:
    parts = _NATURAL_SPLIT_RE.split(str(value).casefold())
    return tuple((1, int(part)) if part.isdigit() else (0, part) for part in parts)


def _display_browse_path(path: Path, data_root: Path, allow_absolute_paths: bool) -> str:
    resolved = path.resolve()
    try:
        rel = resolved.relative_to(data_root)
        return rel.as_posix() if rel != Path(".") else ""
    except ValueError:
        return str(resolved) if allow_absolute_paths else ""


def _browse_parent_path(
    target_dir: Path, data_root: Path, allow_absolute_paths: bool, is_within: Callable[[Path, Path], bool]
) -> str:
    resolved = target_dir.resolve()
    if is_within(resolved, data_root):
        if resolved == data_root:
            return ""
        return _display_browse_path(resolved.parent, data_root, allow_absolute_paths)
    parent = resolved.parent
    if parent == resolved:
        return ""
    return _display_browse_path(parent, data_root, allow_absolute_paths)


def _browse_file_path(name: str, target_dir: Path, data_root: Path, allow_absolute_paths: bool) -> str:
    return _display_browse_path(target_dir / name, data_root, allow_absolute_paths) or name


class _BrowseBudget:
    """Entry allowance shared by one browse request's two directory passes.

    Time is not budgeted here, unlike the recursive walks: this is a single
    `scandir` over one directory, so the entry count is what bounds it, and a
    request that stops halfway through the second pass would report folders
    without their files.
    """

    __slots__ = ("_remaining", "_unlimited", "truncated")

    def __init__(self, max_entries: int) -> None:
        limit = max(0, int(max_entries or 0))
        # "Unlimited" is held separately from the counter on purpose. Reusing
        # `remaining == 0` for both meanings makes an exhausted budget read as
        # an unlimited one, which lets everything through after the allowance
        # runs out -- silently, and only in the second pass.
        self._unlimited = limit == 0
        self._remaining = limit
        self.truncated = False

    def charge(self) -> bool:
        if self._unlimited:
            return True
        if self._remaining <= 0:
            self.truncated = True
            return False
        self._remaining -= 1
        return True


def _sort_browse_items(items: list[dict[str, Any]], sort: str) -> list[dict[str, Any]]:
    if sort == "name_desc":
        return sorted(items, key=lambda item: _natural_sort_key(item["name"]), reverse=True)
    if sort == "mtime_desc":
        return sorted(
            items,
            key=lambda item: (float(item.get("mtime", 0.0)), _natural_sort_key(item["name"])),
            reverse=True,
        )
    if sort == "mtime_asc":
        return sorted(items, key=lambda item: (float(item.get("mtime", 0.0)), _natural_sort_key(item["name"])))
    if sort == "type_asc":
        return sorted(items, key=lambda item: (_natural_sort_key(item["ext"]), _natural_sort_key(item["name"])))
    if sort == "type_desc":
        return sorted(
            items,
            key=lambda item: (_natural_sort_key(item["ext"]), _natural_sort_key(item["name"])),
            reverse=True,
        )
    if sort == "size_asc":
        return sorted(
            items,
            key=lambda item: (int(item.get("sizeBytes", 0)), _natural_sort_key(item["name"])),
        )
    if sort == "size_desc":
        return sorted(
            items,
            key=lambda item: (int(item.get("sizeBytes", 0)), _natural_sort_key(item["name"])),
            reverse=True,
        )
    return sorted(items, key=lambda item: _natural_sort_key(item["name"]))


def _aggregate_browse_series(
    files: list[dict[str, Any]],
    split_series_name: Callable[[str], tuple[str, str, str] | None],
) -> list[dict[str, Any]]:
    singles: list[dict[str, Any]] = []
    groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    hdf_groups: dict[str, dict[str, Any]] = {}
    for item in files:
        ext = str(item.get("ext") or "").lower()
        if ext in _HDF_SERIES_EXTS:
            parsed = _split_hdf_series(str(item.get("name") or ""))
            if parsed is None:
                singles.append(item)
                continue
            kind, prefix, index = parsed
            group = hdf_groups.setdefault(prefix, {"master": None, "data": []})
            if kind == "master":
                group["master"] = item
            else:
                group["data"].append((index, item))
            continue
        if ext not in _BROWSE_SERIES_EXTS:
            singles.append(item)
            continue
        parts = split_series_name(str(item.get("name") or ""))
        if not parts:
            singles.append(item)
            continue
        prefix, digits, suffix = parts
        try:
            index = int(digits)
        except ValueError:
            singles.append(item)
            continue
        key = (prefix, suffix, ext)
        current = groups.get(key)
        if current is None:
            groups[key] = {
                **item,
                "_lead_index": index,
                "mtime": float(item.get("mtime", 0.0)),
                "seriesCount": 1,
            }
            continue
        current["seriesCount"] = int(current.get("seriesCount", 1)) + 1
        current["mtime"] = max(float(current.get("mtime", 0.0)), float(item.get("mtime", 0.0)))
        if index < int(current.get("_lead_index", index)):
            current.update(
                {
                    "name": item["name"],
                    "path": item["path"],
                    "ext": item["ext"],
                    "sizeBytes": item["sizeBytes"],
                    "_lead_index": index,
                }
            )
    for group in hdf_groups.values():
        master = group["master"]
        data_items = sorted(group["data"], key=lambda pair: pair[0])
        lead = master if master is not None else (data_items[0][1] if data_items else None)
        if lead is None:
            continue
        members = ([master] if master is not None else []) + [item for _, item in data_items]
        singles.append(
            {
                **lead,
                "mtime": max(float(member.get("mtime", 0.0)) for member in members),
                "seriesCount": max(1, len(data_items)),
            }
        )
    merged = singles + list(groups.values())
    for item in merged:
        item["isSeriesLead"] = int(item.get("seriesCount", 1)) > 1
        item.pop("_lead_index", None)
    return merged


def register_file_routes(app: FastAPI, deps: FileRouteDeps) -> None:
    @app.get("/api/files", response_model=FilesListResponse)
    def files(folder: str | None = Query(None)) -> FilesListResponse:
        """List discoverable image files from data root or a selected subfolder."""
        trimmed = (folder or "").strip()
        cache_sec = deps.get_scan_cache_sec()
        if trimmed in ("", ".", "./"):
            scan = deps.get_cached_scan(
                "root_files", cache_sec, lambda: deps.scan_files(deps.data_dir)
            )
            return FilesListResponse(files=scan.as_list(), truncated=scan.truncated)
        # A selected subfolder is not cached, unlike the root. The upload flow
        # asks this endpoint to find the file it just wrote, and a TTL long
        # enough to be worth having is long enough to answer "not there".
        root = deps.resolve_dir(trimmed)
        scan = deps.scan_files(root)
        return FilesListResponse(
            files=_prefix_paths(root, deps.data_dir, scan.as_list()),
            truncated=scan.truncated,
        )

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
        cache_sec = deps.get_scan_cache_sec()
        scan = deps.get_cached_scan(
            "root_folders", cache_sec, lambda: deps.scan_folders(deps.data_dir)
        )
        return FoldersListResponse(folders=scan.as_list(), truncated=scan.truncated)

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
    def browse(
        path: str | None = Query(None),
        exts: str | None = Query(None),
        sort: Literal[
            "name_asc",
            "name_desc",
            "mtime_desc",
            "mtime_asc",
            "type_asc",
            "type_desc",
            "size_asc",
            "size_desc",
        ] = Query("name_asc"),
        series_mode: Literal["all", "first_only"] = Query("all"),
    ) -> BrowseResponse:
        """List folders and image files in a directory for web-based file browser."""
        requested_path_missing = False
        try:
            target_dir = deps.resolve_dir(path)
        except HTTPException as exc:
            if exc.status_code == 404:
                target_dir = deps.data_dir.resolve()
                requested_path_missing = bool((path or "").strip())
            else:
                raise

        allowed, allow_expt = _parse_requested_picker_exts(exts, deps.autoload_exts)
        allow_absolute_paths = deps.get_allow_abs_paths()
        data_root = deps.data_dir.resolve()

        # One budget across both passes below. The recursive scans have had one
        # since they were the obvious risk, but this is the endpoint the file
        # browser actually calls, and a beamline folder is frequently flat and
        # enormous -- one directory per run holding a hundred thousand frames.
        # Unbounded, that means a `stat` per entry, a dict per entry, a sort over
        # all of them and the lot serialized to JSON, for a listing nobody can
        # read anyway.
        budget = _BrowseBudget(deps.get_max_scan_entries())

        dirs: set[str] = set()
        try:
            with os.scandir(target_dir) as it:
                for entry in it:
                    if not budget.charge():
                        break
                    if entry.name.startswith("."):
                        continue
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            dirs.add(entry.name)
                    except OSError:
                        continue
        except OSError:
            pass

        file_items: list[dict[str, Any]] = []
        try:
            with os.scandir(target_dir) as it:
                for entry in it:
                    if not budget.charge():
                        break
                    if entry.name.startswith("."):
                        continue
                    try:
                        if entry.is_file(follow_symlinks=False):
                            ext = deps.image_ext_name(entry.name)
                            is_expt = Path(entry.name).suffix.lower() == ".expt"
                            if ext not in allowed and not (allow_expt and is_expt):
                                continue
                            stat = entry.stat(follow_symlinks=False)
                            file_items.append(
                                {
                                    "name": entry.name,
                                    "path": _browse_file_path(
                                        entry.name, target_dir, data_root, allow_absolute_paths
                                    ),
                                    "ext": ".expt" if is_expt and ext not in allowed else ext,
                                    "mtime": stat.st_mtime,
                                    "sizeBytes": stat.st_size,
                                    "isSeriesLead": False,
                                    "seriesCount": 1,
                                }
                            )
                    except OSError:
                        continue
        except OSError:
            pass

        if series_mode == "first_only":
            file_items = _aggregate_browse_series(file_items, deps.split_series_name)
        file_items = _sort_browse_items(file_items, sort)
        typed_file_items = [BrowseFileItem(**item) for item in file_items]

        current_path_display = _display_browse_path(target_dir, data_root, allow_absolute_paths)
        parent_path_display = _browse_parent_path(
            target_dir, data_root, allow_absolute_paths, deps.is_within
        )
        resolved_target = target_dir.resolve()
        can_go_up = (
            resolved_target != data_root
            if deps.is_within(resolved_target, data_root)
            else bool(parent_path_display)
        )

        return BrowseResponse(
            folders=sorted(dirs, key=_natural_sort_key),
            files=[item.name for item in typed_file_items],
            fileItems=typed_file_items,
            currentPath=current_path_display,
            parentPath=parent_path_display,
            root=str(data_root),
            canGoUp=can_go_up,
            allowAbsolutePaths=allow_absolute_paths,
            requestedPathMissing=requested_path_missing,
            truncated=budget.truncated,
        )

    @app.get("/api/autoload/latest", response_model=AutoloadLatestResponse)
    def autoload_latest(
        folder: str | None = Query(None),
        exts: str | None = Query(None),
        pattern: str | None = Query(None),
    ) -> Response:
        """Return metadata for the most recently modified matching image file.

        Cached under the same TTL as the other scans. This endpoint is polled
        about once a second for as long as a live folder is being watched, and
        an uncached walk per poll is how a large data directory brought the
        whole server to a halt: each poll held a worker for longer than the
        interval, so the next one started before the last had finished.
        """
        root = deps.resolve_dir(folder)
        allowed = deps.parse_ext_filter(exts)
        cache_key = f"latest:{root}:{','.join(sorted(allowed))}:{pattern or ''}"
        result = deps.get_cached_scan(
            cache_key,
            deps.get_scan_cache_sec(),
            lambda: deps.latest_image_file(root, allowed, pattern),
        )
        # Carried as a header as well as a field, because the "nothing found"
        # answer is a 204 with no body -- and "nothing found because the walk
        # ran out of budget" is exactly the case a watching client must not
        # mistake for "the folder is empty".
        scan_headers = {"X-Scan-Truncated": "1"} if result.truncated else {}
        latest = result.path
        if not latest:
            deps.logger.debug(
                "Autoload scan: no file found (folder=%s pattern=%s truncated=%s)",
                root,
                pattern or "",
                result.truncated,
            )
            return Response(status_code=204, headers=scan_headers)
        try:
            rel = latest.resolve().relative_to(deps.data_dir.resolve()).as_posix()
            absolute = False
            file_label = rel
        except ValueError:
            if not deps.get_allow_abs_paths():
                raise HTTPException(status_code=400, detail="Invalid file location") from None
            absolute = True
            file_label = str(latest.resolve())
        try:
            # Statted per request rather than taken from the cached scan, so a
            # file being appended to is still seen changing. A file that has
            # been removed since the scan is "nothing to load", not an error.
            mtime = latest.stat().st_mtime
        except OSError:
            return Response(status_code=204, headers=scan_headers)
        deps.logger.debug("Autoload scan: latest=%s absolute=%s", file_label, absolute)
        payload = AutoloadLatestResponse(
            file=file_label,
            ext=deps.image_ext_name(latest.name),
            mtime=mtime,
            absolute=absolute,
            truncated=result.truncated,
        )
        return JSONResponse(content=payload.model_dump(), headers=scan_headers)

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
            if _is_readonly_upload_error(exc):
                raise HTTPException(
                    status_code=503,
                    detail=(
                        f"Upload directory is not writable: {dest.parent}. "
                        "Check permissions or configure a writable upload root."
                    ),
                ) from exc
            raise
        deps.logger.info("Upload complete: %s (%d bytes)", resolved_dest, written)
        # The upload flow asks for the file list next, to find what it just
        # wrote. A cached root scan taken a moment ago does not contain it.
        deps.invalidate_scans()
        try:
            resolved_rel = resolved_dest.relative_to(deps.data_dir.resolve()).as_posix()
            open_path = resolved_rel
        except ValueError:
            open_path = str(resolved_dest)
        return UploadResponse(filename=safe, path=open_path)
