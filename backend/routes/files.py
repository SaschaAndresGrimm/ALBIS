from __future__ import annotations

import os
import platform
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse, Response


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


def _display_available() -> bool:
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


def _run_linux_dialog(cmd: list[str]) -> str | None:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        picked = result.stdout.strip()
        return picked or None
    if result.returncode in {1, 255}:
        return None
    stderr = (result.stderr or "").strip() or "Unknown dialog error"
    raise RuntimeError(stderr)


def _linux_choose_folder() -> str | None:
    if not _display_available():
        raise RuntimeError("No graphical display available")
    zenity = shutil.which("zenity")
    if zenity:
        return _run_linux_dialog(
            [zenity, "--file-selection", "--directory", "--title=Select folder"]
        )
    kdialog = shutil.which("kdialog")
    if kdialog:
        return _run_linux_dialog([kdialog, "--getexistingdirectory", str(Path.home())])
    raise RuntimeError("No supported Linux file dialog found (install zenity or kdialog)")


def _linux_choose_file() -> str | None:
    if not _display_available():
        raise RuntimeError("No graphical display available")
    zenity = shutil.which("zenity")
    if zenity:
        return _run_linux_dialog(
            [
                zenity,
                "--file-selection",
                "--title=Select image file",
                "--file-filter=Image files | *.h5 *.hdf5 *.tif *.tiff *.cbf *.cbf.gz *.edf",
                "--file-filter=All files | *",
            ]
        )
    kdialog = shutil.which("kdialog")
    if kdialog:
        return _run_linux_dialog(
            [
                kdialog,
                "--getopenfilename",
                str(Path.home()),
                "Image files (*.h5 *.hdf5 *.tif *.tiff *.cbf *.cbf.gz *.edf) | All files (*)",
            ]
        )
    raise RuntimeError("No supported Linux file dialog found (install zenity or kdialog)")


def _tk_choose_folder() -> str | None:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:
        raise RuntimeError("Tk folder picker unavailable") from exc

    root = tk.Tk()
    root.withdraw()
    try:
        root.attributes("-topmost", True)
    except Exception:
        pass
    try:
        return filedialog.askdirectory(title="Select Auto Load folder") or None
    finally:
        root.destroy()


def _tk_choose_file() -> str | None:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:
        raise RuntimeError("Tk file picker unavailable") from exc

    root = tk.Tk()
    root.withdraw()
    try:
        root.attributes("-topmost", True)
    except Exception:
        pass
    try:
        return (
            filedialog.askopenfilename(
                title="Select image file",
                filetypes=[
                    ("Image files", "*.h5 *.hdf5 *.tif *.tiff *.cbf *.cbf.gz *.edf"),
                    ("All files", "*.*"),
                ],
            )
            or None
        )
    finally:
        root.destroy()


def register_file_routes(app: FastAPI, deps: FileRouteDeps) -> None:
    files_cache: dict[str, Any] = {"ts": 0.0, "items": []}
    folders_cache: dict[str, Any] = {"ts": 0.0, "items": []}

    @app.get("/api/files")
    def files(folder: str | None = Query(None)) -> dict[str, list[str]]:
        """List discoverable image files from data root or a selected subfolder."""
        trimmed = (folder or "").strip()
        use_cache = trimmed in ("", ".", "./")
        cache_sec = deps.get_scan_cache_sec()
        if use_cache:
            now = time.monotonic()
            if cache_sec > 0 and now - float(files_cache["ts"]) < cache_sec:
                return {"files": list(files_cache["items"])}
            items = deps.scan_files(deps.data_dir)
            files_cache["ts"] = now
            files_cache["items"] = items
            return {"files": items}
        root = deps.resolve_dir(trimmed)
        items = deps.scan_files(root)
        return {"files": _prefix_paths(root, deps.data_dir, items)}

    @app.get("/api/series")
    def series(file: str = Query(...)) -> dict[str, object]:
        path = deps.resolve_image_file(file)
        ext = deps.image_ext_name(path.name)
        if ext in {".h5", ".hdf5"}:
            return {"files": [file], "index": 0, "series": False}
        parts = deps.split_series_name(path.name)
        if not parts:
            return {"files": [file], "index": 0, "series": False}
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
            return {"files": [file], "index": 0, "series": False}
        if not entries:
            return {"files": [file], "index": 0, "series": False}
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
        return {"files": files, "index": index, "series": len(files) > 1}

    @app.get("/api/folders")
    def folders() -> dict[str, list[str]]:
        now = time.monotonic()
        cache_sec = deps.get_scan_cache_sec()
        if cache_sec > 0 and now - float(folders_cache["ts"]) < cache_sec:
            return {"folders": list(folders_cache["items"])}
        items = deps.scan_folders(deps.data_dir)
        folders_cache["ts"] = now
        folders_cache["items"] = items
        return {"folders": items}

    @app.get("/api/choose-folder")
    def choose_folder() -> Response:
        if not deps.get_allow_abs_paths():
            raise HTTPException(status_code=403, detail="Absolute paths are disabled")
        system = platform.system()
        deps.logger.debug("Folder picker requested (os=%s)", system)
        if system == "Darwin":
            script = 'POSIX path of (choose folder with prompt "Select Auto Load folder")'
            try:
                result = subprocess.run(
                    ["osascript", "-e", script],
                    capture_output=True,
                    text=True,
                    check=True,
                )
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or "").lower()
                if "user canceled" in stderr:
                    return Response(status_code=204)
                raise HTTPException(status_code=500, detail="Folder picker failed") from exc
            path = result.stdout.strip()
            if not path:
                return Response(status_code=204)
            return JSONResponse({"path": path})

        try:
            if system == "Linux":
                try:
                    path = _linux_choose_folder()
                except RuntimeError:
                    path = _tk_choose_folder()
            else:
                path = _tk_choose_folder()
        except RuntimeError as exc:
            deps.logger.warning("Folder picker failed (os=%s): %s", system, exc)
            raise HTTPException(
                status_code=500, detail=f"Folder picker unavailable: {exc}"
            ) from exc

        if not path:
            return Response(status_code=204)
        deps.logger.info("Folder picker selected: %s", path)
        return JSONResponse({"path": path})

    @app.get("/api/choose-file")
    def choose_file() -> Response:
        if not deps.get_allow_abs_paths():
            raise HTTPException(status_code=403, detail="Absolute paths are disabled")
        system = platform.system()
        deps.logger.debug("File picker requested (os=%s)", system)
        if system == "Darwin":
            script = 'POSIX path of (choose file with prompt "Select image file")'
            try:
                result = subprocess.run(
                    ["osascript", "-e", script],
                    capture_output=True,
                    text=True,
                    check=True,
                )
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or "").lower()
                if "user canceled" in stderr:
                    return Response(status_code=204)
                raise HTTPException(status_code=500, detail="File picker failed") from exc
            path = result.stdout.strip()
            if not path:
                return Response(status_code=204)
        else:
            try:
                if system == "Linux":
                    try:
                        path = _linux_choose_file()
                    except RuntimeError:
                        path = _tk_choose_file()
                else:
                    path = _tk_choose_file()
            except RuntimeError as exc:
                deps.logger.warning("File picker failed (os=%s): %s", system, exc)
                raise HTTPException(
                    status_code=500, detail=f"File picker unavailable: {exc}"
                ) from exc

            if not path:
                return Response(status_code=204)
        picked = Path(path).expanduser().resolve()
        if deps.image_ext_name(picked.name) not in deps.autoload_exts:
            raise HTTPException(status_code=400, detail="Unsupported file type")
        if not picked.exists():
            raise HTTPException(status_code=404, detail="File not found")
        deps.logger.info("File picker selected: %s", picked)
        return JSONResponse({"path": str(picked)})

    @app.get("/api/browse")
    def browse(path: str | None = Query(None)) -> dict[str, Any]:
        """List folders and image files in a directory for web-based file browser."""
        try:
            target_dir = deps.resolve_dir(path)
        except HTTPException as exc:
            if exc.status_code == 404:
                target_dir = deps.data_dir.resolve()
            else:
                raise

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
                            if ext in deps.autoload_exts:
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

        return {
            "folders": sorted(dirs),
            "files": sorted(files),
            "currentPath": current_path_display,
            "root": str(data_root),
            "canGoUp": can_go_up,
            "allowAbsolutePaths": deps.get_allow_abs_paths(),
        }

    @app.get("/api/autoload/latest")
    def autoload_latest(
        folder: str | None = Query(None),
        exts: str | None = Query(None),
        pattern: str | None = Query(None),
    ) -> Response:
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
                raise HTTPException(status_code=400, detail="Invalid file location")
            absolute = True
            file_label = str(latest.resolve())
        deps.logger.debug("Autoload scan: latest=%s absolute=%s", file_label, absolute)
        return JSONResponse(
            {
                "file": file_label,
                "ext": deps.image_ext_name(latest.name),
                "mtime": latest.stat().st_mtime,
                "absolute": absolute,
            }
        )

    @app.post("/api/upload")
    async def upload(
        file: UploadFile = File(...), folder: str | None = Query(None)
    ) -> dict[str, str]:
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
        written = 0
        chunk_size = 1024 * 1024 * 4
        try:
            with dest.open("wb") as fh:
                while True:
                    chunk = file.file.read(chunk_size)
                    if not chunk:
                        break
                    written += len(chunk)
                    max_upload_bytes = deps.get_max_upload_bytes()
                    if max_upload_bytes and written > max_upload_bytes:
                        raise HTTPException(status_code=413, detail="Upload too large")
                    fh.write(chunk)
        except HTTPException:
            try:
                if dest.exists():
                    dest.unlink()
            except OSError:
                pass
            raise
        deps.logger.info("Upload complete: %s (%d bytes)", dest, written)
        try:
            resolved_rel = dest.relative_to(deps.data_dir.resolve()).as_posix()
            open_path = resolved_rel
        except ValueError:
            open_path = str(dest)
        return {"filename": safe, "path": open_path}
