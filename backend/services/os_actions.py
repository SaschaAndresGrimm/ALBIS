"""OS integration helpers for desktop actions and native pickers."""

from __future__ import annotations

import contextlib
import os
import platform
import shutil
import subprocess
from pathlib import Path


def open_in_system(path: Path) -> bool:
    """Open a local path in the platform default handler.

    Returns True when the platform opener reports success.
    """
    system = platform.system()
    if system == "Windows":
        os.startfile(str(path))  # type: ignore[attr-defined]
        return True
    if system == "Darwin":
        result = subprocess.run(["open", str(path)], check=False)
        return result.returncode == 0
    result = subprocess.run(["xdg-open", str(path)], check=False)
    return result.returncode == 0


def is_applescript_cancel(stderr: str | None) -> bool:
    text = (stderr or "").lower()
    return (
        "user canceled" in text
        or "user cancelled" in text
        or "error: user canceled" in text
        or "error: user cancelled" in text
        or "(-128)" in text
    )


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
            ]
        )
    kdialog = shutil.which("kdialog")
    if kdialog:
        return _run_linux_dialog(
            [
                kdialog,
                "--getopenfilename",
                str(Path.home()),
                "Image files (*.h5 *.hdf5 *.tif *.tiff *.cbf *.cbf.gz *.edf)",
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
    with contextlib.suppress(Exception):
        root.attributes("-topmost", True)
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
    with contextlib.suppress(Exception):
        root.attributes("-topmost", True)
    try:
        return (
            filedialog.askopenfilename(
                title="Select image file",
                filetypes=[("Image files", "*.h5 *.hdf5 *.tif *.tiff *.cbf *.cbf.gz *.edf")],
            )
            or None
        )
    finally:
        root.destroy()


def choose_folder() -> str | None:
    system = platform.system()
    if system == "Darwin":
        script = 'POSIX path of (choose folder with prompt "Select Auto Load folder")'
        result = subprocess.run(
            ["osascript", "-e", script], capture_output=True, text=True, check=True
        )
        picked = result.stdout.strip()
        return picked or None
    if system == "Linux":
        try:
            return _linux_choose_folder()
        except RuntimeError:
            if not _display_available():
                raise
            return _tk_choose_folder()
    return _tk_choose_folder()


def choose_file() -> str | None:
    system = platform.system()
    if system == "Darwin":
        script = (
            'POSIX path of (choose file with prompt "Select image file" '
            'of type {"h5", "hdf5", "tif", "tiff", "cbf", "cbf.gz", "edf"})'
        )
        result = subprocess.run(
            ["osascript", "-e", script], capture_output=True, text=True, check=True
        )
        picked = result.stdout.strip()
        return picked or None
    if system == "Linux":
        try:
            return _linux_choose_file()
        except RuntimeError:
            if not _display_available():
                raise
            return _tk_choose_file()
    return _tk_choose_file()
