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


def _normalize_picker_exts(exts: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    if not exts:
        return (".h5", ".hdf5", ".tif", ".tiff", ".cbf", ".cbf.gz", ".edf")
    normalized: list[str] = []
    for raw in exts:
        token = str(raw or "").strip().lower()
        if not token:
            continue
        if not token.startswith("."):
            token = f".{token}"
        if token not in normalized:
            normalized.append(token)
    return tuple(normalized)


def _picker_patterns(exts: tuple[str, ...]) -> tuple[str, str]:
    suffixes = []
    labels = []
    for ext in exts:
        label = ext.lstrip(".")
        if ext == ".cbf.gz":
            suffixes.append("*.cbf.gz")
        else:
            suffixes.append(f"*{ext}")
        labels.append(label)
    label_text = ", ".join(labels) if labels else "files"
    return " ".join(suffixes), label_text


def _darwin_picker_types(exts: tuple[str, ...]) -> tuple[str, ...]:
    tokens: list[str] = []
    for ext in exts:
        token = ext.lstrip(".")
        if "." in token:
            # AppleScript `choose file of type` matches a single extension token.
            # Multi-part suffixes such as `.cbf.gz` need the terminal segment.
            token = token.rsplit(".", 1)[-1]
        if token and token not in tokens:
            tokens.append(token)
    return tuple(tokens)


def _powershell_single_quote(text: str) -> str:
    return text.replace("'", "''")


def _windows_dialog_runner(script: str) -> str | None:
    shell = shutil.which("powershell") or shutil.which("pwsh")
    if not shell:
        raise RuntimeError("No supported Windows file dialog found (PowerShell unavailable)")
    result = subprocess.run(
        [shell, "-NoProfile", "-NonInteractive", "-STA", "-Command", script],
        capture_output=True,
        text=True,
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if result.returncode == 0:
        picked = result.stdout.strip()
        return picked or None
    detail = (result.stderr or "").strip() or (result.stdout or "").strip() or "Windows dialog failed"
    raise RuntimeError(detail)


def _windows_picker_filter(exts: tuple[str, ...]) -> str:
    patterns = []
    labels = []
    for ext in exts:
        if ext == ".cbf.gz":
            patterns.append("*.cbf.gz")
        else:
            patterns.append(f"*{ext}")
        labels.append(ext.lstrip("."))
    pattern_text = ";".join(patterns) if patterns else "*.*"
    label_text = ", ".join(labels) if labels else "files"
    return f"{label_text} ({pattern_text})|{pattern_text}|All files (*.*)|*.*"


def _windows_choose_folder() -> str | None:
    script = """
Add-Type -AssemblyName System.Windows.Forms
[System.Windows.Forms.Application]::EnableVisualStyles()
$dialog = New-Object System.Windows.Forms.FolderBrowserDialog
$dialog.Description = 'Select Auto Load folder'
$dialog.ShowNewFolderButton = $false
$result = $dialog.ShowDialog()
if ($result -eq [System.Windows.Forms.DialogResult]::OK) {
  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
  [Console]::Write($dialog.SelectedPath)
}
""".strip()
    return _windows_dialog_runner(script)


def _windows_choose_file(exts: tuple[str, ...], prompt: str) -> str | None:
    escaped_prompt = _powershell_single_quote(prompt)
    escaped_filter = _powershell_single_quote(_windows_picker_filter(exts))
    script = f"""
Add-Type -AssemblyName System.Windows.Forms
[System.Windows.Forms.Application]::EnableVisualStyles()
$dialog = New-Object System.Windows.Forms.OpenFileDialog
$dialog.Title = '{escaped_prompt}'
$dialog.Filter = '{escaped_filter}'
$dialog.FilterIndex = 1
$dialog.Multiselect = $false
$dialog.CheckFileExists = $true
$dialog.RestoreDirectory = $true
$result = $dialog.ShowDialog()
if ($result -eq [System.Windows.Forms.DialogResult]::OK) {{
  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
  [Console]::Write($dialog.FileName)
}}
""".strip()
    return _windows_dialog_runner(script)


def _linux_choose_file(exts: tuple[str, ...], prompt: str) -> str | None:
    if not _display_available():
        raise RuntimeError("No graphical display available")
    pattern_text, label_text = _picker_patterns(exts)
    zenity = shutil.which("zenity")
    if zenity:
        return _run_linux_dialog(
            [
                zenity,
                "--file-selection",
                f"--title={prompt}",
                f"--file-filter={label_text} | {pattern_text}",
            ]
        )
    kdialog = shutil.which("kdialog")
    if kdialog:
        return _run_linux_dialog(
            [
                kdialog,
                "--getopenfilename",
                str(Path.home()),
                f"{label_text} ({pattern_text})",
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


def _tk_choose_file(exts: tuple[str, ...], prompt: str) -> str | None:
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:
        raise RuntimeError("Tk file picker unavailable") from exc

    root = tk.Tk()
    root.withdraw()
    with contextlib.suppress(Exception):
        root.attributes("-topmost", True)
    pattern_text, label_text = _picker_patterns(exts)
    try:
        return (
            filedialog.askopenfilename(
                title=prompt,
                filetypes=[(label_text, pattern_text)],
            )
            or None
        )
    finally:
        root.destroy()


def choose_folder() -> str | None:
    system = platform.system()
    if system == "Windows":
        return _windows_choose_folder()
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


def choose_file(
    exts: tuple[str, ...] | list[str] | None = None,
    prompt: str = "Select image file",
) -> str | None:
    normalized_exts = _normalize_picker_exts(exts)
    system = platform.system()
    if system == "Windows":
        return _windows_choose_file(normalized_exts, prompt)
    if system == "Darwin":
        escaped_prompt = prompt.replace('"', '\\"')
        if normalized_exts == (".expt",):
            script = f'POSIX path of (choose file with prompt "{escaped_prompt}")'
        else:
            apple_types = ", ".join(f'"{token}"' for token in _darwin_picker_types(normalized_exts))
            script = (
                f'POSIX path of (choose file with prompt "{escaped_prompt}" '
                f"of type {{{apple_types}}})"
            )
        result = subprocess.run(
            ["osascript", "-e", script], capture_output=True, text=True, check=True
        )
        picked = result.stdout.strip()
        return picked or None
    if system == "Linux":
        try:
            return _linux_choose_file(normalized_exts, prompt)
        except RuntimeError:
            if not _display_available():
                raise
            return _tk_choose_file(normalized_exts, prompt)
    return _tk_choose_file(normalized_exts, prompt)
