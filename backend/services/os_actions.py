"""OS integration helpers for desktop actions and native pickers."""

from __future__ import annotations

import contextlib
import os
import platform
import shutil
import subprocess
import threading
import time
from collections.abc import Callable
from pathlib import Path

# How long to look for the window a Windows file manager or editor opens.
_RAISE_TIMEOUT_S = 5.0
_RAISE_POLL_S = 0.1


def open_in_system(path: Path) -> bool:
    """Open a local path in the platform default handler.

    Returns True when the platform opener reports success.
    """
    system = platform.system()
    if system == "Windows":
        os.startfile(str(path))  # type: ignore[attr-defined]
        # "albis.log - Notepad", or the folder's name for Explorer.
        name = path.name
        _raise_when_shown(lambda title, _cls: name in title)
        return True
    if system == "Darwin":
        result = subprocess.run(["open", str(path)], check=False)
        return result.returncode == 0
    result = subprocess.run(["xdg-open", str(path)], check=False)
    return result.returncode == 0


def reveal_in_file_manager(path: Path) -> bool:
    """Show a file in the platform file manager without opening it.

    Windows selects it in Explorer (`/select` shows, it never runs: opening an
    installer is running it). Elsewhere the containing folder opens.
    """
    if platform.system() == "Windows":
        # One string, not a list: Explorer wants `/select,"<path>"` with only
        # the path quoted, which list quoting would not produce. The path is
        # ALBIS's own, and Windows file names cannot contain a quote.
        subprocess.Popen(f'explorer /select,"{path}"')  # noqa: S602 -- no shell, fixed form
        folder = path.parent
        _raise_when_shown(
            lambda title, cls: cls == "CabinetWClass" and title in {folder.name, str(folder)}
        )
        return True
    return open_in_system(path.parent)


def _raise_when_shown(matches: Callable[[str, str], bool]) -> None:
    """Bring the window the opener just showed to the front, in the background.

    The ALBIS server is not the foreground program -- the browser is, where the
    click happened -- and Windows does not let a background program raise a
    window. So Explorer or Notepad opens behind the browser. Attaching to the
    foreground window's input for the moment of the call is the documented
    way around that (see AttachThreadInput).
    """
    threading.Thread(
        target=_raise_window, args=(matches,), name="raise-window", daemon=True
    ).start()


def _raise_window(matches: Callable[[str, str], bool]) -> bool:
    import ctypes
    from ctypes import wintypes

    user32 = ctypes.WinDLL("user32", use_last_error=True)
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    # Handles are pointer-sized: without these, 64-bit handles are truncated.
    user32.GetForegroundWindow.restype = wintypes.HWND
    for name in ("IsWindowVisible", "IsIconic", "BringWindowToTop", "SetForegroundWindow"):
        getattr(user32, name).argtypes = [wintypes.HWND]
    user32.ShowWindow.argtypes = [wintypes.HWND, ctypes.c_int]
    user32.GetWindowTextLengthW.argtypes = [wintypes.HWND]
    user32.GetWindowTextW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
    user32.GetClassNameW.argtypes = [wintypes.HWND, wintypes.LPWSTR, ctypes.c_int]
    user32.GetWindowThreadProcessId.argtypes = [wintypes.HWND, ctypes.POINTER(wintypes.DWORD)]
    user32.GetWindowThreadProcessId.restype = wintypes.DWORD
    user32.AttachThreadInput.argtypes = [wintypes.DWORD, wintypes.DWORD, wintypes.BOOL]
    enum_proc = ctypes.WINFUNCTYPE(wintypes.BOOL, wintypes.HWND, wintypes.LPARAM)

    found: list[int] = []

    def visit(hwnd: int, _lparam: int) -> bool:
        if not user32.IsWindowVisible(hwnd):
            return True
        title = ctypes.create_unicode_buffer(user32.GetWindowTextLengthW(hwnd) + 1)
        user32.GetWindowTextW(hwnd, title, len(title))
        cls = ctypes.create_unicode_buffer(256)
        user32.GetClassNameW(hwnd, cls, len(cls))
        if matches(title.value, cls.value):
            found.append(hwnd)
            return False
        return True

    callback = enum_proc(visit)
    deadline = time.monotonic() + _RAISE_TIMEOUT_S
    while time.monotonic() < deadline:
        found.clear()
        user32.EnumWindows(callback, 0)
        if found:
            hwnd = found[0]
            foreground = user32.GetForegroundWindow()
            if foreground == hwnd:
                return True
            this_thread = kernel32.GetCurrentThreadId()
            fg_thread = user32.GetWindowThreadProcessId(foreground, None) if foreground else 0
            attached = bool(
                fg_thread
                and fg_thread != this_thread
                and user32.AttachThreadInput(this_thread, fg_thread, True)
            )
            try:
                if user32.IsIconic(hwnd):
                    user32.ShowWindow(hwnd, 9)  # SW_RESTORE
                user32.BringWindowToTop(hwnd)
                return bool(user32.SetForegroundWindow(hwnd))
            finally:
                if attached:
                    user32.AttachThreadInput(this_thread, fg_thread, False)
        time.sleep(_RAISE_POLL_S)
    return False


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


def _linux_choose_folder(prompt: str) -> str | None:
    if not _display_available():
        raise RuntimeError("No graphical display available")
    # Passed as argv elements, never through a shell, so the title needs no
    # escaping on this platform.
    zenity = shutil.which("zenity")
    if zenity:
        return _run_linux_dialog([zenity, "--file-selection", "--directory", f"--title={prompt}"])
    kdialog = shutil.which("kdialog")
    if kdialog:
        return _run_linux_dialog(
            [kdialog, "--getexistingdirectory", str(Path.home()), "--title", prompt]
        )
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


def _applescript_double_quote(text: str) -> str:
    """Escape a string for an AppleScript literal.

    The backslash has to go first. Escaping only the quote turns `a\\"` into
    `a\\\\"`, which AppleScript reads as a backslash followed by the closing
    quote -- the string ends early and whatever follows is script. Nothing
    reaching here is client-supplied (see services/ui_prompts.py), so this is a
    second line of defence rather than the only one.
    """
    return text.replace("\\", "\\\\").replace('"', '\\"')


# A dialog shown by a process that owns no window of its own is not guaranteed
# the foreground on Windows. ALBIS runs its backend windowless -- the picker is
# launched with CREATE_NO_WINDOW -- so the chooser opened *behind* the browser,
# where a tester could not see it at all and the interface looked frozen.
#
# Giving it an owner fixes that: a form that is topmost and activated pulls the
# dialog in front of everything, and the owner stays invisible by being 1x1,
# fully transparent and off the taskbar. It has to be shown rather than merely
# constructed -- an unshown form has no window handle to own anything with.
_WINDOWS_DIALOG_OWNER = """
Add-Type -AssemblyName System.Drawing
$owner = New-Object System.Windows.Forms.Form
$owner.FormBorderStyle = [System.Windows.Forms.FormBorderStyle]::None
$owner.StartPosition = [System.Windows.Forms.FormStartPosition]::CenterScreen
$owner.Size = New-Object System.Drawing.Size(1, 1)
$owner.Opacity = 0
$owner.ShowInTaskbar = $false
$owner.TopMost = $true
$owner.Show()
$owner.Activate()
""".strip()

_WINDOWS_DIALOG_OWNER_CLEANUP = """
$owner.Close()
$owner.Dispose()
""".strip()


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
    detail = (
        (result.stderr or "").strip() or (result.stdout or "").strip() or "Windows dialog failed"
    )
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


def _windows_choose_folder(prompt: str) -> str | None:
    escaped_prompt = _powershell_single_quote(prompt)
    script = f"""
Add-Type -AssemblyName System.Windows.Forms
[System.Windows.Forms.Application]::EnableVisualStyles()
{_WINDOWS_DIALOG_OWNER}
$dialog = New-Object System.Windows.Forms.FolderBrowserDialog
$dialog.Description = '{escaped_prompt}'
$dialog.ShowNewFolderButton = $false
$result = $dialog.ShowDialog($owner)
{_WINDOWS_DIALOG_OWNER_CLEANUP}
if ($result -eq [System.Windows.Forms.DialogResult]::OK) {{
  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8
  [Console]::Write($dialog.SelectedPath)
}}
""".strip()
    return _windows_dialog_runner(script)


def _windows_choose_file(exts: tuple[str, ...], prompt: str) -> str | None:
    escaped_prompt = _powershell_single_quote(prompt)
    escaped_filter = _powershell_single_quote(_windows_picker_filter(exts))
    script = f"""
Add-Type -AssemblyName System.Windows.Forms
[System.Windows.Forms.Application]::EnableVisualStyles()
{_WINDOWS_DIALOG_OWNER}
$dialog = New-Object System.Windows.Forms.OpenFileDialog
$dialog.Title = '{escaped_prompt}'
$dialog.Filter = '{escaped_filter}'
$dialog.FilterIndex = 1
$dialog.Multiselect = $false
$dialog.CheckFileExists = $true
$dialog.RestoreDirectory = $true
$result = $dialog.ShowDialog($owner)
{_WINDOWS_DIALOG_OWNER_CLEANUP}
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


def _tk_choose_folder(prompt: str) -> str | None:
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
        return filedialog.askdirectory(title=prompt) or None
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


def choose_folder(prompt: str = "Select folder") -> str | None:
    """Show the platform's folder chooser under the given title.

    The title used to be the literal "Select Auto Load folder" on every
    platform, shared by five call sites -- so choosing a log directory, an
    export folder or the data root all announced themselves as autoload.
    """
    system = platform.system()
    if system == "Windows":
        return _windows_choose_folder(prompt)
    if system == "Darwin":
        escaped_prompt = _applescript_double_quote(prompt)
        script = f'POSIX path of (choose folder with prompt "{escaped_prompt}")'
        result = subprocess.run(
            ["osascript", "-e", script], capture_output=True, text=True, check=True
        )
        picked = result.stdout.strip()
        return picked or None
    if system == "Linux":
        try:
            return _linux_choose_folder(prompt)
        except RuntimeError:
            if not _display_available():
                raise
            return _tk_choose_folder(prompt)
    return _tk_choose_folder(prompt)


def choose_file(
    exts: tuple[str, ...] | list[str] | None = None,
    prompt: str = "Select image file",
) -> str | None:
    normalized_exts = _normalize_picker_exts(exts)
    system = platform.system()
    if system == "Windows":
        return _windows_choose_file(normalized_exts, prompt)
    if system == "Darwin":
        escaped_prompt = _applescript_double_quote(prompt)
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
