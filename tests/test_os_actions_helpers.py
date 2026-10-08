from __future__ import annotations

import subprocess

from backend.services.os_actions import choose_file, choose_folder, is_applescript_cancel


def test_is_applescript_cancel_recognizes_cancel_signatures() -> None:
    assert is_applescript_cancel("execution error: User canceled. (-128)")
    assert is_applescript_cancel("user cancelled")
    assert not is_applescript_cancel("unexpected failure")


def test_choose_file_omits_darwin_type_filter_for_expt(monkeypatch) -> None:
    captured: list[list[str]] = []

    def _fake_run(cmd, **kwargs):  # noqa: ANN001
        captured.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="/tmp/imported.expt\n", stderr="")

    monkeypatch.setattr("backend.services.os_actions.platform.system", lambda: "Darwin")
    monkeypatch.setattr("backend.services.os_actions.subprocess.run", _fake_run)

    selected = choose_file(exts=[".expt"], prompt="Select geometry file")

    assert selected == "/tmp/imported.expt"
    assert len(captured) == 1
    assert 'choose file with prompt "Select geometry file"' in captured[0][2]
    assert "of type" not in captured[0][2]


def test_choose_file_maps_darwin_compound_extension_to_terminal_suffix(monkeypatch) -> None:
    captured: list[list[str]] = []

    def _fake_run(cmd, **kwargs):  # noqa: ANN001
        captured.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="/tmp/frame_0001.cbf.gz\n", stderr="")

    monkeypatch.setattr("backend.services.os_actions.platform.system", lambda: "Darwin")
    monkeypatch.setattr("backend.services.os_actions.subprocess.run", _fake_run)

    selected = choose_file(exts=[".h5", ".cbf.gz"], prompt="Select image file")

    assert selected == "/tmp/frame_0001.cbf.gz"
    assert len(captured) == 1
    assert 'choose file with prompt "Select image file"' in captured[0][2]
    assert 'of type {"h5", "gz"}' in captured[0][2]
    assert "cbf.gz" not in captured[0][2]


def test_choose_file_uses_windows_powershell_dialog(monkeypatch) -> None:
    captured: list[list[str]] = []

    def _fake_run(cmd, **kwargs):  # noqa: ANN001
        captured.append(cmd)
        assert kwargs["capture_output"] is True
        assert kwargs["text"] is True
        return subprocess.CompletedProcess(
            cmd, 0, stdout="C:\\Users\\test\\frame_0001.h5\r\n", stderr=""
        )

    monkeypatch.setattr("backend.services.os_actions.platform.system", lambda: "Windows")
    monkeypatch.setattr(
        "backend.services.os_actions.shutil.which",
        lambda name: "powershell.exe" if name == "powershell" else None,
    )
    monkeypatch.setattr("backend.services.os_actions.subprocess.run", _fake_run)

    selected = choose_file(exts=[".h5", ".cbf.gz"], prompt="Select image file")

    assert selected == "C:\\Users\\test\\frame_0001.h5"
    assert captured == [
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-STA",
            "-Command",
            captured[0][5],
        ]
    ]
    assert "System.Windows.Forms.OpenFileDialog" in captured[0][5]
    assert "*.h5;*.cbf.gz" in captured[0][5]
    assert "Select image file" in captured[0][5]


def test_choose_folder_uses_windows_powershell_dialog(monkeypatch) -> None:
    captured: list[list[str]] = []

    def _fake_run(cmd, **kwargs):  # noqa: ANN001
        captured.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="C:\\Users\\test\\data\r\n", stderr="")

    monkeypatch.setattr("backend.services.os_actions.platform.system", lambda: "Windows")
    monkeypatch.setattr(
        "backend.services.os_actions.shutil.which",
        lambda name: "powershell.exe" if name == "powershell" else None,
    )
    monkeypatch.setattr("backend.services.os_actions.subprocess.run", _fake_run)

    selected = choose_folder(prompt="Select the log folder")

    assert selected == "C:\\Users\\test\\data"
    assert captured == [
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-STA",
            "-Command",
            captured[0][5],
        ]
    ]
    assert "System.Windows.Forms.FolderBrowserDialog" in captured[0][5]
    # The title is the caller's now, not one hardcoded literal for every
    # chooser in the application. See tests/test_picker_prompts.py.
    assert "Select the log folder" in captured[0][5]


def test_reveal_selects_the_file_in_explorer_and_raises_its_window(monkeypatch, tmp_path) -> None:
    from backend.services import os_actions

    launched: list[object] = []
    matchers: list = []
    monkeypatch.setattr(os_actions.platform, "system", lambda: "Windows")
    monkeypatch.setattr(os_actions.subprocess, "Popen", lambda cmd, **_: launched.append(cmd))
    monkeypatch.setattr(os_actions, "_raise_when_shown", matchers.append)

    installer = tmp_path / "ALBIS Setup.exe"
    assert os_actions.reveal_in_file_manager(installer) is True

    # Selected, not opened, with only the path quoted, as Explorer requires.
    assert launched == [f'explorer /select,"{installer}"']
    # The Explorer window for that folder is raised, not any window of that name.
    (matches,) = matchers
    assert matches(tmp_path.name, "CabinetWClass")
    assert matches(str(tmp_path), "CabinetWClass")
    assert not matches(tmp_path.name, "Chrome_WidgetWin_1")
    assert not matches("ALBIS - Microsoft Edge", "CabinetWClass")


def test_reveal_opens_the_folder_elsewhere(monkeypatch, tmp_path) -> None:
    from backend.services import os_actions

    opened: list = []
    monkeypatch.setattr(os_actions.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(os_actions, "open_in_system", lambda path: opened.append(path) or True)
    assert os_actions.reveal_in_file_manager(tmp_path / "ALBIS.dmg") is True
    assert opened == [tmp_path]


def test_opening_on_windows_raises_the_window_that_shows_the_file(monkeypatch, tmp_path) -> None:
    from backend.services import os_actions

    started: list[str] = []
    matchers: list = []
    monkeypatch.setattr(os_actions.platform, "system", lambda: "Windows")
    monkeypatch.setattr(os_actions.os, "startfile", started.append, raising=False)
    monkeypatch.setattr(os_actions, "_raise_when_shown", matchers.append)

    log = tmp_path / "albis.log"
    assert os_actions.open_in_system(log) is True
    assert started == [str(log)]
    (matches,) = matchers
    assert matches("albis.log - Notepad", "Notepad")
    assert not matches("ALBIS - Google Chrome", "Chrome_WidgetWin_1")


def test_raising_attaches_to_the_foreground_input_and_raises_the_match(monkeypatch) -> None:
    """The Windows calls, against a fake user32: the browser has the focus."""
    import ctypes

    from backend.services import os_actions

    windows = {
        1: ("ALBIS - Google Chrome", "Chrome_WidgetWin_1"),
        2: ("Downloads", "CabinetWClass"),
    }
    calls: list[tuple] = []

    class Fn:
        def __init__(self, name, impl):
            self.name, self.impl = name, impl

        def __call__(self, *args):
            calls.append((self.name, *args))
            return self.impl(*args)

    class User32:
        GetForegroundWindow = Fn("GetForegroundWindow", lambda: 1)
        IsWindowVisible = Fn("IsWindowVisible", lambda hwnd: True)
        IsIconic = Fn("IsIconic", lambda hwnd: True)
        ShowWindow = Fn("ShowWindow", lambda hwnd, cmd: True)
        BringWindowToTop = Fn("BringWindowToTop", lambda hwnd: True)
        SetForegroundWindow = Fn("SetForegroundWindow", lambda hwnd: True)
        GetWindowTextLengthW = Fn("GetWindowTextLengthW", lambda hwnd: len(windows[hwnd][0]))
        GetWindowTextW = Fn(
            "GetWindowTextW", lambda hwnd, buf, n: setattr(buf, "value", windows[hwnd][0])
        )
        GetClassNameW = Fn(
            "GetClassNameW", lambda hwnd, buf, n: setattr(buf, "value", windows[hwnd][1])
        )
        GetWindowThreadProcessId = Fn("GetWindowThreadProcessId", lambda hwnd, pid: 77)
        AttachThreadInput = Fn("AttachThreadInput", lambda a, b, attach: True)
        EnumWindows = Fn("EnumWindows", lambda cb, lparam: [cb(h, 0) for h in windows] and True)

    class Kernel32:
        GetCurrentThreadId = Fn("GetCurrentThreadId", lambda: 5)

    dlls = {"user32": User32(), "kernel32": Kernel32()}
    monkeypatch.setattr(ctypes, "WinDLL", lambda name, **_: dlls[name], raising=False)
    monkeypatch.setattr(ctypes, "WINFUNCTYPE", lambda *_: (lambda fn: fn), raising=False)

    raised = os_actions._raise_window(
        lambda title, cls: cls == "CabinetWClass" and title == "Downloads"
    )

    assert raised is True
    names = [call[0] for call in calls]
    # Attached to the browser's input, restored, raised, detached -- in that order.
    assert ("AttachThreadInput", 5, 77, True) in calls
    assert ("ShowWindow", 2, 9) in calls
    assert ("SetForegroundWindow", 2) in calls
    assert calls[-1] == ("AttachThreadInput", 5, 77, False)
    assert names.index("AttachThreadInput") < names.index("SetForegroundWindow")
