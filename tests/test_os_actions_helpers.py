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

    selected = choose_folder()

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
    assert "Select Auto Load folder" in captured[0][5]
