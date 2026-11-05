"""Where the native file and folder choosers open.

Every chooser used to be shown without a starting folder, so each opened
wherever its toolkit liked. A beamline scientist on Red Hat with `data.root`
set to the experiment's folder landed in their home folder on every
File -> Open from the AppImage, however the config was written.

The tests below cover each toolkit's command line, since the dialogs
themselves cannot be opened here: zenity and kdialog (Linux), Tk (the Linux
fallback and anything else), AppleScript (macOS) and PowerShell (Windows).
"""

from __future__ import annotations

import subprocess
import sys
import types
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.services import os_actions
from backend.services.os_actions import choose_file, choose_folder
from backend.services.picker_start import PickerStartFolders

# A folder name with every quote a script language might end a string on:
# ASCII single and double quotes, and the curly quotes PowerShell also accepts.
AWKWARD = 'cm44169-2 Mark\'s "run" \u2019final\u2018'


@pytest.fixture
def start_dir(tmp_path: Path) -> Path:
    folder = tmp_path / AWKWARD
    folder.mkdir()
    return folder


# --------------------------------------------------------------------------
# Which folder: the data root first, then where the last chooser left off
# --------------------------------------------------------------------------


def test_the_first_chooser_opens_in_the_data_root(tmp_path: Path) -> None:
    starts = PickerStartFolders()
    assert starts.start_for("file", tmp_path) == tmp_path
    assert starts.start_for("folder", tmp_path) == tmp_path


def test_a_file_chooser_reopens_in_the_folder_of_the_file_picked(tmp_path: Path) -> None:
    run = tmp_path / "run_7"
    run.mkdir()
    starts = PickerStartFolders()

    starts.remember("file", run / "frame_master.h5")

    assert starts.start_for("file", tmp_path) == run


def test_a_folder_chooser_reopens_in_the_folder_picked(tmp_path: Path) -> None:
    out = tmp_path / "exports"
    out.mkdir()
    starts = PickerStartFolders()

    starts.remember("folder", out)

    assert starts.start_for("folder", tmp_path) == out
    # The two kinds are remembered apart: picking an export folder does not
    # move where File -> Open starts.
    assert starts.start_for("file", tmp_path) == tmp_path


def test_a_remembered_folder_that_has_gone_falls_back_to_the_data_root(tmp_path: Path) -> None:
    gone = tmp_path / "deleted"
    starts = PickerStartFolders()
    starts.remember("folder", gone)

    assert starts.start_for("folder", tmp_path) == tmp_path


def test_without_any_existing_folder_the_toolkit_decides(tmp_path: Path) -> None:
    # A chooser told to open in a missing folder opens somewhere arbitrary or
    # refuses outright, so none is better than a wrong one.
    assert PickerStartFolders().start_for("file", tmp_path / "missing") is None


def test_an_unreachable_mount_counts_as_missing(tmp_path: Path, monkeypatch) -> None:
    def _stale_mount(self) -> bool:  # noqa: ANN001
        raise OSError(116, "Stale file handle")

    monkeypatch.setattr(Path, "is_dir", _stale_mount)

    assert PickerStartFolders().start_for("file", tmp_path) is None


# --------------------------------------------------------------------------
# The route: the regression itself
# --------------------------------------------------------------------------


def test_file_open_starts_in_the_data_root_and_then_where_it_left_off(
    tmp_path: Path, monkeypatch
) -> None:
    """The reported bug: File -> Open ignored `data.root`.

    Without the fix the chooser is called with no starting folder at all.
    """
    from backend import app as app_module

    monkeypatch.setattr(app_module.picker_start, "_last", {})
    run = tmp_path / "run_7"
    run.mkdir()
    frame = run / "frame_master.h5"
    frame.write_bytes(b"")
    seen: list[Path | None] = []

    def _fake_choose_file(exts=None, prompt="", start_dir=None):  # noqa: ANN001
        seen.append(start_dir)
        return str(frame)

    monkeypatch.setattr("backend.routes.files._choose_file", _fake_choose_file)
    client = TestClient(app_module.app)

    assert client.get("/api/choose-file").status_code == 200
    assert client.get("/api/choose-file").status_code == 200

    assert seen == [app_module.runtime_state.data_dir, frame.resolve().parent]


def test_folder_choosers_start_in_the_data_root_and_then_where_they_left_off(
    tmp_path: Path, monkeypatch
) -> None:
    from backend import app as app_module

    monkeypatch.setattr(app_module.picker_start, "_last", {})
    seen: list[Path | None] = []

    def _fake_choose_folder(prompt="", start_dir=None):  # noqa: ANN001
        seen.append(start_dir)
        return str(tmp_path)

    monkeypatch.setattr("backend.routes.files._choose_folder", _fake_choose_folder)
    client = TestClient(app_module.app)

    client.get("/api/choose-folder", params={"purpose": "export_output"})
    client.get("/api/choose-folder", params={"purpose": "export_output"})

    assert seen == [app_module.runtime_state.data_dir, tmp_path]


def test_a_cancelled_chooser_does_not_move_the_start(tmp_path: Path, monkeypatch) -> None:
    from backend import app as app_module

    monkeypatch.setattr(app_module.picker_start, "_last", {})
    monkeypatch.setattr("backend.routes.files._choose_file", lambda **_kw: None)

    assert TestClient(app_module.app).get("/api/choose-file").status_code == 204
    assert app_module.picker_start._last == {}


# --------------------------------------------------------------------------
# Linux: zenity, kdialog
# --------------------------------------------------------------------------


def _linux(monkeypatch, available: str) -> list[list[str]]:
    captured: list[list[str]] = []

    def _fake_run(cmd, **_kwargs):  # noqa: ANN001
        captured.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="/data/x.h5\n", stderr="")

    monkeypatch.setattr(os_actions.platform, "system", lambda: "Linux")
    monkeypatch.setenv("DISPLAY", ":0")
    monkeypatch.setattr(
        os_actions.shutil, "which", lambda name: f"/usr/bin/{name}" if name == available else None
    )
    monkeypatch.setattr(os_actions.subprocess, "run", _fake_run)
    return captured


@pytest.mark.parametrize("chooser", ["file", "folder"])
def test_zenity_opens_in_the_start_folder(chooser: str, start_dir: Path, monkeypatch) -> None:
    captured = _linux(monkeypatch, "zenity")

    if chooser == "file":
        choose_file(prompt="Select image file", start_dir=start_dir)
    else:
        choose_folder(prompt="Select folder", start_dir=start_dir)

    # A missing name inside the folder: the only spelling that opens the folder
    # itself on zenity 4 as well as 3 (see `_ZENITY_START_NAME`). One argv
    # element: no shell, no quoting.
    assert f"--filename={start_dir / '.albis-start'}" in captured[0]


@pytest.mark.parametrize("chooser", ["file", "folder"])
def test_kdialog_opens_in_the_start_folder(chooser: str, start_dir: Path, monkeypatch) -> None:
    captured = _linux(monkeypatch, "kdialog")

    if chooser == "file":
        choose_file(prompt="Select image file", start_dir=start_dir)
    else:
        choose_folder(prompt="Select folder", start_dir=start_dir)

    assert captured[0][2] == str(start_dir)


def test_without_a_start_folder_linux_choosers_behave_as_before(monkeypatch) -> None:
    captured = _linux(monkeypatch, "zenity")
    choose_file(prompt="Select image file")
    assert not any(arg.startswith("--filename") for arg in captured[0])

    captured = _linux(monkeypatch, "kdialog")
    choose_file(prompt="Select image file")
    assert captured[0][2] == str(Path.home())


# --------------------------------------------------------------------------
# Tk: the fallback where neither zenity nor kdialog is installed
# --------------------------------------------------------------------------


@pytest.mark.parametrize("chooser", ["file", "folder"])
def test_tk_opens_in_the_start_folder(chooser: str, start_dir: Path, monkeypatch) -> None:
    calls: list[dict] = []

    class _FakeTk:
        def withdraw(self) -> None: ...
        def attributes(self, *_args) -> None: ...
        def destroy(self) -> None: ...

    def _record(**kwargs):  # noqa: ANN003
        calls.append(kwargs)
        return "/data/x"

    filedialog = types.SimpleNamespace(askopenfilename=_record, askdirectory=_record)
    tkinter = types.ModuleType("tkinter")
    tkinter.Tk = _FakeTk
    tkinter.filedialog = filedialog
    monkeypatch.setitem(sys.modules, "tkinter", tkinter)
    monkeypatch.setitem(sys.modules, "tkinter.filedialog", filedialog)
    # Linux with a display but no zenity or kdialog: the Tk fallback.
    _linux(monkeypatch, "none")

    if chooser == "file":
        choose_file(prompt="Select image file", start_dir=start_dir)
    else:
        choose_folder(prompt="Select folder", start_dir=start_dir)

    assert calls[0]["initialdir"] == str(start_dir)


# --------------------------------------------------------------------------
# macOS: AppleScript
# --------------------------------------------------------------------------


def _darwin(monkeypatch) -> list[list[str]]:
    captured: list[list[str]] = []

    def _fake_run(cmd, **_kwargs):  # noqa: ANN001
        captured.append(cmd)
        return subprocess.CompletedProcess(cmd, 0, stdout="/data/x.h5\n", stderr="")

    monkeypatch.setattr(os_actions.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(os_actions.subprocess, "run", _fake_run)
    return captured


@pytest.mark.parametrize("chooser", ["file", "folder"])
def test_applescript_takes_the_start_folder_as_an_argument(
    chooser: str, start_dir: Path, monkeypatch
) -> None:
    captured = _darwin(monkeypatch)

    if chooser == "file":
        picked = choose_file(prompt="Select image file", start_dir=start_dir)
    else:
        picked = choose_folder(prompt="Select folder", start_dir=start_dir)

    cmd = captured[0]
    assert picked == "/data/x.h5"
    assert cmd[-1] == str(start_dir)
    script = "\n".join(cmd[:-1])
    assert "default location (POSIX file (item 1 of argv))" in script
    # Not spliced into the script, where its quotes would end the string.
    assert AWKWARD not in script


def test_applescript_keeps_the_type_filter_with_a_start_folder(
    start_dir: Path, monkeypatch
) -> None:
    captured = _darwin(monkeypatch)

    choose_file(exts=[".h5", ".cbf.gz"], prompt="Select image file", start_dir=start_dir)

    assert 'of type {"h5", "gz"} default location' in "\n".join(captured[0])


# --------------------------------------------------------------------------
# Windows: PowerShell
# --------------------------------------------------------------------------


def _windows(monkeypatch) -> list[tuple[list[str], dict]]:
    captured: list[tuple[list[str], dict]] = []

    def _fake_run(cmd, **kwargs):  # noqa: ANN001
        captured.append((cmd, kwargs))
        return subprocess.CompletedProcess(cmd, 0, stdout=b"C:\\data\\x.h5", stderr=b"")

    monkeypatch.setattr(os_actions.platform, "system", lambda: "Windows")
    monkeypatch.setattr(
        os_actions.shutil, "which", lambda name: "powershell.exe" if name == "powershell" else None
    )
    monkeypatch.setattr(os_actions.subprocess, "run", _fake_run)
    return captured


@pytest.mark.parametrize(
    ("chooser", "property_name"),
    [("file", "InitialDirectory"), ("folder", "SelectedPath")],
)
def test_powershell_reads_the_start_folder_from_the_environment(
    chooser: str, property_name: str, start_dir: Path, monkeypatch
) -> None:
    captured = _windows(monkeypatch)

    if chooser == "file":
        choose_file(prompt="Select image file", start_dir=start_dir)
    else:
        choose_folder(prompt="Select folder", start_dir=start_dir)

    cmd, kwargs = captured[0]
    script = cmd[-1]
    assert kwargs["env"]["ALBIS_PICKER_START"] == str(start_dir)
    assert f"$dialog.{property_name} = $env:ALBIS_PICKER_START" in script
    # PowerShell ends a single-quoted string on ’ and ‘ as well as ', which
    # the existing escaping does not double. Kept out of the script entirely.
    assert AWKWARD not in script


def test_an_inherited_start_folder_does_not_leak_into_powershell(monkeypatch) -> None:
    monkeypatch.setenv("ALBIS_PICKER_START", "C:\\somewhere\\else")
    captured = _windows(monkeypatch)

    choose_file(prompt="Select image file")

    assert "ALBIS_PICKER_START" not in captured[0][1]["env"]
