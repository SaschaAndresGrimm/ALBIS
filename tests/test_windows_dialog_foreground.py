"""A native picker must open in front of the browser, not behind it.

ALBIS runs its backend windowless and launches the Windows picker with
`CREATE_NO_WINDOW`, so the PowerShell process that shows the dialog owns no
window at all. Windows does not promise the foreground to a dialog in that
situation, and a tester on Windows found the folder chooser opening *behind*
the browser window: invisible, with the interface apparently doing nothing.

The fix is an owner window -- topmost, activated, and invisible -- for the
dialog to belong to. None of it can be executed here, so these tests read the
PowerShell that would be executed. That is weaker than running it, and it is
why each test names the specific way the dialog goes back to being lost.
"""

from __future__ import annotations

import pytest

from backend.services import os_actions


@pytest.fixture
def folder_script(monkeypatch: pytest.MonkeyPatch) -> str:
    """The script `_windows_choose_folder` would hand to PowerShell."""
    captured: list[str] = []
    monkeypatch.setattr(
        os_actions, "_windows_dialog_runner", lambda script: captured.append(script) or None
    )
    os_actions._windows_choose_folder("Select the log folder")
    return captured[0]


@pytest.fixture
def file_script(monkeypatch: pytest.MonkeyPatch) -> str:
    """The script `_windows_choose_file` would hand to PowerShell."""
    captured: list[str] = []
    monkeypatch.setattr(
        os_actions, "_windows_dialog_runner", lambda script: captured.append(script) or None
    )
    os_actions._windows_choose_file((".h5", ".cbf"), "Select an image")
    return captured[0]


@pytest.fixture(params=["folder_script", "file_script"])
def script(request: pytest.FixtureRequest) -> str:
    """Both pickers, because both were opening behind the browser."""
    return request.getfixturevalue(request.param)


def test_the_dialog_is_shown_with_an_owner(script: str) -> None:
    # `ShowDialog()` with no argument is the bug: the dialog belongs to
    # nothing, so nothing brings it forward.
    assert "$dialog.ShowDialog($owner)" in script
    assert "$dialog.ShowDialog()" not in script


def test_the_owner_is_topmost_and_activated(script: str) -> None:
    # Topmost puts it above the browser; Activate moves the focus, without
    # which the dialog can still open behind on a multi-monitor desktop.
    assert "$owner.TopMost = $true" in script
    assert "$owner.Activate()" in script


def test_the_owner_is_shown_so_it_has_a_window_handle(script: str) -> None:
    """A form that was never shown cannot own a dialog.

    `ShowDialog` needs a real window handle from its owner, and a constructed
    form has none -- the dialog would come up ownerless again, exactly as
    before, with nothing in the script looking wrong.
    """
    assert "$owner.Show()" in script


def test_the_owner_is_invisible(script: str) -> None:
    # It exists only to be owned. A visible 1x1 window flashing up centre
    # screen, or a second ALBIS entry in the taskbar, would be its own bug.
    assert "$owner.Opacity = 0" in script
    assert "$owner.ShowInTaskbar = $false" in script
    assert "New-Object System.Drawing.Size(1, 1)" in script


def test_the_owner_is_disposed(script: str) -> None:
    # The picker runs in its own short-lived process, so a leak here is
    # bounded -- but a form left open keeps a topmost window alive above the
    # browser for as long as that process lives.
    assert "$owner.Close()" in script
    assert "$owner.Dispose()" in script


def test_the_drawing_assembly_is_loaded_before_it_is_used(script: str) -> None:
    """`System.Drawing.Size` needs its assembly.

    PowerShell fails at the `New-Object` rather than at the top, so without
    this the picker would die mid-script and surface as "Folder picker failed".
    """
    assert "Add-Type -AssemblyName System.Drawing" in script
    assert script.index("Add-Type -AssemblyName System.Drawing") < script.index(
        "System.Drawing.Size"
    )


def test_the_result_is_read_before_the_owner_is_disposed(script: str) -> None:
    # Disposing first would throw away the dialog's answer.
    assert script.index("$result = $dialog.ShowDialog($owner)") < script.index("$owner.Dispose()")


def test_the_selection_is_still_written_to_stdout(script: str) -> None:
    """The owner must not have displaced how the answer comes back.

    The braces around this block had to be doubled when the script became an
    f-string, and getting that wrong yields PowerShell that runs and prints
    nothing -- a picker that silently cancels every time.
    """
    assert "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8" in script
    assert "[System.Windows.Forms.DialogResult]::OK" in script
    assert "[Console]::Write(" in script


def test_no_literal_format_placeholder_survived(script: str) -> None:
    """An f-string brace that was not doubled shows up here.

    `{_WINDOWS_DIALOG_OWNER}` left unexpanded, or a `{{` that should have been
    `{`, both produce a script that is wrong in a way no other assertion in
    this file would notice.
    """
    assert "{_WINDOWS" not in script
    assert "{{" not in script
    assert "}}" not in script


def test_the_folder_dialog_keeps_its_own_settings(folder_script: str) -> None:
    assert "FolderBrowserDialog" in folder_script
    assert "$dialog.ShowNewFolderButton = $false" in folder_script
    # Titled by the caller rather than by one literal shared application-wide.
    assert "Select the log folder" in folder_script


def test_the_file_dialog_keeps_its_filter_and_prompt(file_script: str) -> None:
    assert "OpenFileDialog" in file_script
    assert "Select an image" in file_script
    assert "*.h5" in file_script
