from __future__ import annotations

import subprocess

from backend.services.os_actions import choose_file, is_applescript_cancel


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
