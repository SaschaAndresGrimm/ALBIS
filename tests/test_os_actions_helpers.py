from __future__ import annotations

from backend.services.os_actions import is_applescript_cancel


def test_is_applescript_cancel_recognizes_cancel_signatures() -> None:
    assert is_applescript_cancel("execution error: User canceled. (-128)")
    assert is_applescript_cancel("user cancelled")
    assert not is_applescript_cancel("unexpected failure")
