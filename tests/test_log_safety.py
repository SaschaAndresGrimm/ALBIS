"""A logged value cannot forge a second log entry.

CodeQL's py/log-injection alert pointed at the HDF5 tree route, which logs the
group path it was asked for. The path has to name a group that exists in the
opened file, but HDF5 allows a newline in a group name, so the file itself is
enough to split one entry into two -- and ALBIS shows its own log in the
**View Log** dialog, so a forged line is read by the user.
"""

from __future__ import annotations

from backend.services.log_safety import sanitize_log_value


def test_newlines_and_carriage_returns_are_escaped() -> None:
    forged = "/entry\n2026-09-16 12:00:00 CRITICAL Detector on fire"
    cleaned = sanitize_log_value(forged)

    assert "\n" not in cleaned
    assert "\r" not in cleaned
    assert cleaned.startswith("/entry\\n")
    # The text is still there to read, just not as its own line.
    assert "Detector on fire" in cleaned


def test_other_control_characters_are_escaped_as_hex() -> None:
    assert sanitize_log_value("a\x00b") == "a\\x00b"
    assert sanitize_log_value("a\x1bb") == "a\\x1bb"
    assert sanitize_log_value("a\x7fb") == "a\\x7fb"
    assert sanitize_log_value("a b") == "a\\x2028b"


def test_tabs_are_escaped_without_touching_ordinary_text() -> None:
    assert sanitize_log_value("col\tval") == "col\\tval"
    assert sanitize_log_value("/entry/data/data") == "/entry/data/data"


def test_non_ascii_names_stay_readable() -> None:
    """Escaping only what can break a line, so a real name is still a name."""
    assert sanitize_log_value("/entry/データ") == "/entry/データ"
    assert sanitize_log_value("/entry/messgröße") == "/entry/messgröße"


def test_long_values_are_truncated_with_a_marker() -> None:
    cleaned = sanitize_log_value("x" * 600)
    assert len(cleaned) == 512
    assert cleaned.endswith("...")


def test_non_string_values_are_accepted() -> None:
    """Callers pass a `Path` as often as a string, so `str()` is applied here."""
    from pathlib import Path

    path = Path("/data/scan.h5")
    # Compared against `str(path)` rather than a literal: Windows renders the
    # same Path with backslashes, and the separator is not what is under test.
    assert sanitize_log_value(path) == str(path)
    assert sanitize_log_value(7) == "7"
