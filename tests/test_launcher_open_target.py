"""Opening a file the operating system handed to the launcher.

The desktop association is only half the feature: once Windows, Linux or macOS
has decided ALBIS should open a file, ALBIS has to actually do it. Three things
have to hold, and each has a way of failing quietly.

The path has to survive the trip. It reaches the interface through the URL
fragment, so a path with a space or an ampersand in it -- ordinary on a
beamline share -- must not arrive truncated or mangled.

A second double-click must not start a second server. That part already worked
for a bare launch; what is new is that it now has to carry the file across to
the instance already running.

And nothing the operating system passes may stop ALBIS from starting. A viewer
that refuses to open because it did not like an argument nobody typed is worse
than one that shows its splash screen.
"""

from __future__ import annotations

import urllib.parse
from pathlib import Path

import pytest

import albis_launcher
from backend.file_associations import ASSOCIATED_EXTENSIONS


@pytest.fixture
def h5(tmp_path: Path) -> Path:
    path = tmp_path / "frame.h5"
    path.write_bytes(b"\x89HDF\r\n\x1a\n")
    return path


# --------------------------------------------------------------------------
# Deciding what to open
# --------------------------------------------------------------------------


def test_an_associated_file_is_accepted_and_made_absolute(h5: Path) -> None:
    resolved = albis_launcher._resolve_open_target(str(h5))
    assert resolved == h5.resolve()
    assert resolved.is_absolute(), "the interface gets one path, not one relative to a cwd"


@pytest.mark.parametrize("ext", sorted(ASSOCIATED_EXTENSIONS))
def test_every_registered_extension_is_accepted(tmp_path: Path, ext: str) -> None:
    """What the installers register and what the launcher accepts must match.

    Registering an extension the launcher then refuses would put ALBIS in the
    "Open with" menu for a file it silently declines to open.
    """
    path = tmp_path / f"sample{ext}"
    path.write_bytes(b"\x00")
    assert albis_launcher._resolve_open_target(str(path)) == path.resolve()


def test_extension_matching_ignores_case(tmp_path: Path) -> None:
    # Windows and macOS both hand back whatever case is on disk, and a
    # detector writing .H5 is not unusual.
    path = tmp_path / "FRAME.H5"
    path.write_bytes(b"\x00")
    assert albis_launcher._resolve_open_target(str(path)) == path.resolve()


@pytest.mark.parametrize(
    "name",
    [
        "series.cbf.gz",  # ends in .gz; associating that would claim every archive
        "mythen.cfg",
        "notes.txt",
    ],
)
def test_an_unregistered_extension_is_declined(tmp_path: Path, name: str) -> None:
    path = tmp_path / name
    path.write_bytes(b"\x00")
    assert albis_launcher._resolve_open_target(str(path)) is None


def test_a_path_that_is_not_a_file_is_declined(tmp_path: Path) -> None:
    missing = tmp_path / "gone.h5"
    assert albis_launcher._resolve_open_target(str(missing)) is None
    # A directory named like a file: opening it would 404 further in.
    directory = tmp_path / "folder.h5"
    directory.mkdir()
    assert albis_launcher._resolve_open_target(str(directory)) is None


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_nothing_to_open_is_not_an_error(raw: str | None) -> None:
    assert albis_launcher._resolve_open_target(raw) is None


# --------------------------------------------------------------------------
# Parsing the command line the operating system produced
# --------------------------------------------------------------------------


def test_a_bare_path_is_taken_as_the_file_to_open(h5: Path) -> None:
    _applied, _ignored, target = albis_launcher._apply_cli_arguments([str(h5)])
    assert target == h5.resolve()


def test_flags_still_work_alongside_a_path(h5: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ALBIS_SERVER_PORT", raising=False)
    applied, _ignored, target = albis_launcher._apply_cli_arguments(["--port", "9123", str(h5)])
    assert target == h5.resolve()
    assert "--port" in applied


def test_the_macos_process_serial_number_argument_does_not_become_the_file() -> None:
    """`-psn_0_...` is what macOS used to append to a bundle's argv.

    It parses as an option rather than a positional, so it lands in the
    ignored list -- but a regression that made it the path would have ALBIS
    trying to open a file called `-psn_0_12345` on every launch.
    """
    applied, ignored, target = albis_launcher._apply_cli_arguments(["-psn_0_12345"])
    assert target is None
    assert applied == []
    assert "-psn_0_12345" in ignored


def test_an_unopenable_path_is_ignored_rather_than_fatal(tmp_path: Path) -> None:
    # The caller is a file manager, not a person: starting with the splash
    # beats refusing to start.
    _applied, _ignored, target = albis_launcher._apply_cli_arguments([str(tmp_path / "nope.txt")])
    assert target is None


# --------------------------------------------------------------------------
# Handing the path to the interface
# --------------------------------------------------------------------------


def test_the_url_carries_no_fragment_when_there_is_no_file() -> None:
    assert albis_launcher._open_target_url("127.0.0.1", 8000) == "http://127.0.0.1:8000"


def test_the_url_carries_the_path_in_the_fragment(h5: Path) -> None:
    url = albis_launcher._open_target_url("127.0.0.1", 8000, h5.resolve())
    base, _, fragment = url.partition("#")
    assert base == "http://127.0.0.1:8000/"
    assert fragment.startswith(albis_launcher.OPEN_HASH_PREFIX)
    encoded = fragment[len(albis_launcher.OPEN_HASH_PREFIX) :]
    assert urllib.parse.unquote(encoded) == str(h5.resolve())


@pytest.mark.parametrize(
    "name",
    [
        "two words.h5",  # a space must not arrive as "+"
        "run&scan.h5",  # an ampersand would end the fragment value early
        "50%done.h5",  # a stray percent must not read as an escape
        "ünïcode.h5",
        "hash#tag.h5",
    ],
)
def test_awkward_filenames_survive_the_fragment(tmp_path: Path, name: str) -> None:
    path = tmp_path / name
    path.write_bytes(b"\x00")
    url = albis_launcher._open_target_url("127.0.0.1", 8000, path.resolve())
    _base, _, fragment = url.partition("#")
    encoded = fragment[len(albis_launcher.OPEN_HASH_PREFIX) :]
    # Nothing in the encoded form may be mistaken for fragment syntax.
    assert "&" not in encoded
    assert "#" not in encoded
    assert " " not in encoded
    assert urllib.parse.unquote(encoded) == str(path.resolve())


def test_the_fragment_is_not_sent_to_the_server(h5: Path) -> None:
    """Why a fragment and not `?open=`.

    A browser never puts the fragment in the request line, so the path stays
    out of the access log. Asserted as the property it is, since the choice
    would otherwise look arbitrary.
    """
    url = albis_launcher._open_target_url("127.0.0.1", 8000, h5.resolve())
    parsed = urllib.parse.urlparse(url)
    assert parsed.query == ""
    assert parsed.fragment


def test_the_browser_is_opened_at_the_url_with_the_file(
    h5: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    opened: list[str] = []
    monkeypatch.setattr(
        albis_launcher.webbrowser, "open", lambda url, **_kwargs: opened.append(url) or True
    )
    albis_launcher._open_browser("127.0.0.1", 8000, h5.resolve())
    assert len(opened) == 1
    assert opened[0] == albis_launcher._open_target_url("127.0.0.1", 8000, h5.resolve())
