"""Double-clicking a file on a running ALBIS opens one window, not two.

macOS delivers two things when you open a document with an application that is
already running: the activation, and the document itself. The order is not
guaranteed, and the first version of this shipped opening the viewer
immediately on activation -- so when the activation won the race it opened a
window with nothing in it, and the document then opened a second window beside
it. Which is exactly what a tester saw: "it opens an empty window and one
window with the image".

The plain open is deferred now, so the document event can cancel it. These
tests drive the real application delegate, because the bug was in the ordering
of two Cocoa callbacks and a mock of them would have proved nothing.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pytest

import albis_launcher

pytestmark = pytest.mark.skipif(
    sys.platform != "darwin" or getattr(albis_launcher, "_DockMenuHandler", None) is None,
    reason="the delegate only exists on macOS with PyObjC available",
)

GRACE = 0.05
"""Shortened from the shipped 0.45 s so these finish quickly."""


class _StubApp:
    """Stands in for NSApplication, which the delegate replies to."""

    def __init__(self) -> None:
        self.replies: list[object] = []

    def replyToOpenOrPrint_(self, reply: object) -> None:  # noqa: N802 - Cocoa selector
        self.replies.append(reply)


@pytest.fixture
def delegate(monkeypatch: pytest.MonkeyPatch) -> object:
    """A configured delegate whose browser opens are recorded, not performed."""
    opened: list[tuple[str, int, Path | None]] = []
    monkeypatch.setattr(
        albis_launcher,
        "_open_browser",
        lambda host, port, open_path=None: opened.append((host, port, open_path)),
    )
    # Otherwise this reads ~/.config/albis/server.json and picks up whatever
    # ALBIS the developer has running.
    monkeypatch.setattr(albis_launcher, "_load_last_server", lambda: None)

    handler = albis_launcher._DockMenuHandler.alloc().init()
    handler.host = "127.0.0.1"
    handler.port = 8000
    handler.start_ts = time.perf_counter()
    handler.log_dir = "/tmp"
    handler.last_browser_open_mono = 0.0
    handler.browser_open_throttle_sec = 0.8
    handler.document_event_grace_sec = GRACE
    handler.pending_open_timer = None
    handler.activate_grace_until_mono = 0.0
    handler.opened = opened
    return handler


def _settle() -> None:
    """Wait past the grace period, with room for a slow runner."""
    time.sleep(GRACE * 6)


@pytest.fixture
def h5(tmp_path: Path) -> Path:
    path = tmp_path / "frame.h5"
    path.write_bytes(b"\x89HDF\r\n\x1a\n")
    return path


def test_a_document_arriving_after_the_activation_opens_one_window(
    delegate: object, h5: Path
) -> None:
    """The reported bug, in the order that produced it."""
    delegate.applicationDidBecomeActive_(None)
    # The activation has scheduled a plain open; the document lands before it.
    delegate.application_openFiles_(_StubApp(), [str(h5)])
    _settle()

    assert len(delegate.opened) == 1, f"expected one window, got {delegate.opened}"
    assert delegate.opened[0][2] == h5.resolve(), "the one window must be the file"


def test_a_document_arriving_before_the_activation_opens_one_window(
    delegate: object, h5: Path
) -> None:
    """The order that already worked, which must keep working.

    Here the throttle is what suppresses the activation, so this covers the
    other branch of the same behaviour.
    """
    delegate.application_openFiles_(_StubApp(), [str(h5)])
    delegate.applicationDidBecomeActive_(None)
    _settle()

    assert len(delegate.opened) == 1, f"expected one window, got {delegate.opened}"
    assert delegate.opened[0][2] == h5.resolve()


def test_reopen_and_activation_together_still_open_one_window(delegate: object) -> None:
    """A single Dock click delivers both, and always did."""
    delegate.applicationShouldHandleReopen_hasVisibleWindows_(None, False)
    delegate.applicationDidBecomeActive_(None)
    _settle()

    assert len(delegate.opened) == 1
    assert delegate.opened[0][2] is None, "no document was asked for"


def test_an_ordinary_activation_still_opens_the_viewer(delegate: object) -> None:
    # The deferral must not turn "click the Dock icon" into "nothing happens".
    delegate.applicationDidBecomeActive_(None)
    assert delegate.opened == [], "not before the grace period"
    _settle()
    assert len(delegate.opened) == 1
    assert delegate.opened[0][2] is None


def test_a_selection_with_nothing_openable_leaves_the_plain_window(
    delegate: object, tmp_path: Path
) -> None:
    """Cancelling here would open no window at all.

    A `.txt` handed over by mistake is not a reason to swallow the activation:
    with no file to show, the empty viewer is the right outcome.
    """
    junk = tmp_path / "notes.txt"
    junk.write_text("not a frame")

    delegate.applicationDidBecomeActive_(None)
    delegate.application_openFiles_(_StubApp(), [str(junk)])
    _settle()

    assert len(delegate.opened) == 1
    assert delegate.opened[0][2] is None


def test_a_document_is_never_dropped_by_the_throttle(delegate: object, h5: Path) -> None:
    """Two files opened in quick succession open two windows.

    The throttle exists to collapse the duplicate events one Dock click
    delivers. A second deliberate double-click is not a duplicate.
    """
    delegate.application_openFiles_(_StubApp(), [str(h5)])
    delegate.application_openFiles_(_StubApp(), [str(h5)])
    _settle()

    assert len(delegate.opened) == 2


def test_the_first_of_a_multi_file_selection_is_opened(delegate: object, tmp_path: Path) -> None:
    # One viewer, one file: a tab per file would turn a careless select-all
    # into dozens of them.
    paths = []
    for index in range(4):
        path = tmp_path / f"frame_{index}.h5"
        path.write_bytes(b"\x89HDF\r\n\x1a\n")
        paths.append(path)

    delegate.application_openFiles_(_StubApp(), [str(item) for item in paths])
    _settle()

    assert len(delegate.opened) == 1
    assert delegate.opened[0][2] == paths[0].resolve()


def test_the_application_is_told_the_open_was_handled(delegate: object, h5: Path) -> None:
    """Cocoa expects a reply; without one the Finder can report a failure."""
    app = _StubApp()
    delegate.application_openFiles_(app, [str(h5)])
    _settle()

    assert app.replies, "replyToOpenOrPrint_ was never called"


def test_the_delegate_answers_the_selector_cocoa_will_send(delegate: object) -> None:
    """`application:openFiles:` is what macOS sends; PyObjC maps the name.

    A rename to something Cocoa does not recognise would leave every test above
    passing while the feature did nothing at all.
    """
    assert delegate.respondsToSelector_("application:openFiles:")


def test_the_document_cancels_the_pending_open_rather_than_relying_on_the_throttle(
    delegate: object, h5: Path
) -> None:
    """The cancel must carry its own weight.

    With the shipped constants the throttle would suppress the deferred plain
    open anyway -- it fires 0.45 s after the activation, well inside the 0.8 s
    throttle window, by which time the document has already stamped the clock.
    So the two mechanisms overlap, and removing the cancel changes nothing
    while that ratio holds.

    Tuning either constant should not be able to bring the empty window back,
    so the grace is set longer than the throttle here: now only the cancel can
    prevent the second window.
    """
    delegate.document_event_grace_sec = delegate.browser_open_throttle_sec + 0.2

    delegate.applicationDidBecomeActive_(None)
    delegate.application_openFiles_(_StubApp(), [str(h5)])
    time.sleep(delegate.document_event_grace_sec * 2)

    assert len(delegate.opened) == 1, f"expected one window, got {delegate.opened}"
    assert delegate.opened[0][2] == h5.resolve()
