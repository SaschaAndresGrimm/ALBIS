"""A tab's presence connection keeps ALBIS from stopping while the tab is open.

ALBIS stopped itself under two open Firefox tabs on macOS: App Nap silenced
the tabs' timers for a quarter of an hour, and silence was all the idle stop
went by. Throttling slows a page but does not close its connections, so an
open `/api/presence` WebSocket now counts as an open tab, however quiet.
"""

from __future__ import annotations

import logging
import threading

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import albis_launcher
from backend.app import app, runtime_state
from backend.lifecycle import IdleWatch, idle_watch

LOG = logging.getLogger("test")


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_an_open_connection_never_counts_as_idle_however_long_it_is_silent() -> None:
    clock = Clock()
    watch = IdleWatch(clock=clock)
    watch.connect()
    clock.now = 3 * 60 * 60
    assert watch.idle_seconds() == 0
    assert not watch.check(15, lambda: [], lambda: None, LOG)


def test_the_countdown_starts_when_the_last_connection_closes() -> None:
    clock = Clock()
    watch = IdleWatch(clock=clock)
    watch.connect()
    watch.connect()
    clock.now = 60 * 60
    watch.disconnect()
    clock.now = 2 * 60 * 60
    assert watch.idle_seconds() == 0  # one tab still open
    watch.disconnect()
    clock.now += 14 * 60
    assert not watch.check(15, lambda: [], lambda: None, LOG)
    clock.now += 60
    assert watch.check(15, lambda: [], lambda: None, LOG)


def test_more_disconnects_than_connects_do_not_go_negative() -> None:
    watch = IdleWatch(clock=Clock())
    watch.disconnect()
    assert watch.connections() == 0


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(runtime_state, "bind_host", "127.0.0.1")
    return TestClient(app, base_url="http://127.0.0.1")


def test_the_presence_socket_is_counted_while_open(client) -> None:
    before = idle_watch.connections()
    with client.websocket_connect(
        "/api/presence", headers={"host": "127.0.0.1", "origin": "http://127.0.0.1"}
    ):
        assert idle_watch.connections() == before + 1
    # The server notices the close on its own task; give it a moment.
    for _ in range(50):
        if idle_watch.connections() == before:
            break
        threading.Event().wait(0.02)
    assert idle_watch.connections() == before


def test_a_rebound_host_cannot_hold_the_presence_socket(client) -> None:
    with (
        pytest.raises(WebSocketDisconnect) as closed,
        client.websocket_connect("/api/presence", headers={"host": "attacker.example"}),
    ):
        pass
    assert closed.value.code == 1008


def test_another_site_cannot_hold_the_presence_socket(client) -> None:
    before = idle_watch.connections()
    with (
        pytest.raises(WebSocketDisconnect) as closed,
        client.websocket_connect(
            "/api/presence", headers={"host": "127.0.0.1", "origin": "https://evil.example"}
        ),
    ):
        pass
    assert closed.value.code == 1008
    assert idle_watch.connections() == before


class _FakeApp:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def performSelectorOnMainThread_withObject_waitUntilDone_(self, *args):  # noqa: N802
        self.calls.append(args)


def test_the_macos_app_quits_when_its_server_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_app = _FakeApp()
    monkeypatch.setattr(
        albis_launcher, "AppKit", type("AK", (), {"NSApp": staticmethod(lambda: fake_app)})
    )
    monkeypatch.setattr(albis_launcher, "_launcher_log", lambda *_a, **_k: None)
    server = threading.Thread(target=lambda: None)
    server.start()
    albis_launcher._quit_app_when_server_stops(server, 0.0)
    for _ in range(50):
        if fake_app.calls:
            break
        threading.Event().wait(0.02)
    assert fake_app.calls == [("terminate:", None, False)]


def test_without_an_event_loop_the_process_exits_instead(monkeypatch: pytest.MonkeyPatch) -> None:
    exited: list[int] = []

    def no_app():
        raise RuntimeError("no NSApp")

    monkeypatch.setattr(albis_launcher, "AppKit", type("AK", (), {"NSApp": staticmethod(no_app)}))
    monkeypatch.setattr(albis_launcher, "_launcher_log", lambda *_a, **_k: None)
    monkeypatch.setattr(albis_launcher.os, "_exit", exited.append)
    server = threading.Thread(target=lambda: None)
    server.start()
    albis_launcher._quit_app_when_server_stops(server, 0.0)
    for _ in range(50):
        if exited:
            break
        threading.Event().wait(0.02)
    assert exited == [0]
