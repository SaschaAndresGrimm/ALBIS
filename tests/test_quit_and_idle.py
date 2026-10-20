"""File -> Quit ALBIS, and stopping when no tab has been open for a while."""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from backend import app as app_module
from backend.app import app, runtime_state
from backend.lifecycle import IdleWatch, shutdown_controller

LOG = logging.getLogger("test")


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


def test_idle_stops_only_after_the_limit_and_never_while_work_runs() -> None:
    clock = Clock()
    watch = IdleWatch(clock=clock)
    stopped: list[str] = []
    stop = lambda: stopped.append("stop")  # noqa: E731
    clock.now = 14 * 60
    assert not watch.check(15, lambda: [], stop, LOG)
    clock.now = 15 * 60
    assert not watch.check(15, lambda: ["series_sum"], stop, LOG)
    assert watch.check(15, lambda: [], stop, LOG)
    assert stopped == ["stop"]


def test_a_tab_in_touch_resets_it_and_zero_means_never() -> None:
    clock = Clock()
    watch = IdleWatch(clock=clock)
    clock.now = 20 * 60
    watch.touch()
    clock.now = 30 * 60
    assert not watch.check(15, lambda: [], lambda: None, LOG)
    clock.now = 999 * 60
    assert not watch.check(0, lambda: [], lambda: None, LOG)


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch):
    stopped: list[bool] = []
    monkeypatch.setattr(runtime_state, "bind_host", "127.0.0.1")
    monkeypatch.setattr(shutdown_controller, "_hook", lambda: stopped.append(True))
    monkeypatch.setattr(app_module.series_summing, "has_running_job", lambda: False)
    monkeypatch.setattr(app_module.data_export, "has_running_job", lambda: False)
    # Answered at once: the test does not wait for the deferred stop.
    monkeypatch.setattr(shutdown_controller, "request", lambda logger, reason: True)
    yield TestClient(app, base_url="http://127.0.0.1"), stopped


def test_quit_is_offered_and_works_for_a_launcher_server_on_this_computer(client) -> None:
    api, _ = client
    assert api.get("/api/health").json()["can_quit"] is True
    assert api.post("/api/quit", json={}).json() == {"status": "stopping"}


def test_quit_asks_first_while_a_job_writes_its_files(client, monkeypatch) -> None:
    api, _ = client
    monkeypatch.setattr(app_module.data_export, "has_running_job", lambda: True)
    refused = api.post("/api/quit", json={})
    assert refused.status_code == 409
    assert refused.json()["detail"] == {"code": "busy", "busy": ["data_export"]}
    assert api.post("/api/quit", json={"force": True}).status_code == 200


def test_quit_is_not_offered_for_a_shared_server_or_without_the_launcher(
    client, monkeypatch
) -> None:
    api, _ = client
    monkeypatch.setattr(runtime_state, "bind_host", "0.0.0.0")
    assert api.get("/api/health").json()["can_quit"] is False
    assert api.post("/api/quit", json={}).status_code == 403
    monkeypatch.setattr(runtime_state, "bind_host", "127.0.0.1")
    monkeypatch.setattr(shutdown_controller, "_hook", None)
    assert api.post("/api/quit", json={}).status_code == 403


def test_quit_refuses_another_site(client) -> None:
    api, _ = client
    answer = api.post("/api/quit", json={}, headers={"Origin": "https://evil.example"})
    assert answer.status_code == 403
