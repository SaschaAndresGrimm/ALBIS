"""Detector control (beta) against a simulated SIMPLON 1.8 detector control unit."""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from backend.app import app, runtime_state
from tests.fake_simplon import FakeDCUServer


@pytest.fixture()
def dcu() -> Iterator[FakeDCUServer]:
    with FakeDCUServer(init_delay=0.2, max_series_s=0.6) as server:
        yield server


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setattr(runtime_state, "detector_control", True)
    return TestClient(app, base_url="http://127.0.0.1")


def _command(client: TestClient, url: str, subsystem: str, command: str) -> Any:
    return client.post(
        "/api/detector/command", json={"url": url, "subsystem": subsystem, "command": command}
    )


def _wait_for_command(client: TestClient, url: str, timeout: float = 5.0) -> dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = client.get("/api/detector/status", params={"url": url}).json()["command"]
        if job and not job["running"]:
            return job
        time.sleep(0.05)
    raise AssertionError("the command did not finish")


def _initialize(client: TestClient, url: str) -> None:
    assert _command(client, url, "detector", "initialize").status_code == 200
    assert _wait_for_command(client, url)["ok"] is True


def _set(client: TestClient, url: str, subsystem: str, key: str, value: Any) -> Any:
    return client.put(
        "/api/detector/config",
        json={"url": url, "subsystem": subsystem, "key": key, "value": value},
    )


def test_every_endpoint_is_off_until_switched_on(
    monkeypatch: pytest.MonkeyPatch, dcu: FakeDCUServer
) -> None:
    monkeypatch.setattr(runtime_state, "detector_control", False)
    client = TestClient(app, base_url="http://127.0.0.1")
    for response in (
        client.get("/api/detector/describe", params={"url": dcu.url}),
        client.get("/api/detector/status", params={"url": dcu.url}),
        client.get("/api/detector/files", params={"url": dcu.url}),
        _set(client, dcu.url, "detector", "nimages", 5),
        _command(client, dcu.url, "detector", "initialize"),
    ):
        assert response.status_code == 404
        assert "switched off" in response.json()["detail"]
    # Nothing reached the detector.
    assert dcu.dcu.requests == []


def test_before_initialize_only_the_state_is_known(client: TestClient, dcu: FakeDCUServer) -> None:
    payload = client.get("/api/detector/describe", params={"url": dcu.url}).json()
    assert payload["state"] == "na"
    assert all(params == {} for params in payload["params"].values())


def test_initialize_runs_in_the_background_and_reports_its_result(
    client: TestClient, dcu: FakeDCUServer
) -> None:
    job = _command(client, dcu.url, "detector", "initialize").json()
    assert job["command"] == "initialize" and job["running"] is True
    # While it blocks on the detector, status still answers and shows it.
    status = client.get("/api/detector/status", params={"url": dcu.url}).json()
    assert status["command"]["command"] == "initialize"
    assert _wait_for_command(client, dcu.url)["ok"] is True
    assert (
        client.get("/api/detector/status", params={"url": dcu.url}).json()["detector"]["state"]
        == "idle"
    )


def test_parameters_are_described_by_the_detector(client: TestClient, dcu: FakeDCUServer) -> None:
    _initialize(client, dcu.url)
    params = client.get("/api/detector/describe", params={"url": dcu.url}).json()["params"]
    energy = params["detector"]["photon_energy"]
    assert energy["unit"] == "eV" and energy["min"] == 3500.0 and energy["max"] == 40000.0
    assert params["detector"]["trigger_mode"]["allowed_values"] == ["ints", "inte", "exts", "exte"]
    assert params["detector"]["description"]["access_mode"] == "r"
    assert params["filewriter"]["name_pattern"]["value"] == "series_$id"
    assert params["stream"]["header_detail"]["allowed_values"] == ["all", "basic", "none"]
    # Only what the detector has: this one has a single threshold.
    assert "threshold/1/energy" in params["detector"]
    assert "threshold/2/energy" not in params["detector"]


def test_a_write_returns_what_the_detector_changed_alongside(
    client: TestClient, dcu: FakeDCUServer
) -> None:
    _initialize(client, dcu.url)
    result = _set(client, dcu.url, "detector", "count_time", 0.05).json()
    assert "frame_time" in result["changed"]
    assert result["params"]["count_time"]["value"] == pytest.approx(0.05)
    assert result["params"]["frame_time"]["value"] >= 0.05


@pytest.mark.parametrize(
    ("subsystem", "key", "value", "needle"),
    [
        ("detector", "count_time", 99999.0, "at most"),
        ("detector", "nimages", 0, "at least"),
        ("detector", "nimages", 2.5, "uint"),
        ("detector", "trigger_mode", "sideways", "one of"),
        ("detector", "description", "mine", "read-only"),
        ("detector", "pixel_mask", 1, "not a settable parameter"),
        ("system", "datetime/time", "now", "not a settable parameter"),
    ],
)
def test_invalid_writes_are_refused_before_reaching_the_detector(
    client: TestClient, dcu: FakeDCUServer, subsystem: str, key: str, value: Any, needle: str
) -> None:
    _initialize(client, dcu.url)
    puts_before = sum(1 for method, _ in dcu.dcu.requests if method == "PUT")
    response = _set(client, dcu.url, subsystem, key, value)
    assert response.status_code == 400
    assert needle in response.json()["detail"]
    assert sum(1 for method, _ in dcu.dcu.requests if method == "PUT") == puts_before


def test_a_series_is_written_listed_and_downloadable(
    client: TestClient, dcu: FakeDCUServer
) -> None:
    _initialize(client, dcu.url)
    assert _set(client, dcu.url, "filewriter", "mode", "enabled").status_code == 200
    _command(client, dcu.url, "detector", "arm")
    assert _wait_for_command(client, dcu.url)["result"] == {"sequence id": 1}
    _command(client, dcu.url, "detector", "trigger")
    assert (
        client.get("/api/detector/status", params={"url": dcu.url}).json()["filewriter"]["mode"]
        == "enabled"
    )
    assert _wait_for_command(client, dcu.url)["ok"] is True

    files = client.get("/api/detector/files", params={"url": dcu.url}).json()["files"]
    names = [entry["name"] for entry in files]
    assert names == ["series_1_data_000001.h5", "series_1_master.h5"]
    assert all(entry["size"] and entry["size"] > 0 for entry in files)

    download = client.get(
        "/api/detector/files/download", params={"url": dcu.url, "name": "series_1_master.h5"}
    )
    assert download.status_code == 200
    assert download.content == dcu.dcu.files["series_1_master.h5"]
    assert 'filename="series_1_master.h5"' in download.headers["content-disposition"]


@pytest.mark.parametrize(
    "name", ["../etc/passwd", "/etc/passwd", "a//b.h5", "series 1.h5", "x?y=1"]
)
def test_downloads_stay_on_the_detectors_data_directory(
    client: TestClient, dcu: FakeDCUServer, name: str
) -> None:
    response = client.get("/api/detector/files/download", params={"url": dcu.url, "name": name})
    assert response.status_code == 400


def test_one_command_at_a_time_but_abort_always_gets_through(client: TestClient) -> None:
    with FakeDCUServer(init_delay=0.05, max_series_s=10.0) as slow:
        _initialize(client, slow.url)
        _set(client, slow.url, "detector", "nimages", 100000)
        _command(client, slow.url, "detector", "arm")
        _wait_for_command(client, slow.url)
        _command(client, slow.url, "detector", "trigger")
        time.sleep(0.1)
        busy = _command(client, slow.url, "detector", "arm")
        assert busy.status_code == 409 and "trigger" in busy.json()["detail"]
        abort = _command(client, slow.url, "detector", "abort")
        assert abort.status_code == 200 and abort.json()["ok"] is True
        # The blocked trigger returns once the detector has aborted.
        assert _wait_for_command(client, slow.url, timeout=3.0)["command"] == "trigger"


@pytest.mark.parametrize(
    ("subsystem", "command"),
    [
        ("system", "reboot"),
        ("filewriter", "initialize"),
        ("monitor", "initialize"),
        ("detector", "hv_reset"),
        ("detector", "retract_sensor"),
    ],
)
def test_only_whitelisted_commands_are_sent(
    client: TestClient, dcu: FakeDCUServer, subsystem: str, command: str
) -> None:
    response = _command(client, dcu.url, subsystem, command)
    assert response.status_code == 400
    assert not any(method == "PUT" for method, _ in dcu.dcu.requests)


@pytest.mark.parametrize("was_on", [True, False])
def test_resetting_the_stream_keeps_it_as_it_was(
    client: TestClient, dcu: FakeDCUServer, was_on: bool
) -> None:
    _initialize(client, dcu.url)
    if was_on:
        _set(client, dcu.url, "stream", "mode", "enabled")
    dcu.dcu.stream_dropped = 3
    assert _command(client, dcu.url, "stream", "initialize").status_code == 200
    job = _wait_for_command(client, dcu.url)
    assert job["ok"] is True
    assert job["result"] == {"mode": "enabled" if was_on else "disabled"}
    stream = client.get("/api/detector/status", params={"url": dcu.url}).json()["stream"]
    assert stream["dropped"] == 0
    assert stream["mode"] == ("enabled" if was_on else "disabled")


def test_critical_status_values_are_flagged(client: TestClient, dcu: FakeDCUServer) -> None:
    _initialize(client, dcu.url)
    dcu.dcu.critical.add(("filewriter", "buffer_free"))
    status = client.get("/api/detector/status", params={"url": dcu.url}).json()
    assert status["filewriter"]["critical"] == ["buffer_free"]
    assert "critical" not in status["stream"]


def test_a_cross_site_page_cannot_drive_the_detector(
    client: TestClient, dcu: FakeDCUServer
) -> None:
    response = client.post(
        "/api/detector/command",
        json={"url": dcu.url, "subsystem": "detector", "command": "initialize"},
        headers={"Origin": "https://evil.example", "Sec-Fetch-Site": "cross-site"},
    )
    assert response.status_code == 403
    assert dcu.dcu.requests == []


def test_an_unreachable_detector_is_diagnosed(client: TestClient) -> None:
    response = client.get("/api/detector/status", params={"url": "127.0.0.1:9"})
    assert response.status_code == 502
    assert response.json()["detail"]["code"] == "refused"


def test_a_four_threshold_detector_describes_every_threshold(client: TestClient) -> None:
    """A PILATUS4 has four thresholds; each, with its mode, reaches the panel."""
    with FakeDCUServer(init_delay=0.05, thresholds=4) as pilatus4:
        _initialize(client, pilatus4.url)
        params = client.get("/api/detector/describe", params={"url": pilatus4.url}).json()[
            "params"
        ]["detector"]
        for n in range(1, 5):
            assert f"threshold/{n}/energy" in params
            assert f"threshold/{n}/mode" in params
        assert "threshold/difference/mode" in params


def test_an_eiger1_on_simplon_1_6_is_driven_in_its_own_version(client: TestClient) -> None:
    """An EIGER1 serves SIMPLON 1.6.0 only and refuses 1.8.0 ("Incompatible version").

    The panel asks for 1.8.0; describe reads the detector's own version and
    returns it, and everything after uses it. 1.6 names its sensors per board
    and module and gives the free buffer in KB.
    """
    with FakeDCUServer(init_delay=0.05, api_version="1.6.0") as eiger1:
        url = eiger1.url
        described = client.get("/api/detector/describe", params={"url": url}).json()
        assert described["api_version"] == "1.6.0"
        assert described["state"] == "na"
        job = _command_v(client, url, "1.6.0", "detector", "initialize")
        assert job.status_code == 200
        deadline = time.time() + 5
        while time.time() < deadline:
            current = client.get("/api/detector/status", params={"url": url, "version": "1.6.0"})
            if current.json()["command"] and not current.json()["command"]["running"]:
                break
            time.sleep(0.05)
        status = current.json()
        assert status["command"]["ok"] is True
        assert status["detector"]["temperature"] == pytest.approx(27.2)
        assert status["detector"]["humidity"] == pytest.approx(2.1)
        assert status["detector"]["high_voltage"] == pytest.approx(197.7)
        # KB on the wire, bytes to the panel.
        assert status["filewriter"]["buffer_free"] == pytest.approx(800e9, rel=1e-3)
        params = client.get(
            "/api/detector/describe", params={"url": url, "version": "1.6.0"}
        ).json()["params"]
        assert params["detector"]["photon_energy"]["unit"] == "eV"
        # Nothing was ever sent in a version the detector does not serve.
        assert not any(
            "/api/1.8.0/" in path and method == "PUT" for method, path in eiger1.dcu.requests
        )


def _command_v(client: TestClient, url: str, version: str, subsystem: str, command: str) -> Any:
    return client.post(
        "/api/detector/command",
        json={"url": url, "version": version, "subsystem": subsystem, "command": command},
    )
