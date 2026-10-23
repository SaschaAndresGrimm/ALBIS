"""The SIMPLON monitor before it has an image: "no frame", not an error."""

from __future__ import annotations

from fastapi.testclient import TestClient

from backend.app import app
from tests.fake_simplon import FakeDCUServer


def test_a_monitor_without_an_image_answers_no_content() -> None:
    # The simulated detector answers 204 until a series is taken, as a real
    # monitor does; urllib reads that as success with an empty body.
    with FakeDCUServer(init_delay=0.05) as dcu:
        dcu.dcu.state = "idle"
        client = TestClient(app, base_url="http://127.0.0.1")
        response = client.get("/api/simplon/monitor", params={"url": dcu.url, "timeout": 0})
    assert response.status_code == 204
