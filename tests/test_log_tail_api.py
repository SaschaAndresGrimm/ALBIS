from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

import backend.app as backend_app


def _configure_log_path(monkeypatch, tmp_path: Path, content: bytes | str = b"") -> Path:
    log_path = tmp_path / "albis.log"
    if isinstance(content, str):
        log_path.write_text(content, encoding="utf-8")
    else:
        log_path.write_bytes(content)
    monkeypatch.setattr(backend_app, "LOG_PATH", log_path)
    return log_path


def test_log_tail_returns_recent_lines_and_metadata(monkeypatch, tmp_path: Path) -> None:
    lines = [f"line {idx:03d} " + ("x" * 240) for idx in range(1, 121)]
    log_path = _configure_log_path(monkeypatch, tmp_path, "\n".join(lines) + "\n")

    client = TestClient(backend_app.app)
    response = client.get("/api/log-tail", params={"lines": 60})

    assert response.status_code == 200
    payload = response.json()
    assert payload["path"] == str(log_path)
    assert payload["requested_lines"] == 60
    assert payload["returned_lines"] == 60
    assert payload["truncated"] is True
    assert payload["size_bytes"] == log_path.stat().st_size
    assert isinstance(payload["modified_at"], float)
    assert payload["text"] == "".join(
        f"line {idx:03d} " + ("x" * 240) + "\n" for idx in range(61, 121)
    )


def test_log_tail_empty_file_returns_empty_payload(monkeypatch, tmp_path: Path) -> None:
    _configure_log_path(monkeypatch, tmp_path)

    client = TestClient(backend_app.app)
    response = client.get("/api/log-tail")

    assert response.status_code == 200
    payload = response.json()
    assert payload["text"] == ""
    assert payload["returned_lines"] == 0
    assert payload["truncated"] is False
    assert payload["size_bytes"] == 0


def test_log_tail_clamps_requested_line_count(monkeypatch, tmp_path: Path) -> None:
    lines = [f"row {idx:04d}" for idx in range(1, 2102)]
    _configure_log_path(monkeypatch, tmp_path, "\n".join(lines) + "\n")

    client = TestClient(backend_app.app)

    low_response = client.get("/api/log-tail", params={"lines": 10})
    assert low_response.status_code == 200
    low_payload = low_response.json()
    assert low_payload["requested_lines"] == 50
    assert low_payload["returned_lines"] == 50
    assert low_payload["text"].startswith("row 2052\n")

    high_response = client.get("/api/log-tail", params={"lines": 5000})
    assert high_response.status_code == 200
    high_payload = high_response.json()
    assert high_payload["requested_lines"] == 2000
    assert high_payload["returned_lines"] == 2000
    assert high_payload["text"].startswith("row 0102\n")
    assert high_payload["text"].endswith("row 2101\n")


def test_log_tail_does_not_use_full_file_convenience_reads(monkeypatch, tmp_path: Path) -> None:
    _configure_log_path(
        monkeypatch, tmp_path, "\n".join(f"entry {idx}" for idx in range(256)) + "\n"
    )

    def _raise(*_args, **_kwargs):
        raise AssertionError("full-file convenience read should not be used")

    monkeypatch.setattr(Path, "read_bytes", _raise)
    monkeypatch.setattr(Path, "read_text", _raise)

    client = TestClient(backend_app.app)
    response = client.get("/api/log-tail", params={"lines": 80})

    assert response.status_code == 200
    payload = response.json()
    assert payload["requested_lines"] == 80
    assert payload["returned_lines"] == 80
    assert payload["text"].startswith("entry 176\n")
