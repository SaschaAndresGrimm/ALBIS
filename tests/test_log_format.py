from __future__ import annotations

import logging
import logging.config


def test_every_line_has_one_layout_with_the_message_in_one_column(capsys) -> None:
    from backend.app import LOG_FORMAT

    formatter = logging.Formatter(LOG_FORMAT, datefmt="%H:%M")
    lines = [
        formatter.format(logging.LogRecord("albis", level, __file__, 1, "message", None, None))
        for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR)
    ]
    starts = {line.index("albis:") for line in lines}
    assert len(starts) == 1, lines


def test_uvicorn_writes_in_that_layout_without_its_access_log(capsys) -> None:
    from backend.app import uvicorn_log_config

    config = uvicorn_log_config("debug")
    assert config["loggers"]["uvicorn"]["level"] == "DEBUG"
    assert config["loggers"]["uvicorn.access"]["handlers"] == []
    try:
        logging.config.dictConfig(config)
        logging.getLogger("uvicorn.error").info("Started server process")
        logging.getLogger("uvicorn.access").info('127.0.0.1 - "GET / HTTP/1.1" 200')
        err = capsys.readouterr().err
    finally:
        for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
            logger = logging.getLogger(name)
            logger.handlers.clear()
            logger.propagate = True
    # Not "uvicorn.error": its startup is not an error.
    assert "INFO    uvicorn: Started server process" in err
    assert "GET / HTTP/1.1" not in err


def test_routine_polls_stay_out_of_the_log_but_failures_do_not(caplog) -> None:
    from fastapi.testclient import TestClient

    from backend.app import app

    client = TestClient(app)
    with caplog.at_level(logging.DEBUG, logger="albis"):
        client.get("/api/health")
        client.get("/api/handoff/v1/jobs/latest", params={"after_id": 0})
        client.get("/api/export/data/status", params={"job_id": "none"})  # 404
        client.get("/api/files")
    logged = [record.getMessage() for record in caplog.records if " -> " in record.getMessage()]
    assert not any("/api/health" in line or "/jobs/latest" in line for line in logged), logged
    assert any("/api/export/data/status -> 404" in line for line in logged), logged
    assert any(line.startswith("GET /api/files -> ") for line in logged), logged
