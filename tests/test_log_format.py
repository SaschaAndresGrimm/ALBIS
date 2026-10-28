from __future__ import annotations

import logging
import logging.config


def test_every_line_has_one_layout_with_the_message_in_one_column(capsys) -> None:
    from backend.log_format import LOG_FORMAT

    formatter = logging.Formatter(LOG_FORMAT, datefmt="%H:%M")
    lines = [
        formatter.format(logging.LogRecord("albis", level, __file__, 1, "message", None, None))
        for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR)
    ]
    starts = {line.index("albis:") for line in lines}
    assert len(starts) == 1, lines


def test_uvicorn_writes_in_that_layout_without_its_access_log(capsys) -> None:
    from backend.log_format import uvicorn_log_config

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


def test_a_poll_that_keeps_failing_is_logged_once_and_its_recovery_once(caplog) -> None:
    from types import SimpleNamespace

    from backend import app as app_module

    request = SimpleNamespace(
        method="GET", url=SimpleNamespace(path="/api/detector/status", query="url=x")
    )
    app_module._poll_failures.clear()
    with caplog.at_level(logging.DEBUG, logger="albis"):
        for _ in range(5):
            app_module._log_poll(request, 502, 3.0)
        app_module._log_poll(request, 200, 3.0)
        app_module._log_poll(request, 200, 3.0)
    by_level = [(record.levelname, record.getMessage()) for record in caplog.records]
    assert [level for level, _ in by_level] == ["ERROR", "DEBUG", "DEBUG", "DEBUG", "DEBUG", "INFO"]
    assert "[url=x]" in by_level[0][1] and "repeats are DEBUG" in by_level[0][1]
    assert "answers again, after 5 failed poll(s)" in by_level[-1][1]


def test_startup_says_which_albis_on_what(caplog) -> None:
    import platform

    from fastapi.testclient import TestClient

    from backend import app as app_module
    from backend.version import ALBIS_VERSION

    app_module._startup_banner_logged = False
    with caplog.at_level(logging.INFO, logger="albis"), TestClient(app_module.app):
        pass
    lines = [record.getMessage() for record in caplog.records]
    banner = next(line for line in lines if line.startswith(f"ALBIS {ALBIS_VERSION}"))
    assert f"Python {platform.python_version()}" in banner and platform.machine() in banner


def test_the_launcher_writes_in_the_same_layout(tmp_path, capsys, monkeypatch) -> None:
    import time

    import albis_launcher as launcher

    logger = logging.getLogger("albis.launcher")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
    monkeypatch.setattr(launcher, "_LAUNCHER_LOGGER", None)
    monkeypatch.setattr(launcher, "_LAUNCHER_LOG_PATH", None)
    launcher._configure_launcher_logger(tmp_path / "launcher.log")
    launcher._set_launcher_console_level("INFO")
    start = time.perf_counter()
    launcher._launcher_log(start, "socket ready", logging.DEBUG)
    launcher._launcher_log(start, "using 127.0.0.1:8000")
    err = capsys.readouterr().err
    assert "socket ready" not in err  # a startup step: DEBUG, not on the console
    assert "INFO    albis.launcher: using 127.0.0.1:8000 (+" in err
    for handler in launcher._LAUNCHER_LOGGER.handlers:
        handler.flush()
    written = (tmp_path / "launcher.log").read_text(encoding="utf-8")
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    assert "DEBUG   albis.launcher: socket ready (+" in written
