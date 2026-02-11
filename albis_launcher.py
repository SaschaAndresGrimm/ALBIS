from __future__ import annotations

import socket
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path

import uvicorn

from backend.config import get_bool, get_float, get_int, get_str, load_config


class _NullStream:
    """Fallback stdio stream for frozen/windowed builds without a console."""

    def write(self, _data: str) -> int:
        return 0

    def flush(self) -> None:
        return None

    def isatty(self) -> bool:
        return False


def _ensure_stdio_streams() -> None:
    # PyInstaller windowed executables on Windows may start with stdout/stderr set to None.
    # Uvicorn/logging formatters call `.isatty()` on those streams during startup.
    if sys.stdout is None:
        sys.stdout = _NullStream()
    if sys.stderr is None:
        sys.stderr = _NullStream()


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(sock.getsockname()[1])


def _wait_for_server(host: str, port: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    target_host = host if host not in {"0.0.0.0", "::"} else "127.0.0.1"
    while time.time() < deadline:
        try:
            with socket.create_connection((target_host, port), timeout=0.4):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def _wait_for_health(host: str, port: int, timeout: float = 5.0) -> bool:
    deadline = time.time() + timeout
    target_host = host if host not in {"0.0.0.0", "::"} else "127.0.0.1"
    health_url = f"http://{target_host}:{port}/api/health"
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=0.6) as res:
                if 200 <= int(getattr(res, "status", 0) or 0) < 300:
                    return True
        except (OSError, urllib.error.URLError):
            time.sleep(0.1)
    return False


def _launcher_log(start: float, message: str) -> None:
    elapsed_ms = (time.perf_counter() - start) * 1000
    text = f"[ALBIS launcher +{elapsed_ms:8.1f}ms] {message}\n"
    target = sys.stderr if sys.stderr is not None else sys.stdout
    if target is None:
        return
    try:
        target.write(text)
        target.flush()
    except Exception:
        pass


def main() -> None:
    _ensure_stdio_streams()
    start_ts = time.perf_counter()
    _launcher_log(start_ts, "starting")
    app_config, _config_path = load_config()
    _launcher_log(start_ts, f"config loaded ({_config_path})")

    host = get_str(app_config, ("server", "host"), "127.0.0.1")
    port = get_int(app_config, ("launcher", "port"), 0)
    if port <= 0:
        port = _find_free_port()
    _launcher_log(start_ts, f"using {host}:{port}")

    data_root = get_str(app_config, ("data", "root"), "").strip()
    if data_root:
        Path(data_root).expanduser().mkdir(parents=True, exist_ok=True)

    # Use a direct object reference so frozen builds do not rely on dynamic module import strings.
    _launcher_log(start_ts, "importing backend app")
    from backend.app import app as asgi_app
    _launcher_log(start_ts, "backend app imported")

    uvicorn_config = uvicorn.Config(asgi_app, host=host, port=port, log_level="info")
    server = uvicorn.Server(uvicorn_config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    _launcher_log(start_ts, "server thread started")

    startup_timeout = max(0.5, get_float(app_config, ("launcher", "startup_timeout_sec"), 5.0))
    if _wait_for_server(host, port, timeout=startup_timeout):
        _launcher_log(start_ts, "socket ready")
    else:
        _launcher_log(start_ts, "socket wait timed out")
    if _wait_for_health(host, port, timeout=startup_timeout):
        _launcher_log(start_ts, "health endpoint ready")
    else:
        _launcher_log(start_ts, "health check timed out")
    if get_bool(app_config, ("launcher", "open_browser"), True):
        target_host = host if host not in {"0.0.0.0", "::"} else "127.0.0.1"
        _launcher_log(start_ts, "opening browser")
        webbrowser.open(f"http://{target_host}:{port}")

    try:
        while thread.is_alive():
            thread.join(0.5)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
