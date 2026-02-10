from __future__ import annotations

import socket
import sys
import threading
import time
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


def main() -> None:
    _ensure_stdio_streams()
    app_config, _config_path = load_config()

    host = get_str(app_config, ("server", "host"), "127.0.0.1")
    port = get_int(app_config, ("launcher", "port"), 0)
    if port <= 0:
        port = _find_free_port()

    data_root = get_str(app_config, ("data", "root"), "").strip()
    if data_root:
        Path(data_root).expanduser().mkdir(parents=True, exist_ok=True)

    # Use a direct object reference so frozen builds do not rely on dynamic module import strings.
    from backend.app import app as asgi_app

    uvicorn_config = uvicorn.Config(asgi_app, host=host, port=port, log_level="info")
    server = uvicorn.Server(uvicorn_config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    startup_timeout = max(0.5, get_float(app_config, ("launcher", "startup_timeout_sec"), 5.0))
    _wait_for_server(host, port, timeout=startup_timeout)
    if get_bool(app_config, ("launcher", "open_browser"), True):
        target_host = host if host not in {"0.0.0.0", "::"} else "127.0.0.1"
        webbrowser.open(f"http://{target_host}:{port}")

    try:
        while thread.is_alive():
            thread.join(0.5)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
