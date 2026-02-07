from __future__ import annotations

import os
import socket
import threading
import time
import webbrowser
from pathlib import Path

import uvicorn


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
    host = os.environ.get("ALBIS_HOST", "127.0.0.1")
    port = int(os.environ.get("ALBIS_PORT", "0") or 0)
    if port <= 0:
        port = _find_free_port()

    data_root = os.environ.get("ALBIS_DATA_DIR")
    if not data_root:
        data_root = str(Path.home() / "ALBIS-data")
    os.environ.setdefault("VIEWER_DATA_DIR", data_root)
    Path(data_root).expanduser().mkdir(parents=True, exist_ok=True)

    config = uvicorn.Config(
        "backend.app:app",
        host=host,
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    _wait_for_server(host, port)
    target_host = host if host not in {"0.0.0.0", "::"} else "127.0.0.1"
    webbrowser.open(f"http://{target_host}:{port}")

    try:
        while thread.is_alive():
            thread.join(0.5)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
