"""Where the launcher's server listens, and whether it is up.

The launcher's socket and server-discovery helpers: binding the server socket
(reusing the previous start's port when `server.port` is 0), waiting for the
server and its health endpoint to answer, and the `server.json` file that
remembers the last host and port between starts. Kept out of
`albis_launcher.py` so the entry point stays the start-up sequence.
"""

from __future__ import annotations

import json
import socket
import time
import urllib.error
import urllib.request
from pathlib import Path


def _server_info_path() -> Path:
    return Path.home() / ".config" / "albis" / "server.json"


def _load_last_server() -> tuple[str, int] | None:
    path = _server_info_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    host = str(payload.get("host") or "").strip()
    try:
        port = int(payload.get("port") or 0)
    except (TypeError, ValueError):
        port = 0
    if not host or port <= 0:
        return None
    return host, port


def _update_server_status(host: str, port: int, status: str, **extra: object) -> None:
    if not host or port <= 0:
        return
    path = _server_info_path()
    payload = {
        "host": host,
        "port": int(port),
        "status": status,
        "ts": time.time(),
    }
    payload.update(extra)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")
    except OSError:
        return


def _normalize_host(host: str) -> str:
    return host if host not in {"0.0.0.0", "::"} else "127.0.0.1"


def _server_running(host: str, port: int) -> bool:
    if not host or port <= 0:
        return False
    return _wait_for_health(host, port, timeout=0.8)


def _bind_socket(host: str, port: int, *, exclusive: bool = False) -> socket.socket:
    """Bind the server socket: the one place ALBIS binds it.

    It listens on every interface only when `server.host` is set to 0.0.0.0 or
    :: -- an operator's choice for LAN access, guarded by `server.allowed_hosts`
    and the Host check; the default is 127.0.0.1. `exclusive` asks Windows for
    SO_EXCLUSIVEADDRUSE, so the bind cannot take over a port another program
    listens on; elsewhere, and otherwise, SO_REUSEADDR lets a restart reuse a
    port whose old connections are still in TIME_WAIT.
    """
    family = socket.AF_INET6 if (":" in host and host != "0.0.0.0") else socket.AF_INET
    bind_host = "" if host in {"0.0.0.0", "::"} else host
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        option = getattr(socket, "SO_EXCLUSIVEADDRUSE", None) if exclusive else None
        if option is not None:
            sock.setsockopt(socket.SOL_SOCKET, option, 1)
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((bind_host, port))
    except OSError:
        sock.close()
        raise
    return sock


def _create_bound_socket(host: str, port: int = 0) -> socket.socket:
    """Bind a socket and return it live so the port cannot be claimed before uvicorn starts."""
    return _bind_socket(host, port)


def _port_answers(host: str, port: int) -> bool:
    """Whether something already accepts connections on this port."""
    target_host = host if host not in {"0.0.0.0", "::"} else "127.0.0.1"
    try:
        with socket.create_connection((target_host, port), timeout=0.3):
            return True
    except OSError:
        return False


def _bind_last_port(host: str, port: int) -> socket.socket | None:
    """Bind the port of the previous start again, or None if it is not free.

    With `server.port` 0 a new free port was picked on every start, and the
    browser keeps `localStorage` per origin -- scheme, host and port -- so Open
    Recent, the geometry override, the language and the panel layout all came
    back empty after a restart. Reusing the last port keeps the origin.

    A remembered port is treated more carefully than a random one: whatever
    holds it now is not ours. Anything that answers on it is left alone, and on
    Windows the bind is exclusive, since SO_REUSEADDR there would let this
    socket take over a port another program is listening on. Elsewhere
    SO_REUSEADDR stays, so the previous run's connections in TIME_WAIT do not
    block an immediate restart.
    """
    if port <= 0 or port > 65535 or _port_answers(host, port):
        return None
    try:
        return _bind_socket(host, port, exclusive=True)
    except OSError:
        return None


def _bind_auto_port(host: str, last_port: int) -> tuple[socket.socket, bool]:
    """The socket for `server.port` 0: the last start's port if free, else any.

    Returns the bound socket and whether it is the last port.
    """
    sock = _bind_last_port(host, last_port) if last_port > 0 else None
    if sock is not None:
        return sock, True
    return _create_bound_socket(host, 0), False


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
