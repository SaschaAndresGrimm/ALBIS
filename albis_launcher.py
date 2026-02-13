from __future__ import annotations

import socket
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
import json

import uvicorn

try:
    import AppKit
    import Foundation
except Exception:  # pragma: no cover - optional UI helper
    AppKit = None
    Foundation = None

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

def _open_browser(host: str, port: int) -> None:
    target_host = _normalize_host(host)
    webbrowser.open(f"http://{target_host}:{port}")

if Foundation is not None:
    class _DockMenuHandler(Foundation.NSObject):
        def initWithConfig_(self, config: dict | None):
            self = Foundation.NSObject.init(self)
            if self is None:
                return None
            config = config or {}
            self.host = str(config.get("host") or "127.0.0.1")
            try:
                self.port = int(config.get("port") or 0)
            except (TypeError, ValueError):
                self.port = 0
            self.log_dir = str(config.get("log_dir") or "")
            self.status_item = None
            self.status_bar_item = None
            return self

        def _current_host_port(self):
            last = _load_last_server()
            if last:
                return last
            return self.host, self.port

        def _current_url(self) -> str | None:
            host, port = self._current_host_port()
            if not host or port <= 0:
                return None
            return f"http://{_normalize_host(host)}:{port}"

        def openBrowser_(self, _sender):
            url = self._current_url()
            if url:
                webbrowser.open(url)

        def copyURL_(self, _sender):
            url = self._current_url()
            if not url or AppKit is None:
                return
            pasteboard = AppKit.NSPasteboard.generalPasteboard()
            pasteboard.clearContents()
            pasteboard.setString_forType_(url, AppKit.NSPasteboardTypeString)

        def openLogs_(self, _sender):
            try:
                import subprocess

                log_path = Path(self.log_dir or "logs").expanduser().resolve()
                subprocess.run(["open", str(log_path)], check=False)
            except Exception:
                return

        def quit_(self, _sender):
            if AppKit is None:
                return
            AppKit.NSApp().terminate_(None)

        def _update_status_bar(self):
            if AppKit is None or self.status_bar_item is None:
                return
            host, port = self._current_host_port()
            status = "Online" if _server_running(host, port) else "Offline"
            tooltip = f"ALBIS Server: {status} ({_normalize_host(host)}:{port})"
            button = self.status_bar_item.button()
            if button is not None:
                button.setTitle_("ALBIS")
                button.setToolTip_(tooltip)

        def menuWillOpen_(self, _menu):
            if self.status_item is None:
                return
            host, port = self._current_host_port()
            status = "Online" if _server_running(host, port) else "Offline"
            label = f"Server: {status} ({_normalize_host(host)}:{port})"
            self.status_item.setTitle_(label)
            self._update_status_bar()


else:
    _DockMenuHandler = None

def _port_available(host: str, port: int) -> bool:
    if port <= 0:
        return False
    bind_host = host if host not in {"::"} else "::"
    try:
        with socket.socket(socket.AF_INET6 if ":" in bind_host else socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((bind_host, port))
        return True
    except OSError:
        return False

def _start_macos_menus(host: str, port: int, app_config: dict) -> bool:
    if AppKit is None or Foundation is None or sys.platform != "darwin" or _DockMenuHandler is None:
        return False
    try:
        log_dir = get_str(app_config, ("logging", "dir"), "")
        handler = _DockMenuHandler.alloc().initWithConfig_({"host": host, "port": port, "log_dir": log_dir})
        if handler is None:
            return False
        menu = AppKit.NSMenu.alloc().init()
        status_item = AppKit.NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(
            "Server: …", None, ""
        )
        status_item.setEnabled_(False)
        menu.addItem_(status_item)
        menu.addItem_(AppKit.NSMenuItem.separatorItem())

        item_open = AppKit.NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(
            "Open Browser", "openBrowser:", ""
        )
        item_open.setTarget_(handler)
        menu.addItem_(item_open)

        item_copy = AppKit.NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(
            "Copy URL", "copyURL:", ""
        )
        item_copy.setTarget_(handler)
        menu.addItem_(item_copy)

        item_logs = AppKit.NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(
            "Open Logs", "openLogs:", ""
        )
        item_logs.setTarget_(handler)
        menu.addItem_(item_logs)

        menu.addItem_(AppKit.NSMenuItem.separatorItem())

        item_quit = AppKit.NSMenuItem.alloc().initWithTitle_action_keyEquivalent_(
            "Quit", "quit:", ""
        )
        item_quit.setTarget_(handler)
        menu.addItem_(item_quit)

        handler.status_item = status_item
        menu.setDelegate_(handler)

        status_bar = AppKit.NSStatusBar.systemStatusBar()
        status_bar_item = status_bar.statusItemWithLength_(AppKit.NSVariableStatusItemLength)
        status_bar_item.setMenu_(menu)
        handler.status_bar_item = status_bar_item
        handler._update_status_bar()

        app = AppKit.NSApplication.sharedApplication()
        app.setDelegate_(handler)
        app.setDockMenu_(menu)
        return True
    except Exception:
        return False


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

    # If a server is already running, just open the browser and exit.
    if port > 0 and _server_running(host, port):
        _launcher_log(start_ts, f"existing server detected on {host}:{port}")
        _update_server_status(host, port, "running", health=True, source="existing")
        _open_browser(host, port)
        return
    if port > 0 and _wait_for_server(host, port, timeout=0.6):
        _launcher_log(start_ts, f"existing server detected (socket) on {host}:{port}")
        _update_server_status(host, port, "running", health=False, source="existing-socket")
        _open_browser(host, port)
        return
    if port <= 0:
        last = _load_last_server()
        if last:
            last_host, last_port = last
            if _server_running(last_host, last_port):
                _launcher_log(start_ts, f"existing server detected on {last_host}:{last_port}")
                _update_server_status(last_host, last_port, "running", health=True, source="existing")
                _open_browser(last_host, last_port)
                return
            if _wait_for_server(last_host, last_port, timeout=0.6):
                _launcher_log(start_ts, f"existing server detected (socket) on {last_host}:{last_port}")
                _update_server_status(last_host, last_port, "running", health=False, source="existing-socket")
                _open_browser(last_host, last_port)
                return
        port = _find_free_port()
    else:
        if not _port_available(host, port):
            _launcher_log(start_ts, f"port {port} unavailable, choosing free port")
            port = _find_free_port()
    _launcher_log(start_ts, f"using {host}:{port}")

    data_root = get_str(app_config, ("data", "root"), "").strip()
    if data_root:
        root_path = Path(data_root).expanduser()
        if not root_path.is_absolute():
            root_path = (_config_path.parent / root_path).resolve()
        try:
            root_path.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            _launcher_log(start_ts, f"data root unavailable: {root_path} ({exc})")
            fallback_root = Path.home() / ".config" / "albis" / "data"
            if fallback_root != root_path:
                try:
                    fallback_root.mkdir(parents=True, exist_ok=True)
                    _launcher_log(start_ts, f"data root fallback: {fallback_root}")
                except OSError:
                    pass

    # Use a direct object reference so frozen builds do not rely on dynamic module import strings.
    _launcher_log(start_ts, "importing backend app")
    from backend.app import app as asgi_app
    _launcher_log(start_ts, "backend app imported")

    uvicorn_config = uvicorn.Config(asgi_app, host=host, port=port, log_level="info")
    server = uvicorn.Server(uvicorn_config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    _launcher_log(start_ts, "server thread started")
    _update_server_status(host, port, "starting")

    startup_timeout = max(0.5, get_float(app_config, ("launcher", "startup_timeout_sec"), 5.0))
    if _wait_for_server(host, port, timeout=startup_timeout):
        _launcher_log(start_ts, "socket ready")
    else:
        _launcher_log(start_ts, "socket wait timed out")
    if _wait_for_health(host, port, timeout=startup_timeout):
        _launcher_log(start_ts, "health endpoint ready")
        _update_server_status(host, port, "running", health=True)
    else:
        _launcher_log(start_ts, "health check timed out")
        _update_server_status(host, port, "starting", health=False)
    if get_bool(app_config, ("launcher", "open_browser"), True):
        _launcher_log(start_ts, "opening browser")
        _open_browser(host, port)

    if _start_macos_menus(host, port, app_config) and AppKit is not None:
        _launcher_log(start_ts, "dock menu ready")
        AppKit.NSApp().run()
        return

    try:
        while thread.is_alive():
            thread.join(0.5)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
