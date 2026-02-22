from __future__ import annotations

import os
import socket
import subprocess
import sys
import threading
import time
import traceback
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

_MACOS_RUNTIME: dict[str, object] = {}

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
    url = f"http://{target_host}:{port}"
    opened = False
    try:
        opened = bool(webbrowser.open(url, new=1, autoraise=True))
    except Exception:
        opened = False
    if not opened and sys.platform == "darwin":
        try:
            subprocess.run(["open", url], check=False)
        except Exception:
            pass

if Foundation is not None:
    class _DockMenuHandler(Foundation.NSObject):
        def _start_ts(self) -> float:
            try:
                return float(getattr(self, "start_ts"))
            except (TypeError, ValueError, AttributeError):
                return time.perf_counter()

        def _current_host_port(self):
            host = str(getattr(self, "host", "127.0.0.1") or "127.0.0.1")
            try:
                port = int(getattr(self, "port", 0) or 0)
            except (TypeError, ValueError):
                port = 0
            last = _load_last_server()
            if last:
                return last
            return host, port

        def _current_url(self) -> str | None:
            host, port = self._current_host_port()
            if not host or port <= 0:
                return None
            return f"http://{_normalize_host(host)}:{port}"

        def _open_browser_throttled(self, reason: str) -> None:
            host, port = self._current_host_port()
            start_ts = self._start_ts()
            if not host or port <= 0:
                _launcher_log(start_ts, f"macos event: {reason}: no host/port")
                return
            try:
                throttle_sec = float(getattr(self, "browser_open_throttle_sec", 0.8))
            except (TypeError, ValueError):
                throttle_sec = 0.8
            try:
                last_open = float(getattr(self, "last_browser_open_mono", 0.0))
            except (TypeError, ValueError):
                last_open = 0.0
            now = time.monotonic()
            if now - last_open < throttle_sec:
                _launcher_log(start_ts, f"macos event: {reason}: throttled")
                return
            self.last_browser_open_mono = now
            _launcher_log(
                start_ts,
                f"macos event: {reason}: opening browser for {_normalize_host(host)}:{port}",
            )
            _open_browser(host, port)

        def openBrowser_(self, _sender):
            self._open_browser_throttled("menu")

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

                log_path = Path(getattr(self, "log_dir", "") or "logs").expanduser().resolve()
                subprocess.run(["open", str(log_path)], check=False)
            except Exception:
                return

        def quit_(self, _sender):
            if AppKit is None:
                return
            AppKit.NSApp().terminate_(None)

        def _update_status_bar(self):
            status_bar_item = getattr(self, "status_bar_item", None)
            if AppKit is None or status_bar_item is None:
                return
            host, port = self._current_host_port()
            status = "Online" if _server_running(host, port) else "Offline"
            tooltip = f"ALBIS Server: {status} ({_normalize_host(host)}:{port})"
            button = status_bar_item.button()
            if button is not None:
                button.setTitle_("ALBIS")
                button.setToolTip_(tooltip)

        def menuWillOpen_(self, _menu):
            status_item = getattr(self, "status_item", None)
            if status_item is None:
                return
            host, port = self._current_host_port()
            status = "Online" if _server_running(host, port) else "Offline"
            label = f"Server: {status} ({_normalize_host(host)}:{port})"
            status_item.setTitle_(label)
            self._update_status_bar()

        def applicationDockMenu_(self, _sender):
            _launcher_log(self._start_ts(), "macos event: dock menu requested")
            return getattr(self, "dock_menu", None)

        # Handle app re-open from Dock icon (e.g. user clicks the app while it is already running).
        # Opening the viewer URL here avoids the Dock bounce-without-action behavior in windowless apps.
        def applicationShouldHandleReopen_hasVisibleWindows_(self, _app, _has_visible_windows):
            _launcher_log(self._start_ts(), "macos event: reopen requested")
            self._open_browser_throttled("reopen")
            return True

        def applicationDidBecomeActive_(self, _notification):
            # Fallback for cases where Dock re-open is not delivered, but app activation is.
            start_ts = self._start_ts()
            try:
                grace_until = float(getattr(self, "activate_grace_until_mono", 0.0))
            except (TypeError, ValueError):
                grace_until = 0.0
            if time.monotonic() < grace_until:
                _launcher_log(start_ts, "macos event: became active (startup grace)")
                return
            _launcher_log(start_ts, "macos event: became active")
            self._open_browser_throttled("activate")


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

def _should_start_macos_ui_loop() -> bool:
    if sys.platform != "darwin":
        return False
    if not (AppKit is not None and Foundation is not None and _DockMenuHandler is not None):
        return False
    if getattr(sys, "frozen", False):
        return True
    # Source runs should remain Ctrl+C friendly unless explicitly requested.
    flag = os.environ.get("ALBIS_ENABLE_MACOS_UI", "").strip().lower()
    return flag in {"1", "true", "yes", "on"}

def _start_macos_menus(host: str, port: int, app_config: dict, start_ts: float | None = None) -> bool:
    if sys.platform != "darwin":
        return False
    if AppKit is None or Foundation is None or _DockMenuHandler is None:
        if start_ts is not None:
            _launcher_log(start_ts, "macos ui unavailable (missing AppKit/Foundation)")
        return False
    try:
        log_dir = get_str(app_config, ("logging", "dir"), "")
        handler = _DockMenuHandler.alloc().init()
        if handler is None:
            return False
        handler.host = str(host or "127.0.0.1")
        try:
            handler.port = int(port or 0)
        except (TypeError, ValueError):
            handler.port = 0
        handler.log_dir = log_dir
        handler.start_ts = float(start_ts) if start_ts is not None else time.perf_counter()
        handler.last_browser_open_mono = 0.0
        handler.browser_open_throttle_sec = 0.8
        handler.activate_grace_until_mono = time.monotonic() + 2.0
        handler.status_item = None
        handler.status_bar_item = None
        handler.dock_menu = None
        app = AppKit.NSApplication.sharedApplication()
        app.setDelegate_(handler)
        # NSApplication's delegate is not retained by Cocoa; keep a strong Python reference.
        _MACOS_RUNTIME["app"] = app
        _MACOS_RUNTIME["handler"] = handler
    except Exception as exc:
        if start_ts is not None:
            _launcher_log(start_ts, f"dock delegate setup failed: {type(exc).__name__}: {exc}")
            for line in traceback.format_exc().strip().splitlines():
                _launcher_log(start_ts, f"dock delegate traceback: {line}")
        return False

    try:
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
        handler.dock_menu = menu
        menu.setDelegate_(handler)

        status_bar = AppKit.NSStatusBar.systemStatusBar()
        status_bar_item = status_bar.statusItemWithLength_(AppKit.NSVariableStatusItemLength)
        status_bar_item.setMenu_(menu)
        handler.status_bar_item = status_bar_item
        handler._update_status_bar()

        _MACOS_RUNTIME["menu"] = menu
        _MACOS_RUNTIME["status_bar_item"] = status_bar_item
    except Exception as exc:
        if start_ts is not None:
            _launcher_log(start_ts, f"dock/menu setup failed: {type(exc).__name__}: {exc}")
            for line in traceback.format_exc().strip().splitlines():
                _launcher_log(start_ts, f"dock/menu traceback: {line}")
    return True


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
    if target is not None:
        try:
            target.write(text)
            target.flush()
        except Exception:
            pass
    # Persist launcher diagnostics in user config so windowed app runs can be debugged.
    try:
        log_path = Path.home() / ".config" / "albis" / "launcher.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(text)
    except Exception:
        pass

def main() -> None:
    _ensure_stdio_streams()
    start_ts = time.perf_counter()
    _launcher_log(start_ts, "starting")
    app_config, _config_path = load_config()
    _launcher_log(start_ts, f"config loaded ({_config_path})")

    host = get_str(app_config, ("server", "host"), "127.0.0.1")
    # Single-port model: launcher and backend share server.port.
    # Keep fallback to legacy launcher.port for backward compatibility.
    port = get_int(app_config, ("server", "port"), 8000)
    if port <= 0:
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

    if _should_start_macos_ui_loop() and _start_macos_menus(host, port, app_config, start_ts=start_ts):
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
