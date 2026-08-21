from __future__ import annotations

import argparse
import json
import logging
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
from collections.abc import Callable
from contextlib import suppress
from ctypes import wintypes
from logging.handlers import RotatingFileHandler
from pathlib import Path

import uvicorn

from backend.config import (
    CONFIG_PATH_ENV_VAR,
    env_override_keys,
    env_var_name,
    get_bool,
    get_float,
    get_int,
    get_str,
    load_config,
    resolve_data_dir,
    resolve_log_dir,
)
from backend.version import ALBIS_VERSION

try:
    import AppKit
    import Foundation
except Exception:  # pragma: no cover - optional UI helper
    AppKit = None
    Foundation = None

_MACOS_RUNTIME: dict[str, object] = {}
_MACOS_EVENT_LOGS_ENABLED = False
_LAUNCHER_LOG_MAX_BYTES = 1024 * 1024  # 1 MiB
_LAUNCHER_LOG_BACKUP_COUNT = 1
_LAUNCHER_LOGGER: logging.Logger | None = None
_LAUNCHER_LOG_PATH: Path | None = None
_WINDOWS_APP_MUTEX_NAME = "ALBISAppMutex"
_WINDOWS_SHUTDOWN_EVENT_NAME = "ALBISShutdownEvent"
_WINDOWS_WAIT_OBJECT_0 = 0
_WINDOWS_INFINITE = 0xFFFFFFFF
_WINDOWS_RUNTIME_HANDLES: list[int] = []

if sys.platform == "win32":
    import ctypes

    _WINDOWS_KERNEL32 = ctypes.WinDLL("kernel32", use_last_error=True)
    _WINDOWS_KERNEL32.CreateEventW.argtypes = [
        wintypes.LPVOID,
        wintypes.BOOL,
        wintypes.BOOL,
        wintypes.LPCWSTR,
    ]
    _WINDOWS_KERNEL32.CreateEventW.restype = wintypes.HANDLE
    _WINDOWS_KERNEL32.CreateMutexW.argtypes = [wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR]
    _WINDOWS_KERNEL32.CreateMutexW.restype = wintypes.HANDLE
    _WINDOWS_KERNEL32.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
    _WINDOWS_KERNEL32.WaitForSingleObject.restype = wintypes.DWORD
else:
    _WINDOWS_KERNEL32 = None

class _NullStream:
    """Fallback stdio stream for frozen/windowed builds without a console."""

    def write(self, _data: str) -> int:
        return 0

    def flush(self) -> None:
        return None

    def isatty(self) -> bool:
        return False

# The flags worth having on a command line: where to listen, where the data is,
# and how loud to be. Each one sets the environment variable for the same config
# key rather than carrying a second override mechanism alongside it, so there is
# one precedence order to explain and one to test -- command line, then
# environment, then the file.
_CLI_TO_ENV: dict[str, tuple[str, str]] = {
    "host": ("server", "host"),
    "port": ("server", "port"),
    "allowed_hosts": ("server", "allowed_hosts"),
    "data_root": ("data", "root"),
    "log_level": ("logging", "level"),
    "language": ("ui", "language"),
}


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="albis",
        description="ALBIS - browser-based image viewer for detector data.",
        epilog=(
            "Every setting can also come from the environment (ALBIS_SERVER_HOST, "
            "ALBIS_DATA_ROOT, ...) or from albis.config.json. Precedence: these "
            "flags, then the environment, then the file."
        ),
    )
    parser.add_argument("--version", action="version", version=f"ALBIS {ALBIS_VERSION}")
    parser.add_argument("--config", metavar="PATH", help="configuration file to read")
    parser.add_argument("--host", metavar="ADDRESS", help="address to listen on")
    parser.add_argument("--port", metavar="PORT", help="port to listen on (0 picks a free one)")
    parser.add_argument(
        "--allowed-hosts",
        metavar="NAMES",
        help="comma-separated Host header names to answer to, beyond this machine",
    )
    parser.add_argument("--data-root", metavar="PATH", help="directory to browse for data")
    parser.add_argument(
        "--log-level",
        metavar="LEVEL",
        help="DEBUG, INFO, WARNING, ERROR or CRITICAL",
    )
    parser.add_argument("--language", metavar="CODE", help="interface language, e.g. de")
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="do not open a browser window on start",
    )
    return parser


def _apply_cli_arguments(argv: list[str] | None = None) -> tuple[list[str], list[str]]:
    """Turn command-line flags into environment overrides.

    Unknown arguments are returned rather than rejected. A double-clicked
    desktop build is started by the operating system, which passes things of its
    own -- macOS sends `-psn_0_...` and a file path when ALBIS is used to open a
    document -- and refusing to start over an argument nobody typed would be a
    worse failure than ignoring it.
    """
    parser = _build_arg_parser()
    args, unknown = parser.parse_known_args(sys.argv[1:] if argv is None else argv)

    applied: list[str] = []
    if args.config:
        os.environ[CONFIG_PATH_ENV_VAR] = str(Path(args.config).expanduser())
        applied.append("--config")
    for attribute, (section, key) in _CLI_TO_ENV.items():
        value = getattr(args, attribute, None)
        if value is None:
            continue
        os.environ[env_var_name(section, key)] = str(value)
        applied.append(f"--{attribute.replace('_', '-')}")
    if args.no_browser:
        os.environ[env_var_name("launcher", "open_browser")] = "false"
        applied.append("--no-browser")
    return applied, unknown


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
        with suppress(Exception):
            subprocess.run(["open", url], check=False)


if Foundation is not None:
    class _DockMenuHandler(Foundation.NSObject):
        def _start_ts(self) -> float:
            try:
                return float(self.start_ts)  # type: ignore[attr-defined]
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
                _log_macos_event(start_ts, f"{reason}: no host/port")
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
                _log_macos_event(start_ts, f"{reason}: throttled")
                return
            self.last_browser_open_mono = now
            _log_macos_event(start_ts, f"{reason}: opening browser for {_normalize_host(host)}:{port}")
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
            start_ts = self._start_ts()
            try:
                log_path = Path(str(getattr(self, "log_dir", "") or "")).expanduser().resolve()
                log_path.mkdir(parents=True, exist_ok=True)
                result = subprocess.run(["open", str(log_path)], check=False, capture_output=True, text=True)
                if int(result.returncode or 0) != 0:
                    detail = (result.stderr or "").strip() or (result.stdout or "").strip()
                    _log_macos_event(start_ts, f"open logs failed rc={result.returncode} ({detail})")
            except Exception as exc:
                _log_macos_event(start_ts, f"open logs error: {type(exc).__name__}: {exc}")
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

        def _refresh_server_status_item(self):
            status_item = getattr(self, "status_item", None)
            if status_item is None:
                return
            host, port = self._current_host_port()
            status = "Online" if _server_running(host, port) else "Offline"
            label = f"Server: {status} ({_normalize_host(host)}:{port})"
            status_item.setTitle_(label)
            self._update_status_bar()

        def menuWillOpen_(self, _menu):
            self._refresh_server_status_item()

        def applicationDockMenu_(self, _sender):
            _log_macos_event(self._start_ts(), "dock menu requested")
            self._refresh_server_status_item()
            return getattr(self, "dock_menu", None)

        # Handle app re-open from Dock icon (e.g. user clicks the app while it is already running).
        # Opening the viewer URL here avoids the Dock bounce-without-action behavior in windowless apps.
        def applicationShouldHandleReopen_hasVisibleWindows_(self, _app, _has_visible_windows):
            _log_macos_event(self._start_ts(), "reopen requested")
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
                _log_macos_event(start_ts, "became active (startup grace)")
                return
            _log_macos_event(start_ts, "became active")
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

def _start_macos_menus(
    host: str,
    port: int,
    app_config: dict,
    config_path: Path | None = None,
    start_ts: float | None = None,
) -> bool:
    if sys.platform != "darwin":
        return False
    if AppKit is None or Foundation is None or _DockMenuHandler is None:
        if start_ts is not None:
            _launcher_log(start_ts, "macos ui unavailable (missing AppKit/Foundation)")
        return False
    try:
        if config_path is not None:
            log_dir = resolve_log_dir(app_config, config_path)
        else:
            raw_log_dir = get_str(app_config, ("logging", "dir"), "").strip()
            log_dir = (
                Path(raw_log_dir).expanduser().resolve()
                if raw_log_dir
                else (Path.home() / ".config" / "albis" / "logs").resolve()
            )
        handler = _DockMenuHandler.alloc().init()
        if handler is None:
            return False
        handler.host = str(host or "127.0.0.1")
        try:
            handler.port = int(port or 0)
        except (TypeError, ValueError):
            handler.port = 0
        handler.log_dir = str(log_dir)
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
        handler._refresh_server_status_item()

        _MACOS_RUNTIME["menu"] = menu
        _MACOS_RUNTIME["status_bar_item"] = status_bar_item
    except Exception as exc:
        if start_ts is not None:
            _launcher_log(start_ts, f"dock/menu setup failed: {type(exc).__name__}: {exc}")
            for line in traceback.format_exc().strip().splitlines():
                _launcher_log(start_ts, f"dock/menu traceback: {line}")
    return True


def _create_bound_socket(host: str, port: int = 0) -> socket.socket:
    """Bind a socket and return it live so the port cannot be claimed before uvicorn starts."""
    family = socket.AF_INET6 if (":" in host and host != "0.0.0.0") else socket.AF_INET
    bind_host = "" if host in {"0.0.0.0", "::"} else host
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((bind_host, port))
    return sock

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

def _default_launcher_log_path() -> Path:
    return (Path.home() / ".config" / "albis" / "logs" / "launcher.log").resolve()


def _configure_launcher_logger(log_path: Path) -> None:
    global _LAUNCHER_LOGGER, _LAUNCHER_LOG_PATH
    logger = _LAUNCHER_LOGGER
    if logger is None:
        logger = logging.getLogger("albis.launcher")
        logger.setLevel(logging.INFO)
        logger.propagate = False
        _LAUNCHER_LOGGER = logger

    if not any(getattr(handler, "_albis_launcher_stream", False) for handler in logger.handlers):
        stream = logging.StreamHandler(sys.stderr if sys.stderr is not None else sys.stdout)
        stream.setFormatter(logging.Formatter("%(message)s"))
        stream._albis_launcher_stream = True  # type: ignore[attr-defined]
        logger.addHandler(stream)

    resolved_log_path = log_path.expanduser().resolve()
    if resolved_log_path == _LAUNCHER_LOG_PATH:
        return

    for handler in list(logger.handlers):
        if getattr(handler, "_albis_launcher_file", False):
            logger.removeHandler(handler)
            with suppress(Exception):
                handler.close()

    try:
        resolved_log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            resolved_log_path,
            maxBytes=_LAUNCHER_LOG_MAX_BYTES,
            backupCount=_LAUNCHER_LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        file_handler._albis_launcher_file = True  # type: ignore[attr-defined]
        logger.addHandler(file_handler)
        _LAUNCHER_LOG_PATH = resolved_log_path
    except OSError:
        _LAUNCHER_LOG_PATH = None

def _launcher_log(start: float, message: str) -> None:
    elapsed_ms = (time.perf_counter() - start) * 1000
    text = f"[ALBIS launcher +{elapsed_ms:8.1f}ms] {message}"
    try:
        if _LAUNCHER_LOGGER is None:
            _configure_launcher_logger(_default_launcher_log_path())
        if _LAUNCHER_LOGGER is not None:
            _LAUNCHER_LOGGER.info(text)
            return
    except Exception:
        pass
    target = sys.stderr if sys.stderr is not None else sys.stdout
    if target is not None:
        try:
            target.write(text + "\n")
            target.flush()
        except Exception:
            pass

def _log_macos_event(start: float, message: str) -> None:
    if _MACOS_EVENT_LOGS_ENABLED:
        _launcher_log(start, f"macos event: {message}")


def _install_windows_shutdown_listener(start: float, request_shutdown: Callable[[], None]) -> None:
    if _WINDOWS_KERNEL32 is None:
        return

    mutex_handle = _WINDOWS_KERNEL32.CreateMutexW(None, False, _WINDOWS_APP_MUTEX_NAME)
    if not mutex_handle:
        _launcher_log(start, "failed to create Windows app mutex")
        return
    _WINDOWS_RUNTIME_HANDLES.append(int(mutex_handle))

    event_handle = _WINDOWS_KERNEL32.CreateEventW(None, False, False, _WINDOWS_SHUTDOWN_EVENT_NAME)
    if not event_handle:
        _launcher_log(start, "failed to create Windows shutdown event")
        return
    _WINDOWS_RUNTIME_HANDLES.append(int(event_handle))

    def _wait_for_shutdown_event() -> None:
        wait_result = _WINDOWS_KERNEL32.WaitForSingleObject(event_handle, _WINDOWS_INFINITE)
        if wait_result != _WINDOWS_WAIT_OBJECT_0:
            _launcher_log(start, f"Windows shutdown wait failed: {wait_result}")
            return
        _launcher_log(start, "Windows shutdown event received")
        try:
            request_shutdown()
        except Exception as exc:
            _launcher_log(start, f"Windows shutdown callback failed: {type(exc).__name__}: {exc}")

    listener = threading.Thread(
        target=_wait_for_shutdown_event,
        name="albis-windows-shutdown",
        daemon=True,
    )
    listener.start()

def main() -> None:
    global _MACOS_EVENT_LOGS_ENABLED
    _ensure_stdio_streams()
    start_ts = time.perf_counter()
    cli_applied, cli_ignored = _apply_cli_arguments()
    app_config, _config_path = load_config()
    _configure_launcher_logger(resolve_log_dir(app_config, _config_path) / "launcher.log")
    _launcher_log(start_ts, "starting")
    _launcher_log(start_ts, f"config loaded ({_config_path})")
    if cli_applied:
        _launcher_log(start_ts, f"command line set {', '.join(cli_applied)}")
    if cli_ignored:
        _launcher_log(start_ts, f"ignored unrecognized arguments: {' '.join(cli_ignored)}")
    overrides = env_override_keys()
    if overrides:
        _launcher_log(start_ts, f"environment set {', '.join(overrides)}")
    _MACOS_EVENT_LOGS_ENABLED = get_bool(app_config, ("launcher", "debug_macos_events"), False)
    if _MACOS_EVENT_LOGS_ENABLED:
        _launcher_log(start_ts, "macos event debug logging enabled")

    host = get_str(app_config, ("server", "host"), "127.0.0.1")
    # Single-port model: launcher and backend share server.port.
    # Keep fallback to legacy launcher.port for backward compatibility.
    port = get_int(app_config, ("server", "port"), 0)
    if port <= 0:
        port = get_int(app_config, ("launcher", "port"), 0)

    # If a server is already running, just open the browser and exit.
    if port > 0 and _server_running(host, port):
        _launcher_log(start_ts, f"existing server detected on {host}:{port}")
        _update_server_status(host, port, "running", health=True, source="existing")
        _open_browser(host, port)
        return
    bound_sock: socket.socket | None = None
    if port <= 0:
        last = _load_last_server()
        if last:
            last_host, last_port = last
            if _server_running(last_host, last_port):
                _launcher_log(start_ts, f"existing server detected on {last_host}:{last_port}")
                _update_server_status(last_host, last_port, "running", health=True, source="existing")
                _open_browser(last_host, last_port)
                return
        bound_sock = _create_bound_socket(host, 0)
        port = int(bound_sock.getsockname()[1])
    else:
        try:
            bound_sock = _create_bound_socket(host, port)
        except OSError:
            _launcher_log(start_ts, f"port {port} unavailable, choosing free port")
            bound_sock = _create_bound_socket(host, 0)
            port = int(bound_sock.getsockname()[1])
    _launcher_log(start_ts, f"using {host}:{port}")

    data_root = get_str(app_config, ("data", "root"), "").strip()
    if data_root:
        root_path = resolve_data_dir(app_config, _config_path)
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

    # Uvicorn runs its own loggers (uvicorn / uvicorn.access / uvicorn.error)
    # that ignore the "albis" logger level, so its access log stays at INFO
    # unless we mirror the configured level here.
    uvicorn_level = get_str(app_config, ("logging", "level"), "INFO").strip().lower()
    if uvicorn_level not in {"critical", "error", "warning", "info", "debug"}:
        uvicorn_level = "info"
    uvicorn_config = uvicorn.Config(asgi_app, host=host, port=port, log_level=uvicorn_level)
    server = uvicorn.Server(uvicorn_config)

    def _request_windows_shutdown() -> None:
        _update_server_status(host, port, "stopping", source="windows-installer")
        server.should_exit = True

    _install_windows_shutdown_listener(
        start_ts,
        _request_windows_shutdown,
    )
    _sockets = [bound_sock] if bound_sock is not None else []
    thread = threading.Thread(target=server.run, kwargs={"sockets": _sockets}, daemon=True)
    thread.start()
    _launcher_log(start_ts, "server thread started")
    _update_server_status(host, port, "starting")

    startup_timeout = max(0.5, get_float(app_config, ("launcher", "startup_timeout_sec"), 10.0))
    startup_health_timeout = max(0.5, get_float(app_config, ("launcher", "startup_health_timeout_sec"), 15.0))
    if _wait_for_server(host, port, timeout=startup_timeout):
        _launcher_log(start_ts, "socket ready")
    else:
        _launcher_log(start_ts, "socket wait timed out")
    if _wait_for_health(host, port, timeout=startup_health_timeout):
        _launcher_log(start_ts, "health endpoint ready")
        _update_server_status(host, port, "running", health=True)
    else:
        _launcher_log(start_ts, "health check timed out")
        _update_server_status(host, port, "starting", health=False)
    if get_bool(app_config, ("launcher", "open_browser"), True):
        _launcher_log(start_ts, "opening browser")
        _open_browser(host, port)

    if _should_start_macos_ui_loop() and _start_macos_menus(
        host,
        port,
        app_config,
        config_path=_config_path,
        start_ts=start_ts,
    ):
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
