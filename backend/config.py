"""Config loading helpers shared by backend and launcher.

The loader merges user config values onto `DEFAULT_CONFIG` and keeps strict,
predictable path resolution for both source and frozen (packaged) execution.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any

CONFIG_FILE_NAME = "albis.config.json"

DEFAULT_CONFIG: dict[str, Any] = {
    "server": {
        "host": "127.0.0.1",
        "port": 0,
        "reload": False,
    },
    "launcher": {
        "startup_timeout_sec": 5.0,
        "open_browser": True,
        "debug_macos_events": False,
    },
    "data": {
        "root": "",
        "allow_abs_paths": True,
        "scan_cache_sec": 2.0,
        "max_scan_depth": -1,
        "max_upload_mb": 0,
    },
    "logging": {
        "level": "INFO",
        "dir": "",
    },
    "ui": {
        "tool_hints": False,
        "pixel_label_min_cell_px": 18,
        "pixel_label_max_labels": 4000,
        "pixel_label_format": "auto",
        "pixel_label_show_during_drag": False,
        "language": "en",
    },
}

_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
_PIXEL_LABEL_FORMATS = {"auto", "integer", "scientific"}
_UI_LANGUAGES = {"en", "zh-CN", "ja", "fr", "es", "it", "pt", "rm", "de", "sv", "da", "mi", "gsw"}
_ALLOWED_CONFIG_KEYS: dict[str, set[str]] = {
    section: set(values.keys()) for section, values in DEFAULT_CONFIG.items()
}
_CONFIG_VALUE_TYPES: dict[tuple[str, str], tuple[type, ...]] = {
    ("server", "host"): (str,),
    ("server", "port"): (int, float, str),
    ("server", "reload"): (bool, int, float, str),
    ("launcher", "startup_timeout_sec"): (int, float, str),
    ("launcher", "open_browser"): (bool, int, float, str),
    ("launcher", "debug_macos_events"): (bool, int, float, str),
    ("data", "root"): (str,),
    ("data", "allow_abs_paths"): (bool, int, float, str),
    ("data", "scan_cache_sec"): (int, float, str),
    ("data", "max_scan_depth"): (int, float, str),
    ("data", "max_upload_mb"): (int, float, str),
    ("logging", "level"): (str,),
    ("logging", "dir"): (str,),
    ("ui", "tool_hints"): (bool, int, float, str),
    ("ui", "pixel_label_min_cell_px"): (int, float, str),
    ("ui", "pixel_label_max_labels"): (int, float, str),
    ("ui", "pixel_label_format"): (str,),
    ("ui", "pixel_label_show_during_drag"): (bool, int, float, str),
    ("ui", "language"): (str,),
}


def _normalize_ui_language(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "en"
    lower = raw.lower()
    if lower.startswith("zh"):
        return "zh-CN"
    if lower.startswith("ja"):
        return "ja"
    if lower.startswith("fr"):
        return "fr"
    if lower.startswith("es"):
        return "es"
    if lower.startswith("it"):
        return "it"
    if lower.startswith("pt"):
        return "pt"
    if lower.startswith("rm"):
        return "rm"
    if lower.startswith("de"):
        return "de"
    if lower.startswith("sv"):
        return "sv"
    if lower.startswith("da"):
        return "da"
    if lower.startswith("mi"):
        return "mi"
    if lower.startswith("gsw"):
        return "gsw"
    if lower.startswith("en"):
        return "en"
    return "en"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_config_path() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / CONFIG_FILE_NAME
    return _repo_root() / CONFIG_FILE_NAME


def _user_config_dir() -> Path:
    return Path.home() / ".config" / "albis"


def _user_config_path() -> Path:
    return _user_config_dir() / "config.json"


def _candidate_paths() -> list[Path]:
    candidates = [Path.cwd() / CONFIG_FILE_NAME]
    if getattr(sys, "frozen", False):
        candidates.append(Path(sys.executable).resolve().parent / CONFIG_FILE_NAME)
    candidates.append(_repo_root() / CONFIG_FILE_NAME)
    candidates.append(_user_config_path())

    deduped: list[Path] = []
    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if resolved in seen:
            continue
        deduped.append(resolved)
        seen.add(resolved)
    return deduped


def _deep_merge(target: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge(target[key], value)
        else:
            target[key] = value
    return target


def _validate_raw_config(raw: dict[str, Any]) -> None:
    unknown_sections = sorted(set(raw.keys()) - set(DEFAULT_CONFIG.keys()))
    if unknown_sections:
        joined = ", ".join(unknown_sections)
        raise ValueError(f"Unknown config section(s): {joined}")

    for section, section_value in raw.items():
        if section_value is None:
            continue
        if not isinstance(section_value, dict):
            raise ValueError(f"Config section '{section}' must be an object")

        unknown_keys = sorted(set(section_value.keys()) - _ALLOWED_CONFIG_KEYS[section])
        if unknown_keys:
            joined = ", ".join(unknown_keys)
            raise ValueError(f"Unknown key(s) in section '{section}': {joined}")

        for key, value in section_value.items():
            if value is None:
                continue
            expected_types = _CONFIG_VALUE_TYPES.get((section, key))
            if expected_types is None:
                continue
            if not isinstance(value, expected_types):
                expected_names = ", ".join(t.__name__ for t in expected_types)
                raise ValueError(
                    f"Invalid type for '{section}.{key}': expected {expected_names}, "
                    f"got {type(value).__name__}"
                )


def _apply_legacy_config_compat(raw: dict[str, Any]) -> dict[str, Any]:
    """Migrate known legacy keys before strict validation."""
    migrated = copy.deepcopy(raw)
    launcher = migrated.get("launcher")
    if isinstance(launcher, dict) and "port" in launcher:
        legacy_port = launcher.pop("port")
        server = migrated.get("server")
        if server is None:
            server = {}
            migrated["server"] = server
        if isinstance(server, dict) and "port" not in server:
            server["port"] = legacy_port
    return migrated


def _parse_config(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        raw = json.load(fh)
    if not isinstance(raw, dict):
        raise ValueError(f"Config root must be an object in {path}")
    return raw


def _write_default_config(path: Path) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            with path.open("w", encoding="utf-8") as fh:
                json.dump(DEFAULT_CONFIG, fh, indent=2)
                fh.write("\n")
        return True
    except OSError:
        return False


def normalize_config(raw: dict[str, Any] | None) -> dict[str, Any]:
    """Return a fully-typed config payload merged with defaults."""
    merged = copy.deepcopy(DEFAULT_CONFIG)
    if isinstance(raw, dict):
        compatible_raw = _apply_legacy_config_compat(raw)
        _validate_raw_config(compatible_raw)
        _deep_merge(merged, compatible_raw)

    server_host = get_str(merged, ("server", "host"), "127.0.0.1").strip() or "127.0.0.1"
    server_port = max(0, min(65535, get_int(merged, ("server", "port"), 0)))
    startup_timeout = max(0.1, get_float(merged, ("launcher", "startup_timeout_sec"), 5.0))
    scan_cache = max(0.0, get_float(merged, ("data", "scan_cache_sec"), 2.0))
    max_scan_depth = get_int(merged, ("data", "max_scan_depth"), -1)
    if max_scan_depth < -1:
        max_scan_depth = -1
    max_upload_mb = max(0, get_int(merged, ("data", "max_upload_mb"), 0))
    log_level = get_str(merged, ("logging", "level"), "INFO").upper()
    if log_level not in _LOG_LEVELS:
        log_level = "INFO"
    pixel_label_min_cell_px = max(
        8, min(64, get_int(merged, ("ui", "pixel_label_min_cell_px"), 18))
    )
    pixel_label_max_labels = max(
        100, min(100000, get_int(merged, ("ui", "pixel_label_max_labels"), 4000))
    )
    pixel_label_format = (
        get_str(merged, ("ui", "pixel_label_format"), "auto").strip().lower() or "auto"
    )
    if pixel_label_format not in _PIXEL_LABEL_FORMATS:
        pixel_label_format = "auto"
    ui_language = _normalize_ui_language(get_str(merged, ("ui", "language"), "en"))
    if ui_language not in _UI_LANGUAGES:
        ui_language = "en"

    return {
        "server": {
            "host": server_host,
            "port": server_port,
            "reload": get_bool(merged, ("server", "reload"), False),
        },
        "launcher": {
            "startup_timeout_sec": startup_timeout,
            "open_browser": get_bool(merged, ("launcher", "open_browser"), True),
            "debug_macos_events": get_bool(merged, ("launcher", "debug_macos_events"), False),
        },
        "data": {
            "root": get_str(merged, ("data", "root"), ""),
            "allow_abs_paths": get_bool(merged, ("data", "allow_abs_paths"), True),
            "scan_cache_sec": scan_cache,
            "max_scan_depth": max_scan_depth,
            "max_upload_mb": max_upload_mb,
        },
        "logging": {
            "level": log_level,
            "dir": get_str(merged, ("logging", "dir"), ""),
        },
        "ui": {
            "tool_hints": get_bool(merged, ("ui", "tool_hints"), False),
            "pixel_label_min_cell_px": pixel_label_min_cell_px,
            "pixel_label_max_labels": pixel_label_max_labels,
            "pixel_label_format": pixel_label_format,
            "pixel_label_show_during_drag": get_bool(
                merged, ("ui", "pixel_label_show_during_drag"), False
            ),
            "language": ui_language,
        },
    }


def save_config(config: dict[str, Any], path: Path) -> None:
    """Persist a normalized config to disk."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(normalize_config(config), fh, indent=2)
        fh.write("\n")


def load_config() -> tuple[dict[str, Any], Path]:
    config = normalize_config(None)
    config_path: Path | None = None
    for path in _candidate_paths():
        if path.exists():
            config_path = path
            break
    if config_path is None:
        if getattr(sys, "frozen", False):
            user_path = _user_config_path()
            if _write_default_config(user_path):
                return config, user_path
        _write_default_config(_default_config_path())
        return config, _default_config_path()
    if getattr(sys, "frozen", False):
        user_path = _user_config_path()
        exe_dir = Path(sys.executable).resolve().parent
        if user_path.exists():
            config_path = user_path
        else:
            try:
                config_path.resolve().relative_to(exe_dir)
            except ValueError:
                pass
            else:
                try:
                    raw = _parse_config(config_path)
                except Exception:
                    raw = config
                try:
                    save_config(raw, user_path)
                    config_path = user_path
                except OSError:
                    pass
    return normalize_config(_parse_config(config_path)), config_path


def resolve_data_dir(config: dict[str, Any], config_path: Path) -> Path:
    """Resolve runtime data directory for source and frozen execution."""
    data_root = get_str(config, ("data", "root"), "").strip()
    if data_root:
        return resolve_path(data_root, base_dir=config_path.parent)
    if getattr(sys, "frozen", False):
        return (Path.home() / "ALBIS-data").resolve()
    return _repo_root()


def resolve_log_dir(config: dict[str, Any], config_path: Path) -> Path:
    """Resolve runtime log directory for backend and launcher diagnostics."""
    log_dir_cfg = get_str(config, ("logging", "dir"), "").strip()
    if log_dir_cfg:
        return resolve_path(log_dir_cfg, base_dir=config_path.parent)
    if getattr(sys, "frozen", False):
        return (_user_config_dir() / "logs").resolve()
    return (resolve_data_dir(config, config_path) / "logs").resolve()


def get_nested(config: dict[str, Any], keys: tuple[str, ...], default: Any) -> Any:
    value: Any = config
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return default
        value = value[key]
    return value


def get_bool(config: dict[str, Any], keys: tuple[str, ...], default: bool) -> bool:
    value = get_nested(config, keys, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return default


def get_int(config: dict[str, Any], keys: tuple[str, ...], default: int) -> int:
    value = get_nested(config, keys, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def get_float(config: dict[str, Any], keys: tuple[str, ...], default: float) -> float:
    value = get_nested(config, keys, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_str(config: dict[str, Any], keys: tuple[str, ...], default: str) -> str:
    value = get_nested(config, keys, default)
    if value is None:
        return default
    return str(value)


def resolve_path(path_str: str, *, base_dir: Path) -> Path:
    path = Path(path_str).expanduser()
    path = (base_dir / path).resolve() if not path.is_absolute() else path.resolve()
    return path
