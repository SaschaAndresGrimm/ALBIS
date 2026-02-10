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
        "port": 8000,
        "reload": False,
    },
    "launcher": {
        "port": 0,
        "startup_timeout_sec": 5.0,
        "open_browser": True,
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
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _default_config_path() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / CONFIG_FILE_NAME
    return _repo_root() / CONFIG_FILE_NAME


def _user_config_path() -> Path:
    return Path.home() / ".config" / "albis" / "config.json"


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


def load_config() -> tuple[dict[str, Any], Path]:
    config = copy.deepcopy(DEFAULT_CONFIG)
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
    _deep_merge(config, _parse_config(config_path))
    return config, config_path


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
    if isinstance(value, (int, float)):
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
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    else:
        path = path.resolve()
    return path
