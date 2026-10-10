"""Detector control over the SIMPLON API -- beta, off unless switched on.

Reads, writes and commands for the detector, monitor, file writer and stream
subsystems of a DECTRIS detector control unit (DCU), plus listing and
downloading the files its file writer wrote. Built only on what the SIMPLON 1.8
API reference documents (v3.11, 2026-03-12): the DCU has undocumented keys too,
and DECTRIS discourages using them. Older DCUs work too: the detector's own API
version is read on connect and used (an EIGER1 serves 1.6.0 and refuses 1.8.0),
and the few places where 1.6 differs -- sensor names, the free buffer in KB, one
request at a time -- are handled where they occur.

The interface is generated from what the detector says about each parameter --
value, type, unit, limits, allowed values, access -- so a PILATUS4, an EIGER2
and an electron-microscopy detector each get their own correct form without a
line of per-model code here.

Two SIMPLON commands block for a long time: `initialize` (up to two minutes,
per the reference) and `trigger` in an internal trigger mode, which returns only
when the series is done. Commands therefore run in a background thread, one at a
time per detector, and the caller follows them through `status()`. `abort`,
`cancel` and `disarm` bypass that queue: they exist to interrupt it.
"""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from fastapi import HTTPException

from .simplon import _raise_simplon_failure, _simplon_api_base, normalize_simplon_base_url

SUBSYSTEMS = ("detector", "monitor", "filewriter", "stream")

# Documented configuration keys with scalar values, and their documented type.
# Array-valued keys (pixel_mask, flatfield, the goniometer axes, the detector
# orientation matrix) are left out: they are megabytes of data or geometry
# vectors, not something to edit in a form. `threshold/n/...` is expanded for
# n = 1..4; a detector answers only for the thresholds it has.
_DETECTOR_KEYS: dict[str, str] = {
    "auto_sum_strict": "bool",
    "auto_summation": "bool",
    "beam_center_x": "float",
    "beam_center_y": "float",
    "binning_mode": "string",
    "bit_depth_image": "uint",
    "bit_depth_readout": "uint",
    "chi_increment": "float",
    "chi_start": "float",
    "compression": "string",
    "count_time": "float",
    "counting_mode": "string",
    "countrate_correction_applied": "bool",
    "countrate_correction_count_cutoff": "uint",
    "data_collection_date": "string",
    "description": "string",
    "detector_distance": "float",
    "detector_number": "string",
    "detector_orientation_angle": "float",
    "detector_readout_time": "float",
    "eiger_fw_version": "string",
    "element": "string",
    "extg_mode": "string",
    "flatfield_correction_applied": "bool",
    "flux_type": "string",
    "flux_value": "float",
    "frame_count_time": "float",
    "frame_time": "float",
    "incident_energy": "float",
    "instrument_name": "string",
    "kappa_increment": "float",
    "kappa_start": "float",
    "mask_to_zero": "bool",
    "nexpi": "uint",
    "nimages": "uint",
    "ntrigger": "uint",
    "ntriggers_skipped": "uint",
    "number_of_excluded_pixels": "uint",
    "omega_increment": "float",
    "omega_start": "float",
    "phi_increment": "float",
    "phi_start": "float",
    "photon_energy": "float",
    "pixel_format": "string",
    "pixel_mask_applied": "bool",
    "roi_bit_depth": "uint",
    "roi_mode": "string",
    "roi_y_size": "uint",
    "sample_name": "string",
    "sensor_material": "string",
    "sensor_movement_mode": "string",
    "sensor_thickness": "float",
    "software_version": "string",
    "source_name": "string",
    "test_image_mode": "string",
    "test_image_value": "uint",
    "threshold_energy": "float",
    "threshold/difference/mode": "string",
    "threshold/difference/lower_threshold": "uint",
    "threshold/difference/upper_threshold": "uint",
    "trigger_mode": "string",
    "trigger_start_delay": "float",
    "two_theta_increment": "float",
    "two_theta_start": "float",
    "virtual_pixel_correction_applied": "bool",
    "wavelength": "float",
    "x_pixel_size": "float",
    "x_pixels_in_detector": "uint",
    "y_pixel_size": "float",
    "y_pixels_in_detector": "uint",
}
_THRESHOLD_KEYS: dict[str, str] = {
    "energy": "float",
    "mode": "string",
    "number_of_excluded_pixels": "uint",
}
for _n in range(1, 5):
    for _suffix, _type in _THRESHOLD_KEYS.items():
        _DETECTOR_KEYS[f"threshold/{_n}/{_suffix}"] = _type

CONFIG_KEYS: dict[str, dict[str, str]] = {
    "detector": _DETECTOR_KEYS,
    "monitor": {"buffer_size": "uint", "discard_new": "bool", "mode": "string"},
    "filewriter": {
        "compression_enabled": "bool",
        "format": "string",
        "image_nr_start": "uint",
        "mode": "string",
        "name_pattern": "string",
        "nimages_per_file": "uint",
    },
    "stream": {
        "format": "string",
        "header_appendix": "string",
        "header_detail": "string",
        "image_appendix": "string",
        "mode": "string",
    },
}

STATUS_KEYS: dict[str, tuple[str, ...]] = {
    "detector": ("state", "temperature", "humidity", "high_voltage/state"),
    "monitor": ("state", "dropped", "buffer_fill_level"),
    "filewriter": ("state", "buffer_free", "error"),
    "stream": ("state", "dropped"),
}

# Commands the panel may send. `initialize` of the monitor and file writer
# resets an interface another program may be using: the panel offers it under
# each output's More settings, with a tooltip that says so. The stream's is
# a recovery step -- it clears dropped images and errors -- but it also
# switches the stream off, so it runs through `_reset_stream`, which puts the
# stream back on when it was on.
COMMANDS: dict[str, tuple[str, ...]] = {
    "detector": ("initialize", "arm", "trigger", "disarm", "cancel", "abort"),
    "filewriter": ("clear", "initialize"),
    "monitor": ("clear", "initialize"),
    "stream": ("initialize",),
}
# These interrupt a running command rather than wait behind it.
INTERRUPTS = frozenset({("detector", "abort"), ("detector", "cancel"), ("detector", "disarm")})
# Per the reference: initialize takes up to two minutes, arm "a few seconds",
# and trigger returns when an internally triggered series has finished, which
# can be hours. Abort ends a trigger early, so a long bound is safe.
_COMMAND_TIMEOUT_S = {"initialize": 240.0, "arm": 120.0, "trigger": 24 * 3600.0}
_DEFAULT_COMMAND_TIMEOUT_S = 30.0
_READ_TIMEOUT_S = 4.0
_DESCRIBE_WORKERS = 12

# Files the file writer wrote, as the DCU names them under /data/. A name
# pattern may create subdirectories, so `/` is allowed between segments; `..`,
# empty segments and anything outside this alphabet are refused, which keeps a
# download on the DCU's data directory.
_FILE_SEGMENT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


# Before SIMPLON 1.8 (an EIGER1 runs 1.6.0) the detector reports its
# environment per board and module, under other names; the panel shows them in
# the same places. high_voltage is then a voltage, not a state.
_LEGACY_SENSOR_KEYS = {
    "temperature": "board_000/th0_temp",
    "humidity": "board_000/th0_humidity",
    "high_voltage": "module_000/hv",
}
# Sizes as SIMPLON states them; 1.6 gives the file writer's free buffer in KB.
_SIZE_UNITS = {"": 1, "b": 1, "bytes": 1, "kb": 1024, "mb": 1024**2, "gb": 1024**3}


def _version_tuple(version: str) -> tuple[int, ...]:
    try:
        return tuple(int(part) for part in version.split("."))
    except ValueError:
        return ()


def _is_legacy(version: str) -> bool:
    parsed = _version_tuple(version)
    return bool(parsed) and parsed < (1, 8)


def _base(url: str, version: str, subsystem: str) -> str:
    if subsystem not in SUBSYSTEMS:
        raise HTTPException(status_code=400, detail=f"Unknown SIMPLON subsystem: {subsystem}")
    return _simplon_api_base(url, version, subsystem)


def _open(req: urllib.request.Request | str, timeout: float) -> Any:
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    if not body:
        return None
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def _get(url: str, timeout: float = _READ_TIMEOUT_S) -> Any:
    return _open(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout)


def _put(url: str, payload: Any, timeout: float) -> Any:
    data = json.dumps(payload).encode("utf-8") if payload is not None else b""
    req = urllib.request.Request(
        url, data=data, method="PUT", headers={"Content-Type": "application/json"}
    )
    return _open(req, timeout)


def _descriptor(payload: Any) -> dict[str, Any] | None:
    """The parts of a SIMPLON parameter description the interface uses."""
    if not isinstance(payload, dict) or "value" not in payload:
        return None
    out: dict[str, Any] = {
        "value": payload.get("value"),
        "value_type": payload.get("value_type") or "",
        "unit": payload.get("unit") or "",
        # The reference: "When not available, the access_mode is rw."
        "access_mode": payload.get("access_mode") or "rw",
    }
    for key in ("min", "max"):
        if isinstance(payload.get(key), (int, float)) and not isinstance(payload.get(key), bool):
            out[key] = payload[key]
    allowed = payload.get("allowed_values")
    if isinstance(allowed, list) and allowed:
        out["allowed_values"] = allowed
    return out


def _try_get(url: str) -> Any:
    try:
        return _get(url)
    except Exception:
        return None


def _workers(version: str) -> int:
    """How many requests to have in flight at once.

    A SIMPLON 1.8 DCU answers them in parallel. An EIGER1's (1.6) handles one
    at a time: twelve at once queue until a third of them time out, while one
    at a time answers all in about 65 ms each.
    """
    return 1 if _is_legacy(version) else _DESCRIBE_WORKERS


def _get_all(urls: list[str], version: str) -> list[Any]:
    """GET each URL, parsed, or None where the detector has no such key.

    A timeout or a server error is asked again once, on its own: a busy DCU
    drops some of a burst of requests, and a missing setting would otherwise
    simply vanish from the panel.
    """

    def attempt(target: str) -> tuple[Any, bool]:
        try:
            return _get(target), False
        except urllib.error.HTTPError as exc:
            return None, exc.code >= 500
        except Exception:
            return None, True

    with ThreadPoolExecutor(max_workers=_workers(version)) as pool:
        first = list(pool.map(attempt, urls))
    return [
        attempt(target)[0] if failed else payload
        for target, (payload, failed) in zip(urls, first, strict=True)
    ]


def detector_state(url: str, version: str) -> str:
    """The detector's state, or raise a classified failure if it cannot be read."""
    base = _base(url, version, "detector")
    try:
        payload = _get(f"{base}/status/state")
    except Exception as exc:
        _raise_simplon_failure(exc, base, "Failed to read the detector state")
    value = payload.get("value") if isinstance(payload, dict) else None
    return str(value or "").strip().lower()


def api_version(url: str, requested: str) -> str:
    """The SIMPLON version the detector serves, or `requested` if it does not say.

    Each subsystem answers `/<subsystem>/api/version/` with its version, and a
    detector refuses any other one ("Incompatible version"): an EIGER1 serves
    1.6.0 and nothing else.
    """
    root = _base(url, requested, "detector").rsplit("/", 1)[0]
    value = _value_of(_try_get(f"{root}/version/"))
    text = str(value or "").strip()
    return text if text and _version_tuple(text) and len(text) <= 32 else requested


def describe(url: str, version: str) -> dict[str, Any]:
    """Every documented scalar parameter the detector answers for, with its limits.

    The detector's own API version is read first and used, and returned as
    `api_version` for the panel to use from then on. Before `initialize` only
    the state is readable ("Before initializing the detector only the detector
    state is available!"), so the parameters come back empty and `state` says
    why.
    """
    version = api_version(url, version)
    state = detector_state(url, version)
    result: dict[str, Any] = {
        "state": state,
        "api_version": version,
        "params": {name: {} for name in SUBSYSTEMS},
    }
    if state == "na":
        return result
    jobs = [
        (subsystem, key, f"{_base(url, version, subsystem)}/config/{key}")
        for subsystem, keys in CONFIG_KEYS.items()
        for key in keys
    ]
    answers = _get_all([job[2] for job in jobs], version)
    for (subsystem, key, _), payload in zip(jobs, answers, strict=True):
        descriptor = _descriptor(payload)
        if descriptor is not None:
            result["params"][subsystem][key] = descriptor
    return result


def _value_of(payload: Any) -> Any:
    return payload.get("value") if isinstance(payload, dict) else None


def status(url: str, version: str) -> dict[str, Any]:
    """What the detector and its data interfaces are doing, in one round.

    Also reads each interface's `mode`, since another program (a beamline's
    control system) can switch it, and the panel must show what is true now.
    """
    jobs: list[tuple[str, str, str]] = []
    legacy = _is_legacy(version)
    for subsystem, keys in STATUS_KEYS.items():
        base = _base(url, version, subsystem)
        for key in keys:
            if legacy and subsystem == "detector" and key != "state":
                continue
            jobs.append((subsystem, key, f"{base}/status/{key}"))
    if legacy:
        base = _base(url, version, "detector")
        jobs += [
            ("detector", name, f"{base}/status/{key}") for name, key in _LEGACY_SENSOR_KEYS.items()
        ]
    for subsystem in ("monitor", "filewriter", "stream"):
        jobs.append((subsystem, "mode", f"{_base(url, version, subsystem)}/config/mode"))
    answers = _get_all([job[2] for job in jobs], version)
    out: dict[str, dict[str, Any]] = {name: {} for name in SUBSYSTEMS}
    for (subsystem, key, _), payload in zip(jobs, answers, strict=True):
        out[subsystem][key] = _value_of(payload)
        if key == "buffer_free" and isinstance(out[subsystem][key], (int, float)):
            unit = (
                str(payload.get("unit") or "").strip().lower() if isinstance(payload, dict) else ""
            )
            out[subsystem][key] = out[subsystem][key] * _SIZE_UNITS.get(unit, 1)
        # SIMPLON marks a status value it considers an error condition with
        # "state": "critical" (a nearly full disk, a sensor out of range).
        if isinstance(payload, dict) and str(payload.get("state") or "").lower() == "critical":
            out[subsystem].setdefault("critical", []).append(key)
    if out["detector"].get("state") is None:
        # Nothing answered at all: report it rather than an empty dashboard.
        detector_state(url, version)
    return out


def _coerce(value: Any, value_type: str) -> Any:
    """Check a value against a parameter's documented type; SIMPLON insists on a match."""
    is_number = isinstance(value, (int, float)) and not isinstance(value, bool)
    if value_type == "bool" and isinstance(value, bool):
        return value
    if value_type in ("uint", "int") and is_number and isinstance(value, int):
        if value_type == "uint" and value < 0:
            raise HTTPException(status_code=400, detail="This value cannot be negative.")
        return value
    if value_type == "float" and is_number:
        return float(value)
    if value_type == "string" and isinstance(value, str):
        return value
    raise HTTPException(status_code=400, detail=f"Expected a {value_type} value.")


def set_config(url: str, version: str, subsystem: str, key: str, value: Any) -> dict[str, Any]:
    """Write one parameter; return the parameters the detector says it changed.

    The value is checked twice: against the documented type here, and against
    the detector's own current limits and allowed values, read just before the
    write. SIMPLON answers a write with "all parameters implicitly and
    explicitly changed, or that could have been changed"; those are read back so
    the interface shows the detector's real values, limits included.
    """
    keys = CONFIG_KEYS.get(subsystem)
    if keys is None or key not in keys:
        raise HTTPException(
            status_code=400, detail=f"{subsystem}/{key} is not a settable parameter."
        )
    base = _base(url, version, subsystem)
    try:
        current = _descriptor(_get(f"{base}/config/{key}"))
    except Exception as exc:
        _raise_simplon_failure(exc, base, f"Failed to read {key}")
    if current is None:
        raise HTTPException(status_code=404, detail=f"The detector has no parameter {key}.")
    if current["access_mode"] != "rw" and "w" not in current["access_mode"]:
        raise HTTPException(status_code=400, detail=f"{key} is read-only.")
    coerced = _coerce(value, keys[key])
    if "min" in current and isinstance(coerced, (int, float)) and coerced < current["min"]:
        raise HTTPException(status_code=400, detail=f"{key} must be at least {current['min']}.")
    if "max" in current and isinstance(coerced, (int, float)) and coerced > current["max"]:
        raise HTTPException(status_code=400, detail=f"{key} must be at most {current['max']}.")
    allowed = current.get("allowed_values")
    if allowed and coerced not in allowed:
        raise HTTPException(
            status_code=400, detail=f"{key} must be one of {', '.join(map(str, allowed))}."
        )
    try:
        answer = _put(f"{base}/config/{key}", {"value": coerced}, _DEFAULT_COMMAND_TIMEOUT_S)
    except Exception as exc:
        _raise_simplon_failure(exc, base, f"The detector did not accept {key}")
    changed = [str(item) for item in answer] if isinstance(answer, list) else []
    reread = [key] + [item for item in changed if item != key and item in keys]
    params: dict[str, Any] = {}
    for item in reread:
        descriptor = _descriptor(_try_get(f"{base}/config/{item}"))
        if descriptor is not None:
            params[item] = descriptor
    return {"subsystem": subsystem, "changed": changed, "params": params}


class CommandRunner:
    """Runs SIMPLON commands in the background, one at a time per detector."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._jobs: dict[str, dict[str, Any]] = {}

    def _key(self, url: str) -> str:
        return normalize_simplon_base_url(url)

    def current(self, url: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(self._key(url))
            return dict(job) if job else None

    def start(
        self, url: str, version: str, subsystem: str, command: str, value: Any = None
    ) -> dict[str, Any]:
        if command not in COMMANDS.get(subsystem, ()):
            raise HTTPException(
                status_code=400, detail=f"{subsystem}/{command} is not an allowed command."
            )
        target = f"{_base(url, version, subsystem)}/command/{command}"
        key = self._key(url)
        interrupt = (subsystem, command) in INTERRUPTS
        with self._lock:
            running = self._jobs.get(key)
            if running and running.get("running") and not interrupt:
                raise HTTPException(
                    status_code=409,
                    detail=f"The detector is still busy with {running['command']}.",
                )
            job = {
                "subsystem": subsystem,
                "command": command,
                "running": True,
                "started": time.time(),
                "finished": None,
                "ok": None,
                "error": None,
                "result": None,
            }
            if not interrupt or not running or not running.get("running"):
                self._jobs[key] = job
        timeout = _COMMAND_TIMEOUT_S.get(command, _DEFAULT_COMMAND_TIMEOUT_S)
        payload = {"value": value} if value is not None else None

        def run() -> None:
            outcome: dict[str, Any] = {}
            try:
                if (subsystem, command) == ("stream", "initialize"):
                    outcome["result"] = _reset_stream(url, version, target, timeout)
                else:
                    outcome["result"] = _put(target, payload, timeout)
                outcome["ok"] = True
            except HTTPException as exc:
                outcome.update(ok=False, error=exc.detail)
            except Exception as exc:  # noqa: BLE001 -- reported to the user, not raised
                try:
                    _raise_simplon_failure(exc, target, f"{command} failed")
                except HTTPException as http_exc:
                    outcome.update(ok=False, error=http_exc.detail)
            outcome.update(running=False, finished=time.time())
            with self._lock:
                job.update(outcome)

        if interrupt and running and running.get("running"):
            # Sent alongside the blocked command; report it as its own result.
            run()
            return dict(job)
        threading.Thread(target=run, name=f"simplon-{command}", daemon=True).start()
        return dict(job)


def _reset_stream(url: str, version: str, target: str, timeout: float) -> dict[str, Any]:
    """Initialize the stream, then switch it back on if it was on.

    SIMPLON's stream initialize resets dropped images and errors "and mode is
    set to disabled". Someone pressing "reset" expects the stream to keep
    working afterwards, not to find it off.
    """
    mode_url = f"{_base(url, version, 'stream')}/config/mode"
    was_on = _value_of(_get(mode_url)) == "enabled"
    _put(target, None, timeout)
    if was_on:
        _put(mode_url, {"value": "enabled"}, _DEFAULT_COMMAND_TIMEOUT_S)
    return {"mode": "enabled" if was_on else "disabled"}


def list_files(url: str, version: str, *, sizes: bool = True) -> list[dict[str, Any]]:
    """The files on the DCU, with sizes where the DCU states them."""
    base = _base(url, version, "filewriter")
    try:
        payload = _get(f"{base}/files/")
    except Exception:
        try:
            payload = _value_of(_get(f"{base}/status/files"))
        except Exception as exc:
            _raise_simplon_failure(exc, base, "Failed to list the files on the detector")
    # The reference shows a plain list; a PILATUS4 DCU answers like any
    # parameter, {"access_mode": "r", "value": [...]}.
    if isinstance(payload, dict):
        payload = payload.get("value")
    names = [str(name) for name in payload] if isinstance(payload, list) else []
    names = [name for name in names if _safe_file_name(name)]

    def size_of(name: str) -> int | None:
        req = urllib.request.Request(data_url(url, name), method="HEAD")
        try:
            with urllib.request.urlopen(req, timeout=_READ_TIMEOUT_S) as resp:
                length = resp.headers.get("Content-Length")
                return int(length) if length and length.isdigit() else None
        except Exception:
            return None

    if not sizes:
        return [{"name": name, "size": None} for name in names]
    with ThreadPoolExecutor(max_workers=_workers(version)) as pool:
        found = list(pool.map(size_of, names[:500]))
    return [
        {"name": name, "size": found[i] if i < len(found) else None} for i, name in enumerate(names)
    ]


def series_files(url: str, version: str, prefix: str) -> list[str]:
    """The files of one series on the DCU: its master file first, then its data files."""
    if not _safe_file_name(f"{prefix}_master.h5"):
        raise HTTPException(status_code=400, detail="Not a series name the detector writes.")
    names = [entry["name"] for entry in list_files(url, version, sizes=False)]
    master = f"{prefix}_master.h5"
    if master not in names:
        raise HTTPException(
            status_code=404,
            detail=f"{master} is not on the detector. Was the file writer on for this series?",
        )
    data = sorted(name for name in names if name.startswith(f"{prefix}_data_"))
    return [master, *data]


def fetch_series(url: str, version: str, prefix: str, dest_root: Path) -> list[Path]:
    """Copy one series from the DCU into `dest_root/<detector>/`, master first.

    Written to a temporary name and renamed when complete, so an interrupted
    copy never leaves a truncated file that looks whole. The master file links
    its data files by relative name, so they go into the same folder.
    """
    host = urllib.parse.urlparse(normalize_simplon_base_url(url)).netloc
    folder = (dest_root / "detector" / re.sub(r"[^A-Za-z0-9._-]", "_", host)).resolve()
    written: list[Path] = []
    for name in series_files(url, version, prefix):
        target = (folder / name).resolve()
        if folder not in target.parents:
            raise HTTPException(status_code=400, detail="Not a series name the detector writes.")
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = target.with_name(target.name + ".part")
        body, _headers = open_download(url, name)
        try:
            with partial.open("wb") as handle:
                for block in body:
                    handle.write(block)
            partial.replace(target)
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
        written.append(target)
    return written


def _safe_file_name(name: str) -> bool:
    segments = str(name or "").split("/")
    return bool(segments) and all(_FILE_SEGMENT_RE.fullmatch(seg) for seg in segments)


def data_url(url: str, name: str) -> str:
    """The DCU's download URL for one file it wrote (`http://<dcu>/data/<name>`)."""
    if not _safe_file_name(name):
        raise HTTPException(status_code=400, detail="Not a file name the detector writes.")
    # The same validation as the API base, without the API path.
    api_base = _base(url, "1.8.0", "detector")
    root = api_base[: -len("/detector/api/1.8.0")]
    return f"{root}/data/" + "/".join(urllib.parse.quote(seg) for seg in name.split("/"))


def open_download(url: str, name: str) -> tuple[Iterator[bytes], dict[str, str]]:
    """Stream one file from the DCU: the body in chunks, and headers to pass on."""
    target = data_url(url, name)
    try:
        resp = urllib.request.urlopen(target, timeout=30)  # noqa: S310 -- validated http(s) URL
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise HTTPException(
                status_code=404, detail=f"{name} is no longer on the detector."
            ) from exc
        _raise_simplon_failure(exc, target, f"Failed to download {name}")
    except Exception as exc:
        _raise_simplon_failure(exc, target, f"Failed to download {name}")
    headers = {"Content-Disposition": f'attachment; filename="{name.rsplit("/", 1)[-1]}"'}
    length = resp.headers.get("Content-Length")
    if length and length.isdigit():
        headers["Content-Length"] = length

    def chunks() -> Iterator[bytes]:
        try:
            while True:
                block = resp.read(1024 * 1024)
                if not block:
                    break
                yield block
        finally:
            resp.close()

    return chunks(), headers
