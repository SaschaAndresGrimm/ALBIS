"""A simulated DECTRIS detector control unit speaking SIMPLON 1.8.

Follows the SIMPLON 1.8 API reference (v3.11): parameters describe themselves
(value, value_type, unit, min, max, allowed_values, access_mode); a PUT answers
with the parameters it changed; only the state is readable before `initialize`;
commands are PUT on /<subsystem>/api/<version>/command/<name>; the file writer
lists files at /filewriter/api/<version>/files/ and serves them from /data/;
the monitor serves its newest image as TIFF at /monitor/api/<version>/images/monitor.

Used by the tests and, through `scripts/fake_simplon.py`, to try the Detector
tab without hardware. The values are plausible, not a real detector's.
"""

from __future__ import annotations

import base64
import copy
import io
import json
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import h5py
import numpy as np
import tifffile

READOUT_S = 1e-7


def _param(
    value: Any, value_type: str, unit: str = "", access: str = "rw", **extra: Any
) -> dict[str, Any]:
    out = {"value": value, "value_type": value_type, "unit": unit, "access_mode": access}
    out.update(extra)
    return out


def _add_thresholds(detector: dict[str, dict[str, Any]], count: int) -> None:
    """Thresholds 2..count, as a POLLUX (two) or a PILATUS4 (four) has them."""
    for n in range(2, count + 1):
        detector[f"threshold/{n}/energy"] = _param(
            6200.0 + 2000.0 * (n - 1), "float", "eV", min=1750.0, max=20000.0
        )
        detector[f"threshold/{n}/mode"] = _param(
            "disabled", "string", allowed_values=["enabled", "disabled"]
        )
    if count > 1:
        detector["threshold/difference/mode"] = _param(
            "disabled", "string", allowed_values=["enabled", "disabled"]
        )


def _series_files(name: str, total: int) -> tuple[bytes, bytes]:
    """A small but real filewriter2 series: a master linking one data file.

    Real HDF5, so ALBIS can open a series copied from the simulated detector;
    at most 20 frames of 64 x 64, a ring pattern that moves frame to frame.
    """
    frames = max(1, min(int(total), 20))
    y, x = np.mgrid[0:64, 0:64]
    r = np.hypot(x - 32, y - 32)
    stack = np.stack([(100 * (1 + np.cos(r / 3.0 - k))).astype(np.uint32) for k in range(frames)])
    with tempfile.TemporaryDirectory() as tmp:
        data_path = Path(tmp) / f"{Path(name).name}_data_000001.h5"
        master_path = Path(tmp) / f"{Path(name).name}_master.h5"
        with h5py.File(data_path, "w") as h5:
            h5.create_dataset("entry/data/data", data=stack)
        with h5py.File(master_path, "w") as h5:
            group = h5.create_group("entry/data")
            group["data_000001"] = h5py.ExternalLink(data_path.name, "/entry/data/data")
            det = h5.create_group("entry/instrument/detector")
            det["x_pixel_size"] = 75e-6
            det["y_pixel_size"] = 75e-6
            det["description"] = "Dectris EIGER2 Si 4M (simulated)"
        return master_path.read_bytes(), data_path.read_bytes()


def _default_params() -> dict[str, dict[str, dict[str, Any]]]:
    return {
        "detector": {
            "description": _param("Dectris EIGER2 Si 4M (simulated)", "string", access="r"),
            "detector_number": _param("E-08-0123", "string", access="r"),
            "sensor_material": _param("Si", "string", access="r"),
            "photon_energy": _param(12400.0, "float", "eV", min=3500.0, max=40000.0),
            "incident_energy": _param(12400.0, "float", "eV", min=3500.0, max=40000.0),
            "threshold_energy": _param(6200.0, "float", "eV", min=1750.0, max=20000.0),
            "threshold/1/energy": _param(6200.0, "float", "eV", min=1750.0, max=20000.0),
            "threshold/1/mode": _param("enabled", "string", allowed_values=["enabled", "disabled"]),
            "count_time": _param(0.0099999, "float", "s", min=0.0001999, max=3600.0),
            "frame_time": _param(0.01, "float", "s", min=0.0002, max=3600.0),
            "nimages": _param(10, "uint", min=1, max=2000000000),
            "ntrigger": _param(1, "uint", min=1, max=1000000),
            "trigger_mode": _param(
                "ints", "string", allowed_values=["ints", "inte", "exts", "exte"]
            ),
            "compression": _param("bslz4", "string", allowed_values=["bslz4", "lz4"]),
            "countrate_correction_applied": _param(True, "bool"),
            "number_of_excluded_pixels": _param(412, "uint", access="r"),
        },
        "monitor": {
            "mode": _param("disabled", "string", allowed_values=["enabled", "disabled"]),
            "buffer_size": _param(100, "uint", min=1, max=1000),
            "discard_new": _param(True, "bool"),
        },
        "filewriter": {
            "mode": _param("disabled", "string", allowed_values=["enabled", "disabled"]),
            "name_pattern": _param("series_$id", "string"),
            "nimages_per_file": _param(1000, "uint", min=0, max=1000000),
            "compression_enabled": _param(True, "bool"),
        },
        "stream": {
            "mode": _param("disabled", "string", allowed_values=["enabled", "disabled"]),
            "header_detail": _param("basic", "string", allowed_values=["all", "basic", "none"]),
            "format": _param("cbor", "string", allowed_values=["legacy", "cbor"]),
        },
    }


class FakeDCU:
    """State and behaviour of one simulated detector control unit."""

    def __init__(
        self,
        *,
        init_delay: float = 0.2,
        max_series_s: float = 0.3,
        thresholds: int = 1,
        api_version: str = "1.8.0",
    ) -> None:
        self.lock = threading.Lock()
        self.params = _default_params()
        # The one SIMPLON version this DCU serves; any other is "Incompatible
        # version", as on a real one. An EIGER1 serves 1.6.0.
        self.api_version = api_version
        self.legacy = tuple(int(x) for x in api_version.split(".")) < (1, 8)
        _add_thresholds(self.params["detector"], max(1, min(4, thresholds)))
        self.state = "na"
        self.init_delay = init_delay
        self.max_series_s = max_series_s
        self.sequence_id = 0
        self.files: dict[str, bytes] = {}
        self.stream_dropped = 0
        self.monitor_dropped = 0
        self.last_monitor: bytes | None = None
        self.triggers_taken = 0
        self.last_exposures: list[float] = []
        # Answer /files/ as a parameter ({"value": [...]}), as a PILATUS4 does.
        self.wrapped_file_list = False
        self.abort_flag = threading.Event()
        self.requests: list[tuple[str, str]] = []
        # Status values reported with "state": "critical", as (subsystem, key).
        self.critical: set[tuple[str, str]] = set()

    # ---- configuration ----
    def describe(self, subsystem: str, key: str) -> dict[str, Any] | None:
        if self.state == "na" and not (subsystem == "detector" and key == "state"):
            return None
        param = self.params.get(subsystem, {}).get(key)
        return copy.deepcopy(param) if param else None

    def put_config(self, subsystem: str, key: str, value: Any) -> tuple[int, Any]:
        with self.lock:
            if self.state == "na":
                return 404, f"Parameter {key} does not exist"
            param = self.params.get(subsystem, {}).get(key)
            if param is None:
                return 404, f"Parameter {key} does not exist"
            if "w" not in param["access_mode"]:
                return 400, f"{key} is read-only"
            # The panel locks the detector's settings during a series; the
            # reference documents no such lock for the data interfaces, and the
            # live view re-sends the monitor's mode with each poll.
            if subsystem == "detector" and self.state in ("acquire", "initialize"):
                return 400, "Not allowed while the detector is busy"
            # As an EIGER2 answers: enable modes take exactly one image per trigger.
            det = self.params["detector"]
            enable = ("inte", "exte")
            if subsystem == "detector" and (
                (key == "trigger_mode" and value in enable and det["nimages"]["value"] != 1)
                or (key == "nimages" and value != 1 and det["trigger_mode"]["value"] in enable)
            ):
                mode = value if key == "trigger_mode" else det["trigger_mode"]["value"]
                return 400, (
                    "error during request: argument error: failed precondition: "
                    f'number_of_images must be 1 for trigger mode "{mode}"'
                )
            vtype = param["value_type"]
            ok = (
                (vtype == "bool" and isinstance(value, bool))
                or (
                    vtype == "uint"
                    and isinstance(value, int)
                    and not isinstance(value, bool)
                    and value >= 0
                )
                or (
                    vtype == "float"
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                )
                or (vtype == "string" and isinstance(value, str))
            )
            if not ok:
                return 400, f"Wrong type for {key}"
            if "min" in param and value < param["min"] or "max" in param and value > param["max"]:
                return 400, f"{key} out of range"
            if param.get("allowed_values") and value not in param["allowed_values"]:
                return 400, f"{key} not allowed"
            param["value"] = float(value) if vtype == "float" else value
            changed = [key]
            det = self.params["detector"]
            if subsystem == "detector" and key == "count_time":
                det["frame_time"]["value"] = max(det["frame_time"]["value"], value + READOUT_S)
                changed += ["frame_time", "frame_count_time"]
            if subsystem == "detector" and key == "frame_time":
                det["count_time"]["value"] = value - READOUT_S
                changed += ["count_time", "frame_count_time"]
            if subsystem == "detector" and key in ("photon_energy", "incident_energy"):
                det["photon_energy"]["value"] = det["incident_energy"]["value"] = float(value)
                det["threshold_energy"]["value"] = det["threshold/1/energy"]["value"] = value / 2
                changed += [
                    "photon_energy",
                    "incident_energy",
                    "threshold_energy",
                    "threshold/1/energy",
                ]
            return 200, sorted(set(changed))

    # ---- commands ----
    def command(self, subsystem: str, name: str, value: Any = None) -> tuple[int, Any]:
        if subsystem == "detector":
            if name == "initialize":
                with self.lock:
                    self.state = "initialize"
                time.sleep(self.init_delay)
                with self.lock:
                    self.state = "idle"
                return 200, None
            if self.state == "na":
                return 400, "Detector not initialized"
            if name == "arm":
                det = self.params["detector"]
                span = (
                    det["nimages"]["value"] * det["ntrigger"]["value"] * det["frame_time"]["value"]
                )
                if span > 604800:
                    # As an EIGER2 or PILATUS4 refuses it.
                    return 400, (
                        "configured trigger sequence duration (frame_time * number_of_images = "
                        f"{span:.1f}s) exceeds maximum allowed duration of 604800.0s (1 week)"
                    )
                with self.lock:
                    self.sequence_id += 1
                    # External enable waits for its signals in "acquire".
                    exte = self.params["detector"]["trigger_mode"]["value"] == "exte"
                    self.state = "acquire" if exte else "ready"
                    self.stream_dropped = 0
                    self.triggers_taken = 0
                return 200, {"sequence id": self.sequence_id}
            if name == "trigger":
                if self.state != "ready":
                    return 400, "Detector not armed"
                det = self.params["detector"]
                if not str(det["trigger_mode"]["value"]).startswith("int"):
                    return 400, "Trigger is not used in external trigger modes"
                if det["trigger_mode"]["value"] == "inte":
                    # One image per trigger, exposed for the value sent with it
                    # (or the count time); the series ends after the last one.
                    exposure = float(value) if value is not None else det["count_time"]["value"]
                    with self.lock:
                        self.state = "acquire"
                        self.last_exposures.append(exposure)
                    time.sleep(min(exposure, self.max_series_s))
                    with self.lock:
                        self.triggers_taken += 1
                        if self.triggers_taken >= det["ntrigger"]["value"]:
                            self._write_series(det["ntrigger"]["value"])
                            self.state = "idle"
                        else:
                            self.state = "ready"
                    return 200, None
                self.abort_flag.clear()
                with self.lock:
                    self.state = "acquire"
                total = det["nimages"]["value"] * det["ntrigger"]["value"]
                duration = min(total * det["frame_time"]["value"], self.max_series_s)
                aborted = self.abort_flag.wait(duration)
                with self.lock:
                    if not aborted:
                        self._write_series(total)
                    self.state = "idle"
                return 200, None
            if name in ("disarm", "cancel", "abort"):
                self.abort_flag.set()
                with self.lock:
                    self.state = "idle"
                return 200, {"sequence id": self.sequence_id}
        if subsystem == "filewriter" and name == "clear":
            with self.lock:
                self.files.clear()
            return 200, None
        if subsystem == "stream" and name == "initialize":
            with self.lock:
                self.stream_dropped = 0
                self.params["stream"]["mode"]["value"] = "disabled"
            return 200, None
        if subsystem == "monitor" and name == "clear":
            self.monitor_dropped = 0
            return 200, None
        return 404, f"Command {name} does not exist"

    def pixel_mask(self) -> dict[str, Any] | None:
        """The pixel mask as SIMPLON sends arrays: a base64 `__darray__`.

        Same size as the monitor image, with a dead column and a gap row.
        """
        if self.state == "na":
            return None
        mask = np.zeros((256, 256), dtype="<u4")
        mask[:, 40] = 2
        mask[128, :] = 1
        return {
            "value": {
                "__darray__": [1, 0, 0],
                "type": "<u4",
                "shape": list(mask.shape),
                "filters": [],
                "data": base64.b64encode(mask.tobytes()).decode("ascii"),
            },
            "value_type": "uint",
            "access_mode": "rw",
        }

    def monitor_image(self) -> bytes | None:
        """The monitor's newest image: a ring pattern that moves with time.

        None (HTTP 204) while the monitor is off or nothing was acquired yet,
        as a real monitor answers when it has no image.
        """
        if self.params["monitor"]["mode"]["value"] != "enabled" or not self.sequence_id:
            return None
        if self.state != "acquire" and self.last_monitor is not None:
            return self.last_monitor
        y, x = np.mgrid[0:256, 0:256]
        r = np.hypot(x - 128, y - 128)
        phase = time.time() * 3.0
        image = (200 * (1 + np.cos(r / 6.0 - phase)) * np.exp(-r / 120)).astype(np.uint32)
        buffer = io.BytesIO()
        tifffile.imwrite(buffer, image)
        self.last_monitor = buffer.getvalue()
        return self.last_monitor

    def _write_series(self, total: int) -> None:
        if self.params["filewriter"]["mode"]["value"] == "enabled":
            name = str(self.params["filewriter"]["name_pattern"]["value"]).replace(
                "$id", str(self.sequence_id)
            )
            master, data = _series_files(name, total)
            self.files[f"{name}_master.h5"] = master
            self.files[f"{name}_data_000001.h5"] = data
        if self.params["stream"]["mode"]["value"] == "enabled":
            self.stream_dropped += total

    # ---- status ----
    def status(self, subsystem: str, key: str) -> Any:
        if subsystem == "detector":
            if key == "state":
                return self.state
            if self.state == "na":
                return None
            if self.legacy:
                # SIMPLON 1.6 names: per board and module, high voltage in volts.
                return {
                    "board_000/th0_temp": 27.2,
                    "board_000/th0_humidity": 2.1,
                    "module_000/hv": 197.7,
                }.get(key)
            return {"temperature": 25.1, "humidity": 3.4, "high_voltage/state": "READY"}.get(key)
        mode = self.params.get(subsystem, {}).get("mode", {}).get("value")
        busy = self.state == "acquire"
        if subsystem == "filewriter":
            return {
                "state": "disabled" if mode != "enabled" else ("acquire" if busy else "ready"),
                "buffer_free": (800_000_000_000 - sum(map(len, self.files.values())))
                // (1024 if self.legacy else 1),
                "error": [],
                "files": sorted(self.files),
            }.get(key)
        if subsystem == "stream":
            return {
                "state": "disabled" if mode != "enabled" else ("acquire" if busy else "ready"),
                "dropped": self.stream_dropped,
            }.get(key)
        if subsystem == "monitor":
            return {
                "state": "normal",
                "dropped": self.monitor_dropped,
                "buffer_fill_level": [0, 100],
            }.get(key)
        return None


def _handler(dcu: FakeDCU) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: Any) -> None:  # quiet
            pass

        def _send(
            self, code: int, payload: Any = None, *, raw: bytes | None = None, head: bool = False
        ) -> None:
            body = (
                raw
                if raw is not None
                else (b"" if payload is None else json.dumps(payload).encode())
            )
            self.send_response(code)
            self.send_header(
                "Content-Type",
                "application/octet-stream" if raw is not None else "application/json",
            )
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if not head:
                self.wfile.write(body)

        def _route(self) -> tuple[str, str, str, str] | None:
            parts = self.path.split("?", 1)[0].strip("/").split("/")
            # <subsystem>/api/<version>/<task>/<key...>
            if len(parts) >= 4 and parts[1] == "api":
                return parts[0], parts[2], parts[3], "/".join(parts[4:])
            return None

        def _data(self, head: bool) -> bool:
            path = self.path.split("?", 1)[0]
            if not path.startswith("/data/"):
                return False
            name = path[len("/data/") :]
            blob = dcu.files.get(name)
            if blob is None:
                self._send(404, "not found")
            else:
                self._send(200, raw=blob, head=head)
            return True

        def do_HEAD(self) -> None:  # noqa: N802
            if not self._data(head=True):
                self._send(404, head=True)

        def _version_ok(self, version: str) -> bool:
            if version == dcu.api_version:
                return True
            self._send(400, "Incompatible version")
            return False

        def do_GET(self) -> None:  # noqa: N802
            dcu.requests.append(("GET", self.path))
            if self._data(head=False):
                return
            parts = self.path.split("?", 1)[0].strip("/").split("/")
            if len(parts) == 3 and parts[1:] == ["api", "version"]:
                self._send(200, {"value": dcu.api_version, "value_type": "string"})
                return
            route = self._route()
            if not route:
                self._send(404, "not found")
                return
            subsystem, version, task, key = route
            if not self._version_ok(version):
                return
            if task == "config":
                param = (
                    dcu.pixel_mask()
                    if (subsystem, key) == ("detector", "pixel_mask")
                    else dcu.describe(subsystem, key)
                )
                (
                    self._send(200, param)
                    if param
                    else self._send(404, f"Parameter {key} does not exist")
                )
            elif task == "status":
                value = dcu.status(subsystem, key)
                if value is None and not (subsystem == "detector" and key == "state"):
                    self._send(404, f"Parameter {key} does not exist")
                else:
                    answer = {"value": value, "value_type": "string"}
                    if dcu.legacy and (subsystem, key) == ("filewriter", "buffer_free"):
                        answer["unit"] = "KB"
                    if (subsystem, key) in dcu.critical:
                        answer["state"] = "critical"
                    self._send(200, answer)
            elif task == "images" and subsystem == "monitor":
                image = dcu.monitor_image()
                if image is None:
                    self._send(204)
                else:
                    self._send(200, raw=image)
            elif task == "files" and subsystem == "filewriter":
                # A plain list, as the reference shows; a PILATUS4 wraps it.
                names = sorted(dcu.files)
                self._send(
                    200, {"access_mode": "r", "value": names} if dcu.wrapped_file_list else names
                )
            else:
                self._send(404, "not found")

        def do_PUT(self) -> None:  # noqa: N802
            dcu.requests.append(("PUT", self.path))
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b""
            body = json.loads(raw) if raw else {}
            route = self._route()
            if not route:
                self._send(404, "not found")
                return
            subsystem, version, task, key = route
            if not self._version_ok(version):
                return
            if task == "config":
                code, payload = dcu.put_config(subsystem, key, body.get("value"))
            elif task == "command":
                code, payload = dcu.command(subsystem, key, body.get("value"))
            else:
                code, payload = 404, "not found"
            self._send(code, payload)

    return Handler


class FakeDCUServer:
    """Runs a FakeDCU on 127.0.0.1 in a background thread."""

    def __init__(self, port: int = 0, **kwargs: Any) -> None:
        self.dcu = FakeDCU(**kwargs)
        self.server = ThreadingHTTPServer(("127.0.0.1", port), _handler(self.dcu))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.server.server_address[1]}"

    def __enter__(self) -> FakeDCUServer:
        self.thread.start()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.server.shutdown()
        self.server.server_close()
