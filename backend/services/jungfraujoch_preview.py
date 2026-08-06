"""JUNGFRAUJOCH preview stream bridge.

This module subscribes to the Jungfraujoch ZeroMQ preview PUB socket, decodes
CBOR stream-v2 image messages and stores frames in ALBIS remote snapshot cache.
"""

from __future__ import annotations

import contextlib
import math
import re
import socket
import threading
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import numpy as np

from ..image_formats import _normalize_image_array

# Transports a ZeroMQ subscriber can actually use for a preview PUB socket.
_ZMQ_SCHEMES = ("tcp", "ipc", "inproc", "pgm", "epgm")
_SCHEME_RE = re.compile(r"^([a-z][a-z0-9+.\-]*)://", re.IGNORECASE)
# `tcp//host`, `tcp:/host`, `tcp:host` — a known transport with a mangled
# separator. Requires a `:` or `/` after the keyword so a hostname that merely
# starts with one of them is left alone.
_MALFORMED_SCHEME_RE = re.compile(
    rf"^({'|'.join(_ZMQ_SCHEMES)})(?::/*|/+)",
    re.IGNORECASE,
)
_PROBE_TIMEOUT_S = 2.0

try:
    import cbor2
except Exception:  # pragma: no cover - optional at import time
    cbor2 = None  # type: ignore[assignment]

try:
    import zmq
except Exception:  # pragma: no cover - optional at import time
    zmq = None  # type: ignore[assignment]

try:
    from dectris.compression import decompress as _dectris_decompress
except Exception:  # pragma: no cover - optional at import time
    _dectris_decompress = None


_TAG_MULTIDIM_ROW_MAJOR = 40
_TAG_MULTIDIM_COLUMN_MAJOR = 1040
_TAG_DECTRIS_COMPRESSION = 56500

if cbor2 is not None:
    _CBOR_DECODE_VALUE_ERROR = getattr(cbor2, "CBORDecodeValueError", cbor2.CBORDecodeError)
else:  # pragma: no cover - optional dependency missing
    _CBOR_DECODE_VALUE_ERROR = ValueError

_TAG_TYPED_ARRAY_DTYPES: dict[int, str] = {
    64: "u1",
    65: ">u2",
    66: ">u4",
    67: ">u8",
    68: "u1",
    69: "<u2",
    70: "<u4",
    71: "<u8",
    72: "i1",
    73: ">i2",
    74: ">i4",
    75: ">i8",
    77: "<i2",
    78: "<i4",
    79: "<i8",
    80: ">f2",
    81: ">f4",
    82: ">f8",
    84: "<f2",
    85: "<f4",
    86: "<f8",
}


def normalize_jfjoch_endpoint(value: str) -> str:
    """Fold a user-typed preview endpoint into a ZeroMQ-usable form.

    Accepts a bare ``host:port`` (the common case — ``tcp://`` is added) and
    repairs a mangled separator such as ``tcp//host``. Unlike the SIMPLON
    address there is no default port to fall back on: a ZeroMQ TCP endpoint
    without one cannot connect, so validation rejects it rather than guessing.

    Returns "" for empty input; a non-ZeroMQ scheme is passed through unchanged
    so validation can reject it with a meaningful message.
    """
    text = re.sub(r"\s+", "", str(value or ""))
    if not text:
        return ""

    scheme_match = _SCHEME_RE.match(text)
    if scheme_match:
        scheme = scheme_match.group(1)
        if scheme.lower() not in _ZMQ_SCHEMES:
            return text
        text = f"{scheme.lower()}://{text[scheme_match.end():]}"
    else:
        malformed = _MALFORMED_SCHEME_RE.match(text)
        if malformed:
            text = f"{malformed.group(1).lower()}://{text[malformed.end():]}"
        else:
            text = f"tcp://{text}"

    scheme, _, rest = text.partition("://")
    if scheme != "tcp":
        # ipc/inproc addresses are paths, not hosts: `ipc:///tmp/x` means the
        # absolute path /tmp/x, so slashes here are content and must survive.
        return f"{scheme}://{rest}" if rest else ""
    rest = rest.lstrip("/").rstrip("/")
    if not rest:
        return ""
    return f"{scheme}://{rest}"


def validate_jfjoch_endpoint(value: str) -> str:
    """Normalize an endpoint and raise ValueError when it cannot be connected.

    Raises:
        ValueError: with wording that says what to enter, since this surfaces
            to the user as the 400 detail from the preview start route.
    """
    endpoint = normalize_jfjoch_endpoint(value)
    if not endpoint:
        raise ValueError(
            "JUNGFRAUJOCH preview endpoint is required "
            "(for example tcp://192.168.1.5:31003)"
        )
    parsed = urllib.parse.urlparse(endpoint)
    if parsed.scheme not in _ZMQ_SCHEMES:
        raise ValueError(
            f"Unsupported preview transport '{parsed.scheme}' — "
            "use tcp://host:port (for example tcp://192.168.1.5:31003)"
        )
    if parsed.scheme != "tcp":
        return endpoint
    if not parsed.hostname:
        raise ValueError("Preview endpoint is missing a host (for example tcp://192.168.1.5:31003)")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"Preview endpoint has an invalid port: {endpoint}") from exc
    if not port:
        raise ValueError(
            "Preview endpoint needs an explicit port, e.g. tcp://192.168.1.5:31003 "
            "(JUNGFRAUJOCH has no default preview port)"
        )
    return endpoint


def jfjoch_probe_endpoint(endpoint: str) -> dict[str, Any]:
    """Check whether anything accepts TCP connections at the preview endpoint.

    This is a reachability check, not a protocol check: a plain TCP connect
    cannot confirm the peer is a JUNGFRAUJOCH preview publisher, only that the
    host resolves and the port is open. That is still the difference between a
    typo'd host, a wrong port and a firewall, which is what goes wrong in
    practice. Callers report the limit in the wording they show.
    """
    value = validate_jfjoch_endpoint(endpoint)
    parsed = urllib.parse.urlparse(value)
    result: dict[str, Any] = {"endpoint": value, "timeout_s": _PROBE_TIMEOUT_S}
    if parsed.scheme != "tcp":
        # Only TCP is probe-able; ipc/inproc have no host to connect to.
        return {"status": "ok", "code": "not_probed", **result, "message": "Endpoint accepted"}

    host = parsed.hostname or ""
    port = int(parsed.port or 0)
    result |= {"host": host, "port": port}
    try:
        with socket.create_connection((host, port), timeout=_PROBE_TIMEOUT_S):
            pass
    except socket.gaierror:
        return {"status": "error", "code": "dns", **result, "message": f"Host not found: {host}"}
    except ConnectionRefusedError:
        return {
            "status": "error",
            "code": "refused",
            **result,
            "message": f"Connection refused by {host} on port {port}",
        }
    except TimeoutError:
        return {
            "status": "error",
            "code": "timeout",
            **result,
            "message": f"No response from {host} on port {port}",
        }
    except OSError as exc:
        return {
            "status": "error",
            "code": "unreachable",
            **result,
            "message": f"Cannot reach {host} on port {port}: {exc}",
        }
    return {
        "status": "ok",
        "code": "ok",
        **result,
        "message": f"{host} accepts connections on port {port}",
    }


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, np.generic):
        value = value.item()
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(result):
        return None
    return result


def _as_int(value: Any) -> int | None:
    number = _as_float(value)
    if number is None:
        return None
    try:
        return int(number)
    except (TypeError, ValueError, OverflowError):
        return None


def _to_mm(value: float | None) -> float | None:
    if value is None:
        return None
    # Jungfraujoch reports distances in meters.
    if abs(value) < 10.0:
        return value * 1000.0
    return value


def _to_um(value: float | None) -> float | None:
    if value is None:
        return None
    # Jungfraujoch reports pixel size in meters.
    if abs(value) < 1.0:
        return value * 1_000_000.0
    return value


def _decode_typed_array(tag: Any, dtype: str) -> np.ndarray:
    payload = tag.value
    if isinstance(payload, memoryview):
        payload = payload.tobytes()
    if isinstance(payload, bytearray):
        payload = bytes(payload)
    if not isinstance(payload, bytes | np.ndarray):
        raise _CBOR_DECODE_VALUE_ERROR("expected bytes payload in typed array")
    if isinstance(payload, np.ndarray):
        return np.asarray(payload, dtype=np.dtype(dtype))
    return np.frombuffer(payload, dtype=np.dtype(dtype))


def _decode_multi_dim_array(tag: Any, *, column_major: bool) -> np.ndarray:
    value = tag.value
    if not isinstance(value, list | tuple) or len(value) != 2:
        raise _CBOR_DECODE_VALUE_ERROR("expected multidim array pair")
    dimensions_raw, payload = value
    if not isinstance(dimensions_raw, list | tuple) or not dimensions_raw:
        raise _CBOR_DECODE_VALUE_ERROR("invalid multidim dimensions")
    try:
        dimensions = tuple(int(v) for v in dimensions_raw)
    except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
        raise _CBOR_DECODE_VALUE_ERROR("invalid multidim dimensions") from exc
    if any(dim <= 0 for dim in dimensions):
        raise _CBOR_DECODE_VALUE_ERROR("invalid multidim dimensions")
    if isinstance(payload, np.ndarray):
        arr = payload
    elif isinstance(payload, list):
        arr = np.asarray(payload)
    else:
        raise _CBOR_DECODE_VALUE_ERROR("expected array payload in multidim array")
    return arr.reshape(dimensions, order="F" if column_major else "C")


def _decode_dectris_compression(tag: Any) -> bytes:
    value = tag.value
    if _dectris_decompress is None:
        raise RuntimeError("dectris-compression is not available")
    if not isinstance(value, list | tuple) or len(value) != 3:
        raise _CBOR_DECODE_VALUE_ERROR("invalid DECTRIS compression payload")
    algorithm = str(value[0] or "")
    elem_size = int(value[1] or 0)
    encoded = value[2]
    if isinstance(encoded, memoryview):
        encoded = encoded.tobytes()
    if isinstance(encoded, bytearray):
        encoded = bytes(encoded)
    if not isinstance(encoded, bytes):
        raise _CBOR_DECODE_VALUE_ERROR("invalid compressed bytes payload")
    decoded = _dectris_decompress(encoded, algorithm, elem_size=elem_size)
    if isinstance(decoded, memoryview):
        return decoded.tobytes()
    if isinstance(decoded, bytearray):
        return bytes(decoded)
    if isinstance(decoded, bytes):
        return decoded
    raise _CBOR_DECODE_VALUE_ERROR("invalid decompressed payload")


def jfjoch_tag_hook(_decoder: Any, tag: Any) -> Any:
    dtype = _TAG_TYPED_ARRAY_DTYPES.get(tag.tag)
    if dtype:
        return _decode_typed_array(tag, dtype)
    if tag.tag == _TAG_MULTIDIM_ROW_MAJOR:
        return _decode_multi_dim_array(tag, column_major=False)
    if tag.tag == _TAG_MULTIDIM_COLUMN_MAJOR:
        return _decode_multi_dim_array(tag, column_major=True)
    if tag.tag == _TAG_DECTRIS_COMPRESSION:
        return _decode_dectris_compression(tag)
    return tag


def jfjoch_peak_sets_from_spots(spots: Any) -> list[dict[str, Any]]:
    if not isinstance(spots, list):
        return []
    indexed: list[list[float]] = []
    unindexed: list[list[float]] = []
    for item in spots[:20000]:
        if not isinstance(item, dict):
            continue
        x = _as_float(item.get("x"))
        y = _as_float(item.get("y"))
        if x is None or y is None:
            continue
        intensity = _as_float(item.get("I"))
        point = [float(x), float(y)]
        if intensity is not None:
            point.append(float(intensity))
        if bool(item.get("indexed")):
            indexed.append(point)
        else:
            unindexed.append(point)

    peak_sets: list[dict[str, Any]] = []
    if indexed:
        peak_sets.append(
            {
                "name": "Indexed reflections",
                "color": "#ff4f93",
                "style": "jfjoch-indexed",
                "points": indexed,
            }
        )
    if unindexed:
        peak_sets.append(
            {
                "name": "Reflections",
                "color": "#36d8ff",
                "style": "jfjoch-unindexed",
                "points": unindexed,
            }
        )
    return peak_sets


def _select_channel_image(
    msg: dict[str, Any], preferred_channel: str | None
) -> tuple[np.ndarray | None, str | None]:
    channel_map = msg.get("data")
    if not isinstance(channel_map, dict) or not channel_map:
        return None, None
    if preferred_channel and preferred_channel in channel_map:
        key = preferred_channel
        value = channel_map.get(key)
    else:
        key = str(next(iter(channel_map.keys())))
        value = channel_map.get(key)
    if value is None:
        return None, None
    arr = np.asarray(value)
    frame = _normalize_image_array(arr)
    return frame, key


@dataclass(frozen=True)
class _BridgeConfig:
    endpoint: str
    source_id: str
    topic: str
    channel: str


class JungfraujochPreviewBridge:
    """Manage one background preview subscription worker."""

    def __init__(
        self,
        *,
        logger: Any,
        remote_store_frame: Callable[..., int],
    ) -> None:
        self._logger = logger
        self._remote_store_frame = remote_store_frame
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop_event: threading.Event | None = None
        self._running = False
        self._config = _BridgeConfig(
            endpoint="",
            source_id="jungfraujoch",
            topic="",
            channel="",
        )
        self._started_at: float | None = None
        self._last_message_at: float | None = None
        self._last_frame_at: float | None = None
        self._last_frame_seq: int = 0
        self._last_error = ""
        self._ingested_frames = 0
        self._series_number: int | None = None
        self._image_number: int | None = None
        self._display_name = ""
        self._start_meta: dict[str, Any] = {}

    def start(
        self,
        *,
        endpoint: str,
        source_id: str,
        topic: str = "",
        channel: str = "",
    ) -> dict[str, Any]:
        endpoint_value = validate_jfjoch_endpoint(endpoint)
        config = _BridgeConfig(
            endpoint=endpoint_value,
            source_id=str(source_id or "jungfraujoch").strip() or "jungfraujoch",
            topic=str(topic or ""),
            channel=str(channel or ""),
        )

        with self._lock:
            same = (
                self._running
                and self._config.endpoint == config.endpoint
                and self._config.source_id == config.source_id
                and self._config.topic == config.topic
                and self._config.channel == config.channel
            )
        if same:
            return self.status()

        self.stop()
        stop_event = threading.Event()
        worker = threading.Thread(
            target=self._run_worker,
            args=(config, stop_event),
            daemon=True,
            name="albis-jfjoch-preview",
        )
        with self._lock:
            self._config = config
            self._stop_event = stop_event
            self._thread = worker
            self._running = True
            self._started_at = time.time()
            self._last_error = ""
            self._ingested_frames = 0
            self._series_number = None
            self._image_number = None
            self._display_name = ""
            self._start_meta = {}
        worker.start()
        self._logger.info(
            "JUNGFRAUJOCH preview started: endpoint=%s source=%s topic=%s channel=%s",
            config.endpoint,
            config.source_id,
            config.topic or "<all>",
            config.channel or "<first>",
        )
        return self.status()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            thread = self._thread
            stop_event = self._stop_event
        if stop_event is not None:
            stop_event.set()
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)
        with self._lock:
            self._running = False
            self._thread = None
            self._stop_event = None
        return self.status()

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "running": bool(self._running),
                "endpoint": self._config.endpoint,
                "source_id": self._config.source_id,
                "topic": self._config.topic,
                "channel": self._config.channel,
                "started_at": self._started_at,
                "last_message_at": self._last_message_at,
                "last_frame_at": self._last_frame_at,
                "last_frame_seq": self._last_frame_seq,
                "ingested_frames": self._ingested_frames,
                "series_number": self._series_number,
                "image_number": self._image_number,
                "display_name": self._display_name,
                "last_error": self._last_error,
            }

    def _set_error(self, message: str) -> None:
        with self._lock:
            self._last_error = message[:300]
        self._logger.warning("JUNGFRAUJOCH preview: %s", message)

    def _run_worker(self, config: _BridgeConfig, stop_event: threading.Event) -> None:
        if cbor2 is None or zmq is None or _dectris_decompress is None:
            self._set_error(
                "Missing dependency for preview stream (requires cbor2, pyzmq, dectris-compression)"
            )
            with self._lock:
                self._running = False
            return

        socket = None
        try:
            context = zmq.Context.instance()
            socket = context.socket(zmq.SUB)
            socket.setsockopt(zmq.CONFLATE, 1)
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.RCVTIMEO, 250)
            socket.setsockopt(zmq.SUBSCRIBE, str(config.topic).encode("utf-8"))
            socket.connect(config.endpoint)
            while not stop_event.is_set():
                try:
                    parts = socket.recv_multipart()
                except zmq.error.Again:
                    continue
                except Exception as exc:
                    self._set_error(f"ZeroMQ receive failed: {exc}")
                    continue
                if not parts:
                    continue
                payload = parts[-1]
                if not payload:
                    continue
                try:
                    message = cbor2.loads(payload, tag_hook=jfjoch_tag_hook)
                except Exception as exc:
                    self._set_error(f"CBOR decode failed: {exc}")
                    continue
                if not isinstance(message, dict):
                    continue

                now = time.time()
                with self._lock:
                    self._last_message_at = now
                kind = str(message.get("type") or "").strip().lower()
                if kind == "start":
                    self._apply_start_message(message)
                elif kind == "image":
                    self._apply_image_message(config, message, now)
        except Exception as exc:  # pragma: no cover - defensive
            self._set_error(f"Preview worker crashed: {exc}")
        finally:
            if socket is not None:
                with contextlib.suppress(Exception):
                    socket.close(0)

            with self._lock:
                self._running = False

    def _apply_start_message(self, message: dict[str, Any]) -> None:
        with self._lock:
            self._start_meta = dict(message)
            self._series_number = _as_int(message.get("series_id"))

    def _apply_image_message(self, config: _BridgeConfig, message: dict[str, Any], now: float) -> None:
        try:
            frame, channel_name = _select_channel_image(message, config.channel)
        except Exception as exc:
            self._set_error(f"Image payload decode failed: {exc}")
            return
        if frame is None:
            return

        start_meta = dict(self._start_meta) if self._start_meta else {}
        series_id = _as_int(message.get("series_id"))
        image_id = _as_int(message.get("image_id"))
        series_name = str(message.get("series_unique_id") or "").strip()
        image_datetime = str(message.get("series_date") or "").strip()

        resolution = self._resolution_from_start(start_meta)
        display_parts = [f"JUNGFRAUJOCH Preview ({config.source_id})"]
        if series_name:
            display_parts.append(series_name)
        if series_id is not None:
            display_parts.append(f"S{series_id}")
        if image_id is not None:
            display_parts.append(f"Img{image_id}")
        display_name = " ".join(display_parts)

        peak_sets = jfjoch_peak_sets_from_spots(message.get("spots"))
        remote_meta = {
            "display_name": display_name,
            "series_number": series_id,
            "image_number": image_id,
            "image_datetime": image_datetime,
            "resolution": resolution,
            "peak_sets": peak_sets,
            "extra": {
                "kind": "jungfraujoch_preview",
                "channel": channel_name or "",
            },
        }
        try:
            seq = self._remote_store_frame(
                source_id=config.source_id,
                frame=frame,
                meta=remote_meta,
                seq=None,
            )
        except Exception as exc:  # pragma: no cover - defensive
            self._set_error(f"Failed to store preview frame: {exc}")
            return

        with self._lock:
            self._last_frame_at = now
            self._last_frame_seq = int(seq)
            self._ingested_frames += 1
            self._series_number = series_id
            self._image_number = image_id
            self._display_name = display_name
            self._last_error = ""

    def _resolution_from_start(self, start_meta: dict[str, Any]) -> dict[str, Any]:
        center_x = _as_float(start_meta.get("beam_center_x"))
        center_y = _as_float(start_meta.get("beam_center_y"))
        beam_center = [center_x, center_y] if center_x is not None and center_y is not None else None
        return {
            "distance_mm": _to_mm(_as_float(start_meta.get("detector_distance"))),
            "pixel_size_um": _to_um(_as_float(start_meta.get("pixel_size_x"))),
            "energy_ev": _as_float(start_meta.get("incident_energy")),
            "wavelength_a": _as_float(start_meta.get("incident_wavelength")),
            "beam_center_px": beam_center,
        }
