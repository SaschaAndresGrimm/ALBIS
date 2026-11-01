"""Data export: frames the browser sends -- live frames and flatfields."""

from __future__ import annotations

import re
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
from fastapi import HTTPException

if TYPE_CHECKING:
    from .data_export import DataExportDeps


class DataExportLiveMixin:
    """Frames the browser sends, for DataExportService: checked, then written
    as a converted frame is."""

    _deps: DataExportDeps

    def _live_array(
        self, payload: bytes, dtype: str, shape: list[int] | tuple[int, ...]
    ) -> np.ndarray:
        """A frame sent by the browser as an array, its type and size checked."""
        try:
            parsed = np.dtype(str(dtype))
        except TypeError as exc:
            raise HTTPException(status_code=400, detail="Unknown pixel type") from exc
        if f"{parsed.kind}{parsed.itemsize}" not in self.LIVE_DTYPES:
            raise HTTPException(status_code=400, detail="Unsupported pixel type")
        try:
            dims = [int(v) for v in (shape or [])]
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="A frame must be 2D") from exc
        if len(dims) != 2 or min(dims) < 1:
            raise HTTPException(status_code=400, detail="A frame must be 2D")
        pixels = dims[0] * dims[1]
        if pixels > self.LIVE_MAX_PIXELS:
            raise HTTPException(status_code=413, detail="Frame too large")
        if len(payload) != pixels * parsed.itemsize:
            raise HTTPException(
                status_code=400, detail="Pixel data does not match the frame's shape and type"
            )
        return np.frombuffer(payload, dtype=parsed).reshape(dims)

    def save_flatfield(
        self, *, payload: bytes, dtype: str, shape: list[int] | tuple[int, ...]
    ) -> Path:
        """Write a frame sent by the browser as a flatfield TIFF in the data
        folder's `flatfields/`, named by the time it was taken; returns it."""
        arr = self._live_array(payload, dtype, shape)
        out_dir = self._deps.data_dir / "flatfields"
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d_%H%M%S")
        path = out_dir / f"flat_{stamp}.tiff"
        if path.exists():
            path = self._next_available_path(path)
        self._deps.write_tiff(path, arr, {"source_name": "ALBIS flatfield"})
        return path

    # ---- Live frames -----------------------------------------------------
    # A live source (SIMPLON monitor, JUNGFRAUJOCH preview, Remote Stream) has
    # no file to convert: its frames live in the browser's live history. The
    # browser sends them one at a time, with what it knows about each, and
    # they are written as a converted file would be.
    LIVE_DTYPES = frozenset({"u1", "u2", "u4", "i1", "i2", "i4", "f4", "f8"})
    LIVE_MAX_PIXELS = 64 * 1024 * 1024
    LIVE_FLOAT_KEYS = (
        "incident_energy_ev",
        "wavelength_a",
        "detector_distance_m",
        "beam_center_x_px",
        "beam_center_y_px",
        "pixel_size_x_m",
        "pixel_size_y_m",
        "threshold_energy_ev",
        "exposure_time_s",
    )
    # Zero is "not known" for these: a header says nothing rather than 0.
    LIVE_POSITIVE_KEYS = frozenset(LIVE_FLOAT_KEYS) - {"beam_center_x_px", "beam_center_y_px"}
    LIVE_INT_KEYS = ("series_number", "image_number")
    LIVE_TEXT_KEYS = ("image_datetime", "source_name", "detector_description")

    @classmethod
    def live_frame_metadata(cls, raw: dict[str, Any] | None) -> dict[str, Any]:
        """Keep the fields a header can state, each of the right type."""
        source = raw if isinstance(raw, dict) else {}
        meta: dict[str, Any] = {}
        for key in cls.LIVE_FLOAT_KEYS:
            value = cls._finite_float(source.get(key))
            if value is None or (key in cls.LIVE_POSITIVE_KEYS and value <= 0):
                continue
            meta[key] = value
        # A beam centre of 0,0 is a detector's "not set".
        if not meta.get("beam_center_x_px") and not meta.get("beam_center_y_px"):
            meta.pop("beam_center_x_px", None)
            meta.pop("beam_center_y_px", None)
        for key in cls.LIVE_INT_KEYS:
            value = cls._finite_int(source.get(key))
            if value is not None and value >= 0:
                meta[key] = value
        for key in cls.LIVE_TEXT_KEYS:
            text = " ".join(str(source.get(key) or "").split())[:200]
            if text:
                meta[key] = text
        return meta

    def write_live_frame(
        self,
        *,
        payload: bytes,
        dtype: str,
        shape: list[int] | tuple[int, ...],
        name: str,
        output_format: str,
        output_dir: str | None,
        overwrite: bool,
        metadata: dict[str, Any] | None,
        mask: bytes | None = None,
        inpaint: str = "off",
        inpaint_margin: int = 0,
        inpaint_method: str = "interpolate",
        inpaint_value: float = 0.0,
        inpaint_noise: bool = False,
        flatfield: str = "",
        flatfield_scale: str = "counts",
        data_type: str = "auto",
    ) -> tuple[Path, int]:
        """Write one frame sent by the browser; returns the file written and
        how many pixels were clipped to fit `data_type`.

        `mask` is the source's pixel mask (uint32, the frame's shape), sent
        when the frame is to be inpainted: a live frame has no file to read
        one from."""
        arr = self._live_array(payload, dtype, shape)
        dims = list(arr.shape)
        pixels = dims[0] * dims[1]
        if output_format not in {"tiff", "tif", "cbf"}:
            raise HTTPException(status_code=400, detail="Invalid format")
        normalized_format = "tiff" if output_format in {"tiff", "tif"} else "cbf"
        mask_bits = None
        if mask:
            if len(mask) != pixels * 4:
                raise HTTPException(status_code=400, detail="Pixel mask does not match the frame")
            mask_bits = np.frombuffer(mask, dtype="<u4").reshape(dims)
        # No source file: an empty folder means live_export in the data folder.
        out_dir = self._resolve_output_dir(output_dir, self._deps.data_dir / "live")
        safe = self._safe_name(name, "live")
        suffix = ".cbf" if normalized_format == "cbf" else ".tiff"
        if not overwrite and (out_dir / f"{safe}{suffix}").exists():
            # Frames arrive one by one, so the run counter goes in per frame,
            # before the number: simplon_monitor_2_00009, still a series.
            match = re.match(r"^(.*)_(\d+)$", safe)
            head, number = (match.group(1), f"_{match.group(2)}") if match else (safe, "")
            for run in range(2, 10000):
                candidate = f"{head}_{run}{number}"
                if not (out_dir / f"{candidate}{suffix}").exists():
                    safe = candidate
                    break
        written = self._write_frame(
            out_dir=out_dir,
            name=safe,
            output_format=normalized_format,
            arr=arr,
            mask_bits=mask_bits,
            metadata=self.live_frame_metadata(metadata),
            overwrite=overwrite,
            inpaint=inpaint,
            inpaint_margin=inpaint_margin,
            inpaint_method=inpaint_method,
            inpaint_value=inpaint_value,
            inpaint_noise=inpaint_noise,
            flatfield=flatfield,
            flatfield_scale=flatfield_scale,
            data_type=data_type,
        )
        # One line a frame, and the browser sends them one by one: DEBUG.
        self._deps.logger.debug("Live frame exported: %s", written[0])
        return written
