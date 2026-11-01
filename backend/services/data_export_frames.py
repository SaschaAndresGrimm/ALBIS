"""Data export: a frame as it is written -- flatfield, inpainting, markers, data type."""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from fastapi import HTTPException

from ..image_formats import _to_little_endian
from .flatfield import Flat, apply_flat
from .inpainting import METHODS as INPAINT_METHODS
from .inpainting import add_noise, grow_holes
from .inpainting import inpaint as inpaint_frame

# The data types an export can be asked for besides "auto" (signed 32-bit
# integers, or float32 for a flatfielded TIFF). TIFF only: a CBF is signed.
UNSIGNED_TYPES = ("uint16", "uint32")

if TYPE_CHECKING:
    from .data_export import DataExportDeps


class DataExportFrameMixin:
    """Frame conversion for DataExportService: the casts and _normalize_frame."""

    _deps: DataExportDeps

    @staticmethod
    def _cast_frame_to_int(frame: np.ndarray) -> np.ndarray:
        if np.issubdtype(frame.dtype, np.integer):
            min_value = int(np.min(frame, initial=0))
            max_value = int(np.max(frame, initial=0))
        else:
            finite = np.isfinite(frame)
            finite_values = frame[finite]
            if finite_values.size:
                min_value = int(np.rint(np.min(finite_values)))
                max_value = int(np.rint(np.max(finite_values)))
            else:
                min_value = 0
                max_value = 0
            frame = np.rint(np.where(finite, frame, 0))

        dtype = np.int32
        if min_value < np.iinfo(np.int32).min or max_value > np.iinfo(np.int32).max:
            dtype = np.int64
        return np.asarray(frame, dtype=dtype)

    @staticmethod
    def _cast_frame_to_unsigned(frame: np.ndarray, dtype: np.dtype) -> tuple[np.ndarray, int]:
        """`frame` as `dtype`, and how many pixels did not fit: those are
        clipped to 0 or one below the type's highest value, which is the marker
        for a pixel without a value."""
        top = int(np.iinfo(dtype).max) - 1
        if np.issubdtype(frame.dtype, np.integer):
            values = frame
        else:
            values = np.rint(np.where(np.isfinite(frame), frame, 0))
        outside = (values < 0) | (values > top)
        clipped = int(np.count_nonzero(outside))
        if clipped:
            values = np.clip(values, 0, top)
        return np.asarray(values).astype(dtype), clipped

    def _normalize_frame(
        self,
        arr: np.ndarray,
        mask_bits: np.ndarray | None = None,
        saturated_value: int | None = None,
        inpaint: str = "off",
        inpaint_margin: int = 0,
        inpaint_method: str = "interpolate",
        inpaint_value: float = 0.0,
        inpaint_noise: bool = False,
        flat: Flat | None = None,
        flatfield_scale: str = "counts",
        keep_float: bool = False,
        data_type: str = "auto",
    ) -> tuple[np.ndarray, int, int]:
        """The frame as written, how many of its pixels were inpainted, and how
        many were clipped to fit `data_type`.

        With a `flat`, the frame is first divided by it (see flatfield.py);
        pixels with nothing to divide -- the flat's holes, the frame's gaps and
        defective pixels -- have no value. Gaps become -1 and defective,
        saturated, non-finite or uncorrectable pixels -2 -- unless `inpaint`
        fills them from their neighbours: "defects" fills the gaps, defective
        and uncorrectable pixels (by the mask, or a signed frame's own -1 and
        -2 without one), "saturated" the saturated pixels too. `keep_float`
        writes the values as float32 (a flatfielded TIFF) instead of integers.

        `data_type` "uint16" or "uint32" writes unsigned integers instead, for
        software that wants exactly that. No -1 or -2 there: gaps, bad and
        saturated pixels all become the type's highest value (65535), the
        DECTRIS convention for a masked pixel, and a count that does not fit
        is clipped one below it (65534), never wrapped around.
        """
        frame = np.asarray(arr)
        if frame.ndim != 2:
            raise HTTPException(status_code=400, detail="Only 2D frames can be exported")
        frame = np.ascontiguousarray(frame)
        frame = _to_little_endian(frame)

        gap_mask: np.ndarray | None = None
        defective_mask: np.ndarray | None = None
        if mask_bits is not None:
            mask = np.asarray(mask_bits, dtype=np.uint32)
            if mask.shape != frame.shape:
                raise HTTPException(status_code=400, detail="Pixel mask shape does not match frame")
            gap_mask, defective_mask, _ = self._deps.mask_slices(mask)

        saturated_mask: np.ndarray | None = None
        if np.issubdtype(frame.dtype, np.integer):
            saturated_masks: list[np.ndarray] = []
            if np.issubdtype(frame.dtype, np.unsignedinteger):
                saturated_masks.append(frame == np.iinfo(frame.dtype).max)
            if saturated_value is not None:
                saturated_masks.append(frame >= saturated_value)
            for candidate in saturated_masks:
                if np.any(candidate):
                    saturated_mask = (
                        candidate if saturated_mask is None else (saturated_mask | candidate)
                    )
        else:
            bad_values = ~np.isfinite(frame)
            if np.any(bad_values):
                saturated_mask = bad_values

        raw = frame
        cbf_markers = None
        if mask_bits is None and np.issubdtype(raw.dtype, np.signedinteger):
            # A CBF's own markers, where there is no mask to say it.
            cbf_markers = (raw == -1) | (raw == -2)
        if flat is not None:
            if flat.data.shape != raw.shape:
                raise HTTPException(
                    status_code=400, detail="The flatfield's size does not match the frame"
                )
            corrected = apply_flat(raw, flat, flatfield_scale)
            # Nothing to divide where the frame itself has no value.
            for unknown in (gap_mask, defective_mask, cbf_markers):
                if unknown is not None:
                    corrected[unknown] = np.nan
            frame = corrected

        filled = 0
        if inpaint in {"defects", "saturated"}:
            holes = np.zeros(frame.shape, dtype=bool)
            if gap_mask is not None:
                holes |= gap_mask
            if defective_mask is not None:
                holes |= defective_mask
            if cbf_markers is not None:
                holes |= cbf_markers
            if not np.issubdtype(frame.dtype, np.integer):
                holes |= ~np.isfinite(frame)
            if inpaint == "saturated" and saturated_mask is not None:
                holes |= saturated_mask
            # Their neighbours too, when asked: often just as unreliable.
            holes = grow_holes(holes, inpaint_margin)
            filled = int(np.count_nonzero(holes))
            if filled:
                if inpaint_method == "fixed":
                    values = np.full(filled, float(inpaint_value))
                else:
                    method = inpaint_method if inpaint_method in INPAINT_METHODS else "interpolate"
                    values = inpaint_frame(frame, holes, method)[holes]
                if inpaint_noise:
                    # As noisy as measured pixels: invented, and the header
                    # says so. Drawn in raw counts when divided by a flat.
                    values = add_noise(values, np.flatnonzero(holes), raw, flat, flatfield_scale)
                frame = frame.copy()
                if np.issubdtype(frame.dtype, np.integer):
                    info = np.iinfo(frame.dtype)
                    values = np.clip(np.rint(values), info.min, info.max)
                frame[holes] = values.astype(frame.dtype)
                # What was filled is a value now, not a marker.
                gap_mask = defective_mask = None
                if saturated_mask is not None:
                    saturated_mask = saturated_mask & ~holes
                    if not np.any(saturated_mask):
                        saturated_mask = None

        bad_mask = defective_mask
        if saturated_mask is not None:
            bad_mask = saturated_mask if bad_mask is None else (bad_mask | saturated_mask)
        if flat is not None:
            # Left without a value (not inpainted): a bad pixel.
            uncorrected = ~np.isfinite(frame)
            if gap_mask is not None:
                uncorrected &= ~gap_mask
            if np.any(uncorrected):
                bad_mask = uncorrected if bad_mask is None else (bad_mask | uncorrected)

        cast_source = frame
        cast_mask = None
        if gap_mask is not None:
            cast_mask = gap_mask
        if bad_mask is not None:
            cast_mask = bad_mask if cast_mask is None else (cast_mask | bad_mask)
        if cast_mask is not None and np.any(cast_mask):
            cast_source = frame.copy()
            cast_source[cast_mask] = 0

        clipped = 0
        if data_type in UNSIGNED_TYPES:
            # A signed source's own -1 and -2, still there: no value either,
            # not a count below zero.
            if cbf_markers is not None and np.issubdtype(frame.dtype, np.signedinteger):
                left = cbf_markers & ((frame == -1) | (frame == -2))
                if np.any(left):
                    cast_mask = left if cast_mask is None else (cast_mask | left)
                    cast_source = np.where(left, 0, cast_source)
            output, clipped = self._cast_frame_to_unsigned(cast_source, np.dtype(data_type))
            if cast_mask is not None and np.any(cast_mask):
                output[cast_mask] = np.iinfo(output.dtype).max
            return output, filled, clipped
        if keep_float:
            output = np.where(np.isfinite(cast_source), cast_source, 0).astype(np.float32)
        else:
            output = self._cast_frame_to_int(cast_source)
        if gap_mask is not None or bad_mask is not None:
            output = output.copy()
            if bad_mask is not None:
                output[bad_mask] = -2
            if gap_mask is not None:
                output[gap_mask] = -1
        return output, filled, clipped
