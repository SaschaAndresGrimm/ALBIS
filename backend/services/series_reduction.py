"""Reducing a group of frames and choosing the type it is written in.

Part of `SeriesSummingService` (`series_summing.py`): summing, averaging or
taking the median of a group of frames, tracking the range of the results,
and picking the smallest output dtype that holds them without colliding with
the gap marker. `_reduce_group_values` expects the host class to provide
`_raise_if_cancelled` and `_update_job`; the rest is pure numpy.
"""

from __future__ import annotations

from collections.abc import Callable

import numpy as np

_SUFFIXES = {1: "st", 2: "nd", 3: "rd"}


def _ordinal(n: int) -> str:
    """2nd, 3rd, 11th, 21st: the frame step as the header description reads it.

    A bare "th" wrote "every 2th frame" and "every 3th frame" into the header of
    a Combine Frames TIFF.
    """
    if n % 100 in (11, 12, 13):
        return f"{n}th"
    return f"{n}{_SUFFIXES.get(n % 10, 'th')}"


class SeriesReductionMixin:
    """Frame-group reduction and output dtype selection for series jobs."""

    @staticmethod
    def _pick_integer_dtype_for_range(
        min_value: int,
        max_value: int,
        *,
        prefer_unsigned: bool,
        min_itemsize: int,
    ) -> np.dtype | None:
        unsigned_candidates = [
            np.dtype(np.uint8),
            np.dtype(np.uint16),
            np.dtype(np.uint32),
            np.dtype(np.uint64),
        ]
        signed_candidates = [
            np.dtype(np.int8),
            np.dtype(np.int16),
            np.dtype(np.int32),
            np.dtype(np.int64),
        ]
        candidate_sets: list[list[np.dtype]] = []
        if prefer_unsigned and min_value >= 0:
            candidate_sets.append(unsigned_candidates)
        candidate_sets.append(signed_candidates)
        if min_value >= 0 and not prefer_unsigned:
            candidate_sets.append(unsigned_candidates)
        for candidates in candidate_sets:
            for candidate in candidates:
                if candidate.itemsize < min_itemsize:
                    continue
                info = np.iinfo(candidate)
                if min_value >= int(info.min) and max_value <= int(info.max):
                    return candidate
        return None

    @classmethod
    def _sum_output_dtype(
        cls,
        source_dtype: np.dtype,
        max_group_count: int,
        result_min: float | None = None,
        result_max: float | None = None,
    ) -> np.dtype:
        """The integer type a sum is written in.

        Sized from the sums actually produced, not from the worst case: a sum
        of uint32 frames almost always fits uint32, while the worst case
        (uint32 max times the frame count) always asked for uint64. Its gap
        marker, the uint64 max, then made the reader deliver the image as
        float64, with the gaps no longer recognised. The type's own marker
        value (max for unsigned, min for signed) is kept free, so a sum never
        reads as a gap. Without a range, the worst case still applies.
        """
        if not (
            np.issubdtype(source_dtype, np.integer)
            or np.issubdtype(source_dtype, np.unsignedinteger)
        ):
            return source_dtype
        unsigned = bool(np.issubdtype(source_dtype, np.unsignedinteger))
        if result_min is not None and result_max is not None:
            lo = int(np.floor(result_min))
            hi = int(np.ceil(result_max))
            if unsigned:
                min_possible, max_possible = min(lo, 0), hi + 1
            else:
                min_possible, max_possible = lo - 1, hi
        else:
            info = np.iinfo(source_dtype)
            if unsigned:
                min_possible = 0
                max_possible = int(info.max) * int(max_group_count)
            else:
                min_possible = int(info.min) * int(max_group_count)
                max_possible = int(info.max) * int(max_group_count)
        picked = cls._pick_integer_dtype_for_range(
            min_possible,
            max_possible,
            prefer_unsigned=unsigned,
            min_itemsize=source_dtype.itemsize,
        )
        if picked is None:
            return np.dtype(np.float64)
        return picked

    @classmethod
    def _select_output_dtype(
        cls,
        source_dtype: np.dtype,
        operation: str,
        normalize_enabled: bool,
        max_group_count: int,
        result_min: float | None,
        result_max: float | None,
        has_fractional_values: bool,
    ) -> np.dtype:
        source_dtype = np.dtype(source_dtype)
        if normalize_enabled or operation == "mean":
            return np.dtype(np.float32)

        if operation == "sum":
            return cls._sum_output_dtype(source_dtype, max_group_count, result_min, result_max)

        if operation == "median" and (
            np.issubdtype(source_dtype, np.integer)
            or np.issubdtype(source_dtype, np.unsignedinteger)
        ):
            if has_fractional_values:
                return np.dtype(np.float32)
            lo = int(np.floor(result_min)) if result_min is not None else 0
            hi = int(np.ceil(result_max)) if result_max is not None else 0
            picked = cls._pick_integer_dtype_for_range(
                lo,
                hi,
                prefer_unsigned=bool(np.issubdtype(source_dtype, np.unsignedinteger)),
                min_itemsize=source_dtype.itemsize,
            )
            if picked is None:
                return np.dtype(np.float32)
            return picked

        return source_dtype

    @staticmethod
    def _frames_output_label(
        *,
        operation: str,
        group_total: int,
        chunk_idx: int,
        start_idx: int,
        end_idx: int,
        count: int,
        frame_count: int,
        frame_step: int,
    ) -> tuple[str, str]:
        """File-name tag and header description for one image of mode "frames"."""
        every_tag = f"_every{frame_step:03d}" if frame_step > 1 else ""
        every_what = f", every {_ordinal(frame_step)} frame" if frame_step > 1 else ""
        whole = group_total == 1 and start_idx == 0 and end_idx == frame_count - 1
        if whole and frame_step == 1:
            return "_all", f"{operation} of {count} frames"
        span = f"_f{start_idx + 1:06d}-{end_idx + 1:06d}"
        chunk = f"_chunk{chunk_idx + 1:04d}" if group_total > 1 else ""
        what = f"{operation} of {count} frames {start_idx + 1}-{end_idx + 1}{every_what}"
        return f"{chunk}{span}{every_tag}", what

    @staticmethod
    def _cast_result_to_dtype(arr: np.ndarray, output_dtype: np.dtype) -> np.ndarray:
        output_dtype = np.dtype(output_dtype)
        if np.issubdtype(output_dtype, np.integer) or np.issubdtype(
            output_dtype, np.unsignedinteger
        ):
            arr_rounded = np.rint(np.asarray(arr, dtype=np.float64))
            info = np.iinfo(output_dtype)
            # Avoid np.clip integer-bound conversions that can overflow on some
            # platforms for very large unsigned dtypes (for example uint64 max).
            arr_clipped = np.minimum(
                np.maximum(arr_rounded, float(info.min)),
                float(info.max),
            )
            return arr_clipped.astype(output_dtype, casting="unsafe")
        return np.asarray(arr, dtype=output_dtype)

    def _reduce_group_values(
        self,
        *,
        job_id: str,
        frame_indices: list[int],
        operation: str,
        load_frame: Callable[[int], np.ndarray],
        processed_ref: list[int],
        total_steps: int,
        progress_message: Callable[[int], str],
        reduce_message: Callable[[], str],
        normalize_scalar: float | None = None,
        normalize_ref: np.ndarray | None = None,
        normalize_ref_valid: np.ndarray | None = None,
        any_mask: np.ndarray | None = None,
        preprocess_frame: Callable[[np.ndarray, int], np.ndarray] | None = None,
    ) -> np.ndarray | None:
        acc: np.ndarray | None = None
        median_stack: list[np.ndarray] | None = [] if operation == "median" else None

        for frame_idx in frame_indices:
            self._raise_if_cancelled(job_id)
            arr = np.asarray(load_frame(frame_idx), dtype=np.float64)
            if preprocess_frame is not None:
                arr = np.asarray(preprocess_frame(arr, frame_idx), dtype=np.float64)
            if normalize_scalar is not None:
                arr = arr / float(normalize_scalar)
            if normalize_ref is not None and normalize_ref_valid is not None:
                arr = np.divide(
                    arr,
                    normalize_ref,
                    out=np.full_like(arr, np.nan, dtype=np.float64),
                    where=normalize_ref_valid,
                )
            if any_mask is not None:
                arr = arr.copy()
                arr[any_mask] = 0.0

            if operation == "median":
                if median_stack is not None:
                    median_stack.append(arr)
            else:
                if acc is None:
                    acc = np.zeros_like(arr, dtype=np.float64)
                acc += arr

            processed_ref[0] += 1
            progress = min(0.95, processed_ref[0] / total_steps)
            self._update_job(job_id, progress=progress, message=progress_message(frame_idx))

        if operation == "median":
            if not median_stack:
                return None
            self._update_job(
                job_id,
                progress=min(0.95, processed_ref[0] / total_steps),
                message=reduce_message(),
            )
            self._raise_if_cancelled(job_id)
            reduced = np.median(np.stack(median_stack, axis=0), axis=0)
            self._raise_if_cancelled(job_id)
            return np.asarray(reduced)

        if acc is None:
            return None
        if operation == "mean":
            return np.asarray(acc / float(max(1, len(frame_indices))))
        return np.asarray(acc)

    @staticmethod
    def _update_result_range(
        arr: np.ndarray,
        result_min: float | None,
        result_max: float | None,
    ) -> tuple[float | None, float | None]:
        if not arr.size:
            return result_min, result_max
        finite_mask = np.isfinite(arr)
        if not np.any(finite_mask):
            return result_min, result_max
        chunk_min = float(np.min(arr[finite_mask]))
        chunk_max = float(np.max(arr[finite_mask]))
        next_min = chunk_min if result_min is None else min(result_min, chunk_min)
        next_max = chunk_max if result_max is None else max(result_max, chunk_max)
        return next_min, next_max
