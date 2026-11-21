"""Several consecutive frames shown as one: their sum or their mean.

The viewer's "Sum" control asks `GET /api/frame` for frames i to i+N-1 added
together, so a weak signal stands out and a long series can be looked through
N frames at a time. This is a way of looking at the data, not an export: the
result is computed per request and nothing is written (Series Sum, in
`series_summing.py`, is the job that writes files).

A pixel that holds a marker in any of the frames is not a count, so it is not
added: it keeps a marker in the result, and the viewer treats it as it treats
that pixel in a single frame. Two conventions are kept apart:

- the type's maximum (the gap and defective pixels of an EIGER frame, and what
  the viewer counts as saturated) becomes the result type's maximum;
- a negative value in signed data (a PILATUS CBF marks gaps -1 and defective
  pixels -2) stays negative, the most negative seen, so -2 wins over -1.
  Added up, ten -1 gaps made -10, and a pixel defective in one frame only
  mixed its -2 into the counts of the others.

A mean, being float, marks either kind NaN, which the viewer leaves out of its
statistics in the same way.

A series of one file per frame is summed the same way, file by file
(`series_window_files`).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .series_reduction import SeriesReductionMixin

# A sum of a thousand frames of an EIGER2 16M is already some 20 GB read; the
# control offers no more, and the endpoint refuses more.
MAX_FRAME_WINDOW = 1000

FRAME_WINDOW_OPERATIONS = ("sum", "mean")


def frame_window_count(total_frames: int, start: int, count: int) -> int:
    """How many frames a window from `start` covers: `count`, cut at the end.

    At the end of a series the window shrinks rather than running past it, so
    the last frames are still shown, as a sum of fewer.
    """
    remaining = int(total_frames) - int(start)
    return max(1, min(int(count), remaining))


def _marker_of(dtype: np.dtype) -> int | None:
    if np.issubdtype(dtype, np.integer):
        return int(np.iinfo(dtype).max)
    return None


def _accumulator_dtype(dtype: np.dtype) -> np.dtype:
    if np.issubdtype(dtype, np.unsignedinteger):
        return np.dtype(np.uint64)
    if np.issubdtype(dtype, np.integer):
        return np.dtype(np.int64)
    return np.dtype(np.float64)


def reduce_frames(frames: Iterable[np.ndarray], operation: str = "sum") -> np.ndarray:
    """The sum or mean of `frames`, with marker pixels kept as markers.

    A sum of integer frames comes back in the smallest integer type that
    holds it (at least the source's width), with the type's maximum kept free
    for the marker: ten uint32 frames usually stay uint32. A mean is float32.
    """
    if operation not in FRAME_WINDOW_OPERATIONS:
        raise ValueError(f"Unknown operation: {operation}")
    acc: np.ndarray | None = None
    flagged: np.ndarray | None = None
    negative: np.ndarray | None = None
    source_dtype: np.dtype | None = None
    marker: int | None = None
    signed = False
    count = 0
    for frame in frames:
        arr = np.asarray(frame)
        if acc is None:
            source_dtype = arr.dtype
            marker = _marker_of(source_dtype)
            signed = bool(np.issubdtype(source_dtype, np.signedinteger))
            acc = np.zeros(arr.shape, dtype=_accumulator_dtype(source_dtype))
            flagged = np.zeros(arr.shape, dtype=bool)
            # The most negative marker each pixel had; 0 where it had none.
            negative = np.zeros(arr.shape, dtype=np.int64) if signed else None
        elif arr.shape != acc.shape:
            raise ValueError("Frames in a window differ in shape")
        if marker is not None:
            hit = arr == marker
            flagged |= hit
            if negative is not None:
                below = arr < 0
                np.minimum(negative, np.where(below, arr, 0), out=negative)
                hit = hit | below
            # Added as zero, so a marker does not overflow the accumulator.
            acc += np.where(hit, 0, arr).astype(acc.dtype, copy=False)
        else:
            acc += arr
        count += 1
    if acc is None or source_dtype is None or flagged is None:
        raise ValueError("No frames to reduce")
    marked_negative = negative < 0 if negative is not None else None

    if operation == "mean":
        out = (acc / float(count)).astype(np.float32)
        out[flagged] = np.nan
        if marked_negative is not None:
            out[marked_negative] = np.nan
        return out

    if marker is None:
        # Float frames: the sum stays float, in the source's width or wider.
        return acc.astype(np.promote_types(source_dtype, np.float32), copy=False)

    invalid = flagged if marked_negative is None else flagged | marked_negative
    valid = acc[~invalid]
    lo = int(valid.min()) if valid.size else 0
    hi = int(valid.max()) if valid.size else 0
    if marked_negative is not None and marked_negative.any():
        lo = min(lo, int(negative.min()))
    out_dtype = SeriesReductionMixin._sum_output_dtype(source_dtype, count, lo, hi)
    out = acc.astype(out_dtype)
    out_marker = _marker_of(out_dtype)
    out[flagged] = out_marker if out_marker is not None else np.nan
    if marked_negative is not None:
        out[marked_negative] = negative[marked_negative]
    return out


def reduce_frame_window(
    load_frame: Callable[[int], np.ndarray],
    *,
    start: int,
    count: int,
    operation: str = "sum",
) -> np.ndarray:
    """Read frames `start` .. `start + count - 1` one at a time and reduce them.

    One at a time keeps the memory at one frame plus the accumulator, whatever
    the window; DECTRIS files hold one frame per chunk, so reading several at
    once would not decompress any less.
    """
    return reduce_frames((load_frame(start + i) for i in range(count)), operation)


def series_window_files(files: list[Path], index: int, count: int) -> list[Path]:
    """The files a window over a series of one file per frame covers.

    From `index`, `count` of them, cut at the end of the series as a stack is.
    A symbolic link is skipped, as the series listing (`GET /api/series`)
    skips it: a link in the data folder can point anywhere, and the files a
    sum reads must be the ones the request's path was confined to.
    """
    window = files[int(index) : int(index) + max(1, int(count))]
    return [path for path in window if not path.is_symlink()]


@dataclass(frozen=True)
class FrameCombine:
    """Export each window of `count` frames as one image (the view's Sum).

    Export Data writes what the viewer shows: with Sum 10 on, the current
    frame is frames 121-130 added up, and a range is one file per ten frames,
    each frame in one file only.
    """

    count: int = 1
    operation: str = "sum"

    @property
    def active(self) -> bool:
        return self.count > 1

    def windows(self, frames: list[int], frame_count: int, mode: str) -> list[tuple[int, int]]:
        """(first frame, frames in it) for each image to write.

        The current frame's window reaches as far as the series does; a range's
        windows stay inside the range, the last one holding the frames left.
        """
        if not self.active:
            return [(f, 1) for f in frames]
        if mode == "current":
            start = frames[0]
            return [(start, frame_window_count(frame_count, start, self.count))]
        end = frames[-1] + 1
        return [(s, min(self.count, end - s)) for s in frames[:: self.count]]

    def describe(self, start: int, count: int) -> str:
        """The header's account of an image: "sum of 10 frames 121-130"."""
        if count == 1:
            return f"{self.operation} of 1 frame {start + 1}"
        return f"{self.operation} of {count} frames {start + 1}-{start + count}"

    def read(self, load_frame: Callable[[int], np.ndarray], start: int, count: int) -> np.ndarray:
        """The image for the window: the frame itself when not combining."""
        if not self.active:
            return load_frame(start)
        return reduce_frame_window(load_frame, start=start, count=count, operation=self.operation)

    def header(
        self, metadata: dict[str, Any], start: int, count: int, drops: Iterable[str]
    ) -> dict[str, Any]:
        """The first frame's header, made a combined image's.

        The per-exposure fields (`drops`) go, as in a Series Sum's header, and
        so does the count cutoff's twin `saturation_value`: a sum of ten frames
        exceeds one frame's cutoff without a single pixel being saturated, and
        the export would have marked those pixels overloaded.
        """
        if not self.active:
            return metadata
        dropped = set(drops) | {"saturation_value"}
        meta = {key: value for key, value in metadata.items() if key not in dropped}
        meta["combined_description"] = self.describe(start, count)
        return meta

    def name_tag(self) -> str:
        """Part of the file name, so a sum is not taken for a frame: "_sum10"."""
        return f"_{self.operation}{self.count}" if self.active else ""
