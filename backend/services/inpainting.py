"""Inpainting for exported frames: gaps and defective pixels filled from their
neighbours.

Mirrors ``frontend/modules/inpainting.js``, which fills the frame on screen;
see there for the method. In short: every hole pixel is interpolated along its
row and along its column between the valid pixels on either side of its gap
(each side the median of the five nearest), and the two are combined with weights
1/span, so the short way across a gap wins. Pixels with no valid pixel in
their row or column -- where two full-length gaps cross -- are filled in a
further round from the ones filled before. Vectorised, and on the hole pixels
only: each hole's neighbours are found from the runs of holes along its row and
column, so a frame with a few scattered holes costs next to nothing, whatever
its size (about 0.1 s for an EIGER2 4M frame with its gaps).

Both implementations are held to the same cases in
``tests/fixtures/inpainting_cases.json``.
"""

from __future__ import annotations

import numpy as np

MAX_ROUNDS = 8
# Pixels each side's value is taken from: the median of five rejects a spot of
# up to two pixels at a gap's edge.
SIDE_SAMPLES = 5
# The widest margin around flagged pixels that may be filled too.
MAX_MARGIN = 5


def _run_edges(holes: np.ndarray, axis: int, flat: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """For each hole in ``flat`` (row-major), the position along ``axis`` of
    the nearest non-hole pixel before and after it: -1 and the line's length
    where there is none. Found from the runs of holes along that axis, so the
    cost follows the number of holes, not the size of the frame."""
    height, width = holes.shape
    length = holes.shape[axis]
    # The holes in the order the axis runs through them, and their position
    # along it: row-major along a row, column-major down a column.
    order = flat if axis == 1 else np.flatnonzero(holes.T)
    along = order % length
    # A run ends where the next hole is not the next pixel, or the line ends.
    starts = np.ones(order.shape, dtype=bool)
    starts[1:] = (np.diff(order) != 1) | (along[1:] == 0)
    run = np.cumsum(starts) - 1
    first = along[starts]
    ends = np.ones(order.shape, dtype=bool)
    ends[:-1] = starts[1:]
    last = along[ends]
    before = (first - 1)[run]
    after = (last + 1)[run]
    if axis == 1:
        return before.astype(np.int64), after.astype(np.int64)
    # Back to row-major order: a column-major position col * height + row is
    # the pixel row * width + col.
    rows = order % height
    cols = order // height
    by_pixel = np.empty(height * width, dtype=np.int64)
    by_pixel[rows * width + cols] = before
    left = by_pixel[flat]
    by_pixel[rows * width + cols] = after
    return left, by_pixel[flat]


def _first_median(samples: np.ndarray, count: np.ndarray) -> np.ndarray:
    """The median of each row's first ``count`` values (0 where there are
    none). The rest of the row is NaN. Sorting puts NaN last, so the middle
    one or two are at fixed places: much faster than np.nanmedian, which on
    short rows goes through masked arrays, and the same numbers."""
    ordered = np.sort(samples, axis=1)
    index = np.arange(samples.shape[0])
    present = np.maximum(count, 1)
    # The same place for an odd count, the two middle ones for an even one.
    low = ordered[index, (present - 1) // 2]
    high = ordered[index, present // 2]
    return np.where(count > 0, (low + high) / 2.0, 0.0)


def _direction(
    values: np.ndarray, holes: np.ndarray, axis: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """One direction's contribution, along ``axis``, for the hole pixels only:
    their flat indices, value * weight, and weight."""
    length = values.shape[axis]
    flat = np.flatnonzero(holes)
    rows, cols = np.divmod(flat, values.shape[1])
    k = cols if axis == 1 else rows
    left, right = _run_edges(holes, axis, flat)
    has_left = left >= 0
    has_right = right < length

    vals = values.ravel()
    hole_flat = holes.ravel()

    def side(edge: np.ndarray, present: np.ndarray, step: int) -> tuple[np.ndarray, np.ndarray]:
        """Each side the median of up to SIDE_SAMPLES valid pixels beyond the
        gap's edge, placed at their middle: a spot of a pixel or two next to
        the gap does not streak across it, and a gradient stays exact."""
        offsets = np.arange(SIDE_SAMPLES) * step
        position = edge[:, None] + offsets[None, :]
        inside = present[:, None] & (position >= 0) & (position < length)
        clipped = np.clip(position, 0, length - 1)
        width = values.shape[1]
        flat_at = rows[:, None] * width + clipped if axis == 1 else clipped * width + cols[:, None]
        usable = inside & ~hole_flat[flat_at]
        # Stop at the first hole or the line's end.
        usable = np.cumprod(usable, axis=1).astype(bool)
        samples = np.where(usable, vals[flat_at], np.nan)
        count = usable.sum(axis=1)
        return _first_median(samples, count), edge + step * (np.maximum(count, 1) - 1) / 2.0

    left_value, left_at = side(left, has_left, -1)
    right_value, right_at = side(right, has_right, 1)

    both = has_left & has_right
    span = np.where(both, right - left, 1).astype(np.float64)
    t = (k - left_at) / np.where(both, right_at - left_at, 1.0)
    num = np.zeros(flat.shape, dtype=np.float64)
    den = np.zeros(flat.shape, dtype=np.float64)
    num[both] = ((left_value + (right_value - left_value) * t) / span)[both]
    den[both] = (1.0 / span)[both]
    # Open to one side only (the frame's edge): the nearest value, weighted
    # by the distance to it.
    only_left = has_left & ~has_right
    w = 1.0 / (k - left + 1)
    num[only_left] = (left_value * w)[only_left]
    den[only_left] = w[only_left]
    only_right = ~has_left & has_right
    w = 1.0 / (right - k + 1)
    num[only_right] = (right_value * w)[only_right]
    den[only_right] = w[only_right]
    return flat, num, den


def grow_holes(holes: np.ndarray, margin: int) -> np.ndarray:
    """``holes`` widened by ``margin`` pixels all round (a square: the 8 pixels
    around a flagged one at margin 1). Mirrors growHoles in the viewer."""
    r = max(0, min(MAX_MARGIN, int(margin or 0)))
    if not r or holes.ndim != 2 or not holes.any():
        return holes
    grown = holes.copy()
    for shift in range(1, r + 1):
        grown[:, shift:] |= holes[:, :-shift]
        grown[:, :-shift] |= holes[:, shift:]
    rows = grown.copy()
    for shift in range(1, r + 1):
        grown[shift:, :] |= rows[:-shift, :]
        grown[:-shift, :] |= rows[shift:, :]
    return grown


def inpaint(frame: np.ndarray, holes: np.ndarray) -> np.ndarray:
    """A float64 copy of ``frame`` with the ``holes`` filled; the input is unchanged."""
    values = np.array(frame, dtype=np.float64, copy=True)
    remaining = np.array(holes, dtype=bool, copy=True)
    if values.ndim != 2 or remaining.shape != values.shape or not remaining.any():
        return values
    values[remaining] = 0.0
    # Views on the (contiguous) copies: writing through them fills the frame.
    flat_values = values.reshape(-1)
    flat_remaining = remaining.reshape(-1)
    for _ in range(MAX_ROUNDS):
        # Both list the same holes, in the same (row-major) order.
        flat, num_h, den_h = _direction(values, remaining, axis=1)
        _, num_v, den_v = _direction(values, remaining, axis=0)
        num = num_h + num_v
        den = den_h + den_v
        now = den > 0
        if not now.any():
            break
        flat_values[flat[now]] = num[now] / den[now]
        flat_remaining[flat[now]] = False
        if not remaining.any():
            break
    # Nothing measured anywhere in reach (a frame that is all mask): zero.
    values[remaining] = 0.0
    return values
