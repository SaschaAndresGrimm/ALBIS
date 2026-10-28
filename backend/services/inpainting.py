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


# Compare-and-swap pairs of an optimal sorting network for five values: on
# columns of samples, nine vectorised min/max steps sort every row at once,
# several times faster than np.sort on rows that short.
_SORT5 = ((0, 1), (3, 4), (2, 4), (2, 3), (0, 3), (0, 2), (1, 4), (1, 3), (1, 2))
assert SIDE_SAMPLES == 5, "_SORT5 sorts exactly five samples"


def _side_medians(
    vals: np.ndarray,
    hole_flat: np.ndarray,
    line_start: np.ndarray,
    stride: int,
    edge: np.ndarray,
    present: np.ndarray,
    step: np.ndarray,
    length: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Each gap side's value and where it sits: the median of up to
    SIDE_SAMPLES valid pixels beyond the gap's edge, placed at their middle,
    so a spot of a pixel or two next to the gap does not streak across it and
    a gradient stays exact. Both sides of every gap come in at once (`step`
    -1 or +1), sampled column by column: one pass, few temporaries."""
    columns = []
    count = np.zeros(edge.shape, dtype=np.int64)
    usable = present.copy()
    for j in range(SIDE_SAMPLES):
        position = edge + step * j
        # Stop at the first hole or the line's end.
        usable &= (position >= 0) & (position < length)
        at = line_start + np.clip(position, 0, length - 1) * stride
        usable &= ~hole_flat[at]
        # Inf sorts after every value, as NaN did with np.sort.
        columns.append(np.where(usable, vals[at], np.inf))
        count += usable
    for a, b in _SORT5:
        low = np.minimum(columns[a], columns[b])
        columns[b] = np.maximum(columns[a], columns[b])
        columns[a] = low
    ordered = np.stack(columns)
    index = np.arange(edge.shape[0])
    filled = np.maximum(count, 1)
    # The same place for an odd count, the two middle ones for an even one.
    low = ordered[(filled - 1) // 2, index]
    high = ordered[filled // 2, index]
    value = np.where(count > 0, (low + high) / 2.0, 0.0)
    return value, edge + step * (filled - 1) / 2.0


def _direction(
    values: np.ndarray,
    holes: np.ndarray,
    axis: int,
    flat: np.ndarray,
    rows: np.ndarray,
    cols: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """One direction's contribution, along ``axis``, for the hole pixels
    ``flat`` (row-major, at ``rows``, ``cols``): value * weight, and weight."""
    length = values.shape[axis]
    width = values.shape[1]
    k = cols if axis == 1 else rows
    left, right = _run_edges(holes, axis, flat)
    has_left = left >= 0
    has_right = right < length

    n = flat.shape[0]
    line_start = rows * width if axis == 1 else cols
    sides_value, sides_at = _side_medians(
        values.ravel(),
        holes.ravel(),
        np.concatenate((line_start, line_start)),
        1 if axis == 1 else width,
        np.concatenate((left, right)),
        np.concatenate((has_left, has_right)),
        np.concatenate((np.full(n, -1, dtype=np.int64), np.ones(n, dtype=np.int64))),
        length,
    )
    left_value, right_value = sides_value[:n], sides_value[n:]
    left_at, right_at = sides_at[:n], sides_at[n:]

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
    return num, den


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
        # The holes once a round, row-major, for both directions.
        flat = np.flatnonzero(remaining)
        rows, cols = np.divmod(flat, values.shape[1])
        num_h, den_h = _direction(values, remaining, 1, flat, rows, cols)
        num_v, den_v = _direction(values, remaining, 0, flat, rows, cols)
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
