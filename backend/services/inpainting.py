"""Inpainting for exported frames: gaps and defective pixels filled from their
neighbours.

Mirrors ``frontend/modules/inpainting.js``, which fills the frame on screen;
see there for the method. In short: every hole pixel is interpolated along its
row and along its column between the valid pixels on either side of its gap
(each side the median of the five nearest), and the two are combined with weights
1/span, so the short way across a gap wins. Pixels with no valid pixel in
their row or column -- where two full-length gaps cross -- are filled in a
further round from the ones filled before. Vectorised: each round takes two passes
over the frame to find every hole's neighbours, and the arithmetic runs on the
hole pixels only, whatever the gap width (about 0.2 s for an EIGER2 4M frame).

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


def _direction(
    values: np.ndarray, holes: np.ndarray, axis: int
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """One direction's contribution, along ``axis``, for the hole pixels only:
    their flat indices, value * weight, and weight."""
    length = values.shape[axis]
    shape = [1, 1]
    shape[axis] = length
    pos = np.arange(length, dtype=np.int32).reshape(shape)
    # The nearest non-hole position at or before / at or after each pixel:
    # the only passes over the whole frame. Everything after is per hole.
    before = np.maximum.accumulate(np.where(holes, np.int32(-1), pos), axis=axis)
    flipped = np.flip(np.where(holes, np.int32(length), pos), axis=axis)
    after = np.flip(np.minimum.accumulate(flipped, axis=axis), axis=axis)

    flat = np.flatnonzero(holes)
    rows, cols = np.divmod(flat, values.shape[1])
    k = cols if axis == 1 else rows
    left = before.ravel()[flat].astype(np.int64)
    right = after.ravel()[flat].astype(np.int64)
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
        with np.errstate(all="ignore"):
            value = np.where(
                count > 0, np.nanmedian(np.where(count[:, None] > 0, samples, 0.0), axis=1), 0.0
            )
        return value, edge + step * (np.maximum(count, 1) - 1) / 2.0

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
