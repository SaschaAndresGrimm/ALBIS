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

Besides that, two further methods (``METHODS``):

- "nearest": each hole takes the value of the nearer side of its gap (the
  same median of five), in its row or its column, whichever is closer: sharp,
  never blurred, but a slanted edge becomes a step.
- "edges": interpolation along the edges. A run of holes long enough to be a
  gap (EDGE_MIN_RUN) with valid pixels on both sides is filled along the slope
  where the pixels beyond its two sides match best, not straight across: an
  edge crossing a module gap at an angle carries on through it instead of
  turning into a blurred step. The profiles either side -- the median of five
  beyond each boundary pixel -- are smoothed a little along the gap, which
  also keeps neighbouring columns from filling as stripes. Shorter runs, and
  runs open to one side, are interpolated as by "interpolate".

Both implementations are held to the same cases in
``tests/fixtures/inpainting_cases.json``, and do their arithmetic in the same
order, so the two give the same values.
"""

from __future__ import annotations

import numpy as np

MAX_ROUNDS = 8
# Pixels each side's value is taken from: the median of five rejects a spot of
# up to two pixels at a gap's edge.
SIDE_SAMPLES = 5
# The widest margin around flagged pixels that may be filled too.
MAX_MARGIN = 5
METHODS = ("interpolate", "edges", "nearest")
# "edges": the shortest run of holes treated as a gap to follow edges across.
# Narrower ones -- a 2-pixel gap with its margin -- gain next to nothing from
# a slope and made the search three times as long.
EDGE_MIN_RUN = 6
# Slopes tried, in tenths of a pixel sideways per pixel across: up to 3, an
# edge down to about 18 degrees from the gap. Straight across first, then
# outwards, so of two equally good slopes the less steep one wins.
EDGE_SLOPES = (0, *(sign * m for m in range(1, 31) for sign in (1, -1)))
# Half the window the two sides are compared over, and of the smoothing
# along the gap, in pixels; and how much a steeper slope must do better.
EDGE_WINDOW = 6
EDGE_SMOOTH = 2
EDGE_PENALTY = 0.05


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
    method: str = "interpolate",
) -> tuple[np.ndarray, np.ndarray]:
    """One direction's contribution, along ``axis``, for the hole pixels
    ``flat`` (row-major, at ``rows``, ``cols``): value * weight, and weight;
    for "nearest", the nearer side's value and its distance (inf where the
    line has no valid pixel)."""
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

    if method == "nearest":
        to_left = (k - left).astype(np.float64)
        to_right = (right - k).astype(np.float64)
        # The nearer side; on a tie, the one before.
        use_right = has_right & (~has_left | (to_right < to_left))
        value = np.where(use_right, right_value, left_value)
        distance = np.where(use_right, to_right, np.where(has_left, to_left, np.inf))
        return value, distance

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
    if method == "edges":
        _along_edges(values, holes, axis, rows if axis == 1 else cols, k, left, right, num)
    return num, den


def _edge_profile(values: np.ndarray, holes: np.ndarray, axis: int, pos: int, step: int):
    """The pixels beyond one side of a gap, across it: for every line crossing
    position ``pos`` (rows for a run along a row, ``axis`` 1; columns for one
    along a column), the median of up to five valid pixels from there on in
    direction ``step``. Smoothed over EDGE_SMOOTH lines either way, holes
    bridged linearly, the ends continued. None if no line has a value."""
    height, width = values.shape
    if axis == 1:
        cross_len, line_len, stride = height, width, 1
        line_start = np.arange(height, dtype=np.int64) * width
    else:
        cross_len, line_len, stride = width, height, width
        line_start = np.arange(width, dtype=np.int64)
    hole_flat = holes.ravel()
    present = ~hole_flat[line_start + pos * stride]
    raw, _ = _side_medians(
        values.ravel(),
        hole_flat,
        line_start,
        stride,
        np.full(cross_len, pos, dtype=np.int64),
        present,
        np.full(cross_len, step, dtype=np.int64),
        line_len,
    )
    index = np.arange(cross_len)
    total = np.zeros(cross_len)
    count = np.zeros(cross_len, dtype=np.int64)
    for d in range(-EDGE_SMOOTH, EDGE_SMOOTH + 1):
        other = index + d
        inside = (other >= 0) & (other < cross_len)
        src = np.clip(other, 0, cross_len - 1)
        use = inside & present[src]
        total = total + np.where(use, raw[src], 0.0)
        count = count + use
    have = count > 0
    if not have.any():
        return None
    smooth = np.where(have, total / np.maximum(count, 1), 0.0)
    before = np.maximum.accumulate(np.where(have, index, -1))
    after = np.flip(np.minimum.accumulate(np.flip(np.where(have, index, cross_len))))
    out = smooth.copy()
    gap = ~have & (before >= 0) & (after < cross_len)
    p, q, c = before[gap], after[gap], index[gap]
    out[gap] = smooth[p] + (smooth[q] - smooth[p]) * ((c - p) / (q - p))
    tail = ~have & (before >= 0) & (after >= cross_len)
    out[tail] = smooth[before[tail]]
    head = ~have & (before < 0) & (after < cross_len)
    out[head] = smooth[after[head]]
    return out


def _profile_at(profiles: np.ndarray, which: np.ndarray, position: np.ndarray) -> np.ndarray:
    """Profiles ``which`` at fractional ``position``s, linearly between
    pixels, held at the ends."""
    n = profiles.shape[1]
    position = np.minimum(np.maximum(position, 0.0), n - 1)
    i = np.floor(position).astype(np.int64)
    low = profiles[which, i]
    high = profiles[which, np.minimum(i + 1, n - 1)]
    return np.where(i >= n - 1, low, low + (high - low) * (position - i))


def _along_edges(
    values: np.ndarray,
    holes: np.ndarray,
    axis: int,
    cross: np.ndarray,
    k: np.ndarray,
    left: np.ndarray,
    right: np.ndarray,
    num: np.ndarray,
) -> None:
    """For the holes in runs long enough to be a gap, with valid pixels on
    both sides, the value along the best-matching slope instead of straight
    across, in ``num`` (their weight, 1/span, stays)."""
    length = values.shape[axis]
    long_run = (left >= 0) & (right < length) & ((right - left - 1) >= EDGE_MIN_RUN)
    if not long_run.any():
        return
    picked = np.flatnonzero(long_run)
    h_cross, h_left, h_right, h_k = cross[picked], left[picked], right[picked], k[picked]
    _, first, run_of = np.unique(
        h_cross * (length + 1) + h_left, return_index=True, return_inverse=True
    )
    r_cross, r_left, r_right = h_cross[first], h_left[first], h_right[first]
    span = (r_right - r_left).astype(np.float64)

    def profiles(positions: np.ndarray, step: int) -> tuple[np.ndarray, np.ndarray]:
        found, which = [], np.full(positions.shape, -1, dtype=np.int64)
        for pos in np.unique(positions):
            profile = _edge_profile(values, holes, axis, int(pos), step)
            if profile is not None:
                which[positions == pos] = len(found)
                found.append(profile)
        return (np.stack(found) if found else np.zeros((0, 1))), which

    tops, top_of = profiles(r_left, -1)
    bottoms, bottom_of = profiles(r_right, 1)
    usable = (top_of >= 0) & (bottom_of >= 0)
    if not usable.any():
        return
    r = np.flatnonzero(usable)
    slopes = np.array(EDGE_SLOPES, dtype=np.float64) / 10
    penalty = 1 + EDGE_PENALTY * np.abs(slopes)
    offsets = np.arange(-EDGE_WINDOW, EDGE_WINDOW + 1, dtype=np.float64)
    slope = np.zeros(r.shape)
    # Every slope and window position at once, a block of runs at a time.
    # The window is summed in order (a running sum), as the viewer's loop
    # does, and of equal costs the first slope in EDGE_SLOPES wins, as a
    # strict "better than" in that order would pick.
    for block in range(0, r.size, 512):
        rb = r[block : block + 512]
        x = r_cross[rb].astype(np.float64)[:, None, None]
        h = span[rb][:, None, None]
        shift = slopes[None, :, None] * h / 2
        a = _profile_at(tops, top_of[rb][:, None, None], x + shift + offsets)
        b = _profile_at(bottoms, bottom_of[rb][:, None, None], x - shift + offsets)
        cost = np.cumsum(np.abs(a - b), axis=2)[:, :, -1]
        slope[block : block + 512] = slopes[np.argmin(cost * penalty, axis=1)]
    # Back to the holes of the usable runs.
    run_slope = np.full(span.shape, np.nan)
    run_slope[r] = slope
    hole_slope = run_slope[run_of]
    ok = ~np.isnan(hole_slope)
    v = hole_slope[ok]
    hx = h_cross[ok].astype(np.float64)
    to_top = (h_k[ok] - h_left[ok]).astype(np.float64)
    to_bottom = (h_right[ok] - h_k[ok]).astype(np.float64)
    gap_span = (h_right[ok] - h_left[ok]).astype(np.float64)
    t = to_top / gap_span
    top = _profile_at(tops, top_of[run_of[ok]], hx + v * to_top)
    bottom = _profile_at(bottoms, bottom_of[run_of[ok]], hx - v * to_bottom)
    value = (1 - t) * top + t * bottom
    num[picked[ok]] = value * (1.0 / gap_span)


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


def inpaint(frame: np.ndarray, holes: np.ndarray, method: str = "interpolate") -> np.ndarray:
    """A float64 copy of ``frame`` with the ``holes`` filled by ``method``
    (one of METHODS); the input is unchanged."""
    if method not in METHODS:
        raise ValueError(f"unknown inpainting method {method!r}")
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
        a_h, b_h = _direction(values, remaining, 1, flat, rows, cols, method)
        a_v, b_v = _direction(values, remaining, 0, flat, rows, cols, method)
        if method == "nearest":
            # The nearer of the two; on a tie, the row's.
            use_column = b_v < b_h
            filled = np.where(use_column, a_v, a_h)
            now = np.isfinite(np.minimum(b_h, b_v))
        else:
            num = a_h + a_v
            den = b_h + b_v
            now = den > 0
            filled = num / np.where(now, den, 1.0)
        if not now.any():
            break
        flat_values[flat[now]] = filled[now]
        flat_remaining[flat[now]] = False
        if not remaining.any():
            break
    # Nothing measured anywhere in reach (a frame that is all mask): zero.
    values[remaining] = 0.0
    return values
