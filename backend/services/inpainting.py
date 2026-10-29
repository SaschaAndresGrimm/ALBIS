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
# A slope is followed only where its mismatch is below this share of straight
# across. In flat, noisy areas the best of 61 slopes beats straight across by
# chance alone (to about 0.4 of it, sometimes 0.12) and filled the gap with
# streaks along it, reading pixels far to the side; across a real edge,
# straight across compares bright with dark, and the edge's slope brings the
# mismatch down to 0.07 of it, at most 0.17, on PILATUS4 projections.
EDGE_CLEAR = 0.2


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


def _edge_profiles(
    values: np.ndarray, holes: np.ndarray, axis: int, positions: np.ndarray, step: int
) -> tuple[np.ndarray, np.ndarray]:
    """The pixels beyond one side of a gap, across it, for each of
    ``positions`` at once: for every line crossing a position (rows for a run
    along a row, ``axis`` 1; columns for one along a column), the median of up
    to five valid pixels from there on in direction ``step``. Smoothed over
    EDGE_SMOOTH lines either way, holes bridged linearly, the ends continued.
    Returns the profiles, one row per position, and whether each has a value
    at all. All at once, not one by one: a few large array operations instead
    of a hundred small ones, which held the GIL and kept the export's threads
    waiting on each other."""
    height, width = values.shape
    if axis == 1:
        cross_len, line_len, stride = height, width, 1
        base = np.arange(height, dtype=np.int64) * width
    else:
        cross_len, line_len, stride = width, height, width
        base = np.arange(width, dtype=np.int64)
    count_pos = positions.size
    line_start = np.tile(base, count_pos)
    edge = np.repeat(positions.astype(np.int64), cross_len)
    hole_flat = holes.ravel()
    present = ~hole_flat[line_start + edge * stride]
    raw, _ = _side_medians(
        values.ravel(),
        hole_flat,
        line_start,
        stride,
        edge,
        present,
        np.full(edge.size, step, dtype=np.int64),
        line_len,
    )
    raw = raw.reshape(count_pos, cross_len)
    present = present.reshape(count_pos, cross_len)
    index = np.arange(cross_len)
    total = np.zeros((count_pos, cross_len))
    count = np.zeros((count_pos, cross_len), dtype=np.int64)
    for d in range(-EDGE_SMOOTH, EDGE_SMOOTH + 1):
        other = index + d
        inside = (other >= 0) & (other < cross_len)
        src = np.clip(other, 0, cross_len - 1)
        use = inside[None, :] & present[:, src]
        total = total + np.where(use, raw[:, src], 0.0)
        count = count + use
    have = count > 0
    smooth = np.where(have, total / np.maximum(count, 1), 0.0)
    before = np.maximum.accumulate(np.where(have, index, -1), axis=1)
    after = np.flip(
        np.minimum.accumulate(np.flip(np.where(have, index, cross_len), axis=1), axis=1), axis=1
    )
    out = smooth.copy()
    gr, gc = np.nonzero(~have & (before >= 0) & (after < cross_len))
    p, q = before[gr, gc], after[gr, gc]
    out[gr, gc] = smooth[gr, p] + (smooth[gr, q] - smooth[gr, p]) * ((gc - p) / (q - p))
    tr, tc = np.nonzero(~have & (before >= 0) & (after >= cross_len))
    out[tr, tc] = smooth[tr, before[tr, tc]]
    hr, hc = np.nonzero(~have & (before < 0) & (after < cross_len))
    out[hr, hc] = smooth[hr, after[hr, hc]]
    return out, have.any(axis=1)


def _profile_at(profiles: np.ndarray, which: np.ndarray, position: np.ndarray) -> np.ndarray:
    """Profiles ``which`` at fractional ``position``s, linearly between
    pixels, held at the ends."""
    n = profiles.shape[1]
    position = np.minimum(np.maximum(position, 0.0), n - 1)
    i = np.floor(position).astype(np.int64)
    low = profiles[which, i]
    high = profiles[which, np.minimum(i + 1, n - 1)]
    return np.where(i >= n - 1, low, low + (high - low) * (position - i))


def _edge_slopes(
    tops: np.ndarray,
    bottoms: np.ndarray,
    top_of: np.ndarray,
    bottom_of: np.ndarray,
    cross: np.ndarray,
    span: np.ndarray,
) -> np.ndarray:
    """The slope to fill each run along: of EDGE_SLOPES, the one whose
    profiles match best over the window (a steeper one having to do
    EDGE_PENALTY better), if it clearly beats straight across (EDGE_CLEAR);
    else 0. A sample sits at (column + window offset) + shift, the window is
    summed in order, and of equal costs the first slope wins: as the viewer.

    The runs of one gap share both profiles and the span, so for them the
    mismatch is worked out once per slope along the whole gap and summed over
    each run's window: about 13 times less work than per run, which is how
    the odd runs elsewhere are done."""
    slopes = np.array(EDGE_SLOPES, dtype=np.float64) / 10
    penalty = 1 + EDGE_PENALTY * np.abs(slopes)
    offsets = np.arange(-EDGE_WINDOW, EDGE_WINDOW + 1)
    cross_len = tops.shape[1]
    out = np.zeros(cross.shape)
    key = (top_of * (bottoms.shape[0] + 1) + bottom_of) * (int(span.max()) + 1) + span.astype(
        np.int64
    )
    groups, group_of = np.unique(key, return_inverse=True)
    small = np.zeros(cross.shape, dtype=bool)
    positions = np.arange(-EDGE_WINDOW, cross_len + EDGE_WINDOW, dtype=np.float64)
    for g in range(groups.size):
        members = np.flatnonzero(group_of == g)
        if members.size < 64:
            small[members] = True
            continue
        t_i, b_i, h = top_of[members[0]], bottom_of[members[0]], span[members[0]]
        xs = cross[members].astype(np.int64)
        # Every slope at once (one row each): the mismatch at every column
        # (+ window offset) of the gap, then each run's window, in order.
        shift = slopes[:, None] * h / 2
        d = np.abs(
            _profile_at(tops, t_i, positions[None, :] + shift)
            - _profile_at(bottoms, b_i, positions[None, :] - shift)
        )
        cost = np.zeros((slopes.size, xs.size))
        for j in offsets:
            cost = cost + d[:, xs + j + EDGE_WINDOW]
        choice = np.argmin(cost * penalty[:, None], axis=0)
        best_cost = cost[choice, np.arange(xs.size)]
        out[members] = np.where(best_cost < EDGE_CLEAR * cost[0], slopes[choice], 0.0)
    odd = np.flatnonzero(small)
    for block in range(0, odd.size, 512):
        rb = odd[block : block + 512]
        x = cross[rb].astype(np.float64)[:, None, None]
        h = span[rb][:, None, None]
        shift = slopes[None, :, None] * h / 2
        a = _profile_at(tops, top_of[rb][:, None, None], (x + offsets) + shift)
        b = _profile_at(bottoms, bottom_of[rb][:, None, None], (x + offsets) - shift)
        cost = np.cumsum(np.abs(a - b), axis=2)[:, :, -1]
        choice = np.argmin(cost * penalty, axis=1)
        rows = np.arange(choice.size)
        clear = cost[rows, choice] < EDGE_CLEAR * cost[:, 0]
        out[rb] = np.where(clear, slopes[choice], 0.0)
    return out


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
        unique, which = np.unique(positions, return_inverse=True)
        found, has_value = _edge_profiles(values, holes, axis, unique, step)
        return found, np.where(has_value[which], which, -1)

    tops, top_of = profiles(r_left, -1)
    bottoms, bottom_of = profiles(r_right, 1)
    usable = (top_of >= 0) & (bottom_of >= 0)
    if not usable.any():
        return
    r = np.flatnonzero(usable)
    slope = _edge_slopes(tops, bottoms, top_of[r], bottom_of[r], r_cross[r], span[r])
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


# ---------------------------------------------------------------------------
# Poisson noise for filled pixels
#
# A filled gap is smoother than the measured pixels around it, which gives it
# away in an image. On request, each filled pixel's value is taken as the
# expected count there and replaced by a Poisson-distributed one around it.
# Invented, not measured: off by default and stated in the file's header.
#
# The viewer adds the same noise (frontend/modules/inpainting.js): a seed from
# the frame's own pixel values -- so a frame always gets the same noise, and
# every frame of a series different noise -- and per pixel a counter-based
# hash of seed and position, in 32-bit integer arithmetic both languages do
# alike. Below NOISE_NORMAL_FROM counts the draw inverts the Poisson
# distribution with one uniform; above, the normal approximation, rounded.
# Flatfield-corrected values are taken back to raw counts for the draw (times
# the flat, over the scale's factor) and forward again, so the noise has the
# size it would have had in the measurement, on either scale.
# ---------------------------------------------------------------------------

NOISE_NORMAL_FROM = 30
NOISE_SEED_SAMPLES = 64
_U32 = np.uint32


def _hash32(x: np.ndarray) -> np.ndarray:
    """A 32-bit integer hash ("lowbias32"), elementwise; wraps as uint32."""
    x = np.asarray(x, dtype=_U32).copy()
    with np.errstate(over="ignore"):
        x ^= x >> _U32(16)
        x *= _U32(0x7FEB352D)
        x ^= x >> _U32(15)
        x *= _U32(0x846CA68B)
        x ^= x >> _U32(16)
    return x


def noise_seed(raw: np.ndarray) -> int:
    """The noise seed of a frame, from up to 64 of its raw values evenly
    spread over it: whole numbers, modulo 2**32, non-finite as 0."""
    flat = np.asarray(raw).ravel()
    n = flat.size
    samples = min(NOISE_SEED_SAMPLES, n)
    seed = 0x9E3779B9
    for k in range(samples):
        index = (k * (n - 1)) // (samples - 1) if samples > 1 else 0
        value = float(flat[index])
        whole = int(value) % 2**32 if np.isfinite(value) else 0
        seed = int(_hash32(np.array([seed ^ whole], dtype=np.uint64).astype(_U32))[0])
    return seed


def _uniform(seed: int, index: np.ndarray, j: int) -> np.ndarray:
    """A uniform number in (0, 1) per pixel ``index``, the ``j``-th."""
    counter = (np.asarray(index, dtype=np.uint64) * 2 + j).astype(_U32)
    h = _hash32(_hash32(counter) ^ _U32(seed))
    return ((h >> _U32(8)).astype(np.float64) + 0.5) / 16777216


def _poisson(lam: np.ndarray, seed: int, index: np.ndarray) -> np.ndarray:
    """A Poisson-distributed count per pixel ``index`` with mean ``lam``."""
    lam = np.asarray(lam, dtype=np.float64)
    out = np.zeros(lam.shape)
    positive = lam > 0
    small = positive & (lam < NOISE_NORMAL_FROM)
    if small.any():
        lam_s = lam[small]
        u = _uniform(seed, index[small], 0)
        p = np.exp(-lam_s)
        cdf = p.copy()
        k = np.zeros(lam_s.shape)
        active = u > cdf
        step = 0
        while active.any() and step < 1000:
            step += 1
            k[active] = step
            p = p * lam_s / step
            cdf = cdf + p
            active &= u > cdf
        out[small] = k
    large = positive & ~small
    if large.any():
        lam_l = lam[large]
        u1 = _uniform(seed, index[large], 0)
        u2 = _uniform(seed, index[large], 1)
        z = np.sqrt(-2 * np.log(u1)) * np.cos(2 * np.pi * u2)
        out[large] = np.maximum(0.0, np.floor(lam_l + np.sqrt(lam_l) * z + 0.5))
    return out


def add_noise(
    filled: np.ndarray,
    index: np.ndarray,
    raw: np.ndarray,
    flat=None,
    scale: str = "counts",
) -> np.ndarray:
    """The values ``filled`` at the flat pixel ``index``es of a frame, with
    Poisson noise. ``raw`` is the frame as read, for the seed; ``flat`` the
    Flat it was divided by, if any, on ``scale``. As the viewer, the values
    are taken as float32, which is how it holds them."""
    seed = noise_seed(raw)
    values = np.asarray(filled, dtype=np.float32).astype(np.float64)
    if flat is None:
        return _poisson(values, seed, index)
    factor = 1.0 if scale == "transmission" else flat.mean
    flat_data = np.asarray(flat.data, dtype=np.float32).astype(np.float64).ravel()[index]
    divisor = np.where(flat.holes.ravel()[index], flat.mean, flat_data)
    counts = _poisson(values * divisor / factor, seed, index)
    return counts / divisor * factor
