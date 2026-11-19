/**
 * Axis ticks at round numbers for the canvas plots.
 *
 * A tick sits on a multiple of a "nice" step (1, 2 or 5 times a power of ten,
 * and 25, 250, ... from 25 up), never on an even fraction of the data range:
 * 0, 0.02, 0.04 rather than 0, 0.043, 0.087. The axis keeps fitting the data;
 * its ends simply carry no label when they are not round. Each picker takes
 * the densest step whose labels still have room, so a taller plot gets more
 * ticks. Pure: label widths come in through a `measure` callback.
 */

const MAX_TICKS = 400;
const EPS = 1e-9;

function niceSteps(lo, hi, { integer = false, allowQuarter = true } = {}) {
  const steps = [];
  if (!(hi > 0) || !(lo > 0)) return steps;
  const kStart = Math.floor(Math.log10(lo)) - 1;
  const kEnd = Math.ceil(Math.log10(hi)) + 1;
  for (let k = kStart; k <= kEnd; k += 1) {
    for (const m of [1, 2, 2.5, 5]) {
      // 2.5 only from 25 up: 0.025 or 2.5 need a decimal more than the
      // neighbouring steps and read as less round than 0.02 or 2.
      if (m === 2.5 && (!allowQuarter || k < 1)) continue;
      const step = Number((m * 10 ** k).toPrecision(12));
      if (step < lo * (1 - EPS) || step > hi * (1 + EPS)) continue;
      if (integer && !Number.isInteger(step)) continue;
      steps.push(step);
    }
  }
  return steps;
}

function mantissaOf(step) {
  return Number((step / 10 ** Math.floor(Math.log10(step) + EPS)).toPrecision(6));
}

/** Decimals needed to print every multiple of `step` exactly. */
export function stepDecimals(step) {
  if (!Number.isFinite(step) || step <= 0) return 0;
  for (let d = 0; d <= 12; d += 1) {
    const scaled = step * 10 ** d;
    if (Math.abs(scaled - Math.round(scaled)) < 1e-6 * Math.max(1, scaled)) return d;
  }
  return 12;
}

function multiples(min, max, step) {
  const first = Math.ceil(min / step - EPS);
  const last = Math.floor(max / step + EPS);
  if (last - first + 1 > MAX_TICKS) return null;
  const decimals = stepDecimals(step);
  const values = [];
  for (let k = first; k <= last; k += 1) {
    const value = Number((k * step).toFixed(decimals));
    values.push(value === 0 ? 0 : value);
  }
  return values;
}

/**
 * One tick value as text, with as many decimals as its step needs, so every
 * label on an axis has the same precision. `scale` is the largest magnitude on
 * the axis: from a million up, or for steps finer than 0.0001, the whole axis
 * switches to exponent notation rather than mixing 500000 with 1.0e+6.
 */
export function formatTickValue(value, step, scale = Math.abs(value)) {
  if (!Number.isFinite(value)) return "-";
  if (value === 0) return "0";
  const abs = Math.abs(value);
  const safeStep = Number.isFinite(step) && step > 0 ? step : abs;
  const safeScale = Number.isFinite(scale) && scale > 0 ? Math.max(scale, abs) : abs;
  if (safeScale >= 1e6 || safeStep < 1e-4) {
    const leading = Math.floor(Math.log10(safeStep) + EPS);
    const extra = stepDecimals(safeStep / 10 ** leading);
    const digits = Math.floor(Math.log10(safeScale) + EPS) - leading + extra;
    return value.toExponential(Math.max(0, Math.min(6, digits)));
  }
  return value.toFixed(stepDecimals(safeStep));
}

/**
 * Ticks for a linear axis from `min` to `max` drawn over `lengthPx` pixels.
 * A step fits when neighbouring ticks are at least the widest label (from
 * `measure`, 0 without it) plus `minGapPx` apart. `integer` keeps every tick
 * on a whole number, for pixel indices and integer intensities.
 * Returns { values, labels, step }.
 */
export function pickLinearTicks({ min, max, lengthPx, minGapPx = 0, measure = null, integer = false }) {
  if (!Number.isFinite(min) || !Number.isFinite(max)) return { values: [], labels: [], step: 0 };
  if (!(max > min) || !(lengthPx > 0)) {
    const value = integer ? Math.round(min) : min;
    return { values: [value], labels: [formatTickValue(value, Math.abs(value) || 1)], step: 0 };
  }
  const span = max - min;
  const scale = Math.max(Math.abs(min), Math.abs(max));
  let fallback = null;
  for (const step of niceSteps(span / MAX_TICKS, span * 10, { integer })) {
    const values = multiples(min, max, step);
    if (!values || !values.length) continue;
    const labels = values.map((value) => formatTickValue(value, step, scale));
    const widest = measure ? labels.reduce((w, label) => Math.max(w, measure(label)), 0) : 0;
    const result = { values, labels, step };
    if ((lengthPx * step) / span >= widest + minGapPx) return result;
    fallback = result;
  }
  return fallback || { values: [], labels: [], step: 0 };
}

const toLogDisplay = (value) => Math.log10(1 + Math.max(0, value));

/**
 * Ticks for the log axis, which plots log10(1 + value) so that zero stays on
 * the axis. `min` and `max` are in that display space. Ticks go on 0 and the
 * powers of ten (every second, third ... one when the decades are tight), then
 * 5 and 2 x 10^k fill in wherever there is room. A range too narrow for two
 * such ticks gets linear ones. Returns { values, labels } in data units.
 */
export function pickLogTicks({ min, max, lengthPx, minGapPx = 0 }) {
  const empty = { values: [], labels: [] };
  if (!Number.isFinite(min) || !Number.isFinite(max) || !(max > min) || !(lengthPx > 0)) return empty;
  const lo = Math.max(0, 10 ** min - 1);
  const hi = 10 ** max - 1;
  const toPx = (value) => ((toLogDisplay(value) - min) * lengthPx) / (max - min);
  const within = (value) => value >= lo * (1 - EPS) - EPS && value <= hi * (1 + EPS);
  const kLo = Math.floor(Math.log10(lo > 0 ? lo : Math.min(hi, 1)) + EPS) - (lo > 0 ? 0 : 2);
  const kHi = Math.ceil(Math.log10(hi));
  const power = (m, k) => Number((m * 10 ** k).toPrecision(12));
  const roomFor = (value, list) => list.every((other) => Math.abs(toPx(value) - toPx(other)) >= minGapPx);

  const base = within(0) ? [0] : [];
  let accepted = null;
  let decadeStride = 1;
  for (const stride of [1, 2, 3, 4, 5, 10, 20]) {
    const ticks = [...base];
    let fits = true;
    for (let k = kLo; k <= kHi; k += 1) {
      if (((k % stride) + stride) % stride !== 0) continue;
      const value = power(1, k);
      if (!within(value)) continue;
      // A power of ten crowding zero is dropped, not counted against the
      // stride: on a counts axis 1 sits right above 0.
      if (base.length && Math.abs(toPx(value) - toPx(0)) < minGapPx) continue;
      if (!roomFor(value, ticks)) fits = false;
      ticks.push(value);
    }
    if (fits) {
      accepted = ticks;
      decadeStride = stride;
      break;
    }
  }
  accepted = accepted || [...base];
  if (decadeStride === 1) {
    for (const m of [5, 2]) {
      for (let k = kLo; k <= kHi; k += 1) {
        const value = power(m, k);
        if (within(value) && roomFor(value, accepted)) accepted.push(value);
      }
    }
  }
  accepted.sort((a, b) => a - b);
  if (accepted.length < 2) {
    const linear = pickLinearTicks({ min: lo, max: hi, lengthPx, minGapPx });
    if (linear.values.length > accepted.length) return { values: linear.values, labels: linear.labels };
  }
  const scale = Math.max(hi, 0);
  const labels = accepted.map((value) =>
    formatTickValue(value, value > 0 ? 10 ** Math.floor(Math.log10(value) + EPS) : 1, scale),
  );
  return { values: accepted, labels };
}

/**
 * Ticks for a secondary axis whose value is a non-linear function of the
 * position, such as resolution d or Q above a radial profile. `samples` is the
 * value at a run of pixel positions ({ px, value }, ascending px, NaN where it
 * is undefined); a tick goes wherever the curve crosses a round value, so a
 * profile through the beam centre gets ticks on both sides.
 *
 * Steps are tried from coarse to fine. With `uniform` (an axis that is close
 * to linear) the axis gets the finest single step whose labels all fit, so the
 * spacing stays even; without it labels from each finer step are kept wherever
 * they do not collide, so the finer steps fill the room left where the axis
 * is stretched (d, which goes as 1/r, gets 10, 5, 4, 3, 2.5, 2). Labels stay
 * within `leftPx` to `rightPx`. Returns [{ px, value, label }] in px order.
 */
export function pickMappedTicks({ samples, measure, minGapPx = 6, uniform = false, leftPx, rightPx }) {
  const finite = (samples || []).filter((s) => Number.isFinite(s.value) && Number.isFinite(s.px));
  if (finite.length < 2) return [];
  let uMin = Infinity;
  let uMax = -Infinity;
  for (const s of finite) {
    uMin = Math.min(uMin, s.value);
    uMax = Math.max(uMax, s.value);
  }
  if (!(uMax > uMin)) return [];
  const span = uMax - uMin;
  // Steps fine enough for where the curve is flattest, not just span / 400:
  // d near the beam centre runs into the hundreds, and a radial profile's d
  // axis stopped at 3 because steps of 1 and 0.5 were never tried.
  let minAbs = Infinity;
  for (const s of finite) if (s.value !== 0) minAbs = Math.min(minAbs, Math.abs(s.value));
  const finest = Math.max(span / 1e6, Math.min(span / MAX_TICKS, 10 ** (Math.floor(Math.log10(minAbs)) - 2)));
  const steps = niceSteps(finest, span, { allowQuarter: false }).reverse();
  const accepted = [];
  const seen = new Set();
  const collides = (left, right, list) =>
    list.some((tick) => left < tick.right + minGapPx && right > tick.left - minGapPx);
  for (const step of steps) {
    const decimals = stepDecimals(step);
    const added = [];
    const levelKeys = new Set();
    let rejected = false;
    // Round values are looked for segment by segment, so a fine step costs
    // nothing where the curve is steep: there the segment holds more than
    // two of them, they could never be told apart, and it is skipped.
    for (let i = 1; i < samples.length; i += 1) {
      const a = samples[i - 1];
      const b = samples[i];
      if (!Number.isFinite(a.value) || !Number.isFinite(b.value)) continue;
      const first = Math.ceil(Math.min(a.value, b.value) / step - EPS);
      const last = Math.floor(Math.max(a.value, b.value) / step + EPS);
      if (last - first > 2) {
        rejected = true;
        continue;
      }
      for (let k = first; k <= last; k += 1) {
        const value = Number((k * step).toFixed(decimals)) || 0;
        // On a stretched axis a value is round relative to its own size: 2.5,
        // 3.5 and 15 are, 2.3 and 3.4 are not, however much room there is.
        if (!uniform && value !== 0 && (mantissaOf(Math.abs(value)) * 2) % 1 !== 0) continue;
        const da = a.value - value;
        const db = b.value - value;
        const px = da === db ? a.px : a.px + ((b.px - a.px) * da) / (da - db);
        const key = `${value}@${Math.round(px)}`;
        // An exact hit on a sample shows up in the segments on both sides.
        if (levelKeys.has(key)) continue;
        levelKeys.add(key);
        if (!uniform && seen.has(key)) continue;
        const label = formatTickValue(value, step);
        const width = measure ? measure(label) : 0;
        const left = px - width / 2;
        const right = px + width / 2;
        if (left < leftPx || right > rightPx) continue;
        if ((!uniform && collides(left, right, accepted)) || collides(left, right, added)) {
          rejected = true;
          continue;
        }
        added.push({ px, value, label, left, right, key });
      }
    }
    if (uniform) {
      // One step for the whole axis: the finest whose labels all fit.
      if (rejected && accepted.length >= 2) break;
      accepted.length = 0;
      seen.clear();
    }
    for (const tick of added) {
      accepted.push(tick);
      seen.add(tick.key);
    }
    if (uniform && rejected) break;
  }
  return accepted
    .sort((a, b) => a.px - b.px)
    .map(({ px, value, label }) => ({ px, value, label }));
}
