import {
  formatTickValue,
  pickLinearTicks,
  pickLogTicks,
  pickMappedTicks,
  stepDecimals,
} from "../modules/plot_tick_utils.js";

const measure = (label) => label.length * 6;
const logDisplay = (value) => Math.log10(1 + value);

describe("plot_tick_utils", () => {
  /**
   * The ROI profile plots split the padded data range into equal parts and
   * labelled whatever value fell there: a profile peaking at 0.13 read
   * 0, 0.043, 0.087, 0.13. Ticks now sit on multiples of a round step.
   */
  describe("pickLinearTicks", () => {
    it("labels a profile peaking at 0.13 in round steps", () => {
      const ticks = pickLinearTicks({ min: 0, max: 0.134, lengthPx: 200, minGapPx: 28 });
      expect(ticks.labels).toEqual(["0", "0.02", "0.04", "0.06", "0.08", "0.10", "0.12"]);
    });

    it("leaves the ends unlabelled when they are not round", () => {
      const ticks = pickLinearTicks({ min: 0.017, max: 0.044, lengthPx: 200, minGapPx: 28 });
      expect(ticks.labels).toEqual(["0.020", "0.025", "0.030", "0.035", "0.040"]);
    });

    it("puts pixel columns on round integers", () => {
      const ticks = pickLinearTicks({
        min: 562, max: 1699, lengthPx: 400, minGapPx: 16, measure, integer: true,
      });
      expect(ticks.labels).toEqual(["600", "800", "1000", "1200", "1400", "1600"]);
    });

    it("never gives an integer axis a fractional step", () => {
      const ticks = pickLinearTicks({ min: 0, max: 3, lengthPx: 400, minGapPx: 16, measure, integer: true });
      expect(ticks.values).toEqual([0, 1, 2, 3]);
    });

    it("thins the ticks when the plot is short", () => {
      const tall = pickLinearTicks({ min: 0, max: 100, lengthPx: 300, minGapPx: 28 });
      const short = pickLinearTicks({ min: 0, max: 100, lengthPx: 80, minGapPx: 28 });
      expect(short.values.length).toBeLessThan(tall.values.length);
      expect(short.values.length).toBeGreaterThanOrEqual(2);
    });

    it("keeps a negative range", () => {
      const ticks = pickLinearTicks({ min: -2.3, max: 92, lengthPx: 200, minGapPx: 28 });
      expect(ticks.values[0]).toBe(0);
      const below = pickLinearTicks({ min: -50, max: -10, lengthPx: 200, minGapPx: 28 });
      expect(below.labels).toEqual(["-50", "-40", "-30", "-20", "-10"]);
    });

    it("prints multiples of the step without float noise", () => {
      const ticks = pickLinearTicks({ min: 0, max: 0.7, lengthPx: 400, minGapPx: 28 });
      for (const value of ticks.values) {
        expect(String(value).length).toBeLessThanOrEqual(4);
      }
    });

    it("uses exponent notation for the whole axis, not part of it", () => {
      const ticks = pickLinearTicks({ min: 0, max: 3.5e6, lengthPx: 200, minGapPx: 28 });
      expect(ticks.labels).toContain("5.0e+5");
      expect(ticks.labels).toContain("1.0e+6");
    });

    it("returns one tick for an empty range", () => {
      expect(pickLinearTicks({ min: 4, max: 4, lengthPx: 200 }).values).toEqual([4]);
      expect(pickLinearTicks({ min: Number.NaN, max: 1, lengthPx: 200 }).values).toEqual([]);
    });
  });

  describe("pickLogTicks", () => {
    it("labels zero and the powers of ten on a counts axis", () => {
      const ticks = pickLogTicks({ min: 0, max: logDisplay(5e5), lengthPx: 200, minGapPx: 28 });
      expect(ticks.labels).toEqual(["0", "10", "100", "1000", "10000", "100000"]);
    });

    it("fills in 2 and 5 where there is room", () => {
      const ticks = pickLogTicks({ min: 0, max: logDisplay(80), lengthPx: 200, minGapPx: 28 });
      expect(ticks.labels).toEqual(["0", "1", "10", "20", "50"]);
    });

    it("handles values below one", () => {
      const ticks = pickLogTicks({ min: 0, max: logDisplay(0.13), lengthPx: 200, minGapPx: 28 });
      expect(ticks.labels).toEqual(["0", "0.02", "0.05", "0.1"]);
    });

    it("falls back to linear ticks in a narrow range", () => {
      const ticks = pickLogTicks({ min: logDisplay(9), max: logDisplay(10.5), lengthPx: 200, minGapPx: 28 });
      expect(ticks.labels).toEqual(["9.0", "9.5", "10.0", "10.5"]);
    });
  });

  describe("pickMappedTicks", () => {
    const samplesFor = (f) =>
      Array.from({ length: 201 }, (_, i) => ({ px: 40 + 2 * i, value: f(10 + (490 * i) / 200) }));
    const opts = { measure, minGapPx: 8, leftPx: 2, rightPx: 446 };

    it("spaces a near-linear axis (Q) evenly", () => {
      const ticks = pickMappedTicks({ samples: samplesFor((r) => r / 40), uniform: true, ...opts });
      const values = ticks.map((tick) => tick.value);
      expect(values).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]);
      // Each tick sits where the curve has that value: r = 40 * Q.
      for (const tick of ticks) {
        const r = 10 + ((tick.px - 40) / 400) * 490;
        expect(r / 40).toBeCloseTo(tick.value, 6);
      }
    });

    it("gives a 1/r axis (d) round values that fill the stretched end", () => {
      const ticks = pickMappedTicks({ samples: samplesFor((r) => 600 / r), ...opts });
      const values = ticks.map((tick) => tick.value);
      expect(values).toEqual(expect.arrayContaining([50, 10, 5, 4, 3, 2]));
      // Filled from steps of 1 and 5 only: no 2.2, 2.1, 2 runs.
      expect(values).not.toContain(2.2);
      for (const value of values) expect(stepDecimals(value)).toBeLessThanOrEqual(1);
      for (let i = 1; i < ticks.length; i += 1) {
        const gap = ticks[i].px - ticks[i - 1].px;
        expect(gap).toBeGreaterThanOrEqual((measure(ticks[i].label) + measure(ticks[i - 1].label)) / 2 + 8);
      }
    });

    // d runs into the hundreds next to the beam centre, which made the range
    // so wide that steps below 10 were never tried: a radial profile's d axis
    // stopped at 3 with the 2.5 Å ring inside the ROI.
    it("reaches the fine d values of a radial profile that starts at the centre", () => {
      const samples = Array.from({ length: 201 }, (_, i) => ({
        px: 40 + 2 * i,
        value: i === 0 ? Number.NaN : 600 / (i * 1.3),
      }));
      const values = pickMappedTicks({ samples, ...opts, minGapPx: 14 }).map((tick) => tick.value);
      expect(values).toEqual(expect.arrayContaining([10, 5, 4, 3, 2.5]));
    });

    it("ticks both sides of a profile through the beam centre", () => {
      const samples = Array.from({ length: 201 }, (_, i) => ({ px: 40 + 2 * i, value: Math.abs(i - 100) / 8 }));
      const labels = pickMappedTicks({ samples, uniform: true, ...opts }).map((tick) => tick.label);
      expect(labels).toEqual(["12", "10", "8", "6", "4", "2", "0", "2", "4", "6", "8", "10", "12"]);
    });

    it("skips positions where the value is undefined", () => {
      const samples = [
        { px: 0, value: Number.NaN },
        { px: 100, value: 1 },
        { px: 200, value: 3 },
      ];
      const values = pickMappedTicks({ samples, ...opts, leftPx: 0, rightPx: 300 }).map((t) => t.value);
      expect(values).toEqual(expect.arrayContaining([1, 2, 3]));
      expect(pickMappedTicks({ samples: [], ...opts })).toEqual([]);
    });
  });

  describe("formatTickValue", () => {
    it("prints the decimals the step needs", () => {
      expect(formatTickValue(0.1, 0.02)).toBe("0.10");
      expect(formatTickValue(1200, 200)).toBe("1200");
      expect(formatTickValue(0, 0.02)).toBe("0");
      expect(formatTickValue(Number.NaN, 1)).toBe("-");
    });

    it("switches to exponent notation for fine steps", () => {
      expect(formatTickValue(1.2e-5, 2e-6)).toBe("1.2e-5");
    });
  });
});
