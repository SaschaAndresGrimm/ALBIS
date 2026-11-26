import {
  buildRoiHistogram,
  normalizeRoiHistogramBinCount,
  ROI_HISTOGRAM_MAX_BINS,
  roiHistogramHiddenCounts,
  roiHistogramMaxViewBins,
} from "../modules/roi_histogram_engine.js";
import { buildLevelSample } from "../modules/level_bands_utils.js";

// A diffraction ROI in miniature: most pixels a few counts, a thin tail, and
// one bright pixel far out (the 4086 in a CBF that put 96 % of the pixels in
// the first of 192 bars).
function diffractionLike(n = 20000) {
  const values = new Int32Array(n);
  let seed = 7;
  const random = () => {
    seed = (seed * 1103515245 + 12345) % 2147483648;
    return seed / 2147483648;
  };
  for (let i = 0; i < n; i += 1) values[i] = Math.floor(-Math.log(1 - random()) * 6);
  values[n - 1] = 4086;
  return values;
}

const sum = (array) => array.reduce((a, b) => a + b, 0);

describe("roi_histogram_engine", () => {
  it("builds one bin per value for a short whole-number range", () => {
    const hist = buildRoiHistogram([1, 2, 2, 3, 3, 3]);
    expect(hist.xTickMode).toBe("integer");
    expect(hist.xStart).toBe(1);
    expect(hist.xStep).toBe(1);
    expect(hist.binWidth).toBe(1);
    expect(hist.data).toEqual([1, 2, 3]);
    expect(hist.view).toBeNull();
  });

  it("gives fixed bins whole-number widths with edges between values", () => {
    const hist = buildRoiHistogram([0, 1, 2, 3], { mode: "fixed", count: 2 });
    expect(hist.xStep).toBe(2);
    expect(hist.binWidth).toBe(2);
    // Bins {0, 1} and {2, 3}, drawn from -0.5 to 1.5 and 1.5 to 3.5.
    expect(hist.xStart).toBe(0.5);
    expect(hist.data).toEqual([2, 2]);
  });

  it("splits float data into the fixed number of bins", () => {
    const hist = buildRoiHistogram([0, 0.5, 1.5, 3], { mode: "fixed", count: 2 });
    expect(hist.xTickMode).toBe("");
    expect(hist.xStep).toBe(1.5);
    expect(hist.xStart).toBe(0.75);
    expect(hist.data).toEqual([2, 2]);
  });

  it("clamps fixed histogram bin counts", () => {
    expect(normalizeRoiHistogramBinCount(1)).toBe(2);
    expect(normalizeRoiHistogramBinCount(9999)).toBe(512);
    expect(normalizeRoiHistogramBinCount("not-a-number")).toBe(128);
    const hist = buildRoiHistogram([0.5, 1, 2, 3], { mode: "fixed", count: 1 });
    expect(hist.data).toHaveLength(2);
  });

  it("opens on the range without the far tail, but bins all of it", () => {
    const values = diffractionLike();
    const hist = buildRoiHistogram(values, { integral: true });
    // The view ends well short of the bright pixel...
    expect(hist.view.min).toBe(-0.5);
    expect(hist.view.max).toBeLessThan(100);
    // ...which is still in the last bin, one value wide.
    expect(hist.binWidth).toBe(1);
    expect(hist.xStart + (hist.data.length - 1) * hist.xStep).toBe(4086);
    expect(hist.data.at(-1)).toBe(1);
    expect(sum(hist.data)).toBe(values.length);
    expect(hist.outsideAbove).toBe(0);
  });

  it("leaves a compact distribution's range alone", () => {
    const values = Array.from({ length: 1000 }, (_, i) => Math.round(100 + 20 * Math.sin(i)));
    const hist = buildRoiHistogram(values);
    expect(hist.view).toBeNull();
    expect(sum(hist.data)).toBe(1000);
  });

  it("puts the same number of whole values in every bin (no comb)", () => {
    // Every value 0..999 once: equal bins must hold equal counts.
    const values = Int32Array.from({ length: 1000 }, (_, i) => i);
    const hist = buildRoiHistogram(values, { integral: true, maxViewBins: 100 });
    expect(Number.isInteger(hist.binWidth)).toBe(true);
    expect(hist.binWidth).toBeGreaterThan(1);
    const full = hist.data.slice(0, -1);
    expect(new Set(full).size).toBe(1);
    expect(full[0]).toBe(hist.binWidth);
  });

  it("keeps auto bins at least two screen pixels wide", () => {
    expect(roiHistogramMaxViewBins(200)).toBe(100);
    expect(roiHistogramMaxViewBins(0)).toBe(192);
    expect(roiHistogramMaxViewBins(5000)).toBe(512);
    const values = Int32Array.from({ length: 100000 }, (_, i) => i % 1000);
    const hist = buildRoiHistogram(values, { integral: true, maxViewBins: 100 });
    expect(hist.data.length).toBeLessThanOrEqual(100);
  });

  it("bins the level bands' table of counts the same as the values", () => {
    const values = diffractionLike(5000);
    const fromValues = buildRoiHistogram(Array.from(values), { integral: true });
    const levelSample = buildLevelSample(values, { integral: true });
    expect(levelSample.kind).toBe("table");
    const fromTable = buildRoiHistogram(values, { integral: true, levelSample });
    expect(fromTable).toEqual(fromValues);
  });

  it("counts a tail beyond the bin budget instead of binning it", () => {
    const values = new Int32Array(10000).map((_, i) => i % 10);
    values[0] = 1_000_000;
    const hist = buildRoiHistogram(values, { integral: true });
    expect(hist.data.length).toBeLessThanOrEqual(ROI_HISTOGRAM_MAX_BINS);
    expect(hist.outsideAbove).toBe(1);
    expect(sum(hist.data) + hist.outsideAbove).toBe(values.length);
  });

  it("finds float quantiles and keeps every value", () => {
    const values = Array.from({ length: 5000 }, (_, i) => (i % 100) / 10);
    values.push(1e6);
    const hist = buildRoiHistogram(values);
    expect(hist.xTickMode).toBe("");
    expect(hist.view.max).toBeLessThan(20);
    expect(sum(hist.data) + hist.outsideAbove).toBe(values.length);
  });

  it("ignores values that are not finite", () => {
    const hist = buildRoiHistogram([1, Number.NaN, 2, Infinity]);
    expect(sum(hist.data)).toBe(2);
    expect(buildRoiHistogram([Number.NaN])).toBeNull();
  });

  it("counts the pixels outside a view, with the nearest hidden edges", () => {
    // Bins centred on 0..9, one value wide.
    const data = [5, 1, 1, 1, 1, 1, 1, 1, 2, 3];
    const hidden = roiHistogramHiddenCounts({
      data,
      xStart: 0,
      xStep: 1,
      viewMin: 0.5,
      viewMax: 7.5,
      outsideAbove: 4,
    });
    expect(hidden.below).toBe(5);
    expect(hidden.belowEdge).toBe(0.5);
    expect(hidden.above).toBe(2 + 3 + 4);
    expect(hidden.aboveEdge).toBe(7.5);
    // A bar cut by the view's edge is in view.
    expect(roiHistogramHiddenCounts({ data, xStart: 0, xStep: 1, viewMin: 0.2, viewMax: 9.5 }).below).toBe(0);
  });
});
