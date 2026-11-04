import { describe, expect, it } from "vitest";

import {
  buildLevelSample,
  countLevelBands,
  formatLevelBand,
  levelSampleFromCounts,
} from "../modules/level_bands_utils.js";

// The brute-force definition every sample has to agree with.
function bruteForce(values, bg, fg) {
  let below = 0;
  let between = 0;
  let above = 0;
  for (const v of values) {
    if (v < bg) below += 1;
    else if (v > fg) above += 1;
    else between += 1;
  }
  return { below, between, above, total: values.length };
}

describe("countLevelBands", () => {
  const ints = Uint16Array.from([0, 0, 1, 2, 5, 5, 5, 9, 12, 40]);

  it("puts values equal to BG or FG between them", () => {
    const sample = buildLevelSample(ints);
    expect(sample.kind).toBe("table");
    expect(countLevelBands(sample, 2, 9)).toEqual({ below: 3, between: 5, above: 2, total: 10 });
  });

  it("agrees with counting one by one, for integer and float samples and fractional levels", () => {
    let seed = 11;
    const random = () => {
      seed = (seed * 1103515245 + 12345) % 2147483648;
      return seed / 2147483648;
    };
    const intValues = Int32Array.from({ length: 5000 }, () => Math.floor(random() * 300) - 20);
    const floatValues = Float32Array.from({ length: 5000 }, () => random() * 300 - 20);
    for (const [bg, fg] of [[0, 100], [-5.5, 7.25], [12.5, 12.5], [150, 40], [-100, 1000], [299, 300]]) {
      expect(countLevelBands(buildLevelSample(intValues), bg, fg)).toEqual(bruteForce(intValues, bg, fg));
      expect(countLevelBands(buildLevelSample(floatValues), bg, fg)).toEqual(bruteForce(floatValues, bg, fg));
    }
  });

  it("counts nothing between when BG is above FG", () => {
    expect(countLevelBands(buildLevelSample(ints), 9, 2).between).toBe(0);
    expect(countLevelBands(buildLevelSample(Float64Array.from(ints)), 9, 2).between).toBe(0);
  });

  it("takes whole numbers from a plain array when told they are", () => {
    expect(buildLevelSample([3, 1, 2], { integral: true }).kind).toBe("table");
    expect(buildLevelSample([3, 1, 2]).kind).toBe("values");
  });

  it("keeps the values when integers span too far for a table", () => {
    const wide = Uint32Array.from([0, 4_000_000_000]);
    const sample = buildLevelSample(wide);
    expect(sample.kind).toBe("values");
    expect(countLevelBands(sample, 1, 10)).toEqual({ below: 1, between: 0, above: 1, total: 2 });
  });

  it("matches a table built from counts", () => {
    const sample = levelSampleFromCounts(Uint32Array.from([2, 0, 3]), 10); // 10, 10, 12, 12, 12
    expect(countLevelBands(sample, 11, 12)).toEqual({ below: 2, between: 3, above: 0, total: 5 });
  });

  it("counts nothing without pixels or levels", () => {
    expect(countLevelBands(buildLevelSample([]), 0, 1)).toEqual({ below: 0, between: 0, above: 0, total: 0 });
    expect(countLevelBands(buildLevelSample(ints), NaN, 1).total).toBe(10);
    expect(countLevelBands(buildLevelSample(ints), NaN, 1).between).toBe(0);
  });
});

describe("formatLevelBand", () => {
  it("shows the count and its share with one decimal", () => {
    expect(formatLevelBand(12345, 1095920)).toBe("12345 (1.1 %)");
    expect(formatLevelBand(0, 10)).toBe("0 (0.0 %)");
    expect(formatLevelBand(10, 10)).toBe("10 (100.0 %)");
  });

  it("never rounds a pixel to zero or all of them to a hundred", () => {
    expect(formatLevelBand(1, 16_000_000)).toBe("1 (< 0.1 %)");
    expect(formatLevelBand(15_999_999, 16_000_000)).toBe("15999999 (> 99.9 %)");
  });

  it("shows a dash without pixels", () => {
    expect(formatLevelBand(0, 0)).toBe("-");
  });
});
