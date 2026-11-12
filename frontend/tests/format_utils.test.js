import { describe, expect, it } from "vitest";

import {
  dirnameFromPath,
  formatEnergy,
  formatInspectorValue,
  formatLevelValue,
  formatRoiTick,
  formatStat,
  middleTruncate,
  toolbarCharBudget,
} from "../modules/format_utils.js";

describe("format_utils", () => {
  it("shortens a long path in the middle, keeping both ends", () => {
    expect(middleTruncate("/data/beamtime/sample_0007_master.h5", 16)).toBe("/data/b…ster.h5");
    expect(middleTruncate("short.h5", 16)).toBe("short.h5");
    expect(middleTruncate("", 16)).toBe("");
    // Never narrower than six characters, whatever the room.
    expect(middleTruncate("abcdefghij", 2)).toBe("ab…ij");
  });

  it("writes statistics as integers, three decimals, or exponent for the extremes", () => {
    expect(formatStat(1234)).toBe("1234");
    expect(formatStat(12.34567)).toBe("12.346");
    expect(formatStat(2.5e7)).toBe("2.500e+7");
    expect(formatStat(0.0001)).toBe("1.000e-4");
    expect(formatStat(0)).toBe("0");
    expect(formatStat(Number.NaN)).toBe("-");
  });

  it("gives plot ticks fewer decimals the larger they are, and no trailing zeros", () => {
    expect(formatRoiTick(1500)).toBe("1500");
    expect(formatRoiTick(150.25)).toBe("150.3");
    expect(formatRoiTick(15.256)).toBe("15.26");
    expect(formatRoiTick(1.5)).toBe("1.5");
    expect(formatRoiTick(2)).toBe("2");
    expect(formatRoiTick(250000)).toBe("2.50e+5");
    expect(formatRoiTick(Infinity)).toBe("-");
  });

  it("writes an energy as a whole number when it nearly is one", () => {
    expect(formatEnergy(8040.02)).toBe("8040");
    expect(formatEnergy(12398.4)).toBe("12398.4");
    expect(formatEnergy(Number.NaN)).toBe("");
  });

  it("shows inspector values as text, objects as indented JSON", () => {
    expect(formatInspectorValue(null)).toBe("-");
    expect(formatInspectorValue("eiger")).toBe("eiger");
    expect(formatInspectorValue(3)).toBe("3");
    expect(formatInspectorValue(true)).toBe("true");
    expect(formatInspectorValue({ a: 1 })).toBe('{\n  "a": 1\n}');
    const cyclic = {};
    cyclic.self = cyclic;
    expect(formatInspectorValue(cyclic)).toBe("[object Object]");
  });

  it("finds a path's folder, on Windows paths too", () => {
    expect(dirnameFromPath("/data/run/img.cbf")).toBe("/data/run");
    expect(dirnameFromPath("C:\\data\\img.cbf")).toBe("C:/data");
    expect(dirnameFromPath("/img.cbf")).toBe("/");
    expect(dirnameFromPath("img.cbf")).toBe("");
    expect(dirnameFromPath("")).toBe("");
  });

  it("formats a BG or FG level by the frame's data type", () => {
    expect(formatLevelValue(12.6, "uint32")).toBe("13");
    expect(formatLevelValue(1.2167, "<f4")).toBe("1.217");
    expect(formatLevelValue(Number.NaN, "<f4")).toBe("");
    expect(formatLevelValue(5, "")).toBe("5");
  });
});

describe("toolbarCharBudget", () => {
  it("fits a long file name in a wide header", () => {
    // The header's middle on a 1700px window; measured from the label itself
    // (160px) it was 21 characters, and "7118_E1_00007.cbf 2 / 9" was cut.
    expect(toolbarCharBudget(900)).toBe(121);
  });

  it("keeps a floor on a narrow header and without a measurement", () => {
    expect(toolbarCharBudget(100)).toBe(24);
    expect(toolbarCharBudget(undefined)).toBe(24);
  });
});
