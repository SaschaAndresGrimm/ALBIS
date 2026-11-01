import { describe, expect, it } from "vitest";

import {
  diffMasks,
  filenameFrom,
  hostOf,
  maskFileName,
  maskFileStem,
  safeNamePart,
  shapeText,
  summarizeMask,
} from "../modules/pixel_mask_utils.js";

describe("mask arithmetic", () => {
  it("counts masked pixels by reason, a gap first", () => {
    const summary = summarizeMask(new Uint32Array([0, 1, 2, 3, 8, 0x80000000, 0]));
    expect(summary).toEqual({ masked: 5, gap: 2, defective: 2, other: 1 });
  });

  it("says what replacing one mask with another changes", () => {
    const before = new Uint32Array([0, 0, 1, 2, 4]);
    const after = new Uint32Array([0, 2, 0, 8, 4]);
    expect(diffMasks(before, after)).toEqual({ added: 1, removed: 1, changed: 1 });
  });
});

describe("mask names", () => {
  it("names a saved mask after its source and the time, without growing on each round", () => {
    const now = new Date(2026, 9, 23, 17, 8);
    expect(maskFileName("run_7", { now })).toBe("run_7_pixel_mask_20261023-1708");
    expect(maskFileName("run_7", { edited: true, now })).toBe("run_7_pixel_mask_edited_20261023-1708");
    expect(maskFileName("run_7_pixel_mask_edited_20261022-0900", { now })).toBe("run_7_pixel_mask_20261023-1708");
    // A detector's mask opened from the Detector tab: its file's stamp has seconds.
    expect(maskFileName("E-1_pixel_mask_20261022-090012", { now })).toBe("E-1_pixel_mask_20261023-1708");
    expect(maskFileName("SIMPLON monitor (192.168.20.231) S21", { now })).toBe(
      "SIMPLON_monitor_192.168.20.231_S21_pixel_mask_20261023-1708",
    );
    expect(maskFileName("", { now })).toBe("detector_pixel_mask_20261023-1708");
    expect(hostOf("192.168.20.231")).toBe("192.168.20.231");
    expect(hostOf("http://det.lab:8080/detector/api")).toBe("det.lab");
  });

  it("names a mask after its file, without the series suffix", () => {
    expect(maskFileStem("data/run_7_master.h5")).toBe("run_7");
    expect(maskFileStem("C:\\masks\\good.tiff")).toBe("good");
    expect(maskFileStem("")).toBe("pixel_mask");
  });

  it("keeps a name part to characters every file system takes", () => {
    expect(safeNamePart("  a b//c..d  ")).toBe("a_b_c..d");
    expect(safeNamePart("__.x.__")).toBe("x");
    expect(safeNamePart(null)).toBe("");
  });

  it("takes a host from an address with or without a scheme, or nothing", () => {
    expect(hostOf("")).toBe("");
    expect(hostOf("det.lab:80")).toBe("det.lab");
  });
});

describe("display helpers", () => {
  it("writes a shape as width by height, or a dash", () => {
    expect(shapeText([512, 1028])).toBe("1028 × 512");
    expect(shapeText([1, 2, 3])).toBe("—");
    expect(shapeText(null)).toBe("—");
  });

  it("takes the file name from a Content-Disposition header, else the fallback", () => {
    expect(filenameFrom('attachment; filename="run_7_pixel_mask.h5"', "x.h5")).toBe("run_7_pixel_mask.h5");
    expect(filenameFrom("attachment; filename=m.tiff", "x.h5")).toBe("m.tiff");
    expect(filenameFrom(null, "x.h5")).toBe("x.h5");
  });
});
