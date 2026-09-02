import { describe, expect, it } from "vitest";

import {
  canExportAnimation,
  canExportData,
  canSaveImage,
} from "../modules/command_availability.js";

const isHdfFile = (file) => /\.(h5|hdf5)$/i.test(String(file || ""));

describe("canSaveImage", () => {
  it("is false with nothing on screen", () => {
    expect(canSaveImage({ hasFrame: false, dataRaw: null })).toBe(false);
  });

  it("is false for a stale frame left over from a failed load", () => {
    // dataRaw outlives hasFrame across a failed frame load and a dataset
    // rescan, so it alone would save the previous frame's pixels.
    expect(canSaveImage({ hasFrame: false, dataRaw: new Uint16Array([1]) })).toBe(false);
  });

  it("is true once a frame is on screen", () => {
    expect(canSaveImage({ hasFrame: true, dataRaw: new Uint16Array([1]) })).toBe(true);
  });

  it("does not require a file, so a live stream qualifies", () => {
    expect(canSaveImage({ file: "", hasFrame: true, dataRaw: new Uint16Array([1]) })).toBe(true);
  });
});

describe("canExportAnimation", () => {
  it("is false for a single frame, which cannot be animated", () => {
    expect(canExportAnimation({ file: "/data/one.cbf", frameCount: 1, seriesFiles: [] })).toBe(
      false
    );
  });

  it("is false with no file open", () => {
    expect(canExportAnimation({ file: "", frameCount: 12, seriesFiles: [] })).toBe(false);
  });

  it("is false when the frames have no source", () => {
    expect(
      canExportAnimation({ file: "/data/stack.h5", dataset: "", frameCount: 12, seriesFiles: [] })
    ).toBe(false);
  });

  it("is true for a multi-frame dataset", () => {
    expect(
      canExportAnimation({
        file: "/data/stack.h5",
        dataset: "/entry/data/data",
        frameCount: 12,
        seriesFiles: [],
      })
    ).toBe(true);
  });

  it("is true for a file series", () => {
    expect(
      canExportAnimation({
        file: "/data/series_0001.cbf",
        frameCount: 2,
        seriesFiles: ["a.cbf", "b.cbf"],
      })
    ).toBe(true);
  });
});

describe("canExportData", () => {
  it("is false with no file open", () => {
    expect(canExportData({ file: "", dataset: "" }, isHdfFile)).toBe(false);
  });

  it("is false for an HDF5 file before a dataset is chosen", () => {
    expect(canExportData({ file: "/data/stack.h5", dataset: "" }, isHdfFile)).toBe(false);
  });

  it("is true for an HDF5 file with a dataset chosen", () => {
    expect(
      canExportData({ file: "/data/stack.h5", dataset: "/entry/data/data" }, isHdfFile)
    ).toBe(true);
  });

  it("is true for a plain image file, which needs no dataset", () => {
    expect(canExportData({ file: "/data/frame.cbf", dataset: "" }, isHdfFile)).toBe(true);
  });
});
