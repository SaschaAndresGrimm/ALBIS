import { describe, expect, it } from "vitest";
import {
  FRAME_STEP_OPTIONS,
  FRAME_SUM_PRESETS,
  nextFrameSum,
  stepOptionsFor,
  activeFrameSum,
  frameSumCacheSuffix,
  frameSumQuery,
  frameSumWindow,
  framePositionLabel,
  normalizeFrameSum,
  stepForFrameSum,
} from "../modules/frame_sum_utils.js";

const hdf5 = (extra = {}) => ({ frameCount: 100, frameSum: 10, seriesFiles: [], autoload: { mode: "file" }, ...extra });

describe("the frames a summed view covers", () => {
  it("starts at the frame index and holds the sum", () => {
    expect(frameSumWindow(20, 10, 100)).toEqual({ start: 20, end: 29, count: 10 });
  });

  it("holds the frames left at the end of the series, as the backend does", () => {
    expect(frameSumWindow(95, 10, 100)).toEqual({ start: 95, end: 99, count: 5 });
    expect(frameSumWindow(99, 10, 100)).toEqual({ start: 99, end: 99, count: 1 });
  });

  it("takes any whole count from 1 to 1000", () => {
    expect(normalizeFrameSum(7)).toBe(7);
    expect(normalizeFrameSum("3")).toBe(3);
    expect(normalizeFrameSum(2.6)).toBe(3);
    expect(normalizeFrameSum(5000)).toBe(1000);
    expect(normalizeFrameSum(0)).toBe(1);
    expect(normalizeFrameSum("abc")).toBe(1);
  });
});

describe("where the sum applies", () => {
  it("applies to a stack of frames in one file and to a series of one file per frame", () => {
    expect(activeFrameSum(hdf5())).toBe(10);
    expect(activeFrameSum(hdf5({ seriesFiles: ["a.cbf", "b.cbf"] }))).toBe(10);
  });

  it("does not apply to a live source or a single frame", () => {
    expect(activeFrameSum(hdf5({ autoload: { mode: "simplon" } }))).toBe(1);
    expect(activeFrameSum(hdf5({ frameCount: 1 }))).toBe(1);
  });
});

describe("the request and the cache", () => {
  it("asks for the window only when there is one", () => {
    expect(frameSumQuery(1, "sum")).toBe("");
    expect(frameSumQuery(10, "sum")).toBe("&count=10&op=sum");
    expect(frameSumQuery(10, "mean")).toBe("&count=10&op=mean");
    expect(frameSumQuery(10, "median")).toBe("&count=10&op=sum");
  });

  it("keeps a summed frame apart from the plain one and from the mean in the cache", () => {
    expect(frameSumCacheSuffix(1, "sum")).toBe("");
    const sum = frameSumCacheSuffix(10, "sum");
    expect(sum).not.toBe("");
    expect(sum).not.toBe(frameSumCacheSuffix(10, "mean"));
    expect(sum).not.toBe(frameSumCacheSuffix(20, "sum"));
  });
});

describe("the step follows the sum", () => {
  it("offers every preset as a step, and any other sum besides the usual steps", () => {
    FRAME_SUM_PRESETS.forEach((value) => expect(stepOptionsFor(1, 1)).toContain(value));
    expect(stepOptionsFor(3, 1)).toEqual([1, 2, 3, 5, 10, 20, 50, 100, 1000]);
    expect(stepOptionsFor(1, 1)).toEqual(FRAME_STEP_OPTIONS);
  });

  it("follows a sum that is not a usual step", () => {
    expect(stepForFrameSum(1, 1, 3)).toBe(3);
    expect(stepForFrameSum(3, 3, 7)).toBe(7);
  });

  it("moves a step that matched the old sum to the new one, so no frame is skipped or shown twice", () => {
    expect(stepForFrameSum(1, 1, 10)).toBe(10);
    expect(stepForFrameSum(10, 10, 50)).toBe(50);
    expect(stepForFrameSum(10, 10, 1)).toBe(1);
  });

  it("keeps a step the user set to something else: a sliding window", () => {
    expect(stepForFrameSum(1, 10, 20)).toBe(1);
    expect(stepForFrameSum(100, 1, 10)).toBe(100);
  });
});

describe("the frame position label", () => {
  it("shows the range of frames when several are summed", () => {
    expect(framePositionLabel(120, 3600, 1)).toBe("121 / 3600");
    expect(framePositionLabel(120, 3600, 10)).toBe("121–130 / 3600");
    expect(framePositionLabel(3599, 3600, 10)).toBe("3600 / 3600");
  });
});

describe("[ and ]", () => {
  it("step the count along 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000", () => {
    expect(nextFrameSum(1, 1)).toBe(2);
    expect(nextFrameSum(3, 1)).toBe(5);
    expect(nextFrameSum(3, -1)).toBe(2);
    expect(nextFrameSum(100, 1)).toBe(200);
    expect(nextFrameSum(1000, 1)).toBe(1000);
    expect(nextFrameSum(1, -1)).toBe(1);
  });
});
