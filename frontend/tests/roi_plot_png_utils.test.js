import { describe, expect, it, vi } from "vitest";

import {
  composeRoiPlotPng,
  fitCanvasText,
  roiPlotPngCaption,
  roiPlotPngFilename,
} from "../modules/roi_plot_png_utils.js";

const state = { file: "data/CuHF2_01.nxs", dataset: "/entry/data/data", frameIndex: 0, frameCount: 1750, thresholdCount: 1 };
const roiState = { mode: "box", start: { x: 286, y: 182 }, end: { x: 795, y: 382 } };

describe("roiPlotPngFilename", () => {
  it("names the file, frame, ROI mode and plot", () => {
    expect(roiPlotPngFilename(state, roiState, "x")).toBe("CuHF2_01_frame_1_roi_box_profile_x.png");
    expect(roiPlotPngFilename(state, roiState, "hist")).toBe("CuHF2_01_frame_1_roi_box_histogram.png");
    expect(roiPlotPngFilename(state, roiState, "surface")).toBe("CuHF2_01_frame_1_roi_box_3d_view.png");
    expect(roiPlotPngFilename(state, { mode: "line" }, "line")).toBe("CuHF2_01_frame_1_roi_line_line_profile.png");
    expect(roiPlotPngFilename(state, { mode: "annulus" }, "line")).toBe("CuHF2_01_frame_1_roi_annulus_radial_profile.png");
  });

  it("adds the threshold when there are several", () => {
    expect(roiPlotPngFilename({ ...state, thresholdCount: 2, thresholdIndex: 1 }, roiState, "y")).toBe(
      "CuHF2_01_frame_1_thr2_roi_box_profile_y.png"
    );
  });
});

describe("roiPlotPngCaption", () => {
  it("says what the plot was measured on", () => {
    expect(roiPlotPngCaption(state, roiState)).toBe(
      "CuHF2_01.nxs · /entry/data/data · frame 1/1750 · ROI: box from 286 182 to 795 382"
    );
  });
});

function fakeContext() {
  const calls = [];
  const ctx = {
    calls,
    measureText: (text) => ({ width: String(text).length * 10 }),
    fillRect: (...args) => calls.push(["fillRect", ...args]),
    fillText: (...args) => calls.push(["fillText", ...args]),
    drawImage: (...args) => calls.push(["drawImage", ...args]),
  };
  return ctx;
}

describe("fitCanvasText", () => {
  it("keeps text that fits and cuts the rest with an ellipsis", () => {
    const ctx = fakeContext();
    expect(fitCanvasText(ctx, "short", 100)).toBe("short");
    expect(fitCanvasText(ctx, "a much longer caption", 60)).toBe("a muc…");
  });
});

describe("composeRoiPlotPng", () => {
  it("puts the title and caption above the plot, on an opaque background", () => {
    const ctx = fakeContext();
    const createCanvas = vi.fn((width, height) => ({ width, height, getContext: () => ctx }));
    const source = { width: 800, height: 400 };
    const colors = { background: "bg", plot: "plot", text: "text", muted: "muted" };
    const canvas = composeRoiPlotPng({ source, title: "Profile along X", caption: "file", dpr: 2, colors, createCanvas });

    const header = Math.round((10 + 13 + 4 + 11 + 5) * 2);
    expect(canvas.width).toBe(800);
    expect(canvas.height).toBe(400 + header);
    expect(ctx.calls[0]).toEqual(["fillRect", 0, 0, 800, 400 + header]);
    expect(ctx.calls.filter(([name]) => name === "fillText").map(([, text]) => text)).toEqual(["Profile along X", "file"]);
    expect(ctx.calls.at(-1)).toEqual(["drawImage", source, 0, header]);
  });

  it("draws nothing for an empty plot", () => {
    expect(composeRoiPlotPng({ source: { width: 0, height: 0 }, title: "", colors: {}, createCanvas: vi.fn() })).toBeNull();
  });
});
