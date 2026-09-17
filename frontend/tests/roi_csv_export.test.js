import { vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (key) => {
    const labels = {
      "analysis.roi.plot.histogram": "ROI Histogram",
      "csv.axis.index": "Index",
      "csv.axis.value": "Value",
      "csv.section.x_projection": "X Projection",
      "csv.section.y_projection": "Y Projection",
      "roi.plot.line_profile": "Line Profile",
      "roi.plot.radial_profile": "Radial Profile",
    };
    return labels[key] || key;
  },
}));

import {
  buildRoiCsvExportPayload,
  roiCsvExportUnavailableReason,
} from "../modules/roi_csv_export.js";

// The mocked `t` above returns the key for anything it does not know, which is
// what these assertions match on.
describe("roiCsvExportUnavailableReason", () => {
  it("names the missing frame first", () => {
    expect(
      roiCsvExportUnavailableReason({ hasFrame: false }, { enabled: true, active: true })
    ).toBe("roi.section.load_frame");
  });

  it("names the disabled overlay next", () => {
    expect(
      roiCsvExportUnavailableReason({ hasFrame: true }, { enabled: false, active: true })
    ).toBe("roi.section.disabled");
  });

  it("names the undefined region last", () => {
    expect(
      roiCsvExportUnavailableReason({ hasFrame: true }, { enabled: true, active: false })
    ).toBe("status.roi.no_data");
  });

  it("is empty once there is a region to export", () => {
    expect(
      roiCsvExportUnavailableReason({ hasFrame: true }, { enabled: true, active: true })
    ).toBe("");
  });
});

describe("roi_csv_export", () => {
  it("returns null when ROI export is unavailable", () => {
    const payload = buildRoiCsvExportPayload({
      state: { file: "scan.h5", thresholdCount: 1, thresholdIndex: 0, frameIndex: 0 },
      roiState: { enabled: false, active: false },
      lineMeta: null,
      xMeta: null,
      yMeta: null,
      histMeta: null,
    });
    expect(payload).toBeNull();
  });

  it("puts each plot in its own pair of columns, not one after the other", () => {
    const payload = buildRoiCsvExportPayload({
      state: {
        file: "folder/sample_0001.h5",
        dataset: "/entry/data/data",
        thresholdCount: 2,
        thresholdIndex: 1,
        frameIndex: 4,
        frameCount: 12,
        backendVersion: "0.19.0",
        backendCommit: "abc1234",
      },
      roiState: {
        enabled: true,
        active: true,
        mode: "line",
        start: { x: 10, y: 20 },
        end: { x: 40, y: 60 },
        lineProfile: [10, 20, 30],
        xProjection: null,
        yProjection: null,
        histogramDistribution: [1, 2, 3],
      },
      lineMeta: { xLabel: "Pixels", yLabel: "Intensity", xStart: 0, xStep: 1 },
      xMeta: null,
      yMeta: null,
      histMeta: { xLabel: "Intensity", yLabel: "Count", xStart: 1, xStep: 2 },
    });

    expect(payload).not.toBeNull();
    expect(payload.filename).toBe("sample_0001_frame_5_thr2_roi_line.csv");

    const lines = payload.content.split("\n");
    const comments = lines.filter((line) => line.startsWith("#"));
    const table = lines.filter((line) => !line.startsWith("#"));

    // Every comment leads, so a reader that skips the prefix sees one table.
    expect(lines.slice(0, comments.length)).toEqual(comments);
    expect(comments[0]).toBe("# Produced by ALBIS 0.19.0 (abc1234)");
    expect(comments[1]).toBe("# Source: sample_0001.h5 /entry/data/data frame 5/12 threshold 2/2");
    expect(comments[2]).toBe("# ROI: line from 10 20 to 40 60");

    // One header row, the two plots side by side, each keeping its own x axis.
    expect(table[0]).toBe(
      "Line Profile: Pixels,Line Profile: Intensity,ROI Histogram: Intensity,ROI Histogram: Count"
    );
    expect(table.slice(1)).toEqual(["0,10,1,1", "1,20,3,2", "2,30,5,3"]);
  });

  it("stops a shorter series early rather than padding the file out", () => {
    const payload = buildRoiCsvExportPayload({
      state: { file: "scan.h5", thresholdCount: 1, thresholdIndex: 0, frameIndex: 0 },
      roiState: {
        enabled: true,
        active: true,
        mode: "box",
        start: { x: 0, y: 0 },
        end: { x: 3, y: 1 },
        lineProfile: null,
        xProjection: [5, 6, 7, 8],
        yProjection: [9, 10],
        histogramDistribution: null,
      },
      lineMeta: null,
      xMeta: { xLabel: "X", yLabel: "Mean", xStart: 0, xStep: 1 },
      yMeta: { xLabel: "Y", yLabel: "Mean", xStart: 0, xStep: 1 },
      histMeta: null,
    });

    const table = payload.content.split("\n").filter((line) => !line.startsWith("#"));
    expect(table[0]).toBe(
      "X Projection: X,X Projection: Mean,Y Projection: Y,Y Projection: Mean"
    );
    // The y projection is two rows shorter, so its cells simply run out. The
    // labels are qualified because both projections call their y axis "Mean".
    expect(table.slice(1)).toEqual(["0,5,0,9", "1,6,1,10", "2,7,,", "3,8,,"]);
  });

  it("omits a plot that is not on screen instead of writing an empty column", () => {
    const payload = buildRoiCsvExportPayload({
      state: { file: "scan.h5", thresholdCount: 1, thresholdIndex: 0, frameIndex: 0 },
      roiState: {
        enabled: true,
        active: true,
        mode: "box",
        start: { x: 0, y: 0 },
        end: { x: 1, y: 1 },
        lineProfile: null,
        xProjection: [1, 2],
        yProjection: [],
        histogramDistribution: null,
      },
      lineMeta: null,
      xMeta: { xLabel: "X", yLabel: "Mean", xStart: 0, xStep: 1 },
      yMeta: { xLabel: "Y", yLabel: "Mean", xStart: 0, xStep: 1 },
      histMeta: null,
    });

    const table = payload.content.split("\n").filter((line) => !line.startsWith("#"));
    expect(table[0]).toBe("X Projection: X,X Projection: Mean");
  });

  it("falls back to the translated axis names when a plot has no labels", () => {
    const payload = buildRoiCsvExportPayload({
      state: { file: "scan.h5", thresholdCount: 1, thresholdIndex: 0, frameIndex: 0 },
      roiState: {
        enabled: true,
        active: true,
        mode: "line",
        start: { x: 0, y: 0 },
        end: { x: 2, y: 0 },
        lineProfile: [4, 5],
        xProjection: null,
        yProjection: null,
        histogramDistribution: null,
      },
      lineMeta: {},
      xMeta: null,
      yMeta: null,
      histMeta: null,
    });

    const table = payload.content.split("\n").filter((line) => !line.startsWith("#"));
    expect(table[0]).toBe("Line Profile: Index,Line Profile: Value");
  });
});
