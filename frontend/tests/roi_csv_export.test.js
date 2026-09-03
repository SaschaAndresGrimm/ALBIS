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

  it("builds CSV payload with filename and sections", () => {
    const payload = buildRoiCsvExportPayload({
      state: {
        file: "folder/sample_0001.h5",
        thresholdCount: 2,
        thresholdIndex: 1,
        frameIndex: 4,
      },
      roiState: {
        enabled: true,
        active: true,
        mode: "line",
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
    expect(payload.content).toContain("# Line Profile");
    expect(payload.content).toContain("# ROI Histogram");
  });
});
