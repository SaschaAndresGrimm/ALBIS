import { describe, expect, it } from "vitest";

import {
  buildCsvTable,
  csvField,
  csvProvenanceLines,
  formatCsvNumber,
  plotSeriesColumns,
} from "../modules/csv_export_utils.js";

describe("csvField", () => {
  it("leaves ordinary text and numbers alone", () => {
    expect(csvField("Intensity")).toBe("Intensity");
    expect(csvField(42)).toBe("42");
    expect(csvField(null)).toBe("");
  });

  it("quotes what would otherwise break the row", () => {
    // Reachable through a translated column header: nothing stops a locale
    // from writing "Recuento, total", and unquoted that shifts every column
    // after it by one.
    expect(csvField("Recuento, total")).toBe('"Recuento, total"');
    expect(csvField('say "hi"')).toBe('"say ""hi"""');
    expect(csvField("two\nlines")).toBe('"two\nlines"');
  });
});

describe("formatCsvNumber", () => {
  it("writes finite numbers and blanks the rest", () => {
    expect(formatCsvNumber(0)).toBe("0");
    expect(formatCsvNumber(-1.5)).toBe("-1.5");
    expect(formatCsvNumber(NaN)).toBe("");
    expect(formatCsvNumber(Infinity)).toBe("");
    expect(formatCsvNumber(undefined)).toBe("");
  });
});

describe("buildCsvTable", () => {
  it("writes one header row and pads the short columns", () => {
    const lines = buildCsvTable([
      { label: "a", values: [1, 2, 3] },
      { label: "b", values: [4] },
    ]);
    expect(lines).toEqual(["a,b", "1,4", "2,", "3,"]);
  });

  it("drops a column with no values rather than heading an empty one", () => {
    const lines = buildCsvTable([
      { label: "a", values: [1] },
      { label: "b", values: [] },
    ]);
    expect(lines).toEqual(["a", "1"]);
  });

  it("returns nothing when there is nothing to write", () => {
    expect(buildCsvTable([])).toEqual([]);
    expect(buildCsvTable(null)).toEqual([]);
    expect(buildCsvTable([{ label: "a", values: [] }])).toEqual([]);
  });
});

describe("plotSeriesColumns", () => {
  it("rebuilds the x axis from the plot's start and step", () => {
    const columns = plotSeriesColumns({
      title: "Histogram",
      data: [5, 6, 7],
      meta: { xLabel: "Intensity", yLabel: "Count", xStart: 1.5, xStep: 0.5 },
    });
    expect(columns.map((column) => column.label)).toEqual([
      "Histogram: Intensity",
      "Histogram: Count",
    ]);
    expect(columns[0].values).toEqual(["1.5", "2", "2.5"]);
    expect(columns[1].values).toEqual(["5", "6", "7"]);
  });

  it("treats a missing or zero step as one", () => {
    const columns = plotSeriesColumns({
      title: "P",
      data: [1, 2],
      meta: { xStart: 10, xStep: 0 },
    });
    expect(columns[0].values).toEqual(["10", "11"]);
  });

  it("contributes nothing for a series with no data", () => {
    expect(plotSeriesColumns({ title: "P", data: [], meta: {} })).toEqual([]);
    expect(plotSeriesColumns({ title: "P", data: null, meta: {} })).toEqual([]);
  });
});

describe("csvProvenanceLines", () => {
  it("names the build and what the numbers came from", () => {
    const lines = csvProvenanceLines({
      backendVersion: "0.19.0",
      backendCommit: "abc1234",
      file: "/data/runs/scan_0001.h5",
      dataset: "/entry/data/data",
      frameIndex: 2,
      frameCount: 10,
      thresholdIndex: 0,
      thresholdCount: 2,
    });
    expect(lines).toEqual([
      "# Produced by ALBIS 0.19.0 (abc1234)",
      "# Source: scan_0001.h5 /entry/data/data frame 3/10 threshold 1/2",
    ]);
  });

  it("leaves out what it does not know", () => {
    // An unstamped build reports no commit, and a single-frame image has
    // neither a frame count nor thresholds worth stating.
    const lines = csvProvenanceLines({
      backendVersion: "0.19.0",
      file: "frame.cbf",
      frameIndex: 0,
      frameCount: 1,
      thresholdCount: 1,
    });
    expect(lines).toEqual(["# Produced by ALBIS 0.19.0", "# Source: frame.cbf frame 1"]);
  });

  it("appends the caller's own notes as further comment lines", () => {
    const lines = csvProvenanceLines({ file: "a.h5", frameIndex: 0 }, ["ROI: box from 1 2 to 3 4", ""]);
    expect(lines[0]).toBe("# Produced by ALBIS");
    expect(lines[lines.length - 1]).toBe("# ROI: box from 1 2 to 3 4");
  });

  it("survives a state with nothing in it", () => {
    expect(csvProvenanceLines(null)).toEqual(["# Produced by ALBIS"]);
  });
});
