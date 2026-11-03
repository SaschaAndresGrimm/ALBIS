/**
 * The inspector showed "object" beside every scalar string in a NeXus master:
 * the shape `()` joined to an empty string, leaving h5py's dtype on its own.
 */

import { describe, expect, it } from "vitest";

import {
  INSPECTOR_VALUE_CHARS,
  formatInspectorShape,
  formatInspectorScalar,
  inspectorRowMeta,
  splitInspectorPath,
  splitMatches,
} from "../modules/inspector_format_utils.js";

const LABELS = { typeLabel: "dataset", scalarLabel: "scalar" };

describe("formatInspectorShape", () => {
  it("joins dimensions and names a scalar", () => {
    expect(formatInspectorShape([40000, 2, 1064, 1030])).toBe("40000×2×1064×1030");
    expect(formatInspectorShape([], "scalar")).toBe("scalar");
    expect(formatInspectorShape(undefined, "scalar")).toBe("");
  });
});

describe("formatInspectorScalar", () => {
  it("appends units and keeps false and zero", () => {
    expect(formatInspectorScalar(12.4, "keV")).toBe("12.4 keV");
    expect(formatInspectorScalar(0)).toBe("0");
    expect(formatInspectorScalar(false)).toBe("false");
    expect(formatInspectorScalar(null)).toBe("");
  });
});

describe("inspectorRowMeta", () => {
  it("shows a scalar's value, with shape and dtype in the tooltip", () => {
    const meta = inspectorRowMeta({ type: "dataset", shape: [], dtype: "string", value: "NXmx" }, LABELS);
    expect(meta.text).toBe("NXmx");
    expect(meta.title).toBe("NXmx\nscalar string");
  });

  it("never shows a bare 'object' for an unsummarised scalar", () => {
    const meta = inspectorRowMeta({ type: "dataset", shape: [], dtype: "string" }, LABELS);
    expect(meta.text).toBe("string");
  });

  it("clips a long value and keeps all of it in the tooltip", () => {
    const value = "a".repeat(120);
    const meta = inspectorRowMeta({ type: "dataset", shape: [1], dtype: "string", value }, LABELS);
    expect(meta.text).toHaveLength(INSPECTOR_VALUE_CHARS);
    expect(meta.text.endsWith("…")).toBe(true);
    expect(meta.title.startsWith(value)).toBe(true);
  });

  it("shows shape and dtype for an array", () => {
    const meta = inspectorRowMeta({ type: "dataset", shape: [3, 4], dtype: "uint32" }, LABELS);
    expect(meta.text).toBe("3×4 uint32");
  });

  it("shows a link's target, and nothing for a group, whose icon says it all", () => {
    expect(inspectorRowMeta({ type: "link", target: "a.h5:/x" }, LABELS).text).toBe("a.h5:/x");
    expect(inspectorRowMeta({ type: "group" }, { typeLabel: "group" }).text).toBe("");
    expect(inspectorRowMeta({ type: "other" }, { typeLabel: "node" }).text).toBe("node");
  });
});

describe("splitInspectorPath", () => {
  it("separates the name from its parent", () => {
    expect(splitInspectorPath("/entry/data/start_time")).toEqual({ name: "start_time", parent: "/entry/data" });
    expect(splitInspectorPath("/entry")).toEqual({ name: "entry", parent: "/" });
    expect(splitInspectorPath("/")).toEqual({ name: "/", parent: "/" });
  });
});

describe("splitMatches", () => {
  const join = (pieces) => pieces.map((p) => p.text).join("");

  it("marks every case-insensitive occurrence and joins back exactly", () => {
    const pieces = splitMatches("Start_Time_start", "start");
    expect(pieces).toEqual([
      { text: "Start", match: true },
      { text: "_Time_", match: false },
      { text: "start", match: true },
    ]);
    expect(join(pieces)).toBe("Start_Time_start");
  });

  it("returns the text whole without a query or a match", () => {
    expect(splitMatches("count_time", "")).toEqual([{ text: "count_time", match: false }]);
    expect(splitMatches("count_time", "zzz")).toEqual([{ text: "count_time", match: false }]);
    expect(splitMatches("", "x")).toEqual([{ text: "", match: false }]);
  });
});
