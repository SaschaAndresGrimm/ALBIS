import { describe, expect, it } from "vitest";

import {
  buildGeometryRequestKey,
  getGeometryScopeKey,
  isExptPath,
} from "../modules/geometry_override_utils.js";

describe("geometry_override_utils", () => {
  it("builds a stable per-series scope key across neighboring frames", () => {
    const state = {
      file: "series_0002.tiff",
      seriesFiles: ["series_0001.tiff", "series_0002.tiff", "series_0003.tiff"],
    };

    expect(getGeometryScopeKey(state, "series_0002.tiff")).toBe("series:series_0001.tiff:3");
    expect(getGeometryScopeKey({ ...state, file: "series_0003.tiff" }, "series_0003.tiff")).toBe(
      "series:series_0001.tiff:3",
    );
  });

  it("validates .expt overrides and request keys", () => {
    expect(isExptPath("P12M_geometry/imported.expt")).toBe(true);
    expect(isExptPath("geometry.txt")).toBe(false);
    expect(buildGeometryRequestKey("file:sum_0001.h5")).toBe("file:sum_0001.h5");
    expect(buildGeometryRequestKey("file:sum_0001.h5", "P12M_geometry/imported.expt")).toBe(
      "file:sum_0001.h5|geometry:P12M_geometry/imported.expt",
    );
  });
});
