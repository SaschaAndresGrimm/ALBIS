import { describe, expect, it } from "vitest";

import {
  GEOMETRY_OVERRIDE_STORAGE_KEY,
  createGeometryOverride,
  loadGeometryOverride,
  pixelAspectFrom,
  resolveGeometryParams,
  saveGeometryOverride,
} from "../modules/geometry_params.js";

const HEADER = {
  distanceMm: 200,
  pixelSizeXUm: 172,
  pixelSizeYUm: 172,
  energyEv: 12000,
  centerX: 1000,
  centerY: 1100,
};

function memoryStorage() {
  const map = new Map();
  return {
    getItem: (key) => (map.has(key) ? map.get(key) : null),
    setItem: (key, value) => map.set(key, String(value)),
    map,
  };
}

describe("resolveGeometryParams", () => {
  it("uses the source as it is when nothing overrides it", () => {
    const { values, origins } = resolveGeometryParams({ source: HEADER, sourceOrigin: "image" });

    expect(values).toEqual(HEADER);
    expect(origins.distanceMm).toBe("image");
  });

  it("reports what the source does not state as missing, never as zero", () => {
    const { values, origins } = resolveGeometryParams({ source: { energyEv: 8048 }, sourceOrigin: "image" });

    expect(values.distanceMm).toBeNull();
    expect(values.centerX).toBeNull();
    expect(origins.distanceMm).toBe("missing");
  });

  it("lets an override replace the source field by field while it is on", () => {
    const override = { enabled: true, values: { distanceMm: 305 }, geometryFile: "" };
    const { values, origins, sourceValues } = resolveGeometryParams({ source: HEADER, override });

    expect(values.distanceMm).toBe(305);
    expect(origins.distanceMm).toBe("override");
    expect(sourceValues.distanceMm).toBe(200);
    expect(values.energyEv).toBe(12000);
  });

  it("ignores an override that is switched off, without forgetting it", () => {
    const override = { enabled: false, values: { distanceMm: 305 }, geometryFile: "" };

    expect(resolveGeometryParams({ source: HEADER, override }).values.distanceMm).toBe(200);
    expect(override.values.distanceMm).toBe(305);
  });

  it("fills the pose from a geometry only where the source has none", () => {
    const reference = { distanceMm: 250.13, centerX: 1074.5, centerY: 2593.4 };
    const { values, origins } = resolveGeometryParams({
      source: { distanceMm: 260.13, energyEv: 4500 },
      sourceOrigin: "image",
      reference,
    });

    expect(values.distanceMm).toBe(260.13);
    expect(values.centerX).toBe(1074.5);
    expect(origins.centerX).toBe("geometry");
  });

  it("takes the whole pose from a geometry the user chose", () => {
    const reference = { distanceMm: 250.13, centerX: 1074.5, centerY: 2593.4 };
    const { values } = resolveGeometryParams({ source: HEADER, reference, poseFromGeometry: true });

    expect(values.distanceMm).toBe(250.13);
    expect(values.centerX).toBe(1074.5);
    expect(values.energyEv).toBe(12000);
  });

  it("still lets the user override a geometry's pose", () => {
    const reference = { distanceMm: 250.13, centerX: 1074.5, centerY: 2593.4 };
    const override = { enabled: true, values: { centerX: 1080 }, geometryFile: "" };

    expect(resolveGeometryParams({ source: HEADER, reference, poseFromGeometry: true, override }).values.centerX).toBe(
      1080,
    );
  });

  it("applies one typed pixel size to both axes of a square-pixel source", () => {
    const override = { enabled: true, values: { pixelSizeXUm: 75 }, geometryFile: "" };
    const { values } = resolveGeometryParams({ source: HEADER, override });

    expect(values.pixelSizeXUm).toBe(75);
    expect(values.pixelSizeYUm).toBe(75);
    expect(pixelAspectFrom(values)).toBe(1);
  });

  it("keeps a genuinely non-square source's own Y when only X is typed", () => {
    const override = { enabled: true, values: { pixelSizeXUm: 80 }, geometryFile: "" };
    const { values } = resolveGeometryParams({
      source: { ...HEADER, pixelSizeXUm: 75, pixelSizeYUm: 225 },
      override,
    });

    expect(values.pixelSizeYUm).toBe(225);
  });

  it("refuses values that cannot be a distance, size or energy", () => {
    const override = { enabled: true, values: { distanceMm: 0, energyEv: -1, centerX: -5 }, geometryFile: "" };
    const { values } = resolveGeometryParams({ source: HEADER, override });

    expect(values.distanceMm).toBe(200);
    expect(values.energyEv).toBe(12000);
    expect(values.centerX).toBe(-5);
  });
});

describe("the override persists", () => {
  it("round-trips through storage", () => {
    const storage = memoryStorage();
    saveGeometryOverride(
      { enabled: true, values: { distanceMm: 305, centerX: 1080 }, geometryFile: " /data/refined.expt " },
      storage,
    );

    const loaded = loadGeometryOverride(storage);
    expect(loaded.enabled).toBe(true);
    expect(loaded.values.distanceMm).toBe(305);
    expect(loaded.values.centerX).toBe(1080);
    expect(loaded.values.energyEv).toBeNull();
    expect(loaded.geometryFile).toBe("/data/refined.expt");
  });

  it("starts fresh from missing, corrupt or foreign storage", () => {
    const storage = memoryStorage();
    expect(loadGeometryOverride(storage)).toEqual(createGeometryOverride());

    storage.setItem(GEOMETRY_OVERRIDE_STORAGE_KEY, "{not json");
    expect(loadGeometryOverride(storage)).toEqual(createGeometryOverride());

    storage.setItem(GEOMETRY_OVERRIDE_STORAGE_KEY, JSON.stringify({ v: 99, enabled: true }));
    expect(loadGeometryOverride(storage)).toEqual(createGeometryOverride());
  });

  it("survives storage that throws", () => {
    const broken = {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    };

    expect(loadGeometryOverride(broken)).toEqual(createGeometryOverride());
    expect(() => saveGeometryOverride(createGeometryOverride(), broken)).not.toThrow();
  });
});
