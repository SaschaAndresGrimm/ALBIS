import {
  applyGeometryOverrides,
  buildGeometryRingSegments,
  getGeometryReferencePose,
  getGeometryResolutionAtPixel,
  pickGeometryRingLabelPoint,
  prepareRingGeometry,
} from "../modules/ring_geometry_utils.js";

function createGeometry() {
  return prepareRingGeometry({
    mode: "geometry",
    detector: "pilatus-12m-dls-cshape",
    source: "P12M_geometry/imported.expt",
    panels: [
      {
        name: "row-00",
        origin_mm: [-100, -90, 100],
        fast_axis: [1, 0, 0],
        slow_axis: [0, 1, 0],
        pixel_size_mm: [1, 1],
        image_size_px: [200, 80],
        raw_offset_px: [0, 0],
      },
      {
        name: "row-01",
        origin_mm: [-100, 10, 100],
        fast_axis: [1, 0, 0],
        slow_axis: [0, 1, 0],
        pixel_size_mm: [1, 1],
        image_size_px: [200, 80],
        raw_offset_px: [0, 100],
      },
    ],
  });
}

function createCenteredGeometry() {
  return prepareRingGeometry({
    mode: "geometry",
    detector: "test-centered",
    source: "test/reference.expt",
    panels: [
      {
        name: "center",
        origin_mm: [-60, -60, 100],
        fast_axis: [1, 0, 0],
        slow_axis: [0, 1, 0],
        pixel_size_mm: [1, 1],
        image_size_px: [120, 120],
        raw_offset_px: [0, 0],
      },
    ],
  });
}

function createGapGeometry() {
  return prepareRingGeometry({
    mode: "geometry",
    detector: "test-gap",
    source: "test/gap.expt",
    panels: [
      {
        name: "upper",
        origin_mm: [-60, -80, 100],
        fast_axis: [1, 0, 0],
        slow_axis: [0, 1, 0],
        pixel_size_mm: [1, 1],
        image_size_px: [120, 50],
        raw_offset_px: [0, 0],
      },
      {
        name: "lower",
        origin_mm: [-60, 30, 100],
        fast_axis: [1, 0, 0],
        slow_axis: [0, 1, 0],
        pixel_size_mm: [1, 1],
        image_size_px: [120, 50],
        raw_offset_px: [0, 70],
      },
    ],
  });
}

describe("ring_geometry_utils", () => {
  it("builds drawable ring segments for prepared geometry", () => {
    const geometry = createGeometry();
    const segments = buildGeometryRingSegments({
      geometry,
      energyEv: 12398.4193,
      dSpacing: 5,
      sampleCount: 360,
    });

    expect(geometry.panels).toHaveLength(2);
    expect(segments.length).toBeGreaterThan(0);
    expect(segments.some((segment) => segment.length >= 2)).toBe(true);
    expect(pickGeometryRingLabelPoint(segments)).toBeTruthy();
  });

  it("returns consistent resolution values on geometry ring pixels", () => {
    const geometry = createGeometry();
    const targetD = 5;
    const segments = buildGeometryRingSegments({
      geometry,
      energyEv: 12398.4193,
      dSpacing: targetD,
      sampleCount: 360,
    });
    const point = pickGeometryRingLabelPoint(segments);

    expect(point).toBeTruthy();
    const resolved = getGeometryResolutionAtPixel(
      Math.round(point.x),
      Math.round(point.y),
      geometry,
      12398.4193,
    );
    expect(resolved).not.toBeNull();
    expect(resolved).toBeCloseTo(targetD, 1);
  });

  it("returns null for pixels that fall into detector gaps", () => {
    const geometry = createGeometry();
    expect(getGeometryResolutionAtPixel(100, 90, geometry, 12398.4193)).toBeNull();
  });

  it("applies beam-center and distance overrides to the prepared geometry", () => {
    const geometry = createCenteredGeometry();
    const reference = getGeometryReferencePose(geometry);

    expect(reference).toBeTruthy();
    const adjusted = applyGeometryOverrides(geometry, {
      centerX: reference.centerX + 7.5,
      centerY: reference.centerY - 4.25,
      distanceMm: reference.distanceMm + 25,
    });
    const adjustedReference = getGeometryReferencePose(adjusted);

    expect(adjustedReference).toBeTruthy();
    expect(adjustedReference.centerX).toBeCloseTo(reference.centerX + 7.5, 6);
    expect(adjustedReference.centerY).toBeCloseTo(reference.centerY - 4.25, 6);
    expect(adjustedReference.distanceMm).toBeCloseTo(reference.distanceMm + 25, 6);
  });

  it("derives a usable reference pose even when the direct beam falls into a detector gap", () => {
    const geometry = createGapGeometry();
    const reference = getGeometryReferencePose(geometry);

    expect(reference).toBeTruthy();
    expect(reference.centerX).toBeCloseTo(59.5, 6);
    expect(reference.centerY).toBeGreaterThan(49.5);
    expect(reference.centerY).toBeLessThan(80.5);
    expect(reference.distanceMm).toBeCloseTo(100, 6);
  });
});
