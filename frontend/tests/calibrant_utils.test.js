import { describe, expect, it } from "vitest";

import { CALIBRANTS, CALIBRANT_GROUPS } from "../modules/calibrant_data.js";
import {
  MAX_REFERENCE_RINGS,
  calibrantShortName,
  detectorMinDSpacing,
  getCalibrant,
  referenceRingLabel,
  selectReferenceRings,
} from "../modules/calibrant_utils.js";
import { braggTwoTheta, prepareRingGeometry } from "../modules/ring_geometry_utils.js";

// 12398.4193 eV is 1 Å, so a d-spacing is easy to turn into a radius by hand.
const ONE_ANGSTROM_EV = 12398.4193;

describe("calibrant data", () => {
  it("gives every material a known group (or none, for ice), a label and its rings, highest d first", () => {
    const ids = new Set();
    for (const calibrant of CALIBRANTS) {
      expect(ids.has(calibrant.id)).toBe(false);
      ids.add(calibrant.id);
      if (calibrant.group !== null) expect(CALIBRANT_GROUPS).toContain(calibrant.group);
      expect(calibrant.label).toBeTruthy();
      expect(calibrant.lines.length).toBeGreaterThan(0);
      expect(calibrant.lines.length).toBeLessThanOrEqual(MAX_REFERENCE_RINGS);
      const ds = calibrant.lines.map(([d]) => d);
      expect(ds).toEqual([...ds].sort((a, b) => b - a));
      expect(new Set(ds).size).toBe(ds.length);
    }
    expect(CALIBRANTS.filter((calibrant) => calibrant.group === null).map((c) => c.id)).toEqual(["ice_Ih"]);
  });

  it("carries the standards that diffraction is checked against", () => {
    for (const id of ["Si", "LaB6", "CeO2", "AgBh", "Al", "ice_Ih"]) {
      expect(getCalibrant(id)).not.toBeNull();
    }
    expect(getCalibrant("Al").lines[0]).toEqual([2.33798, "111"]);
    expect(getCalibrant("AgBh").lines[0]).toEqual([58.38, "001"]);
    expect(getCalibrant("nope")).toBeNull();
    expect(getCalibrant("")).toBeNull();
  });
});

describe("referenceRingLabel", () => {
  it("names the ring by formula and hkl, or by d where the source has no hkl", () => {
    const lab6 = getCalibrant("LaB6");
    expect(calibrantShortName(lab6)).toBe("LaB₆");
    expect(calibrantShortName(getCalibrant("AgBh"))).toBe("AgBh");
    expect(referenceRingLabel(lab6, 4.1569, "100")).toBe("LaB₆ 100");
    expect(referenceRingLabel(getCalibrant("CrOx"), 3.645, "")).toBe("CrOx 3.65 Å");
  });
});

describe("selectReferenceRings", () => {
  const al = getCalibrant("Al");

  it("keeps the rings down to the detector's smallest d", () => {
    const rings = selectReferenceRings(al, 1.4);
    expect(rings.map((ring) => ring.hkl)).toEqual(["111", "200", "220"]);
    expect(rings[0]).toEqual({ d: 2.33798, hkl: "111", label: "Al 111" });
  });

  it("keeps a ring that sits exactly on the edge", () => {
    expect(selectReferenceRings(al, 2.02475).map((ring) => ring.hkl)).toEqual(["111", "200"]);
  });

  it("takes them all, up to the limit, when the edge is unknown", () => {
    expect(selectReferenceRings(al, null)).toHaveLength(al.lines.length);
    expect(selectReferenceRings(getCalibrant("LaB6"), null)).toHaveLength(MAX_REFERENCE_RINGS);
    expect(selectReferenceRings(al, null, 2)).toHaveLength(2);
  });

  it("draws nothing for no material", () => {
    expect(selectReferenceRings(null, 1)).toEqual([]);
  });
});

describe("detectorMinDSpacing", () => {
  const planar = {
    mode: "planar",
    energyEv: ONE_ANGSTROM_EV,
    distanceMm: 100,
    pixelSizeUm: 100,
    centerX: 0,
    centerY: 0,
  };

  it("is the d at the corner farthest from the beam on a flat detector", () => {
    // Corner (1000, 0): 100 mm out at 100 mm, so 2θ = 45°.
    const d = detectorMinDSpacing(planar, { width: 1000, height: 0.0001 });
    expect(d).toBeCloseTo(1 / (2 * Math.sin(Math.PI / 8)), 4);
    // The ring at that d is the one that just reaches the corner.
    expect(braggTwoTheta(d, ONE_ANGSTROM_EV)).toBeCloseTo(Math.PI / 4, 6);
  });

  it("measures a taller pixel along y", () => {
    const square = detectorMinDSpacing(planar, { width: 100, height: 100 });
    const tall = detectorMinDSpacing(planar, { width: 100, height: 100, pixelAspect: 3 });
    expect(tall).toBeLessThan(square);
  });

  it("is unknown without the geometry to compute it", () => {
    expect(detectorMinDSpacing({ ...planar, energyEv: null }, { width: 10, height: 10 })).toBeNull();
    expect(detectorMinDSpacing({ ...planar, distanceMm: null }, { width: 10, height: 10 })).toBeNull();
    expect(detectorMinDSpacing(planar, {})).toBeNull();
  });

  it("looks at every panel's corners for a multi-panel geometry", () => {
    // Two 100 x 100 panels of 0.1 mm pixels, 100 mm down the beam, the second
    // one 50 mm farther out: its far corner sets the edge.
    const panel = (x0, originXmm) => ({
      raw_offset_px: [x0, 0],
      image_size_px: [100, 100],
      origin_mm: [originXmm, -5, 100],
      fast_axis: [1, 0, 0],
      slow_axis: [0, 1, 0],
      pixel_size_mm: [0.1, 0.1],
    });
    const geometry = prepareRingGeometry({ mode: "geometry", panels: [panel(0, -5), panel(150, 50)] });
    const d = detectorMinDSpacing({ mode: "geometry", geometry, energyEv: ONE_ANGSTROM_EV });
    // Far corner of panel 2: x = 50 + 99.5 * 0.1, y = -5 + 99.5 * 0.1, z = 100.
    const x = 50 + 99.5 * 0.1;
    const y = -5 + 99.5 * 0.1;
    const twoTheta = Math.atan2(Math.hypot(x, y), 100);
    expect(d).toBeCloseTo(1 / (2 * Math.sin(twoTheta / 2)), 4);
  });
});
