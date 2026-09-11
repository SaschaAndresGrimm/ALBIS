import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  CLONE_SLOT_PREFIX,
  captureWindowState,
  claimClonePayload,
  discardClonePayload,
  parseWindowClonePayload,
  readCloneTokenFromHash,
  stashClonePayload,
  sweepStaleClonePayloads,
} from "../modules/window_clone_state.js";

import { createAnalysisState, createRoiState } from "../modules/state.js";

/**
 * The payload is the whole correctness argument for duplicating a window: what
 * it carries appears in the clone, and what it omits is re-derived there. The
 * omissions matter more than the inclusions -- a copied render offset or a
 * copied frame count shows the user something that is not true of their window.
 */

function fullState(overrides = {}) {
  return {
    file: "/data/series_master.h5",
    dataset: "/entry/data/data",
    frameIndex: 7,
    thresholdIndex: 1,
    autoScale: false,
    min: 12,
    max: 900,
    colormap: "albulaHdr",
    invert: true,
    histLogX: true,
    histLogY: false,
    pixelLabels: true,
    maskEnabled: true,
    maskAuto: false,
    maskSaturatedEnabled: true,
    maskFile: "mask.h5",
    maskPath: "/data/mask.h5",
    // None of the following may appear in a payload.
    dataRaw: new Uint16Array(1024),
    dataFloat: new Float32Array(1024),
    maskRaw: new Uint32Array(1024),
    maskShape: [512, 512],
    histogram: { counts: [1, 2, 3] },
    stats: { mean: 4 },
    globalStats: { mean: 5 },
    renderOffsetX: 137,
    renderOffsetY: 42,
    width: 1028,
    height: 512,
    pixelAspect: 3,
    frameCount: 1800,
    thresholdCount: 2,
    hasFrame: true,
    isLoading: false,
    playing: true,
    buildStampAtLoad: "abc123",
    autoload: { mode: "simplon", running: true, simplonUrl: "http://192.168.20.47" },
    ...overrides,
  };
}

const VIEWPORT = { zoom: 2.5, centerX: 514, centerY: 256 };

describe("captureWindowState carries the settings", () => {
  it("takes source identity, contrast, display, mask and overlays", () => {
    const analysisState = {
      ...createAnalysisState(),
      ringsEnabled: true,
      rings: [8, 4, 2],
      ringCount: 3,
      distanceMm: 120.5,
      energyEv: 12400,
      centerX: 514.2,
      centerY: 257.8,
      peaksEnabled: true,
      peakCount: 100,
      peakMinSnr: 5,
    };
    const roiState = {
      ...createRoiState(),
      active: true,
      mode: "box",
      start: { x: 10, y: 20 },
      end: { x: 110, y: 60 },
    };

    const payload = captureWindowState({
      state: fullState(),
      analysisState,
      roiState,
      viewport: VIEWPORT,
    });

    expect(payload.v).toBe(1);
    expect(payload.source).toEqual({
      file: "/data/series_master.h5",
      dataset: "/entry/data/data",
      frameIndex: 7,
      thresholdIndex: 1,
    });
    expect(payload.viewport).toEqual(VIEWPORT);
    expect(payload.contrast).toEqual({
      autoScale: false,
      min: 12,
      max: 900,
      colormap: "albulaHdr",
      invert: true,
    });
    expect(payload.analysis.rings).toEqual([8, 4, 2]);
    expect(payload.analysis.peakCount).toBe(100);
    expect(payload.roi.mode).toBe("box");
    expect(payload.roi.start).toEqual({ x: 10, y: 20 });
    expect(payload.mask.path).toBe("/data/mask.h5");
  });

  /**
   * A whitelist test rather than a blacklist of today's fields: a field added
   * to state.js tomorrow must not be able to leak in unnoticed, and several of
   * these would actively mislead if copied.
   */
  it("carries no decoded data, no window-sized value and no live-source config", () => {
    const payload = captureWindowState({
      state: fullState(),
      analysisState: createAnalysisState(),
      roiState: createRoiState(),
      viewport: VIEWPORT,
    });
    // Key names, not a substring search: "histogram" occurs inside the
    // legitimate "histLogX"/"histogramEnabled", which is how the first version
    // of this test failed against a correct payload.
    const keys = new Set();
    (function walk(node) {
      if (!node || typeof node !== "object") return;
      for (const [key, value] of Object.entries(node)) {
        keys.add(key);
        walk(value);
      }
    })(payload);

    for (const forbidden of [
      "dataRaw",
      "dataFloat",
      "maskRaw",
      "maskShape",
      "histogram",
      "stats",
      "globalStats",
      "renderOffsetX",
      "renderOffsetY",
      "pixelAspect",
      "frameCount",
      "thresholdCount",
      "buildStampAtLoad",
      "autoload",
      "simplonUrl",
      "peaks",
      "externalPeakSets",
      "ringGeometry",
      "geometryManualKey",
      "playing",
    ]) {
      expect(keys).not.toContain(forbidden);
    }
    // And it stays small enough for a storage slot.
    expect(JSON.stringify(payload).length).toBeLessThan(4096);
  });

  it("omits a manual geometry override that is not in force", () => {
    const payload = captureWindowState({
      state: fullState(),
      analysisState: { ...createAnalysisState(), geometryDistanceManual: false },
      roiState: createRoiState(),
      viewport: VIEWPORT,
    });
    expect(payload.analysis.manual).toEqual({});
  });

  it("records which manual overrides are in force, but never their key", () => {
    const payload = captureWindowState({
      state: fullState(),
      analysisState: {
        ...createAnalysisState(),
        geometryDistanceManual: true,
        geometryCenterXManual: true,
        geometryManualKey: "a-key-from-the-other-window",
      },
      roiState: createRoiState(),
      viewport: VIEWPORT,
    });
    expect(payload.analysis.manual).toEqual({ distance: true, centerX: true });
    expect(JSON.stringify(payload)).not.toContain("a-key-from-the-other-window");
  });

  it("refuses to capture a window with no file", () => {
    expect(
      captureWindowState({
        state: fullState({ file: "" }),
        analysisState: createAnalysisState(),
        roiState: createRoiState(),
        viewport: VIEWPORT,
      })
    ).toBeNull();
  });

  it("keeps an unknown geometry unknown instead of calling it zero", () => {
    // createAnalysisState leaves these null until a file or the user supplies
    // them. Number(null) is 0, so a naive capture turns "no detector distance"
    // into "a detector distance of 0 mm" and the clone reports d-spacings the
    // original never showed.
    const payload = captureWindowState({
      state: fullState(),
      analysisState: createAnalysisState(),
      roiState: createRoiState(),
      viewport: VIEWPORT,
    });
    expect(payload.analysis.distanceMm).toBeNull();
    expect(payload.analysis.pixelSizeUm).toBeNull();
    expect(payload.analysis.energyEv).toBeNull();
    expect(payload.analysis.centerX).toBeNull();
    expect(payload.analysis.centerY).toBeNull();
  });

  it("still carries a real zero when the value genuinely is zero", () => {
    const payload = captureWindowState({
      state: fullState(),
      analysisState: { ...createAnalysisState(), distanceMm: 0, centerX: 0 },
      roiState: createRoiState(),
      viewport: VIEWPORT,
    });
    expect(payload.analysis.distanceMm).toBe(0);
    expect(payload.analysis.centerX).toBe(0);
  });

  it("survives a missing viewport rather than inventing one", () => {
    const payload = captureWindowState({
      state: fullState(),
      analysisState: createAnalysisState(),
      roiState: createRoiState(),
      viewport: null,
    });
    expect(payload.viewport).toBeNull();
  });
});

describe("parseWindowClonePayload", () => {
  it("round-trips what capture produced", () => {
    const payload = captureWindowState({
      state: fullState(),
      analysisState: createAnalysisState(),
      roiState: createRoiState(),
      viewport: VIEWPORT,
    });
    expect(parseWindowClonePayload(JSON.stringify(payload))).toEqual(payload);
  });

  it.each([
    ["nothing", ""],
    ["truncated json", '{"v":1,"source":{'],
    ["not an object", '"hello"'],
    ["a future version", '{"v":2,"source":{"file":"/x.h5"}}'],
    ["no source file", '{"v":1,"source":{"file":""}}'],
    ["no source at all", '{"v":1}'],
  ])("returns null for %s", (_label, raw) => {
    expect(parseWindowClonePayload(raw)).toBeNull();
  });
});

describe("the handoff slot is single-use", () => {
  beforeEach(() => {
    window.localStorage.clear();
  });

  const payloadOf = () =>
    captureWindowState({
      state: fullState(),
      analysisState: createAnalysisState(),
      roiState: createRoiState(),
      viewport: VIEWPORT,
    });

  it("hands the payload over exactly once", () => {
    const token = stashClonePayload(payloadOf());
    expect(token).toBeTruthy();
    expect(claimClonePayload(token)?.source.file).toBe("/data/series_master.h5");
    // A reload of the clone must not replay it.
    expect(claimClonePayload(token)).toBeNull();
  });

  it("leaves nothing behind in storage after a claim", () => {
    const token = stashClonePayload(payloadOf());
    claimClonePayload(token);
    const leftover = Object.keys(window.localStorage).filter((k) =>
      k.startsWith(CLONE_SLOT_PREFIX)
    );
    expect(leftover).toEqual([]);
  });

  it("discards a slot whose window the browser blocked", () => {
    const token = stashClonePayload(payloadOf());
    discardClonePayload(token);
    expect(claimClonePayload(token)).toBeNull();
  });

  it("sweeps a slot nobody ever claimed", () => {
    vi.useFakeTimers();
    try {
      const token = stashClonePayload(payloadOf());
      expect(sweepStaleClonePayloads(Date.now())).toBe(0);
      // Well past the TTL.
      expect(sweepStaleClonePayloads(Date.now() + 120_000)).toBe(1);
      expect(claimClonePayload(token)).toBeNull();
    } finally {
      vi.useRealTimers();
    }
  });

  it("leaves unrelated keys alone when sweeping", () => {
    window.localStorage.setItem("albis.recentFiles", '["/data/a.h5"]');
    stashClonePayload(payloadOf());
    sweepStaleClonePayloads(Date.now() + 120_000);
    expect(window.localStorage.getItem("albis.recentFiles")).toBe('["/data/a.h5"]');
  });

  it("claims nothing for an unknown token", () => {
    expect(claimClonePayload("no-such-token")).toBeNull();
    expect(claimClonePayload("")).toBeNull();
  });
});

describe("readCloneTokenFromHash", () => {
  it.each([
    ["#albis-clone=abc-123", "abc-123"],
    ["albis-clone=abc-123", "abc-123"],
    ["#other=1&albis-clone=tok9", "tok9"],
    ["#albis-clone=", ""],
    ["#something-else=1", ""],
    ["", ""],
    [undefined, ""],
  ])("reads %s", (hash, expected) => {
    expect(readCloneTokenFromHash(hash)).toBe(expected);
  });

  it("does not match a lookalike key", () => {
    expect(readCloneTokenFromHash("#not-albis-clone=tok")).toBe("");
  });
});
