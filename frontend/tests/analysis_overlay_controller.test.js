import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock() {
  return vi.fn(async () => ({
    ok: true,
    json: async () => ({
      "peaks.state.active_updating": "Updating peaks",
      "summary.active": "Active",
      "peaks.state.detected_count": "{{count}} peaks detected",
      "peaks.summary.count": "{{count}} peaks",
      "analysis.peaks.none_detected": "No peaks detected",
      "analysis.peaks.load_frame": "Load a frame to detect peaks.",
      "analysis.peaks.enable_hint": "Enable Peak Finder to detect peaks.",
      "summary.off": "Off",
      "summary.waiting_frame": "Waiting for frame",
      "peaks.state.enable_hint": "Enable Peak Finder to detect peaks.",
      "peaks.state.load_frame": "Load a frame",
      "peaks.state.none_on_frame": "No peaks on this frame",
      "peaks.summary.none": "None",
    }),
  }));
}

function buildController(createAnalysisOverlayController, stateOverrides = {}) {
  const peaksBody = document.createElement("div");
  const peaksSectionStateEl = document.createElement("div");
  const peaksSummaryEl = document.createElement("div");

  const controller = createAnalysisOverlayController({
    state: {
      hasFrame: true,
      isLoading: true,
      playing: false,
      dataRaw: new Float32Array([0, 0, 0]),
      width: 3,
      height: 1,
      maskEnabled: false,
      maskAvailable: false,
      maskRaw: null,
      maskShape: null,
      maskSaturatedEnabled: false,
      ...stateOverrides,
    },
    analysisState: {
      ringsEnabled: false,
      ringMode: "planar",
      ringGeometry: null,
      ringGeometrySource: "",
      distanceMm: null,
      pixelSizeUm: null,
      energyEv: null,
      centerX: null,
      centerY: null,
      rings: [],
      peaksEnabled: true,
      peakCount: 25,
      peaks: [{ x: 9, y: 8, intensity: 7 }],
      selectedPeaks: [],
      peakSelectionAnchor: null,
    },
    elements: {
      ringsDistance: null,
      ringsPixel: null,
      ringsEnergy: null,
      ringsCenterX: null,
      ringsCenterY: null,
      ringInputs: [],
      ringsSectionStateEl: null,
      ringsSummaryEl: null,
      peaksSectionStateEl,
      peaksSummaryEl,
      peaksBody,
      peaksCountInput: null,
      peaksCountHint: null,
    },
    constants: {
      defaultRingCount: 3,
      peakBadMaskBits: 0,
    },
    callbacks: {
      setSectionBadgeState: (el, _kind, text) => {
        if (el) el.textContent = text;
      },
      setSummaryChip: (el, text) => {
        if (el) el.textContent = text;
      },
      buildSkeletonList: () => document.createElement("div"),
      formatStat: (value) => String(value),
      schedulePeakOverlay: vi.fn(),
      setFieldHint: vi.fn(),
      getActiveSaturationMax: () => null,
      isSaturatedValue: () => false,
    },
  });

  return { controller, peaksBody, peaksSectionStateEl, peaksSummaryEl };
}

describe("analysis_overlay_controller", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("renders fresh peak rows during playback loading once detection is complete", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createAnalysisOverlayController } = await import("../modules/analysis_overlay_controller.js");

    const { controller, peaksBody, peaksSectionStateEl, peaksSummaryEl } = buildController(createAnalysisOverlayController);

    peaksBody.innerHTML = '<div class="peaks-row">stale peak row</div>';

    controller.renderPeakList();

    const renderedRows = peaksBody.querySelectorAll(".peaks-row");
    expect(renderedRows).toHaveLength(1);
    expect(renderedRows[0].textContent).toContain("9");
    expect(renderedRows[0].textContent).toContain("8");
    expect(renderedRows[0].textContent).toContain("7");
    expect(peaksBody.textContent).not.toContain("stale peak row");
    expect(peaksSectionStateEl.textContent).toContain("Updating peaks");
    expect(peaksSummaryEl.textContent).toContain("Active");
  });

  it("rejects lone hot pixels and reports sub-pixel centroids", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock();
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createAnalysisOverlayController } = await import("../modules/analysis_overlay_controller.js");

    const W = 21;
    const H = 21;
    const data = new Float32Array(W * H);
    const at = (x, y) => y * W + x;
    // A multi-pixel spot whose intensity is skewed toward +x so its centre of
    // mass lands between pixels.
    data[at(5, 6)] = 100;
    data[at(6, 6)] = 80;
    data[at(4, 6)] = 20;
    data[at(5, 5)] = 40;
    data[at(5, 7)] = 40;
    // A lone hot pixel (zinger): brighter than the spot but no shoulder.
    data[at(15, 15)] = 500;

    const { controller } = buildController(createAnalysisOverlayController, {
      dataRaw: data,
      width: W,
      height: H,
    });

    // SNR disabled so ranking is by intensity; isolates the footprint gate and
    // centroid refinement from the SNR test.
    const peaks = controller.detectPeaks(10, 0);

    // The hot pixel outranks the spot on intensity but must be filtered out.
    expect(peaks.some((p) => p.px === 15 && p.py === 15)).toBe(false);

    const spot = peaks.find((p) => p.px === 5 && p.py === 6);
    expect(spot).toBeTruthy();
    // Centroid pulled toward the brighter +x neighbour, staying within 1px.
    expect(spot.x).toBeGreaterThan(5);
    expect(spot.x).toBeLessThan(6);
    expect(spot.y).toBeCloseTo(6, 5);
  });
});
