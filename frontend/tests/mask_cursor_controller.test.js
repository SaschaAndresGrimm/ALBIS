import { describe, expect, it, vi } from "vitest";

vi.mock("../modules/i18n.js", () => ({
  t: (key, vars = {}) =>
    key === "cursor.measured"
      ? ` (measured ${vars.value})`
      : `X ${vars.x} Y ${vars.y} Value ${vars.value}${vars.resolutionText || ""}`,
}));

import { createMaskCursorController } from "../modules/mask_cursor_controller.js";

describe("mask_cursor_controller", () => {
  it("returns off-image coordinates for circular ROI drags when explicitly allowed", () => {
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });

    const controller = createMaskCursorController({
      apiBase: "/api",
      state: {
        hasFrame: true,
        width: 100,
        height: 100,
        zoom: 1,
        renderOffsetX: 0,
        renderOffsetY: 0,
      },
      elements: {
        canvasWrap,
        canvasShell: document.createElement("div"),
        cursorOverlay: document.createElement("div"),
        histTooltip: document.createElement("div"),
        maskToggle: document.createElement("input"),
        maskSaturatedToggle: document.createElement("input"),
        simplonUrl: document.createElement("input"),
        simplonVersion: document.createElement("input"),
      },
      callbacks: {
        isHdfFile: vi.fn(),
        parseDtype: vi.fn(),
        parseShape: vi.fn(),
        typedArrayFrom: vi.fn(),
        getActiveSaturationMax: vi.fn(() => null),
        redraw: vi.fn(),
        getDtypeInfo: vi.fn(),
        formatValue: vi.fn(),
        isSaturatedValue: vi.fn(() => false),
        getResolutionAtPixel: vi.fn(() => null),
        getEffectiveScrollLeft: vi.fn(() => 0),
        getEffectiveScrollTop: vi.fn(() => 0),
      },
      core: {
        roiStats: { updateGlobalStats: vi.fn() },
        roiInteraction: { scheduleRoiUpdate: vi.fn() },
        autoloadStatus: { setAutoloadStatus: vi.fn() },
      },
    });

    const point = controller.getImagePointFromEvent(
      { clientX: 10, clientY: 25 },
      { allowOutside: true, allowOutsideViewport: true },
    );

    expect(point).toEqual({ x: -10, y: -5 });
  });

  it("renders whole-number float samples without trailing decimals in the cursor readout", () => {
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });
    const canvasShell = document.createElement("div");
    canvasShell.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });
    const cursorOverlay = document.createElement("div");

    const controller = createMaskCursorController({
      apiBase: "/api",
      state: {
        hasFrame: true,
        width: 1,
        height: 1,
        zoom: 1,
        renderOffsetX: 0,
        renderOffsetY: 0,
        dataRaw: new Float32Array([24]),
        dtype: "<f4",
        maskEnabled: false,
        maskAvailable: false,
        maskRaw: null,
        maskShape: null,
        maskSaturatedEnabled: false,
      },
      elements: {
        canvasWrap,
        canvasShell,
        cursorOverlay,
        histTooltip: document.createElement("div"),
        maskToggle: document.createElement("input"),
        maskSaturatedToggle: document.createElement("input"),
        simplonUrl: document.createElement("input"),
        simplonVersion: document.createElement("input"),
      },
      callbacks: {
        isHdfFile: vi.fn(),
        parseDtype: vi.fn(),
        parseShape: vi.fn(),
        typedArrayFrom: vi.fn(),
        getActiveSaturationMax: vi.fn(() => null),
        redraw: vi.fn(),
        getDtypeInfo: vi.fn(() => ({ kind: "f", bits: 32 })),
        formatValue: vi.fn(() => "24.000"),
        isSaturatedValue: vi.fn(() => false),
        getResolutionAtPixel: vi.fn(() => null),
        getEffectiveScrollLeft: vi.fn(() => 0),
        getEffectiveScrollTop: vi.fn(() => 0),
      },
      core: {
        roiStats: { updateGlobalStats: vi.fn() },
        roiInteraction: { scheduleRoiUpdate: vi.fn() },
        autoloadStatus: { setAutoloadStatus: vi.fn() },
      },
    });

    controller.updateCursorOverlay({ clientX: 20.5, clientY: 30.5 });

    expect(cursorOverlay.textContent).toBe("X 0 Y 0 Value 24");
  });

  it("moves the readout clear of the magnifier's lens when the lens appears under a still mouse", () => {
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 0, top: 0, width: 400, height: 400 });
    const canvasShell = document.createElement("div");
    canvasShell.getBoundingClientRect = () => ({ left: 0, top: 0, width: 400, height: 400 });
    const cursorOverlay = document.createElement("div");
    let clearance = 0;

    const controller = createMaskCursorController({
      apiBase: "/api",
      state: {
        hasFrame: true,
        width: 4,
        height: 4,
        zoom: 100,
        renderOffsetX: 0,
        renderOffsetY: 0,
        dataRaw: new Uint32Array(16).fill(5),
        dtype: "<u4",
        maskEnabled: false,
        maskAvailable: false,
        maskRaw: null,
        maskShape: null,
        maskSaturatedEnabled: false,
      },
      elements: {
        canvasWrap,
        canvasShell,
        cursorOverlay,
        histTooltip: document.createElement("div"),
        maskToggle: document.createElement("input"),
        maskSaturatedToggle: document.createElement("input"),
        simplonUrl: document.createElement("input"),
        simplonVersion: document.createElement("input"),
      },
      callbacks: {
        isHdfFile: vi.fn(),
        parseDtype: vi.fn(),
        parseShape: vi.fn(),
        typedArrayFrom: vi.fn(),
        getActiveSaturationMax: vi.fn(() => null),
        redraw: vi.fn(),
        getDtypeInfo: vi.fn(() => ({ kind: "u", bits: 32 })),
        formatValue: vi.fn(() => "5"),
        isSaturatedValue: vi.fn(() => false),
        getCursorClearance: () => clearance,
        getResolutionAtPixel: vi.fn(() => null),
        getEffectiveScrollLeft: vi.fn(() => 0),
        getEffectiveScrollTop: vi.fn(() => 0),
      },
      core: {
        roiStats: { updateGlobalStats: vi.fn() },
        roiInteraction: { scheduleRoiUpdate: vi.fn() },
        autoloadStatus: { setAutoloadStatus: vi.fn() },
      },
    });

    controller.updateCursorOverlay({ clientX: 100, clientY: 100 });
    expect(cursorOverlay.style.left).toBe("112px");

    // Z pressed: the lens shows, the mouse has not moved.
    clearance = 78;
    controller.repositionCursorOverlay();
    expect(cursorOverlay.style.left).toBe("190px");
    expect(cursorOverlay.style.top).toBe("190px");

    // A hidden readout stays hidden and where it was.
    controller.hideCursorOverlay();
    clearance = 0;
    controller.repositionCursorOverlay();
    expect(cursorOverlay.classList.contains("is-visible")).toBe(false);
    expect(cursorOverlay.style.left).toBe("190px");
  });

  it("shows the corrected value with the measured one beside it, while flatfield correction is on", () => {
    const canvasWrap = document.createElement("div");
    canvasWrap.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });
    const canvasShell = document.createElement("div");
    canvasShell.getBoundingClientRect = () => ({ left: 20, top: 30, width: 100, height: 100 });
    const cursorOverlay = document.createElement("div");

    const controller = createMaskCursorController({
      apiBase: "/api",
      state: {
        hasFrame: true,
        width: 1,
        height: 1,
        zoom: 1,
        renderOffsetX: 0,
        renderOffsetY: 0,
        dataRaw: new Float32Array([24]),
        dtype: "<f4",
        maskEnabled: false,
        maskAvailable: false,
        maskRaw: null,
        maskShape: null,
        maskSaturatedEnabled: false,
      },
      elements: {
        canvasWrap,
        canvasShell,
        cursorOverlay,
        histTooltip: document.createElement("div"),
        maskToggle: document.createElement("input"),
        maskSaturatedToggle: document.createElement("input"),
        simplonUrl: document.createElement("input"),
        simplonVersion: document.createElement("input"),
      },
      callbacks: {
        isHdfFile: vi.fn(),
        parseDtype: vi.fn(),
        parseShape: vi.fn(),
        typedArrayFrom: vi.fn(),
        getActiveSaturationMax: vi.fn(() => null),
        redraw: vi.fn(),
        getDtypeInfo: vi.fn(() => ({ kind: "f", bits: 32 })),
        formatValue: vi.fn((v) => String(v)),
        isSaturatedValue: vi.fn(() => false),
        getResolutionAtPixel: vi.fn(() => null),
        getEffectiveScrollLeft: vi.fn(() => 0),
        getEffectiveScrollTop: vi.fn(() => 0),
        getInpaintedFrame: () => ({ data: new Float32Array([0.5]), holes: null }),
      },
      core: {
        roiStats: { updateGlobalStats: vi.fn() },
        roiInteraction: { scheduleRoiUpdate: vi.fn() },
        autoloadStatus: { setAutoloadStatus: vi.fn() },
      },
    });

    controller.updateCursorOverlay({ clientX: 20.5, clientY: 30.5 });

    // Value 0.5, then the measured 24 beside it.
    expect(cursorOverlay.textContent).toBe("X 0 Y 0 Value 0.5 (measured 24)");
  });
});

describe("mask_cursor_controller mask request races", () => {
  // Two files from the same detector produce same-shaped masks, so
  // alignMaskToFrame cannot tell them apart. Tag the contents to see which one
  // actually landed.
  function setup() {
    const state = {
      file: "A.h5",
      shape: [10, 64, 64],
      width: 64,
      height: 64,
      hasFrame: true,
      maskFile: "",
      maskRaw: null,
      maskShape: null,
      maskAvailable: false,
      maskEnabled: false,
      maskAuto: true,
      maskPath: "",
    };
    const release = {};
    const fetchMock = vi.fn((url) => {
      const file = decodeURIComponent(String(url).split("file=")[1].split("&")[0]);
      return new Promise((resolve) => {
        release[file] = () =>
          resolve({
            ok: true,
            arrayBuffer: async () => new Uint32Array(64 * 64).fill(file === "A.h5" ? 111 : 222).buffer,
            headers: {
              get: (name) =>
                name === "X-Dtype" ? "<u4" : name === "X-Shape" ? "64,64" : `${file}:/mask`,
            },
          });
      });
    });
    global.fetch = fetchMock;

    const input = () => document.createElement("input");
    const controller = createMaskCursorController({
      apiBase: "/api",
      state,
      elements: {
        canvasWrap: document.createElement("div"),
        canvasShell: document.createElement("div"),
        cursorOverlay: document.createElement("div"),
        histTooltip: document.createElement("div"),
        maskToggle: input(),
        maskSaturatedToggle: input(),
        simplonUrl: input(),
        simplonVersion: input(),
      },
      callbacks: {
        isHdfFile: () => true,
        parseDtype: () => "<u4",
        parseShape: (value) => String(value).split(",").map(Number),
        typedArrayFrom: (buffer) => new Uint32Array(buffer),
        getActiveSaturationMax: () => null,
        redraw: vi.fn(),
        getDtypeInfo: () => null,
        formatValue: String,
        isSaturatedValue: () => false,
        getResolutionAtPixel: () => null,
        getEffectiveScrollLeft: () => 0,
        getEffectiveScrollTop: () => 0,
      },
      core: {
        roiStats: { updateGlobalStats: vi.fn() },
        roiInteraction: { scheduleRoiUpdate: vi.fn() },
        autoloadStatus: { setAutoloadStatus: vi.fn() },
      },
    });
    return { state, controller, release, fetchMock };
  }

  it("switches the mask off for a file without one, and logs only a failure to ask", async () => {
    // A 404 is the server saying "no mask in this file", routine for many
    // files; logging it would fill the backend log (console.error goes there).
    const logged = vi.spyOn(console, "error").mockImplementation(() => {});
    vi.spyOn(console, "warn").mockImplementation(() => {});
    const { state, controller } = setup();
    state.maskEnabled = true;
    global.fetch = vi.fn(async () => ({ ok: false, status: 404, json: async () => ({ detail: "no mask" }) }));
    await controller.loadMask();
    expect(state.maskEnabled).toBe(false);
    expect(state.maskAvailable).toBe(false);
    expect(logged).not.toHaveBeenCalled();

    state.maskFile = "";
    global.fetch = vi.fn(async () => {
      throw new TypeError("Failed to fetch");
    });
    await controller.loadMask();
    expect(state.maskAvailable).toBe(false);
    expect(logged).toHaveBeenCalled();
  });

  it("ignores a mask that arrives after the file has changed", async () => {
    const { state, controller, release } = setup();

    const pendingA = controller.loadMask();
    state.file = "B.h5";
    const pendingB = controller.loadMask();

    release["B.h5"]();
    await pendingB;
    expect(state.maskRaw[0]).toBe(222);

    release["A.h5"]();
    await pendingA;

    // The stale response must not overwrite the mask that belongs to B, and
    // must not leave B's cache key describing A's data.
    expect(state.maskRaw[0]).toBe(222);
    expect(state.maskPath).toBe("B.h5:/mask");
  });

  it("does not leave a stale mask cached against the new file", async () => {
    const { state, controller, release, fetchMock } = setup();

    const pendingA = controller.loadMask();
    state.file = "B.h5";
    const pendingB = controller.loadMask();
    release["B.h5"]();
    await pendingB;
    release["A.h5"]();
    await pendingA;

    // Reloading must not serve A's mask from B's cache entry, which is how the
    // wrong mask used to survive for the rest of the session.
    await controller.loadMask();
    expect(state.maskRaw[0]).toBe(222);
    expect(fetchMock.mock.calls.length).toBe(2);
  });

  it("drops a mask still in flight when the session is cleared", async () => {
    const { state, controller, release } = setup();

    const pendingA = controller.loadMask();
    controller.clearMaskState();
    release["A.h5"]();
    await pendingA;

    expect(state.maskRaw).toBeNull();
    expect(state.maskAvailable).toBe(false);
  });
});

describe("mask_cursor_controller with a pinned mask (Data -> Pixel Mask)", () => {
  function build(state, pinned, extraElements = {}) {
    const callbacks = {
      isHdfFile: vi.fn(() => true),
      parseDtype: vi.fn(),
      parseShape: vi.fn(),
      typedArrayFrom: vi.fn(),
      getActiveSaturationMax: vi.fn(() => null),
      redraw: vi.fn(),
      getDtypeInfo: vi.fn(),
      formatValue: vi.fn(),
      isSaturatedValue: vi.fn(() => false),
      getResolutionAtPixel: vi.fn(() => null),
      getEffectiveScrollLeft: vi.fn(() => 0),
      getEffectiveScrollTop: vi.fn(() => 0),
      getPinnedMask: vi.fn(() => pinned.current),
      onMaskChanged: vi.fn(),
    };
    const controller = createMaskCursorController({
      apiBase: "/api",
      state,
      elements: { maskToggle: document.createElement("input"), ...extraElements },
      callbacks,
      core: {
        autoloadStatus: { setAutoloadStatus: vi.fn() },
        roiStats: { updateGlobalStats: vi.fn() },
        roiInteraction: { scheduleRoiUpdate: vi.fn() },
      },
    });
    return { controller, callbacks };
  }

  it("uses the pinned mask instead of fetching the file's", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const pinned = { current: { data: new Uint32Array([0, 1, 0, 2]), shape: [2, 2], name: "beamstop" } };
    const state = { file: "run_master.h5", width: 2, height: 2, hasFrame: true, maskAuto: true };
    const { controller, callbacks } = build(state, pinned);

    await controller.loadMask(true);
    expect(fetchMock).not.toHaveBeenCalled();
    expect(Array.from(state.maskRaw)).toEqual([0, 1, 0, 2]);
    expect(state.maskAvailable).toBe(true);
    expect(state.maskEnabled).toBe(true);
    expect(controller.isPinnedMaskInUse()).toBe(true);
    expect(callbacks.onMaskChanged).toHaveBeenCalled();
    vi.unstubAllGlobals();
  });

  it("puts the pinned mask back when a new file or live frame cleared it", () => {
    const pinned = { current: { data: new Uint32Array([1, 0, 0, 0]), shape: [2, 2], name: "m" } };
    const state = { file: "a.h5", width: 2, height: 2 };
    const { controller } = build(state, pinned);
    controller.clearMaskState();
    expect(state.maskRaw).toBeNull();
    // What every frame goes through:
    controller.alignMaskToFrame();
    expect(Array.from(state.maskRaw)).toEqual([1, 0, 0, 0]);
  });

  it("goes back to the file's own mask once unpinned", async () => {
    const fetchMock = vi.fn(async () => ({
      ok: true,
      headers: { get: (name) => ({ "X-Dtype": "<u4", "X-Shape": "2,2" })[name] ?? null },
      arrayBuffer: async () => new Uint32Array([0, 0, 0, 8]).buffer,
    }));
    vi.stubGlobal("fetch", fetchMock);
    const pinned = { current: { data: new Uint32Array([1, 1, 1, 1]), shape: [2, 2], name: "m" } };
    const state = { file: "run_master.h5", width: 2, height: 2, hasFrame: true };
    const { controller, callbacks } = build(state, pinned);
    callbacks.parseDtype.mockImplementation((value) => value);
    callbacks.parseShape.mockImplementation((value) => value.split(",").map(Number));
    callbacks.typedArrayFrom.mockImplementation((buffer) => new Uint32Array(buffer));
    await controller.refreshPinnedMask();
    expect(Array.from(state.maskRaw)).toEqual([1, 1, 1, 1]);

    pinned.current = null;
    await controller.refreshPinnedMask();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(Array.from(state.maskRaw)).toEqual([0, 0, 0, 8]);
    expect(controller.isPinnedMaskInUse()).toBe(false);
    vi.unstubAllGlobals();
  });

  it("does not fetch a live detector's mask over a pinned one", async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal("fetch", fetchMock);
    const simplonUrl = document.createElement("input");
    simplonUrl.value = "192.168.1.10";
    const pinned = { current: { data: new Uint32Array([1]), shape: [1, 1], name: "m" } };
    const { controller } = build({ width: 1, height: 1, autoload: {} }, pinned, { simplonUrl });
    await controller.fetchSimplonMask();
    expect(fetchMock).not.toHaveBeenCalled();
    // Unpinned, the same call does ask the detector.
    pinned.current = null;
    fetchMock.mockResolvedValue({ status: 204, ok: true });
    await controller.fetchSimplonMask();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    vi.unstubAllGlobals();
  });
});
