import { beforeEach, describe, expect, it, vi } from "vitest";

const { showConfirmDialogMock } = vi.hoisted(() => ({ showConfirmDialogMock: vi.fn() }));
vi.mock("../modules/dialogs.js", () => ({
  showConfirmDialog: showConfirmDialogMock,
  showPromptDialog: vi.fn(),
}));
vi.mock("../modules/i18n.js", () => ({
  getLanguage: () => "en",
  t: (key, vars = {}) => (Object.keys(vars).length ? `${key} ${JSON.stringify(vars)}` : key),
}));

import {
  createPixelMaskController,
  diffMasks,
  hostOf,
  maskFileName,
  maskFileStem,
  maskThreshold,
  summarizeMask,
} from "../modules/pixel_mask_controller.js";

function maskResponse(data, shape, where = "") {
  return new Response(new Uint32Array(data).buffer, {
    headers: { "X-Dtype": "<u4", "X-Shape": shape.join(","), "X-Mask-Path": where },
  });
}

function jsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

function buildElements() {
  const make = (tag = "div") => document.createElement(tag);
  const elements = {
    section: make("section"),
    summaryChip: make("span"),
    stateEl: make(),
    sourceEl: make(),
    countsEl: make(),
    loadBtn: make("button"),
    saveH5Btn: make("button"),
    saveTiffBtn: make("button"),
    saveBtn: make("button"),
    saveMenu: Object.assign(make(), { hidden: true }),
    useDataBtn: make("button"),
    detSection: make("section"),
    detSummary: make("span"),
    detInfo: make("p"),
    detLoadBtn: make("button"),
    detSendBtn: make("button"),
    detBackup: make(),
    detBackupText: make("span"),
    detRestoreBtn: make("button"),
    detClearBtn: make("button"),
    editBtn: make("button"),
    discardBtn: make("button"),
    editsEl: make(),
    sendBtn: make("button"),
    backupRow: make(),
    backupText: make("span"),
    restoreBtn: make("button"),
    findBtn: make("button"),
    findPanel: make(),
    findHot: Object.assign(make("input"), { type: "checkbox", checked: true }),
    findHotSigma: Object.assign(make("input"), { value: "8" }),
    findCold: Object.assign(make("input"), { type: "checkbox", checked: true }),
    findColdPercent: Object.assign(make("input"), { value: "20" }),
    findResultEl: make(),
    findAddBtn: make("button"),
    findCloseBtn: make("button"),
  };
  Object.values(elements).forEach((el) => document.body.appendChild(el));
  return elements;
}

function memoryStorage() {
  const items = new Map();
  return {
    getItem: (key) => (items.has(key) ? items.get(key) : null),
    setItem: (key, value) => items.set(key, String(value)),
  };
}

function setup({ state = {}, detector = null, monitor = null, local = true } = {}) {
  const elements = buildElements();
  const callbacks = {
    refreshMask: vi.fn(),
    getDetector: vi.fn(() => detector),
    getMonitorDetector: vi.fn(() => monitor),
    onDetectorMaskWritten: vi.fn(),
    logDetector: vi.fn(),
    setStatus: vi.fn(),
    setSummaryChip: vi.fn((el, text) => {
      el.textContent = text;
    }),
    setSectionBadgeState: vi.fn((el, tone, text) => {
      el.dataset.tone = tone;
      el.textContent = text;
    }),
    openFileDialog: vi.fn(),
    openEditor: vi.fn(() => true),
    closeEditor: vi.fn(),
    resetEditor: vi.fn(),
    isEditing: vi.fn(() => false),
    setMarkers: vi.fn(),
  };
  const fullState = { maskRaw: null, maskShape: null, maskAvailable: false, hasFrame: false, file: "", ...state };
  const controller = createPixelMaskController({
    apiBase: "/api",
    state: fullState,
    elements,
    callbacks,
    storage: memoryStorage(),
    isBackendLocal: () => local,
  });
  return { controller, elements, callbacks, state: fullState };
}

const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

beforeEach(() => {
  document.body.innerHTML = "";
  showConfirmDialogMock.mockReset();
  vi.unstubAllGlobals();
});

describe("mask arithmetic", () => {
  it("counts masked pixels by reason, a gap first", () => {
    const summary = summarizeMask(new Uint32Array([0, 1, 2, 3, 8, 0x80000000, 0]));
    expect(summary).toEqual({ masked: 5, gap: 2, defective: 2, other: 1 });
  });

  it("says what replacing one mask with another changes", () => {
    const before = new Uint32Array([0, 0, 1, 2, 4]);
    const after = new Uint32Array([0, 2, 0, 8, 4]);
    expect(diffMasks(before, after)).toEqual({ added: 1, removed: 1, changed: 1 });
  });

  it("reads the threshold from where a mask was found", () => {
    expect(maskThreshold("/entry/instrument/detector/threshold_2_channel/pixel_mask")).toBe(2);
    expect(maskThreshold("/entry/instrument/detector/detectorSpecific/pixel_mask")).toBe(0);
  });

  it("names a saved mask after its source and the time, without growing on each round", () => {
    const now = new Date(2026, 9, 23, 17, 8);
    expect(maskFileName("run_7", { now })).toBe("run_7_pixel_mask_20261023-1708");
    expect(maskFileName("run_7", { edited: true, now })).toBe("run_7_pixel_mask_edited_20261023-1708");
    expect(maskFileName("run_7_pixel_mask_edited_20261022-0900", { now })).toBe("run_7_pixel_mask_20261023-1708");
    expect(maskFileName("SIMPLON monitor (192.168.20.231) S21", { now })).toBe(
      "SIMPLON_monitor_192.168.20.231_S21_pixel_mask_20261023-1708",
    );
    expect(maskFileName("", { now })).toBe("detector_pixel_mask_20261023-1708");
    expect(hostOf("192.168.20.231")).toBe("192.168.20.231");
    expect(hostOf("http://det.lab:8080/detector/api")).toBe("det.lab");
  });

  it("names a live view's mask after the detector's host, not the frame label", () => {
    const { controller, elements } = setup({
      state: {
        maskRaw: new Uint32Array([1, 0]),
        maskShape: [1, 2],
        maskAvailable: true,
        file: "SIMPLON monitor (192.168.20.231) S21 Img0 23.10.2026, 17:05:22",
        autoload: { running: true, mode: "simplon", simplonUrl: "http://192.168.20.231" },
      },
    });
    controller.setEnabled(true);
    expect(controller.currentMask().name).toMatch(/^192\.168\.20\.231_pixel_mask_\d{8}-\d{4}$/);
    expect(elements.sourceEl.textContent).toContain("pixel_mask.source.live");
  });

  it("names a mask after its file, without the series suffix", () => {
    expect(maskFileStem("data/run_7_master.h5")).toBe("run_7");
    expect(maskFileStem("C:\\masks\\good.tiff")).toBe("good");
    expect(maskFileStem("")).toBe("pixel_mask");
  });
});

describe("pixel mask controller", () => {
  it("stays hidden, and pins nothing, until switched on", () => {
    const { controller, elements } = setup({ detector: { url: "http://det", version: "1.8.0" } });
    expect(elements.section.hidden).toBe(true);
    expect(elements.detSection.hidden).toBe(true);
    expect(controller.getPinnedMask()).toBeNull();
  });

  it("describes the data's own mask once switched on", () => {
    const { controller, elements } = setup({
      state: {
        maskRaw: new Uint32Array([1, 0, 2, 0]),
        maskShape: [2, 2],
        maskAvailable: true,
        hasFrame: true,
        file: "run_master.h5",
        maskPath: "/entry/instrument/detector/pixel_mask",
      },
    });
    controller.setEnabled(true);
    expect(elements.section.hidden).toBe(false);
    expect(elements.summaryChip.textContent).toBe("pixel_mask.summary.data");
    expect(elements.stateEl.textContent).toContain('"count":"2"');
    expect(elements.sourceEl.textContent).toContain("/entry/instrument/detector/pixel_mask");
    expect(elements.saveTiffBtn.disabled).toBe(false);
    expect(elements.useDataBtn.hidden).toBe(true);
    // No detector connected: nothing on the Detector tab.
    expect(elements.detSection.hidden).toBe(true);
  });

  it("warns when the mask does not fit the image", () => {
    const { controller, elements } = setup({
      state: { maskRaw: new Uint32Array(4), maskShape: [2, 2], maskAvailable: false, hasFrame: true, dataRaw: new Uint16Array(6), width: 3, height: 2 },
    });
    controller.setEnabled(true);
    expect(elements.stateEl.dataset.tone).toBe("warning");
    expect(elements.stateEl.textContent).toContain("pixel_mask.state.mismatch");
  });

  it("pins a mask loaded from a file, and lets it go again", async () => {
    const fetchMock = vi.fn(async (url) => {
      if (url.startsWith("/api/choose-file")) return jsonResponse({ path: "/masks/beamstop.tiff" });
      if (url.startsWith("/api/pixel-mask/file")) return maskResponse([0, 4, 0, 0, 0, 1], [2, 3]);
      throw new Error(`unexpected ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    const { controller, elements, callbacks } = setup();
    controller.setEnabled(true);

    await controller.loadFromFile();
    expect(fetchMock.mock.calls[1][0]).toBe("/api/pixel-mask/file?file=%2Fmasks%2Fbeamstop.tiff");
    const pinned = controller.getPinnedMask();
    expect(pinned.shape).toEqual([2, 3]);
    expect(Array.from(pinned.data)).toEqual([0, 4, 0, 0, 0, 1]);
    expect(callbacks.refreshMask).toHaveBeenCalledTimes(1);
    expect(elements.summaryChip.textContent).toBe("pixel_mask.summary.file");
    expect(elements.useDataBtn.hidden).toBe(false);

    elements.useDataBtn.click();
    await flush();
    expect(controller.getPinnedMask()).toBeNull();
    expect(callbacks.refreshMask).toHaveBeenCalledTimes(2);
  });

  it("loads the mask of the threshold shown from a file with one per threshold", async () => {
    const fetchMock = vi.fn(async (url) => {
      if (url.startsWith("/api/choose-file")) return jsonResponse({ path: "/data/series_160_master.h5" });
      return maskResponse([0, 1], [1, 2], "/entry/instrument/detector/threshold_3_channel/pixel_mask");
    });
    vi.stubGlobal("fetch", fetchMock);
    const { controller, elements } = setup({ state: { thresholdIndex: 2 } });
    controller.setEnabled(true);
    await controller.loadFromFile();
    expect(fetchMock.mock.calls[1][0]).toBe("/api/pixel-mask/file?file=%2Fdata%2Fseries_160_master.h5&threshold=2");
    expect(elements.sourceEl.textContent).toContain("pixel_mask.source.threshold");
    expect(elements.sourceEl.textContent).toMatch(/n\\?":3/);
    expect(controller.currentMask().name).toMatch(/^series_160_threshold_3_pixel_mask_\d{8}-\d{4}$/);
  });

  it("drops a pinned mask when switched off", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url) =>
        url.startsWith("/api/choose-file") ? jsonResponse({ path: "m.h5" }) : maskResponse([1], [1, 1]),
      ),
    );
    const { controller, elements, callbacks } = setup();
    controller.setEnabled(true);
    await controller.loadFromFile();
    controller.setEnabled(false);
    expect(controller.getPinnedMask()).toBeNull();
    expect(elements.section.hidden).toBe(true);
    expect(callbacks.refreshMask).toHaveBeenCalledTimes(2);
  });

  it("uses ALBIS's own file picker when the backend is not on this computer", async () => {
    const fetchMock = vi.fn(async () => maskResponse([0], [1, 1]));
    vi.stubGlobal("fetch", fetchMock);
    const { controller, callbacks } = setup({ local: false });
    callbacks.openFileDialog.mockResolvedValue("masks/remote.h5");
    controller.setEnabled(true);
    await controller.loadFromFile();
    expect(callbacks.openFileDialog).toHaveBeenCalledWith({ exts: ".h5,.hdf5,.tif,.tiff" });
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(controller.getPinnedMask()).not.toBeNull();
  });

  it("reports a file without a mask instead of pinning nothing", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url) =>
        url.startsWith("/api/choose-file")
          ? jsonResponse({ path: "plain.h5" })
          : jsonResponse({ detail: "Pixel mask not found" }, 404),
      ),
    );
    const { controller, callbacks } = setup();
    controller.setEnabled(true);
    await controller.loadFromFile();
    expect(controller.getPinnedMask()).toBeNull();
    expect(callbacks.setStatus).toHaveBeenLastCalledWith("pixel_mask.status.load_failed", { tone: "error" });
  });
});

describe("pixel mask on the Detector tab", () => {
  const detector = { url: "http://det", version: "1.8.0", serial: "E-1", model: "EIGER2", locked: false };

  function detectorFetch({ onDetector, sendResult = { key: "pixel_mask", masked: 2, previous_masked: 1, backup: "masks/backups/b.tiff", verified: true } }) {
    return vi.fn(async (url, init = {}) => {
      if (url.startsWith("/api/pixel-mask/detector") && (init.method || "GET") === "GET") return onDetector();
      if (url === "/api/pixel-mask/detector" && init.method === "POST") return jsonResponse(sendResult);
      throw new Error(`unexpected ${init.method || "GET"} ${url}`);
    });
  }

  it("shows the section only with a connected detector, and locks Send while it works", () => {
    const { controller, elements } = setup({
      detector: { ...detector, locked: true },
      state: { maskRaw: new Uint32Array([1]), maskShape: [1, 1], maskAvailable: true },
    });
    controller.setEnabled(true);
    expect(elements.detSection.hidden).toBe(false);
    expect(elements.detSendBtn.disabled).toBe(true);
    expect(elements.detSummary.hidden).toBe(false);
  });

  it("cannot send without a mask in ALBIS, and says why", () => {
    const { controller, elements } = setup({ detector });
    controller.setEnabled(true);
    expect(elements.detSendBtn.disabled).toBe(true);
    expect(elements.detSendBtn.dataset.helpReason).toBe("pixel_mask.detector.no_mask");
  });

  it("pins the detector's mask on Use in ALBIS", async () => {
    vi.stubGlobal("fetch", detectorFetch({ onDetector: () => maskResponse([0, 1, 2, 0], [2, 2], "pixel_mask") }));
    const { controller, elements } = setup({ detector });
    controller.setEnabled(true);
    await controller.loadFromDetector();
    expect(controller.getPinnedMask().origin).toBe("detector");
    expect(elements.summaryChip.textContent).toBe("pixel_mask.summary.detector");
    expect(elements.detInfo.textContent).toContain('"count":"2"');
  });

  it("sends the mask in use after confirming what changes, and remembers the backup", async () => {
    const fetchMock = detectorFetch({ onDetector: () => maskResponse([1, 0, 0, 0], [2, 2]) });
    vi.stubGlobal("fetch", fetchMock);
    showConfirmDialogMock.mockResolvedValue(true);
    const { controller, elements, callbacks } = setup({
      detector,
      state: { maskRaw: new Uint32Array([1, 2, 0, 0]), maskShape: [2, 2], maskAvailable: true },
    });
    controller.setEnabled(true);

    await controller.sendCurrent();
    const question = showConfirmDialogMock.mock.calls[0][0];
    expect(question.danger).toBe(true);
    expect(question.message).toContain('"added":"1"');
    expect(question.message).toContain('"removed":"0"');
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    const meta = JSON.parse(post[1].body.get("meta"));
    expect(meta).toEqual({ url: "http://det", version: "1.8.0", serial: "E-1", shape: [2, 2] });
    expect(callbacks.logDetector).toHaveBeenCalledWith(expect.stringContaining("pixel_mask.detector.log.sent"));
    expect(elements.detBackup.hidden).toBe(false);
    expect(elements.detBackupText.textContent).toContain("pixel_mask.detector.backup");
  });

  it("sends nothing when the question is declined", async () => {
    const fetchMock = detectorFetch({ onDetector: () => maskResponse([0, 0], [1, 2]) });
    vi.stubGlobal("fetch", fetchMock);
    showConfirmDialogMock.mockResolvedValue(false);
    const { controller } = setup({ detector, state: { maskRaw: new Uint32Array([1, 0]), maskShape: [1, 2] } });
    controller.setEnabled(true);
    await controller.sendCurrent();
    expect(fetchMock.mock.calls.some(([, init]) => init?.method === "POST")).toBe(false);
  });

  it("does not ask, or send, a mask of another size", async () => {
    const fetchMock = detectorFetch({ onDetector: () => maskResponse([0, 0, 0, 0], [2, 2]) });
    vi.stubGlobal("fetch", fetchMock);
    const { controller, callbacks } = setup({ detector, state: { maskRaw: new Uint32Array([1, 0]), maskShape: [1, 2] } });
    controller.setEnabled(true);
    await controller.sendCurrent();
    expect(showConfirmDialogMock).not.toHaveBeenCalled();
    expect(fetchMock.mock.calls.some(([, init]) => init?.method === "POST")).toBe(false);
    expect(callbacks.setStatus.mock.calls.at(-1)[0]).toContain("pixel_mask.detector.status.shape");
  });

  it("does not ask when the detector already has the mask", async () => {
    vi.stubGlobal("fetch", detectorFetch({ onDetector: () => maskResponse([1, 0], [1, 2]) }));
    const { controller, callbacks } = setup({ detector, state: { maskRaw: new Uint32Array([1, 0]), maskShape: [1, 2] } });
    controller.setEnabled(true);
    await controller.sendCurrent();
    expect(showConfirmDialogMock).not.toHaveBeenCalled();
    expect(callbacks.setStatus.mock.calls.at(-1)[0]).toBe("pixel_mask.detector.status.same");
  });

  it("says so when the detector does not confirm the new mask", async () => {
    vi.stubGlobal(
      "fetch",
      detectorFetch({
        onDetector: () => maskResponse([0, 0], [1, 2]),
        sendResult: { key: "pixel_mask", masked: 1, previous_masked: 0, backup: "masks/backups/b.tiff", verified: false },
      }),
    );
    showConfirmDialogMock.mockResolvedValue(true);
    const { controller, callbacks } = setup({ detector, state: { maskRaw: new Uint32Array([1, 0]), maskShape: [1, 2] } });
    controller.setEnabled(true);
    await controller.sendCurrent();
    expect(callbacks.logDetector).toHaveBeenCalledWith("pixel_mask.detector.log.unverified", "error");
    expect(callbacks.setStatus).toHaveBeenLastCalledWith("pixel_mask.detector.status.unverified", { tone: "warning" });
  });

  it("restores the backup it kept, and keeps it for another restore", async () => {
    let onDetector = () => maskResponse([0, 0], [1, 2]);
    const fetchMock = vi.fn(async (url, init = {}) => {
      if (url.startsWith("/api/pixel-mask/file")) return maskResponse([0, 0], [1, 2]);
      if (url.startsWith("/api/pixel-mask/detector") && !init.method) return onDetector();
      if (init.method === "POST") {
        return jsonResponse({ key: "pixel_mask", masked: 1, previous_masked: 0, backup: `masks/backups/${fetchMock.mock.calls.length}.tiff`, verified: true });
      }
      throw new Error(`unexpected ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    showConfirmDialogMock.mockResolvedValue(true);
    const { controller, callbacks } = setup({ detector, state: { maskRaw: new Uint32Array([1, 0]), maskShape: [1, 2] } });
    controller.setEnabled(true);
    await controller.sendCurrent();
    const firstBackup = fetchMock.mock.calls.length;

    onDetector = () => maskResponse([1, 0], [1, 2]);
    await controller.restoreBackup();
    expect(fetchMock.mock.calls.some(([url]) => url === `/api/pixel-mask/file?file=${encodeURIComponent(`masks/backups/${firstBackup}.tiff`)}`)).toBe(true);
    expect(showConfirmDialogMock.mock.calls.at(-1)[0].title).toBe("pixel_mask.detector.restore_title");
    expect(callbacks.logDetector.mock.calls.at(-1)[0]).toContain("pixel_mask.detector.log.restored");

    // Restore again reads the same backup, not the mask the restore replaced.
    await controller.restoreBackup();
    const fileReads = fetchMock.mock.calls.filter(([url]) => url.startsWith("/api/pixel-mask/file"));
    expect(new Set(fileReads.map(([url]) => url)).size).toBe(1);
    await flush();
  });
});

describe("editing in the Pixel Mask section", () => {
  const frame = { hasFrame: true, dataRaw: new Uint16Array(4), width: 2, height: 2, file: "run_master.h5" };

  it("needs an image to edit on", () => {
    const { controller, elements, callbacks } = setup();
    controller.setEnabled(true);
    expect(elements.editBtn.disabled).toBe(true);
    expect(elements.editBtn.dataset.helpReason).toBe("pixel_mask.edit.no_image");
    controller.beginEdit();
    expect(callbacks.openEditor).not.toHaveBeenCalled();
  });

  it("edits a copy of the data's mask, and drops it again if nothing changed", () => {
    const own = new Uint32Array([1, 0, 0, 0]);
    const { controller, callbacks, state } = setup({
      state: { ...frame, maskRaw: own, maskShape: [2, 2], maskAvailable: true, maskPath: "/entry/pixel_mask" },
    });
    controller.setEnabled(true);
    controller.beginEdit();
    expect(callbacks.openEditor).toHaveBeenCalledTimes(1);
    const target = controller.getEditTarget();
    expect(Array.from(target.data)).toEqual([1, 0, 0, 0]);
    expect(target.data).not.toBe(own);
    expect(target.base).not.toBe(target.data);

    controller.onEditClose();
    expect(controller.getPinnedMask()).toBeNull();
    expect(state.maskRaw).toBe(own);
  });

  it("starts from an empty mask when the data has none", () => {
    const { controller } = setup({ state: frame });
    controller.setEnabled(true);
    controller.beginEdit();
    expect(Array.from(controller.getEditTarget().data)).toEqual([0, 0, 0, 0]);
    expect(controller.getPinnedMask().origin).toBe("new");
  });

  it("refuses a mask that does not fit the image", () => {
    const { controller, callbacks } = setup({
      state: { ...frame, maskRaw: new Uint32Array(6), maskShape: [2, 3], maskAvailable: false },
    });
    controller.setEnabled(true);
    controller.beginEdit();
    expect(callbacks.openEditor).not.toHaveBeenCalled();
    expect(callbacks.setStatus).toHaveBeenLastCalledWith("pixel_mask.edit.mismatch", { tone: "warning" });
  });

  it("marks the section edited, replaces the array on each step, and notes edited pixels", () => {
    const { controller, elements, callbacks } = setup({ state: frame });
    controller.setEnabled(true);
    controller.beginEdit();
    const before = controller.getEditTarget().data;
    before[3] = 16;
    controller.onEditCommit();
    const after = controller.getEditTarget().data;
    expect(after).not.toBe(before);
    expect(after[3]).toBe(16);
    expect(callbacks.refreshMask).toHaveBeenCalled();
    expect(elements.summaryChip.textContent).toBe("pixel_mask.summary.edited");
    expect(elements.editsEl.hidden).toBe(false);
    expect(elements.discardBtn.hidden).toBe(false);
    expect(controller.cursorNote(3)).toBe("pixel_mask.cursor.masked");
    expect(controller.cursorNote(0)).toBe("");
    // Closed with edits: they stay, still marked.
    controller.onEditClose();
    expect(controller.getPinnedMask()).not.toBeNull();
    expect(elements.summaryChip.textContent).toBe("pixel_mask.summary.edited");
  });

  it("asks before unsaved edits are replaced, and keeps them when told to", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url) => (url.startsWith("/api/choose-file") ? jsonResponse({ path: "other.tiff" }) : maskResponse([8, 8, 8, 8], [2, 2]))),
    );
    const { controller } = setup({ state: frame });
    controller.setEnabled(true);
    controller.beginEdit();
    controller.getEditTarget().data[0] = 16;
    controller.onEditCommit();

    showConfirmDialogMock.mockResolvedValue(false);
    await controller.loadFromFile();
    expect(showConfirmDialogMock.mock.calls[0][0].title).toBe("pixel_mask.discard_title");
    expect(controller.getPinnedMask().data[0]).toBe(16);

    showConfirmDialogMock.mockResolvedValue(true);
    await controller.loadFromFile();
    expect(Array.from(controller.getPinnedMask().data)).toEqual([8, 8, 8, 8]);
  });

  it("does not ask once the edits are saved", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url) => {
        if (url === "/api/pixel-mask/encode") return new Response("x", { headers: { "Content-Disposition": 'attachment; filename="m.tiff"' } });
        if (url.startsWith("/api/choose-file")) return jsonResponse({ path: "other.tiff" });
        return maskResponse([8, 8, 8, 8], [2, 2]);
      }),
    );
    URL.createObjectURL = vi.fn(() => "blob:x");
    URL.revokeObjectURL = vi.fn();
    const { controller } = setup({ state: frame });
    controller.setEnabled(true);
    controller.beginEdit();
    controller.getEditTarget().data[0] = 16;
    controller.onEditCommit();
    await controller.save("tiff");
    await controller.loadFromFile();
    expect(showConfirmDialogMock).not.toHaveBeenCalled();
  });

  it("discards edits of a loaded mask back to it, and of the data's mask by letting go", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url) => (url.startsWith("/api/choose-file") ? jsonResponse({ path: "m.tiff" }) : maskResponse([2, 0, 0, 0], [2, 2]))),
    );
    showConfirmDialogMock.mockResolvedValue(true);
    const { controller, callbacks } = setup({ state: frame });
    controller.setEnabled(true);
    await controller.loadFromFile();
    controller.beginEdit();
    controller.getEditTarget().data[1] = 16;
    controller.onEditCommit();
    await controller.discardEdits();
    expect(Array.from(controller.getPinnedMask().data)).toEqual([2, 0, 0, 0]);
    expect(controller.getPinnedMask().origin).toBe("file");
    expect(callbacks.resetEditor).toHaveBeenCalled();
    expect(controller.hasEdits()).toBe(false);
  });
});

describe("sending from the Pixel Mask section", () => {
  const monitor = { url: "http://192.168.20.231", version: "1.8.0", serial: "", model: "", locked: false };

  it("offers Send only with a detector to send to", () => {
    const withMask = { maskRaw: new Uint32Array([1, 0]), maskShape: [1, 2], maskAvailable: true };
    const none = setup({ state: withMask });
    none.controller.setEnabled(true);
    expect(none.elements.sendBtn.hidden).toBe(true);

    const live = setup({ state: withMask, monitor });
    live.controller.setEnabled(true);
    expect(live.elements.sendBtn.hidden).toBe(false);
    expect(live.elements.sendBtn.disabled).toBe(false);
    expect(live.elements.sendBtn.title).toContain("192.168.20.231");
  });

  it("sends to the detector the monitor shows, naming the backup after its host", async () => {
    const fetchMock = vi.fn(async (url, init = {}) => {
      if (url.startsWith("/api/pixel-mask/detector?")) return maskResponse([0, 0], [1, 2]);
      if (init.method === "POST") {
        return jsonResponse({ key: "pixel_mask", masked: 1, previous_masked: 0, backup: "masks/backups/b.tiff", verified: true });
      }
      throw new Error(`unexpected ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    showConfirmDialogMock.mockResolvedValue(true);
    const { controller, elements } = setup({
      state: { maskRaw: new Uint32Array([1, 0]), maskShape: [1, 2], maskAvailable: true },
      monitor,
    });
    controller.setEnabled(true);
    elements.sendBtn.click();
    await flush();
    await flush();
    expect(fetchMock.mock.calls[0][0]).toBe("/api/pixel-mask/detector?url=http%3A%2F%2F192.168.20.231&version=1.8.0");
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(JSON.parse(post[1].body.get("meta")).serial).toBe("192.168.20.231");
    expect(elements.backupRow.hidden).toBe(false);
  });

  it("prefers the Detector tab's connection to the monitor's", () => {
    const detector = { url: "http://det", version: "1.8.0", serial: "E-1", model: "EIGER2", locked: true };
    const { controller, elements } = setup({
      state: { maskRaw: new Uint32Array([1, 0]), maskShape: [1, 2], maskAvailable: true },
      detector,
      monitor,
    });
    controller.setEnabled(true);
    expect(elements.sendBtn.title).toContain("EIGER2 E-1");
    expect(elements.sendBtn.disabled).toBe(true);
  });
});

describe("Find bad pixels in the Pixel Mask section", () => {
  function flatFrame(width, height, level) {
    const data = new Uint16Array(width * height).fill(level);
    for (let i = 0; i < data.length; i += 1) data[i] = level + ((i * 7) % 11) - 5;
    return data;
  }

  const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

  it("circles what it finds in the image shown, and adds it to the mask as an edit", async () => {
    const width = 64;
    const height = 64;
    const dataRaw = flatFrame(width, height, 1000);
    dataRaw[5 * width + 9] = 9000;
    dataRaw[30 * width + 40] = 0;
    const { controller, elements, callbacks } = setup({
      state: { hasFrame: true, dataRaw, width, height, file: "flat_master.h5" },
    });
    controller.setEnabled(true);
    controller.openFind();
    expect(elements.findPanel.hidden).toBe(false);
    await wait(20);
    expect(elements.findResultEl.textContent).toContain("pixel_mask.find.part.hot");
    expect(elements.findResultEl.textContent).toContain("pixel_mask.find.part.dead");
    expect(elements.findResultEl.textContent).not.toContain("pixel_mask.find.part.cold");
    const markers = callbacks.setMarkers.mock.calls.at(-1)[0];
    expect(markers.groups[0].indices).toEqual([5 * width + 9]);
    expect(elements.findAddBtn.disabled).toBe(false);

    controller.addFoundToMask();
    const pinned = controller.getPinnedMask();
    expect(pinned.data[5 * width + 9]).toBe(8);
    expect(pinned.data[30 * width + 40]).toBe(2);
    expect(controller.hasEdits()).toBe(true);
    expect(elements.findPanel.hidden).toBe(true);
    expect(callbacks.setMarkers).toHaveBeenLastCalledWith(null);
    expect(callbacks.setStatus.mock.calls.at(-1)[0]).toContain("pixel_mask.find.added");
  });

  it("looks again when a setting changes", async () => {
    const width = 64;
    const height = 64;
    const dataRaw = flatFrame(width, height, 1000);
    dataRaw[5 * width + 9] = 9000;
    const { controller, elements } = setup({ state: { hasFrame: true, dataRaw, width, height } });
    controller.setEnabled(true);
    controller.openFind();
    await wait(20);
    expect(elements.findResultEl.textContent).toContain("pixel_mask.find.part.hot");
    elements.findHot.checked = false;
    elements.findHot.dispatchEvent(new Event("change"));
    await wait(20);
    expect(elements.findResultEl.textContent).toBe("pixel_mask.find.none");
    expect(elements.findAddBtn.disabled).toBe(true);
  });

  it("needs an image", async () => {
    const { controller, elements } = setup();
    controller.setEnabled(true);
    expect(elements.findBtn.disabled).toBe(true);
  });
});

describe("clearing the detector's mask", () => {
  const detector = { url: "http://det", version: "1.8.0", serial: "E-1", model: "EIGER2", locked: false };

  it("sends the detector's own mask with nothing but its gaps, after asking", async () => {
    const fetchMock = vi.fn(async (url, init = {}) => {
      if (url.startsWith("/api/pixel-mask/detector?")) return maskResponse([1, 2, 8, 0, 3], [1, 5]);
      if (init.method === "POST") {
        return jsonResponse({ key: "pixel_mask", masked: 2, previous_masked: 4, backup: "masks/backups/b.tiff", verified: true });
      }
      throw new Error(`unexpected ${url}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    showConfirmDialogMock.mockResolvedValue(true);
    const { controller, elements, callbacks } = setup({ detector });
    controller.setEnabled(true);
    elements.detClearBtn.click();
    await flush();
    await flush();
    const question = showConfirmDialogMock.mock.calls[0][0];
    expect(question.title).toBe("pixel_mask.detector.clear_title");
    expect(question.message).toContain('"removed":"3"');
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    const sent = new Uint32Array(await post[1].body.get("pixels").arrayBuffer());
    expect(Array.from(sent)).toEqual([1, 0, 0, 0, 1]);
    expect(callbacks.logDetector.mock.calls.at(-1)[0]).toContain("pixel_mask.detector.log.cleared");
    expect(elements.detBackup.hidden).toBe(false);
  });

  it("says so when there is nothing to clear", async () => {
    vi.stubGlobal("fetch", vi.fn(async () => maskResponse([1, 0, 1], [1, 3])));
    const { controller, callbacks } = setup({ detector });
    controller.setEnabled(true);
    await controller.clearDetectorMask();
    expect(showConfirmDialogMock).not.toHaveBeenCalled();
    expect(callbacks.setStatus.mock.calls.at(-1)[0]).toBe("pixel_mask.detector.status.already_clear");
  });
});

describe("after a write to the detector", () => {
  const detector = { url: "http://det", version: "1.8.0", serial: "E-1", model: "EIGER2", locked: false };
  const okWrite = { key: "pixel_mask", masked: 1, previous_masked: 3, backup: "masks/backups/b.tiff", verified: true };

  it("drops a copy of the detector's old mask on Clear, and tells the live view", async () => {
    let onDetector = [1, 2, 8];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url, init = {}) => {
        if (url.startsWith("/api/pixel-mask/detector?")) return maskResponse(onDetector, [1, 3]);
        if (init.method === "POST") return jsonResponse(okWrite);
        throw new Error(`unexpected ${url}`);
      }),
    );
    showConfirmDialogMock.mockResolvedValue(true);
    const { controller, callbacks } = setup({ detector });
    controller.setEnabled(true);
    await controller.loadFromDetector();
    expect(controller.getPinnedMask().detectorHost).toBe("det");

    await controller.clearDetectorMask();
    expect(controller.getPinnedMask()).toBeNull();
    expect(callbacks.onDetectorMaskWritten).toHaveBeenCalledWith("http://det");
  });

  it("asks before dropping edits made to that copy, and leaves both alone if told no", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url, init = {}) => {
        if (url.startsWith("/api/pixel-mask/detector?")) return maskResponse([1, 2, 0, 0], [2, 2]);
        if (init.method === "POST") return jsonResponse(okWrite);
        throw new Error(`unexpected ${url}`);
      }),
    );
    const { controller, callbacks } = setup({
      detector,
      state: { hasFrame: true, dataRaw: new Uint16Array(4), width: 2, height: 2 },
    });
    controller.setEnabled(true);
    await controller.loadFromDetector();
    controller.beginEdit();
    controller.getEditTarget().data[3] = 16;
    controller.onEditCommit();

    showConfirmDialogMock.mockResolvedValueOnce(false);
    await controller.clearDetectorMask();
    expect(showConfirmDialogMock.mock.calls[0][0].title).toBe("pixel_mask.discard_title");
    expect(controller.getPinnedMask().data[3]).toBe(16);
    expect(callbacks.onDetectorMaskWritten).not.toHaveBeenCalled();
  });

  it("keeps a mask loaded from a file, which no write makes out of date", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url, init = {}) => {
        if (url.startsWith("/api/choose-file")) return jsonResponse({ path: "m.tiff" });
        if (url.startsWith("/api/pixel-mask/file")) return maskResponse([0, 4, 0], [1, 3]);
        if (url.startsWith("/api/pixel-mask/detector?")) return maskResponse([1, 2, 8], [1, 3]);
        if (init.method === "POST") return jsonResponse(okWrite);
        throw new Error(`unexpected ${url}`);
      }),
    );
    showConfirmDialogMock.mockResolvedValue(true);
    const { controller } = setup({ detector });
    controller.setEnabled(true);
    await controller.loadFromFile();
    await controller.clearDetectorMask();
    expect(controller.getPinnedMask().origin).toBe("file");
  });
});
