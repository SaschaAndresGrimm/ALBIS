import fs from "node:fs";
import path from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const EN = JSON.parse(fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8"));

const DESCRIPTORS = {
  detector: {
    description: { value: "<img src=x onerror=alert(1)> EIGER2 4M", value_type: "string", access_mode: "r", unit: "" },
    detector_number: { value: "E-08-0123", value_type: "string", access_mode: "r", unit: "" },
    photon_energy: { value: 12400, value_type: "float", unit: "eV", access_mode: "rw", min: 3500, max: 40000 },
    incident_energy: { value: 12400, value_type: "float", unit: "eV", access_mode: "rw", min: 3500, max: 40000 },
    threshold_energy: { value: 6200, value_type: "float", unit: "eV", access_mode: "rw", min: 1750, max: 20000 },
    count_time: { value: 0.0099999, value_type: "float", unit: "s", access_mode: "rw", min: 0.0001999, max: 3600 },
    frame_time: { value: 0.01, value_type: "float", unit: "s", access_mode: "rw", min: 0.0002, max: 3600 },
    nimages: { value: 10, value_type: "uint", unit: "", access_mode: "rw", min: 1, max: 2000000000 },
    ntrigger: { value: 1, value_type: "uint", unit: "", access_mode: "rw", min: 1, max: 1000000 },
    trigger_mode: { value: "ints", value_type: "string", unit: "", access_mode: "rw", allowed_values: ["ints", "exts"] },
    countrate_correction_applied: { value: true, value_type: "bool", unit: "", access_mode: "rw" },
  },
  filewriter: {
    mode: { value: "disabled", value_type: "string", access_mode: "rw", allowed_values: ["enabled", "disabled"] },
    name_pattern: { value: "series_$id", value_type: "string", access_mode: "rw" },
  },
  stream: { mode: { value: "disabled", value_type: "string", access_mode: "rw", allowed_values: ["enabled", "disabled"] } },
  monitor: { mode: { value: "enabled", value_type: "string", access_mode: "rw", allowed_values: ["enabled", "disabled"] } },
};

async function loadModule(routes = {}) {
  vi.resetModules();
  global.fetch = vi.fn(async (url, init) => {
    const text = String(url);
    if (text.includes("locales")) return { ok: true, json: async () => EN };
    for (const [needle, handler] of Object.entries(routes)) {
      if (text.includes(needle)) return handler(text, init);
    }
    throw new Error(`Unexpected fetch: ${text}`);
  });
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  return import("../modules/detector_control_controller.js");
}

function buildElements() {
  document.body.innerHTML = `
    <button id="tab" hidden></button>
    <div id="content">
      <input id="url" /><button id="connect"></button><p id="message" class="is-hidden"></p>
      <span id="summary"></span>
      <div id="live" hidden>
        <strong id="model"></strong><span id="serial"></span>
        <span id="state"></span><span id="state-text"></span>
        <div id="progress" hidden><i id="bar"></i><span id="pl"></span><span id="pr"></span></div>
        <div id="confirm" hidden><span id="confirm-text"></span><button id="no"></button><button id="yes"></button></div>
        <button id="primary"></button><button id="stop" hidden></button>
        <div id="sensors"></div>
        <div id="notice" hidden><span id="notice-text"></span><button id="notice-ok"></button></div>
      </div>
      <span id="lock" hidden></span>
      <div id="params"></div><div id="outputs"></div><div id="files"></div>
      <ol id="log"></ol><div id="advanced"></div><div id="commands"></div>
    </div>`;
  const $ = (id) => document.getElementById(id);
  return {
    tab: $("tab"), content: $("content"), urlInput: $("url"), connectBtn: $("connect"), message: $("message"),
    summary: $("summary"), live: $("live"), model: $("model"), serial: $("serial"), statePill: $("state"),
    stateText: $("state-text"), progress: $("progress"), progressBar: $("bar"), progressLeft: $("pl"),
    progressRight: $("pr"), confirm: $("confirm"), confirmText: $("confirm-text"), confirmYes: $("yes"),
    confirmNo: $("no"), primaryBtn: $("primary"), stopBtn: $("stop"), sensors: $("sensors"), notice: $("notice"),
    noticeText: $("notice-text"), noticeDismiss: $("notice-ok"), sections: [], paramsHost: $("params"),
    lockNote: $("lock"), outputsHost: $("outputs"), filesHost: $("files"), logHost: $("log"),
    advancedHost: $("advanced"), commandsHost: $("commands"),
  };
}

async function setup(routes = {}) {
  const mod = await loadModule(routes);
  const elements = buildElements();
  const setPanelTab = vi.fn();
  let tab = "detector";
  const controller = mod.createDetectorControlController({
    apiBase: "/api",
    elements,
    callbacks: { getPanelTab: () => tab, setPanelTab: (id) => { tab = id; setPanelTab(id); }, watchLive: vi.fn() },
  });
  controller._setConnection("http://192.168.1.10");
  controller._setParams(structuredClone(DESCRIPTORS));
  controller._renderAll();
  return { mod, controller, elements, setPanelTab };
}

describe("detector control helpers", () => {
  beforeEach(() => {
    localStorage.clear();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("checks typed values against the detector's own limits", async () => {
    const { validateInput } = await loadModule();
    const count = DESCRIPTORS.detector.count_time;
    expect(validateInput(count, "0.05")).toEqual({ ok: true, value: 0.05 });
    expect(validateInput(count, "0,05")).toEqual({ ok: true, value: 0.05 });
    expect(validateInput(count, "99999").ok).toBe(false);
    expect(validateInput(count, "abc").error).toBe(EN["detector.error.number"]);
    expect(validateInput(DESCRIPTORS.detector.nimages, "2.5").error).toBe(EN["detector.error.whole"]);
    expect(validateInput(DESCRIPTORS.detector.trigger_mode, "sideways").ok).toBe(false);
    expect(validateInput(DESCRIPTORS.detector.countrate_correction_applied, "false")).toEqual({ ok: true, value: false });
  });

  it("states ranges in the parameter's own unit", async () => {
    const { rangeText } = await loadModule();
    expect(rangeText(DESCRIPTORS.detector.photon_energy)).toBe("3500 eV – 40000 eV");
    expect(rangeText(DESCRIPTORS.detector.count_time)).toBe("200 µs – 60:00 min");
    expect(rangeText(DESCRIPTORS.detector.nimages)).toBe("1 or more");
    expect(rangeText(DESCRIPTORS.detector.trigger_mode)).toBe("2 options");
  });

  it("offers the one next step the detector state allows", async () => {
    const { primaryAction } = await loadModule();
    expect(primaryAction("na")).toBe("initialize");
    expect(primaryAction("error")).toBe("initialize");
    expect(primaryAction("idle")).toBe("acquire");
    expect(primaryAction("ready", "ints")).toBe("trigger");
    expect(primaryAction("ready", "exts")).toBeNull();
    expect(primaryAction("acquire")).toBeNull();
  });
});

describe("detector control panel", () => {
  beforeEach(() => {
    localStorage.clear();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("is hidden while switched off, and never left open", async () => {
    const { controller, elements, setPanelTab } = await setup();
    controller.setEnabled(true);
    expect(elements.tab.hidden).toBe(false);
    controller.setEnabled(false);
    expect(elements.tab.hidden).toBe(true);
    expect(setPanelTab).toHaveBeenCalledWith("view");
  });

  it("builds the form from what the detector describes", async () => {
    const { elements } = await setup();
    const params = elements.paramsHost;
    const keys = [...params.querySelectorAll(".detector-param code")].map((c) => c.textContent);
    // X-ray detector: photon_energy wins over its incident_energy alias.
    expect(keys).toEqual(["photon_energy", "threshold_energy", "count_time", "frame_time", "nimages", "ntrigger", "trigger_mode"]);
    const trigger = params.querySelector("#detector-p-detector-trigger_mode");
    expect([...trigger.options].map((o) => o.textContent)).toEqual([
      EN["detector.trigger_mode.ints"],
      EN["detector.trigger_mode.exts"],
    ]);
    // Read-only values are shown, not editable, and a detector string is text, not markup.
    const info = elements.advancedHost.querySelector("#detector-p-detector-description");
    expect(info.tagName).toBe("DIV");
    expect(info.textContent).toContain("<img src=x");
    expect(elements.advancedHost.querySelector("img")).toBeNull();
  });

  it("writes a value and shows what the detector changed alongside", async () => {
    const put = vi.fn(async (_url, init) => {
      const body = JSON.parse(init.body);
      expect(body).toMatchObject({ subsystem: "detector", key: "count_time", value: 0.05 });
      return {
        ok: true,
        json: async () => ({
          changed: ["count_time", "frame_time"],
          params: {
            count_time: { ...DESCRIPTORS.detector.count_time, value: 0.05 },
            frame_time: { ...DESCRIPTORS.detector.frame_time, value: 0.0500001 },
          },
        }),
      };
    });
    const { elements } = await setup({ "/detector/config": put });
    const input = elements.paramsHost.querySelector("#detector-p-detector-count_time");
    input.value = "0.05";
    input.dispatchEvent(new Event("change"));
    await vi.waitFor(() => expect(put).toHaveBeenCalledTimes(1));
    await vi.waitFor(() => {
      const frame = elements.paramsHost.querySelector("#detector-p-detector-frame_time");
      expect(frame.value).toBe("0.0500001");
    });
    const row = elements.paramsHost.querySelector('[data-key="detector:frame_time"]');
    expect(row.querySelector(".detector-hint").textContent).toBe(EN["detector.hint.adjusted"]);
    expect(elements.logHost.textContent).toContain("Frame time adjusted by the detector to 0.0500001");
  });

  it("refuses an out-of-range value without contacting the detector", async () => {
    const put = vi.fn();
    const { elements } = await setup({ "/detector/config": put });
    const input = elements.paramsHost.querySelector("#detector-p-detector-nimages");
    input.value = "0";
    input.dispatchEvent(new Event("change"));
    expect(put).not.toHaveBeenCalled();
    expect(input.getAttribute("aria-invalid")).toBe("true");
    expect(input.closest(".detector-param").querySelector(".detector-hint").className).toContain("is-error");
  });

  it("names the next step for each detector state, and locks settings while busy", async () => {
    const { controller, elements } = await setup();
    controller._setStatus({ detector: { state: "na" } });
    expect(elements.primaryBtn.textContent).toBe(EN["detector.action.initialize"]);
    controller._setStatus({ detector: { state: "idle", temperature: 25.1, "high_voltage/state": "READY" } });
    expect(elements.primaryBtn.textContent).toBe(EN["detector.action.acquire"]);
    expect(elements.stopBtn.hidden).toBe(true);
    expect(elements.sensors.textContent).toContain("25.1 °C");
    controller._setStatus({ detector: { state: "acquire" } });
    expect(elements.primaryBtn.disabled).toBe(true);
    expect(elements.stopBtn.hidden).toBe(false);
    expect(elements.paramsHost.querySelector("#detector-p-detector-nimages").disabled).toBe(true);
  });

  it("warns when nothing will be saved", async () => {
    const { elements } = await setup();
    expect(elements.outputsHost.textContent).toContain(EN["detector.output.nothing_saved"]);
  });
});

describe("detector control panel, after the first hardware test", () => {
  beforeEach(() => {
    localStorage.clear();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("rounds for display only, and shortens units", async () => {
    const { displayValue, unitSymbol } = await loadModule();
    expect(displayValue({ value: 8047.7798, value_type: "float", unit: "eV" })).toBe("8047.8");
    expect(displayValue({ value: 9.9999999, value_type: "float", unit: "s" })).toBe("10");
    expect(displayValue({ value: 1.5406013, value_type: "float", unit: "angstrom" })).toBe("1.5406");
    expect(displayValue({ value: 2000000000, value_type: "uint" })).toBe("2000000000");
    expect(unitSymbol("angstrom")).toBe("Å");
    expect(unitSymbol("eV")).toBe("eV");
  });

  it("flags only the values the detector really moved", async () => {
    const put = vi.fn(async () => ({
      ok: true,
      json: async () => ({
        // "changed, or may have been changed": threshold and frame time are listed,
        // but only the frame time moved.
        changed: ["count_time", "frame_time", "threshold_energy"],
        params: {
          count_time: { ...DESCRIPTORS.detector.count_time, value: 0.05 },
          frame_time: { ...DESCRIPTORS.detector.frame_time, value: 0.0500001 },
          threshold_energy: { ...DESCRIPTORS.detector.threshold_energy },
        },
      }),
    }));
    const { elements } = await setup({ "/detector/config": put });
    const input = elements.paramsHost.querySelector("#detector-p-detector-count_time");
    input.value = "0.05";
    input.dispatchEvent(new Event("change"));
    await vi.waitFor(() => expect(elements.logHost.textContent).toContain("Frame time adjusted"));
    expect(elements.logHost.textContent).not.toContain("Threshold adjusted");
    const threshold = elements.paramsHost.querySelector('[data-key="detector:threshold_energy"] .detector-hint');
    expect(threshold.textContent).not.toBe(EN["detector.hint.adjusted"]);
  });

  it("shows each threshold with its mode, and the alias only once", async () => {
    const mod = await loadModule();
    const elements = buildElements();
    const controller = mod.createDetectorControlController({ apiBase: "/api", elements, callbacks: {} });
    const mode = (value) => ({ value, value_type: "string", access_mode: "rw", allowed_values: ["enabled", "disabled"] });
    const energy = (value) => ({ value, value_type: "float", unit: "eV", access_mode: "rw", min: 1000, max: 20000 });
    controller._setParams({
      detector: {
        photon_energy: energy(8047.7798),
        threshold_energy: energy(4023.8899),
        "threshold/1/energy": energy(4023.8899),
        "threshold/1/mode": mode("disabled"),
        "threshold/2/energy": energy(9254.9468),
        "threshold/2/mode": mode("disabled"),
        "threshold/difference/mode": mode("enabled"),
      },
    });
    controller._renderAll();
    const labels = [...elements.paramsHost.querySelectorAll(".detector-param label")].map((l) => l.textContent);
    expect(labels).toEqual([
      "Photon energy",
      "Threshold 1",
      "Threshold 1 images",
      "Threshold 2",
      "Threshold 2 images",
      "Difference image (1 − 2)",
    ]);
    const modeSelect = elements.paramsHost.querySelector("#detector-p-detector-threshold_1_mode");
    expect([...modeSelect.options].map((o) => o.textContent)).toEqual(["On", "Off"]);
    // A dropdown needs no "2 options" hint.
    expect(modeSelect.closest(".detector-param").querySelector(".detector-hint").textContent).toBe("");
    expect(elements.advancedHost.querySelector('[data-key="detector:threshold/1/energy"]')).toBeNull();
  });

  it("names the next file honestly", async () => {
    const { controller, elements } = await setup();
    const fw = (pattern) => ({
      ...structuredClone(DESCRIPTORS),
      filewriter: {
        mode: { value: "enabled", value_type: "string", access_mode: "rw", allowed_values: ["enabled", "disabled"] },
        name_pattern: { value: pattern, value_type: "string", access_mode: "rw" },
      },
    });
    controller._setParams(fw("scan_$id"));
    controller._renderAll();
    // The series number is not known before this panel has armed one.
    expect(elements.outputsHost.textContent).toContain("Next: scan_$id_master.h5");
    controller._setParams(fw("fixed_name"));
    controller._renderAll();
    expect(elements.outputsHost.textContent).toContain(EN["detector.output.no_id"]);
    // Free text gets the whole row.
    expect(elements.outputsHost.querySelector('[data-key="filewriter:name_pattern"]').classList.contains("is-wide")).toBe(true);
  });

  it("groups Advanced and filters it as you type", async () => {
    const { controller, elements } = await setup();
    controller._setParams({
      ...structuredClone(DESCRIPTORS),
      detector: {
        ...structuredClone(DESCRIPTORS.detector),
        test_image_value: { value: 1, value_type: "uint", access_mode: "rw" },
        flatfield_correction_applied: { value: true, value_type: "bool", access_mode: "rw" },
      },
      filewriter: { ...structuredClone(DESCRIPTORS.filewriter), format: { value: "a", value_type: "string", access_mode: "rw", allowed_values: ["a", "b"] } },
      stream: { ...structuredClone(DESCRIPTORS.stream), format: { value: "cbor", value_type: "string", access_mode: "rw", allowed_values: ["legacy", "cbor"] } },
    });
    controller._renderAll();
    const headings = [...elements.advancedHost.querySelectorAll(".detector-group-label")].map((h) => h.textContent);
    expect(headings).toEqual(expect.arrayContaining(["Corrections", "Test images", "File writer", "Stream", "Detector information"]));
    // An untranslated setting shows its key once, not twice.
    const testRow = elements.advancedHost.querySelector('[data-key="detector:test_image_value"]');
    expect(testRow.querySelector("code")).toBeNull();

    const filter = elements.advancedHost.querySelector(".detector-filter");
    filter.value = "format";
    filter.dispatchEvent(new Event("input"));
    const visible = [...elements.advancedHost.querySelectorAll(".detector-param")].filter((r) => !r.hidden).map((r) => r.dataset.key);
    expect(visible).toEqual(["filewriter:format", "stream:format"]);
  });
});
