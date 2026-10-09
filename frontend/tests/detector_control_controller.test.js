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
      <label id="address"><input id="url" /><button id="connect"></button></label><p id="message" class="is-hidden"></p>
      <span id="summary"></span>
      <div id="live" hidden>
        <strong id="model"></strong><span id="serial"></span>
        <span id="state"></span><span id="state-text"></span>
        <div id="progress" hidden><i id="bar"></i><span id="pl"></span><span id="pr"></span></div>
        <div id="confirm" hidden><span id="confirm-text"></span><button id="no"></button><button id="yes"></button></div>
        <div class="detector-actions"><button id="primary"></button><button id="stop" hidden></button></div>
        <input id="follow" type="checkbox" checked />
        <div id="sensors"></div>
        <div id="notice" hidden><span id="notice-text"></span><button id="notice-ok"></button></div>
      </div>
      <span id="lock" hidden></span>
      <div id="params"></div><div id="outputs"></div><div id="files"></div>
      <ol id="log"></ol><div id="advanced"></div><div id="commands"></div><div id="troubleshooting"></div>
    </div>`;
  const $ = (id) => document.getElementById(id);
  return {
    tab: $("tab"), content: $("content"), urlInput: $("url"), connectBtn: $("connect"), addressHost: $("address"), message: $("message"),
    summary: $("summary"), live: $("live"), model: $("model"), serial: $("serial"), statePill: $("state"),
    stateText: $("state-text"), progress: $("progress"), progressBar: $("bar"), progressLeft: $("pl"),
    progressRight: $("pr"), confirm: $("confirm"), confirmText: $("confirm-text"), confirmYes: $("yes"),
    confirmNo: $("no"), primaryBtn: $("primary"), stopBtn: $("stop"), followToggle: $("follow"), sensors: $("sensors"), notice: $("notice"),
    noticeText: $("notice-text"), noticeDismiss: $("notice-ok"), sections: [], paramsHost: $("params"),
    lockNote: $("lock"), outputsHost: $("outputs"), filesHost: $("files"), logHost: $("log"),
    advancedHost: $("advanced"), commandsHost: $("commands"), troubleshootingHost: $("troubleshooting"),
  };
}

async function setup(routes = {}) {
  const mod = await loadModule(routes);
  const elements = buildElements();
  const setPanelTab = vi.fn();
  let tab = "detector";
  const watchLive = vi.fn();
  const controller = mod.createDetectorControlController({
    apiBase: "/api",
    elements,
    callbacks: { getPanelTab: () => tab, setPanelTab: (id) => { tab = id; setPanelTab(id); }, watchLive },
  });
  controller._setConnection("http://192.168.1.10");
  controller._setParams(structuredClone(DESCRIPTORS));
  controller._renderAll();
  return { mod, controller, elements, setPanelTab, watchLive };
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
    const keys = [...params.querySelectorAll(".detector-param")].map((row) => row.dataset.key.split(":")[1]);
    // X-ray detector: photon_energy wins over its incident_energy alias.
    expect(keys).toEqual(["nimages", "ntrigger", "trigger_mode", "frame_time", "count_time", "photon_energy", "threshold_energy"]);
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

  // One row per threshold energy, then which images the detector delivers: a
  // POLLUX has two thresholds, a PILATUS4 four, an EIGER2 one -- whose mode
  // stays in Advanced, since switching the only threshold off would switch off
  // the images.
  const mode = (value) => ({ value, value_type: "string", access_mode: "rw", allowed_values: ["enabled", "disabled"] });
  const energy = (value) => ({ value, value_type: "float", unit: "eV", access_mode: "rw", min: 1000, max: 20000 });
  async function thresholds(detector, routes = {}) {
    const mod = await loadModule(routes);
    const elements = buildElements();
    const controller = mod.createDetectorControlController({ apiBase: "/api", elements, callbacks: {} });
    controller._setConnection("http://192.168.1.10");
    controller._setParams({ detector: { photon_energy: energy(8047.7798), ...detector } });
    controller._renderAll();
    const host = elements.paramsHost;
    const labels = [...host.querySelectorAll(".detector-param:not(.detector-images) .detector-param-label > label")].map((l) => l.textContent);
    const chips = () => [...host.querySelectorAll(".detector-chip")].map((c) => `${c.textContent}:${c.getAttribute("aria-pressed")}`);
    return { mod, elements, host, labels, chips };
  }

  it("shows two thresholds' energies, and their images as chips", async () => {
    const { elements, host, labels, chips } = await thresholds({
      threshold_energy: energy(4023.8899),
      "threshold/1/energy": energy(4023.8899),
      "threshold/1/mode": mode("disabled"),
      "threshold/2/energy": energy(9254.9468),
      "threshold/2/mode": mode("disabled"),
      "threshold/difference/mode": mode("enabled"),
    });
    expect(labels).toEqual(["Photon energy", "Threshold 1", "Threshold 2"]);
    // As on a POLLUX: only the difference image, from both thresholds.
    expect(chips()).toEqual(["Threshold 1:false", "Threshold 2:false", "Difference (1 − 2):true"]);
    // The energies are in use either way: nothing is dimmed or switched.
    expect(host.querySelector(".is-off, .detector-param-label .detector-switch")).toBeNull();
    expect(host.textContent).not.toContain(EN["detector.images.none"]);
    const tip = host.querySelector(".detector-images .info-tip");
    expect(tip.dataset.infoKey).toBe("detector.help.images");
    expect(tip.dataset.infoDetail).toContain("SIMPLON: detector/config/threshold/difference/mode");
    // Neither the alias nor the modes are repeated in Advanced.
    for (const key of ["threshold/1/energy", "threshold/1/mode", "threshold/2/mode", "threshold/difference/mode"]) {
      expect(elements.advancedHost.querySelector(`[data-key="detector:${key}"]`)).toBeNull();
    }
  });

  it("shows all four thresholds of a PILATUS4, and warns when no image is selected", async () => {
    const detector = { threshold_energy: energy(5000) };
    for (const n of [1, 2, 3, 4]) {
      detector[`threshold/${n}/energy`] = energy(4000 + n * 1000);
      detector[`threshold/${n}/mode`] = mode("disabled");
    }
    const { host, labels, chips } = await thresholds(detector);
    expect(labels).toEqual(["Photon energy", "Threshold 1", "Threshold 2", "Threshold 3", "Threshold 4"]);
    expect(chips()).toEqual(["Threshold 1:false", "Threshold 2:false", "Threshold 3:false", "Threshold 4:false"]);
    expect(host.textContent).toContain(EN["detector.images.none"]);
  });

  it("shows a single threshold without a choice of images, its mode left to Advanced", async () => {
    const { elements, labels, chips } = await thresholds({
      threshold_energy: energy(4023.8899),
      "threshold/1/energy": energy(4023.8899),
      "threshold/1/mode": mode("enabled"),
    });
    expect(labels).toEqual(["Photon energy", "Threshold"]);
    expect(chips()).toEqual([]);
    // ...as a switch, like every on/off setting.
    expect(elements.advancedHost.querySelector('[data-key="detector:threshold/1/mode"] .detector-switch input').checked).toBe(true);
    const bare = await thresholds({ "threshold/1/energy": energy(4000) });
    expect(bare.labels).toEqual(["Photon energy", "Threshold"]);
  });

  it("selects an image through the detector, and shows what it has if refused", async () => {
    const put = vi.fn(async () => ({ ok: false, status: 400, json: async () => ({ detail: "not now" }) }));
    const { host } = await thresholds(
      {
        "threshold/1/energy": energy(4000),
        "threshold/1/mode": mode("enabled"),
        "threshold/2/energy": energy(9000),
        "threshold/2/mode": mode("disabled"),
      },
      { "/detector/config": put },
    );
    host.querySelector('.detector-chip[data-key="threshold/2/mode"]').click();
    await vi.waitFor(() => expect(put).toHaveBeenCalledTimes(1));
    expect(JSON.parse(put.mock.calls[0][1].body)).toMatchObject({ key: "threshold/2/mode", value: "enabled" });
    await vi.waitFor(() => expect(host.querySelector(".detector-images .detector-hint").textContent).toBe("not now"));
    expect(host.querySelector('.detector-chip[data-key="threshold/2/mode"]').getAttribute("aria-pressed")).toBe("false");
  });

  it("makes every on/off setting a switch", async () => {
    const put = vi.fn(async (_url, init) => {
      const body = JSON.parse(init.body);
      return { ok: true, json: async () => ({ changed: [body.key], params: { [body.key]: { ...DESCRIPTORS.detector.countrate_correction_applied, value: body.value } } }) };
    });
    const { elements } = await setup({ "/detector/config": put });
    const box = elements.advancedHost.querySelector('[data-key="detector:countrate_correction_applied"] .detector-switch input');
    expect(box.checked).toBe(true);
    box.checked = false;
    box.dispatchEvent(new Event("change"));
    await vi.waitFor(() => expect(put).toHaveBeenCalledTimes(1));
    expect(JSON.parse(put.mock.calls[0][1].body)).toMatchObject({ key: "countrate_correction_applied", value: false });
    await vi.waitFor(() => expect(elements.logHost.textContent).toContain("set to Off"));
  });

  it("lays the groups out as columns and sums up the series in the header", async () => {
    const { mod, controller, elements } = await setup();
    const groups = [...elements.paramsHost.querySelectorAll(".detector-groups > .detector-group")].map((group) => group.dataset.group);
    expect(groups).toEqual(["series", "timing", "energy"]);
    expect(mod.thresholdRows({})).toEqual({ energies: [], images: [] });
    const summary = document.createElement("span");
    const params = structuredClone(DESCRIPTORS);
    const ctl = mod.createDetectorControlController({ apiBase: "/api", elements: { ...elements, seriesSummary: summary }, callbacks: {} });
    ctl._setParams(params);
    ctl._setStatus({ detector: { state: "idle" } });
    // 10 images × 1 trigger at 0.01 s, internally triggered.
    expect(summary.textContent).toBe("10 images · 100 ms");
    params.detector.trigger_mode.value = "exts";
    ctl._setParams(params);
    ctl._setStatus({ detector: { state: "idle" } });
    expect(summary.textContent).toBe("10 images");
    ctl._setStatus({ detector: { state: "acquire" } });
    expect(summary.hidden).toBe(true);
    void controller;
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

describe("detector control panel, recovery", () => {
  beforeEach(() => {
    localStorage.clear();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  const enabledOutputs = () => {
    const params = structuredClone(DESCRIPTORS);
    params.filewriter.mode.value = "enabled";
    params.stream.mode.value = "enabled";
    return params;
  };
  const recoverText = (elements) => elements.sensors.nextElementSibling.textContent;

  it("offers re-initialize in the detector card only when high voltage is not ready", async () => {
    const { controller, elements } = await setup();
    controller._setStatus({ detector: { state: "idle", "high_voltage/state": "READY" } });
    expect(recoverText(elements)).toBe("");
    controller._setStatus({ detector: { state: "idle", "high_voltage/state": "RAMP" } });
    expect(recoverText(elements)).toContain("High voltage is RAMP");
    // In "na" the main button already says Initialize.
    controller._setStatus({ detector: { state: "na" } });
    expect(recoverText(elements)).toBe("");
  });

  it("asks before re-initializing, and names a failed command", async () => {
    const { controller, elements } = await setup();
    controller._setStatus({
      detector: { state: "idle" },
      command: { subsystem: "detector", command: "arm", running: false, ok: false },
    });
    expect(recoverText(elements)).toContain("The last command (arm) failed");
    elements.sensors.nextElementSibling.querySelector("button").click();
    expect(elements.confirm.hidden).toBe(false);
    expect(elements.confirmText.textContent).toBe(EN["detector.confirm.initialize"]);
  });

  it("explains dropped stream images and offers a reset only on a stream error", async () => {
    const { controller, elements } = await setup();
    controller._setParams(enabledOutputs());
    controller._renderAll();
    controller._setStatus({ detector: { state: "idle" }, stream: { state: "ready", dropped: 3, mode: "enabled" } });
    expect(elements.outputsHost.textContent).toContain("Last series: 3 images not received");
    expect(elements.outputsHost.querySelector(".detector-meta .info-tip").dataset.infoKey).toBe("detector.help.dropped");
    expect(elements.outputsHost.textContent).not.toContain("Reset stream…");
    controller._setStatus({ detector: { state: "idle" }, stream: { state: "error", dropped: 0, mode: "enabled" } });
    expect(elements.outputsHost.textContent).toContain(EN["detector.output.stream_error"]);
    [...elements.outputsHost.querySelectorAll("button")].find((b) => b.textContent === "Reset stream…").click();
    expect(elements.confirmText.textContent).toBe(EN["detector.confirm.reset_stream"]);
  });

  it("warns when the detector's storage runs low and offers to clear it", async () => {
    const { controller, elements } = await setup();
    controller._setParams(enabledOutputs());
    controller._renderAll();
    controller._setStatus({ detector: { state: "idle" }, filewriter: { mode: "enabled", state: "ready", buffer_free: 3.2e9 } });
    expect(elements.outputsHost.textContent).not.toContain(EN["detector.output.storage_low"]);
    controller._setStatus({ detector: { state: "idle" }, filewriter: { mode: "enabled", state: "ready", buffer_free: 3.2e9, critical: ["buffer_free"] } });
    expect(elements.outputsHost.textContent).toContain(EN["detector.output.storage_low"]);
    [...elements.outputsHost.querySelectorAll("button")].find((b) => b.textContent === EN["detector.action.delete_files"]).click();
    expect(elements.confirmText.textContent).toBe(EN["detector.confirm.delete_unlisted"]);
  });

  it("gathers all three in their own Troubleshooting section", async () => {
    const { controller, elements } = await setup();
    controller._setParams(enabledOutputs());
    controller._renderAll();
    expect(elements.advancedHost.querySelector(".detector-fixes")).toBeNull();
    const box = elements.troubleshootingHost.querySelector(".detector-fixes");
    expect([...box.querySelectorAll("button")].map((b) => b.textContent)).toEqual([
      "Re-initialize…",
      "Reset stream…",
      EN["detector.action.delete_files"],
    ]);
  });
});

describe("detector control panel, following a series", () => {
  beforeEach(() => {
    localStorage.clear();
  });
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  // An externally triggered series: arm, then wait for triggers -- no trigger command.
  async function armed({ armOk = true, monitorOn = false } = {}) {
    const calls = [];
    let job = null;
    const routes = {
      "/detector/command": async (_url, init) => {
        const body = JSON.parse(init.body);
        calls.push(`command ${body.subsystem}/${body.command}`);
        job = { subsystem: body.subsystem, command: body.command, running: false, ok: armOk, result: armOk ? { "sequence id": 7 } : null, error: armOk ? null : "refused" };
        return { ok: true, json: async () => ({ ...job, running: true }) };
      },
      "/detector/status": async () => ({ ok: true, json: async () => ({ detector: { state: armOk ? "ready" : "idle" }, command: job }) }),
      "/detector/config": async (_url, init) => {
        const body = JSON.parse(init.body);
        calls.push(`config ${body.subsystem}/${body.key}=${body.value}`);
        return { ok: true, json: async () => ({ changed: [body.key], params: { [body.key]: { ...DESCRIPTORS.monitor.mode, value: body.value } } }) };
      },
    };
    const ctx = await setup(routes);
    const params = structuredClone(DESCRIPTORS);
    params.detector.trigger_mode.value = "exts";
    params.filewriter.mode.value = "enabled";
    params.monitor.mode.value = monitorOn ? "enabled" : "disabled";
    ctx.controller._setParams(params);
    ctx.controller._renderAll();
    ctx.controller._setStatus({ detector: { state: "idle" } });
    return { ...ctx, calls };
  }

  it("switches the monitor on and the viewer to it once the series is armed", async () => {
    const { elements, watchLive, calls } = await armed();
    elements.primaryBtn.click();
    await vi.waitFor(() => expect(watchLive).toHaveBeenCalledWith("http://192.168.1.10", "1.8.0"), { timeout: 3000 });
    expect(calls).toEqual(["command detector/arm", "config monitor/mode=enabled"]);
    expect(elements.logHost.textContent).toContain(EN["detector.log.following"]);
  });

  it("leaves the viewer alone when unticked, and remembers that", async () => {
    const { elements, watchLive, calls } = await armed({ monitorOn: true });
    elements.followToggle.checked = false;
    elements.followToggle.dispatchEvent(new Event("change"));
    expect(localStorage.getItem("albis.detectorControl.followLive")).toBe("0");
    elements.primaryBtn.click();
    await vi.waitFor(() => expect(elements.logHost.textContent).toContain("arm done"), { timeout: 3000 });
    expect(watchLive).not.toHaveBeenCalled();
    expect(calls).toEqual(["command detector/arm"]);
  });

  it("does not switch when the detector refuses to arm", async () => {
    const { elements, watchLive, calls } = await armed({ armOk: false });
    elements.primaryBtn.click();
    await vi.waitFor(() => expect(elements.logHost.textContent).toContain("arm failed"), { timeout: 3000 });
    expect(watchLive).not.toHaveBeenCalled();
    expect(calls).toEqual(["command detector/arm"]);
  });
});

describe("detector control panel, progress", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("moves the progress bar on the clock, between status polls", async () => {
    let job = null;
    const routes = {
      "/detector/command": async (_url, init) => {
        const body = JSON.parse(init.body);
        job = { subsystem: "detector", command: body.command, running: true, ok: null };
        return { ok: true, json: async () => job };
      },
      // The trigger never finishes during this test; the detector is acquiring.
      "/detector/status": async () => ({ ok: true, json: async () => ({ detector: { state: "acquire" }, command: job }) }),
    };
    const { controller, elements } = await setup(routes);
    const params = structuredClone(DESCRIPTORS);
    params.detector.nimages.value = 100;
    params.detector.frame_time.value = 0.1; // a 10 s series
    controller._setParams(params);
    controller._renderAll();
    controller._setStatus({ detector: { state: "ready" } });
    vi.useFakeTimers();
    elements.primaryBtn.click(); // in "ready" with an internal trigger mode: Trigger
    await vi.advanceTimersByTimeAsync(250);
    const early = parseFloat(elements.progressBar.style.width);
    await vi.advanceTimersByTimeAsync(300);
    const later = parseFloat(elements.progressBar.style.width);
    expect(elements.progress.hidden).toBe(false);
    // 300 ms of a 10 s series is 3 %, shown without waiting for the next poll.
    expect(later - early).toBeCloseTo(3, 0);
  });
});

describe("detector control panel, explanations", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("explains the main settings with a ? that names the SIMPLON key, and keeps keys in Advanced", async () => {
    const { mod, controller, elements } = await setup();
    const params = structuredClone(DESCRIPTORS);
    params.filewriter.mode.value = "enabled";
    controller._setParams(params);
    controller._renderAll();
    // Main view: a name and a "?", no key under it.
    const count = elements.paramsHost.querySelector('[data-key="detector:count_time"]');
    expect(count.querySelector("code")).toBeNull();
    const tip = count.querySelector(".info-tip");
    expect(tip.dataset.infoKey).toBe("detector.help.count_time");
    expect(tip.dataset.infoDetail).toBe(`SIMPLON: detector/config/count_time\nAllowed: ${"200 µs – 60:00 min"}`);
    expect(EN[tip.dataset.infoKey]).toMatch(/Exposure time/);
    // Every setting in the main view has an explanation.
    const rows = [...elements.paramsHost.querySelectorAll(".detector-param"), ...elements.outputsHost.querySelectorAll(".detector-param")];
    for (const row of rows) expect(EN[row.querySelector(".info-tip")?.dataset.infoKey], row.dataset.key).toBeTruthy();
    // The data interfaces too, by their mode.
    expect(elements.outputsHost.querySelector(".detector-output .info-tip").dataset.infoDetail).toBe("SIMPLON: filewriter/config/mode");
    // Advanced keeps the key for experts.
    // In Advanced, a setting with an explanation gets the "?" too, not its key...
    const incident = elements.advancedHost.querySelector('[data-key="detector:incident_energy"]');
    expect(incident.querySelector("code")).toBeNull();
    expect(incident.querySelector(".info-tip").dataset.infoKey).toBe("detector.help.incident_energy");
    // ...and the others, mostly untranslated, show the key as their name.
    expect(elements.advancedHost.querySelector('[data-key="detector:countrate_correction_applied"] label').textContent).toBe("countrate_correction_applied");
    // Thresholds share one explanation.
    expect(mod.helpKey("detector", "threshold/3/energy")).toBe("detector.help.threshold_energy");
    expect(mod.helpKey("detector", "threshold/2/mode")).toBe("detector.help.threshold_mode");
    expect(mod.helpKey("detector", "flatfield_correction_applied")).toBe("");
  });
});

describe("detector control panel, compact cards", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("shrinks the address to a line under the detector's name once connected", async () => {
    const routes = {
      "/detector/describe": async () => ({ ok: true, json: async () => ({ state: "idle", params: structuredClone(DESCRIPTORS) }) }),
      "/detector/status": async () => ({ ok: true, json: async () => ({ detector: { state: "idle" } }) }),
      "/detector/files": async () => ({ ok: true, json: async () => ({ files: [] }) }),
    };
    const { controller, elements } = await setup(routes);
    elements.urlInput.value = "192.168.30.90";
    await controller.connect();
    expect(elements.addressHost.hidden).toBe(true);
    // The name says it connected; no second "Connected to ..." line.
    expect(elements.message.textContent).toBe("");
    const where = elements.model.parentElement.nextElementSibling;
    expect(where.textContent).toBe("192.168.30.90 ↗ · Change");
    // The address opens the detector's own web interface, in a new tab.
    const webUi = where.querySelector("a");
    expect(webUi.href).toBe("http://192.168.30.90/");
    expect(webUi.target).toBe("_blank");
    expect(webUi.title).toBe(EN["detector.where.web_ui"]);
    where.querySelector("button").click();
    expect(elements.addressHost.hidden).toBe(false);
    expect(where.hidden).toBe(true);
  });

  it("puts the file writer's facts on one line, and an empty file list too", async () => {
    const { controller, elements } = await setup();
    const params = structuredClone(DESCRIPTORS);
    params.filewriter.mode.value = "enabled";
    params.monitor.mode.value = "enabled";
    controller._setParams(params);
    controller._renderAll();
    controller._setStatus({ detector: { state: "idle" }, filewriter: { mode: "enabled", state: "ready", buffer_free: 3.2e9 }, monitor: { mode: "enabled", state: "normal" } });
    const meta = elements.outputsHost.querySelector(".detector-output .detector-meta");
    expect(meta.textContent).toBe("Next: series_$id_master.h5 · 3.2 GB free · Data page");
    expect(meta.querySelector("a").href).toBe("http://192.168.1.10/data/");
    // Watch live sits in the monitor's header line.
    const monitorHead = [...elements.outputsHost.querySelectorAll(".detector-output-head")].pop();
    expect(monitorHead.textContent).toContain("Watch live images");
    expect(elements.filesHost.textContent).toBe("Files on the detector: none · Refresh");
  });

  it("lays out Advanced on single lines, with readable values", async () => {
    const { mod, controller, elements } = await setup();
    const params = structuredClone(DESCRIPTORS);
    params.detector.sensor_thickness = { value: 0.00045, value_type: "float", unit: "m", access_mode: "r" };
    params.detector.test_image_mode = { value: "", value_type: "string", access_mode: "rw", allowed_values: ["gates", "pattern"] };
    params.stream.header_appendix = { value: "", value_type: "string", access_mode: "rw" };
    controller._setParams(params);
    controller._renderAll();
    const host = elements.advancedHost;
    expect(host.querySelector('[data-key="detector:sensor_thickness"] .detector-readonly').textContent).toBe("450 µm");
    expect(mod.readableValue({ value: 0.075, unit: "m" })).toBe("75 mm");
    expect(mod.readableValue({ value: 2, unit: "m" })).toBe("2 m");
    const mode = host.querySelector("#detector-p-detector-test_image_mode");
    expect(mode.selectedOptions[0].textContent).toBe("(none)");
    // Free text shares the line in Advanced.
    expect(host.querySelector('[data-key="stream:header_appendix"]').classList.contains("is-wide")).toBe(false);
    expect(host.querySelector(".detector-adv-group.is-info .detector-group-label").textContent).toBe("Detector information");
    // Deleting the files is the step that cannot be undone.
    const del = [...elements.troubleshootingHost.querySelectorAll("button")].find((b) => b.textContent === EN["detector.action.delete_files"]);
    expect(del.classList.contains("is-danger")).toBe(true);
  });
});

describe("detector control panel, safe links", () => {
  it("links the data page only for an http(s) detector address", async () => {
    const { detectorDataPage } = await loadModule();
    expect(detectorDataPage("http://192.168.30.90")).toBe("http://192.168.30.90/data/");
    expect(detectorDataPage("https://dcu.example.org/")).toBe("https://dcu.example.org/data/");
    expect(detectorDataPage("javascript://%0aalert(1)")).toBe("");
    expect(detectorDataPage("data:text/html,<script>alert(1)</script>")).toBe("");
    expect(detectorDataPage("")).toBe("");
    const { detectorPage } = await loadModule();
    expect(detectorPage("http://192.168.20.181")).toBe("http://192.168.20.181/");
    expect(detectorPage("javascript://%0aalert(1)")).toBe("");
    delete global.fetch;
  });
});

describe("detector control panel, older detectors", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("uses the SIMPLON version the detector reports, and shows a 1.6 high voltage in volts", async () => {
    const versions = [];
    const routes = {
      "/detector/describe": async (url) => {
        versions.push(new URL(url, "http://x").searchParams.get("version"));
        return { ok: true, json: async () => ({ state: "idle", api_version: "1.6.0", params: structuredClone(DESCRIPTORS) }) };
      },
      "/detector/status": async (url) => {
        versions.push(new URL(url, "http://x").searchParams.get("version"));
        return { ok: true, json: async () => ({ detector: { state: "idle", temperature: 27.2, humidity: 2.1, high_voltage: 197.7 } }) };
      },
      "/detector/files": async () => ({ ok: true, json: async () => ({ files: [] }) }),
    };
    const { controller, elements } = await setup(routes);
    controller.setEnabled(true);
    elements.urlInput.value = "192.168.20.181";
    await controller.connect();
    // Asked once in the default version; from then on in the detector's own.
    expect(versions[0]).toBe("1.8.0");
    expect(versions.length).toBeGreaterThan(1);
    expect(versions.slice(1).every((v) => v === "1.6.0")).toBe(true);
    const sensors = elements.sensors.textContent;
    expect(sensors).toContain("27.2 °C");
    expect(sensors).toContain("198 V");
    expect(elements.sensors.querySelector('[data-tone="ok"]').textContent).toBe("198 V");
    controller.setEnabled(false);
  });
});

describe("detector control panel, enable modes", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  const withMode = (mode, nimages = 1) => {
    const params = structuredClone(DESCRIPTORS);
    params.detector.trigger_mode = { ...params.detector.trigger_mode, value: mode, allowed_values: ["ints", "inte", "exts", "exte"] };
    params.detector.nimages.value = nimages;
    return params;
  };

  it("names the enable modes, and sets images per trigger to 1 before choosing one", async () => {
    const calls = [];
    const put = vi.fn(async (_url, init) => {
      const body = JSON.parse(init.body);
      calls.push(`${body.key}=${body.value}`);
      const base = body.key === "nimages" ? DESCRIPTORS.detector.nimages : DESCRIPTORS.detector.trigger_mode;
      return { ok: true, json: async () => ({ changed: [body.key], params: { [body.key]: { ...base, value: body.value } } }) };
    });
    const { controller, elements } = await setup({ "/detector/config": put });
    controller._setParams(withMode("ints", 10));
    controller._renderAll();
    const select = elements.paramsHost.querySelector("#detector-p-detector-trigger_mode");
    expect([...select.options].map((o) => o.textContent)).toEqual(["Internal, series", "Internal, enable", "External, series", "External, enable"]);
    select.value = "inte";
    select.dispatchEvent(new Event("change"));
    await vi.waitFor(() => expect(calls).toEqual(["nimages=1", "trigger_mode=inte"]));
    expect(elements.logHost.textContent).toContain(EN["detector.log.enable_one_image"]);
  });

  it("shows what applies in each enable mode", async () => {
    const { controller, elements } = await setup();
    controller._setParams(withMode("inte"));
    controller._renderAll();
    const host = elements.paramsHost;
    // Images per trigger is fixed by the mode.
    expect(host.querySelector('[data-key="detector:nimages"] .detector-readonly').textContent).toBe("1 (one per trigger)");
    // Internal enable: count time is the default exposure; frame time does not apply.
    expect(host.querySelector('[data-key="detector:count_time"]')).not.toBeNull();
    expect(host.querySelector('[data-key="detector:frame_time"]')).toBeNull();
    expect(elements.advancedHost.querySelector('[data-key="detector:frame_time"]')).not.toBeNull();
    expect(host.textContent).toContain(EN["detector.enable.count_time_note"]);
    // External enable: the signal sets the exposure, no times in the main view.
    controller._setParams(withMode("exte"));
    controller._renderAll();
    expect(host.querySelector('[data-key="detector:count_time"]')).toBeNull();
    expect(host.textContent).toContain(EN["detector.enable.signal_note"]);
  });

  it("sends each internal-enable Trigger with its own exposure", async () => {
    const sent = [];
    let job = null;
    const routes = {
      "/detector/command": async (_url, init) => {
        const body = JSON.parse(init.body);
        sent.push(body);
        job = { subsystem: "detector", command: body.command, running: false, ok: true };
        return { ok: true, json: async () => ({ ...job, running: true }) };
      },
      "/detector/status": async () => ({ ok: true, json: async () => ({ detector: { state: "ready" }, command: job }) }),
      "/detector/files": async () => ({ ok: true, json: async () => ({ files: [] }) }),
    };
    const summary = document.createElement("span");
    const mod = await loadModule(routes);
    const elements = buildElements();
    const controller = mod.createDetectorControlController({ apiBase: "/api", elements: { ...elements, seriesSummary: summary }, callbacks: {} });
    controller._setConnection("http://192.168.20.88");
    const params = withMode("inte");
    params.detector.ntrigger.value = 100;
    controller._setParams(params);
    controller._renderAll();
    controller._setStatus({ detector: { state: "ready" } });
    expect(summary.textContent).toBe("100 images · exposure per trigger");
    expect(elements.stateText.textContent).toBe("Armed. Image 1 of 100: set its exposure, then Trigger.");
    const exposure = elements.primaryBtn.previousElementSibling;
    expect(exposure.hidden).toBe(false);
    const input = exposure.querySelector("input");
    // Starts at the count time.
    expect(input.value).toBe("0.0099999");
    input.value = "99999";
    elements.primaryBtn.click();
    expect(sent).toEqual([]);
    expect(input.getAttribute("aria-invalid")).toBe("true");
    input.value = "0.25";
    elements.primaryBtn.click();
    await vi.waitFor(() => expect(sent.length).toBe(1), { timeout: 3000 });
    expect(sent[0]).toMatchObject({ command: "trigger", value: 0.25 });
    await vi.waitFor(() => expect(elements.logHost.textContent).toContain("Image 1 of 100 taken (250 ms)"), { timeout: 3000 });
  });

  it("explains an armed external-enable detector waiting for its signals", async () => {
    const { controller, elements } = await setup();
    const params = withMode("exte");
    params.detector.ntrigger.value = 5;
    controller._setParams(params);
    controller._renderAll();
    controller._setStatus({ detector: { state: "acquire" } });
    expect(elements.stateText.textContent).toBe("Armed. Waiting for 5 trigger signals; the length of each sets its exposure.");
    expect(elements.primaryBtn.previousElementSibling.hidden).toBe(true);
  });

  it("shows the detector's own reason when it refuses", async () => {
    const put = vi.fn(async () => ({
      ok: false,
      status: 502,
      json: async () => ({ detail: { code: "http_error", http_status: 400, detector_message: 'number_of_images must be 1 for trigger mode "inte"' } }),
    }));
    const { controller, elements } = await setup({ "/detector/config": put });
    controller._setParams(withMode("inte"));
    controller._renderAll();
    const input = elements.paramsHost.querySelector("#detector-p-detector-ntrigger");
    input.value = "3";
    input.dispatchEvent(new Event("change"));
    await vi.waitFor(() => expect(elements.paramsHost.textContent).toContain('The detector refused: number_of_images must be 1 for trigger mode "inte"'));
  });
});

describe("detector control panel, progress text", () => {
  it("keeps one format for the whole series, so the text does not jump", async () => {
    const { formatProgress } = await loadModule();
    // About 10 s: one decimal, also on whole seconds.
    expect(formatProgress(6.5, 10)).toEqual({ elapsed: "6.5 s", total: "10.0 s" });
    expect(formatProgress(6, 10)).toEqual({ elapsed: "6.0 s", total: "10.0 s" });
    expect(formatProgress(6.56, 10).elapsed).toBe("6.6 s");
    // A single short exposure: two decimals.
    expect(formatProgress(0.1, 0.25)).toEqual({ elapsed: "0.10 s", total: "0.25 s" });
    // Minutes: m:ss.
    expect(formatProgress(65.4, 300)).toEqual({ elapsed: "1:05", total: "5:00 min" });
    delete global.fetch;
  });
});

describe("detector control panel, quick actions, pre-flight and results", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  // A small detector: settings it stores, commands it runs at once.
  function fakeDetector({ triggerRuns = false } = {}) {
    const store = structuredClone(DESCRIPTORS);
    store.filewriter.mode.value = "enabled";
    store.detector.nimages.value = 10;
    const calls = [];
    let state = "idle";
    let job = null;
    let files = [];
    let opened = null;
    const routes = {
      "/detector/config": async (_url, init) => {
        const body = JSON.parse(init.body);
        calls.push(`${body.subsystem}/${body.key}=${body.value}`);
        store[body.subsystem][body.key] = { ...store[body.subsystem][body.key], value: body.value };
        return { ok: true, json: async () => ({ changed: [body.key], params: { [body.key]: store[body.subsystem][body.key] } }) };
      },
      "/detector/command": async (_url, init) => {
        const body = JSON.parse(init.body);
        calls.push(`command ${body.command}`);
        if (body.command === "arm") state = "ready";
        if (body.command === "trigger") {
          if (triggerRuns) {
            state = "acquire";
            job = { subsystem: "detector", command: "trigger", running: true, ok: null };
            return { ok: true, json: async () => job };
          }
          state = "idle";
          files = [{ name: "series_7_master.h5", size: 1000 }, { name: "series_7_data_000001.h5", size: 4000 }];
        }
        if (body.command === "abort") {
          state = "idle";
          job = { subsystem: "detector", command: "trigger", running: false, ok: true };
          return { ok: true, json: async () => ({ ok: true }) };
        }
        job = { subsystem: "detector", command: body.command, running: false, ok: true, result: body.command === "arm" ? { "sequence id": 7 } : null };
        return { ok: true, json: async () => ({ ...job, running: true }) };
      },
      "/detector/status": async () => ({ ok: true, json: async () => ({ detector: { state }, filewriter: { mode: store.filewriter.mode.value, buffer_free: 8e11 }, command: job }) }),
      "/detector/files": async () => ({ ok: true, json: async () => ({ files }) }),
      "/detector/series/fetch": async (_url, init) => {
        opened = JSON.parse(init.body);
        return { ok: true, json: async () => ({ path: "detector/192.168.1.10/series_7_master.h5", files: ["series_7_master.h5"], bytes: 1000 }) };
      },
    };
    return { store, calls, routes, opened: () => opened };
  }

  async function panel(fake) {
    const mod = await loadModule(fake.routes);
    const elements = buildElements();
    const openPath = vi.fn(async () => {});
    const watchLive = vi.fn();
    const controller = mod.createDetectorControlController({ apiBase: "/api", elements, callbacks: { openPath, watchLive } });
    controller._setConnection("http://192.168.1.10");
    controller._setParams(fake.store);
    controller._renderAll();
    controller._setStatus({ detector: { state: "idle" }, filewriter: { mode: "enabled", buffer_free: 8e11 } });
    const buttons = () => [...elements.primaryBtn.parentElement.querySelectorAll("button")].filter((b) => !b.hidden).map((b) => b.textContent);
    const button = (label) => [...elements.primaryBtn.parentElement.querySelectorAll("button")].find((b) => b.textContent === label);
    return { mod, controller, elements, openPath, watchLive, buttons, button };
  }

  it("snaps one image and puts the series settings back", async () => {
    const fake = fakeDetector();
    const { elements, buttons, button, watchLive } = await panel(fake);
    expect(buttons()).toEqual(["Acquire", "Snap · 1 s", "Continuous · 10 Hz"]);
    button("Snap · 1 s").click();
    await vi.waitFor(() => expect(elements.logHost.textContent).toContain(EN["detector.log.restored"]), { timeout: 4000 });
    // A fixed 1 s exposure, not the series' count time; only what differs is
    // changed, and put back in reverse order.
    expect(fake.calls).toEqual([
      "detector/frame_time=1",
      "detector/count_time=1",
      "detector/nimages=1",
      "command arm",
      "command trigger",
      "detector/nimages=10",
      "detector/count_time=0.0099999",
      "detector/frame_time=0.01",
    ]);
    expect(watchLive).toHaveBeenCalled();
    expect(fake.store.detector.nimages.value).toBe(10);
    expect(elements.logHost.textContent).not.toContain("adjusted by the detector");
  });

  it("runs Continuous until Stop, saving nothing, then puts everything back", async () => {
    const fake = fakeDetector({ triggerRuns: true });
    const { elements, button } = await panel(fake);
    button("Continuous · 10 Hz").click();
    await vi.waitFor(() => expect(fake.calls).toContain("command trigger"), { timeout: 4000 });
    // 10 Hz for at most 10 hours: inside a detector's one-week limit.
    expect(fake.calls.slice(0, 4)).toEqual([
      "detector/frame_time=0.1",
      "detector/count_time=0.1",
      "detector/nimages=360000",
      "filewriter/mode=disabled",
    ]);
    // No question: Continuous saves nothing that Stop could lose.
    elements.stopBtn.hidden = false;
    elements.stopBtn.click();
    expect(elements.confirm.hidden).toBe(true);
    await vi.waitFor(() => expect(elements.logHost.textContent).toContain(EN["detector.log.restored"]), { timeout: 4000 });
    expect(fake.calls.slice(-4)).toEqual([
      "filewriter/mode=enabled",
      "detector/nimages=10",
      "detector/count_time=0.0099999",
      "detector/frame_time=0.01",
    ]);
    expect(elements.logHost.textContent).not.toContain("changed by another program");
  });

  it("says what a series needs before it starts, and asks when something is off", async () => {
    const fake = fakeDetector();
    fake.store.detector.x_pixels_in_detector = { value: 2068, value_type: "uint", access_mode: "r" };
    fake.store.detector.y_pixels_in_detector = { value: 2162, value_type: "uint", access_mode: "r" };
    fake.store.detector.bit_depth_image = { value: 32, value_type: "uint", access_mode: "r" };
    fake.store.detector.compression = { value: "bslz4", value_type: "string", access_mode: "rw", allowed_values: ["bslz4", "lz4", "none"] };
    const { controller, elements } = await panel(fake);
    const preflight = () => elements.primaryBtn.parentElement.nextElementSibling.textContent;
    // 10 images x 4.47 Mpx x 4 bytes, against 800 GB free.
    // bslz4 at an estimated 4x: 178.8 MB raw.
    expect(preflight()).toBe("✓ Ready · about 44.7 MB with bslz4, estimated, 800.0 GB free");
    controller._setStatus({ detector: { state: "idle" }, filewriter: { mode: "enabled", buffer_free: 4e7 } });
    expect(preflight()).toContain("This series needs about 44.7 MB with bslz4, estimated; the detector has 40.0 MB free.");
    fake.store.detector.threshold_energy.value = 13000;
    controller._setParams(fake.store);
    controller._renderAll();
    controller._setStatus({ detector: { state: "idle" }, filewriter: { mode: "enabled", buffer_free: 4e7 } });
    expect(preflight()).toContain("The threshold (13000 eV) is above the photon energy (12400 eV)");
    elements.primaryBtn.click();
    expect(elements.confirm.hidden).toBe(false);
    expect(elements.confirmText.textContent).toMatch(/needs about .* Acquire anyway\?$/);
    expect(fake.calls).toEqual([]);
  });

  it("states how fast a series runs", async () => {
    const fake = fakeDetector();
    const { elements } = await panel(fake);
    const timing = elements.paramsHost.querySelector('[data-group="timing"]');
    expect(timing.textContent).toContain("100 Hz · 0.1 µs between images");
  });

  it("offers the finished series to open in ALBIS", async () => {
    const fake = fakeDetector();
    const { elements, button, openPath } = await panel(fake);
    button("Acquire").click();
    const result = () => elements.primaryBtn.parentElement.nextElementSibling.nextElementSibling;
    await vi.waitFor(() => expect(result().textContent).toContain("Series 7: 10 images"), { timeout: 4000 });
    expect(result().textContent).toContain("2 files, 5 kB");
    [...result().querySelectorAll("button")].find((b) => b.textContent === "Open in ALBIS").click();
    await vi.waitFor(() => expect(openPath).toHaveBeenCalledWith("detector/192.168.1.10/series_7_master.h5"));
    expect(fake.opened()).toMatchObject({ prefix: "series_7" });
    expect(elements.logHost.textContent).toContain("Series 7 opened in ALBIS");
  });
});

describe("detector control panel, comparing values", () => {
  it("treats a detector's rounding as the same value", async () => {
    const { sameValue } = await loadModule();
    expect(sameValue(0.1, 0.1)).toBe(true);
    expect(sameValue(0.099999999, 0.1)).toBe(true);
    expect(sameValue(0.0999, 0.1)).toBe(false);
    expect(sameValue("ints", "ints")).toBe(true);
    expect(sameValue(10, 10)).toBe(true);
    delete global.fetch;
  });
});

describe("detector control panel, quick action settings", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
    localStorage.clear();
  });

  it("sets Snap's exposure and Continuous' rate under Advanced, remembered per browser", async () => {
    localStorage.clear();
    const { controller, elements } = await setup();
    controller._setStatus({ detector: { state: "idle" } });
    const host = elements.advancedHost.querySelector(".detector-quick-settings");
    expect(host.querySelector(".detector-group-label").textContent).toBe("Quick actions");
    const exposure = host.querySelector("#detector-quick-snapExposure");
    expect(exposure.value).toBe("1");
    exposure.value = "0.5";
    exposure.dispatchEvent(new Event("change"));
    const rate = host.querySelector("#detector-quick-continuousRate");
    rate.value = "100000000";
    rate.dispatchEvent(new Event("change"));
    expect(rate.getAttribute("aria-invalid")).toBe("true");
    const labels = [...elements.primaryBtn.parentElement.querySelectorAll(".detector-quick")].map((b) => b.textContent);
    expect(labels).toEqual(["Snap · 500 ms", "Continuous · 10 Hz"]);
    expect(JSON.parse(localStorage.getItem("albis.detectorControl.quick"))).toMatchObject({ snapExposure: 0.5, continuousRate: 10 });
  });

  it("leaves compression out of the estimate when the file writer does not compress", async () => {
    const { controller, elements } = await setup();
    const params = structuredClone(DESCRIPTORS);
    params.filewriter.mode.value = "enabled";
    params.filewriter.compression_enabled = { value: false, value_type: "bool", access_mode: "rw" };
    params.detector.compression = { value: "bslz4", value_type: "string", access_mode: "rw", allowed_values: ["bslz4", "lz4", "none"] };
    params.detector.x_pixels_in_detector = { value: 1000, value_type: "uint", access_mode: "r" };
    params.detector.y_pixels_in_detector = { value: 1000, value_type: "uint", access_mode: "r" };
    controller._setParams(params);
    controller._renderAll();
    controller._setStatus({ detector: { state: "idle" }, filewriter: { mode: "enabled", buffer_free: 8e11 } });
    expect(elements.primaryBtn.parentElement.nextElementSibling.textContent).toBe("✓ Ready · about 40.0 MB uncompressed, 800.0 GB free");
  });
});
