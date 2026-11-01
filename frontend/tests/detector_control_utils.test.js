import fs from "node:fs";
import path from "node:path";

import { beforeAll, describe, expect, it, vi } from "vitest";

// The pure helpers of the Detector tab that detector_control_controller.test.js
// does not reach by name; the rest are tested there.
const EN = JSON.parse(fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8"));

let utils;

beforeAll(async () => {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  utils = await import("../modules/detector_control_utils.js");
});

describe("detector_control_utils", () => {
  it("knows an on/off setting whichever way the detector types it", () => {
    expect(utils.isBinary({ value_type: "bool", value: false })).toBe(true);
    expect(utils.isBinary({ value_type: "string", allowed_values: ["disabled", "enabled"] })).toBe(true);
    expect(utils.isBinary({ value_type: "string", allowed_values: ["enabled", "disabled", "auto"] })).toBe(false);
    expect(utils.isBinary(undefined)).toBe(false);
    expect(utils.isOn({ value_type: "bool", value: true })).toBe(true);
    expect(utils.isOn({ value_type: "string", value: "enabled" })).toBe(true);
    expect(utils.isOn({ value_type: "string", value: "disabled" })).toBe(false);
  });

  it("names the enable trigger modes", () => {
    expect(utils.isEnableMode("inte")).toBe(true);
    expect(utils.isEnableMode("exte")).toBe(true);
    expect(utils.isEnableMode("ints")).toBe(false);
    expect(utils.isEnableMode(undefined)).toBe(false);
  });

  it("formats a byte count for people", () => {
    expect(utils.formatBytes(2.5e12)).toBe("2.5 TB");
    expect(utils.formatBytes(161e9)).toBe("161.0 GB");
    expect(utils.formatBytes(22.3e6)).toBe("22.3 MB");
    expect(utils.formatBytes(10)).toBe("1 kB");
    expect(utils.formatBytes(-1)).toBe("");
    expect(utils.formatBytes("x")).toBe("");
  });

  it("fills in the units a detector leaves out, in every subsystem", () => {
    const all = utils.withKnownUnits({
      detector: { detector_distance: { value: 0.1 }, omega_start: { value: 0 }, photon_energy: { value: 8000, unit: "eV" } },
      stream: {},
    });
    expect(all.detector.detector_distance.unit).toBe("m");
    expect(all.detector.omega_start.unit).toBe("deg");
    expect(all.detector.photon_energy.unit).toBe("eV");
  });

  it("labels parameters and their options", () => {
    expect(utils.paramLabel("threshold/2/energy")).toBe(EN["detector.param.threshold_n"].replace("{{n}}", "2"));
    expect(utils.paramLabel("nimages")).toBe(EN["detector.param.nimages"]);
    expect(utils.paramLabel("roi_mode")).toBe("roi_mode");
    expect(utils.optionLabel("trigger_mode", "exts")).toBe(EN["detector.trigger_mode.exts"]);
    expect(utils.optionLabel("anything", "enabled")).toBe(EN["detector.value.on"]);
    expect(utils.optionLabel("anything", "disabled")).toBe(EN["detector.value.off"]);
    expect(utils.optionLabel("roi_mode", "4M")).toBe("4M");
  });
});
