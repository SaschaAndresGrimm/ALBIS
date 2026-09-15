import { afterEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";

/**
 * The two path fields that are blank by default.
 *
 * `data.root` and `logging.dir` default to empty, and empty means "work it out
 * at start" -- from whether this is a packaged build and where the config file
 * sits. So a tester on Windows opened Preferences and found both fields blank
 * while the paths plainly existed and were in use.
 *
 * The backend now reports what it resolved, and it goes in the placeholder
 * rather than the value: filling the value in looks identical and then pins
 * the path on the next save, freezing a location that is meant to follow the
 * installation.
 */

const CATALOGUE = JSON.parse(fs.readFileSync("frontend/locales/en.json", "utf8"));

const EFFECTIVE = {
  data: { root: "C:\\Users\\sascha\\ALBIS-data" },
  logging: { dir: "C:\\Users\\sascha\\.config\\albis\\logs" },
};

async function mount(payload) {
  vi.resetModules();
  document.body.innerHTML = `
    <div id="settings-modal">
      <input id="settings-data-root" type="text" />
      <input id="settings-log-dir" type="text" />
      <select id="settings-log-level"><option value="INFO">INFO</option></select>
      <input id="settings-allow-abs" type="checkbox" />
      <input id="settings-scan-cache" type="text" />
      <input id="settings-max-scan-depth" type="text" />
      <input id="settings-max-upload" type="text" />
      <input id="settings-server-port" type="text" />
      <input id="settings-server-reload" type="checkbox" />
      <input id="settings-startup-timeout" type="text" />
      <input id="settings-open-browser" type="checkbox" />
    </div>`;

  global.fetch = vi.fn(async (url) => {
    if (String(url).includes("/api/settings")) {
      return { ok: true, status: 200, json: async () => payload };
    }
    // Everything else is the locale catalogue, which i18n asks for by a
    // relative URL -- matching on "/locales/" misses it.
    return { ok: true, status: 200, json: async () => CATALOGUE };
  });

  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createSettingsController } = await import("../modules/settings_controller.js");

  const controller = createSettingsController({
    apiBase: "/api",
    state: {},
    constants: { pixelLabelDefaultMinCellPx: 18, pixelLabelDefaultMaxLabels: 4000 },
    elements: {
      settingsModal: document.getElementById("settings-modal"),
      settingsDataRoot: document.getElementById("settings-data-root"),
      settingsLogDir: document.getElementById("settings-log-dir"),
      settingsLogLevel: document.getElementById("settings-log-level"),
      settingsAllowAbs: document.getElementById("settings-allow-abs"),
      settingsScanCache: document.getElementById("settings-scan-cache"),
      settingsMaxScanDepth: document.getElementById("settings-max-scan-depth"),
      settingsMaxUpload: document.getElementById("settings-max-upload"),
      // fillSettingsForm dereferences these without a guard, so the form
      // never reaches the path fields if they are absent.
      settingsServerPort: document.getElementById("settings-server-port"),
      settingsServerReload: document.getElementById("settings-server-reload"),
      settingsStartupTimeout: document.getElementById("settings-startup-timeout"),
      settingsOpenBrowser: document.getElementById("settings-open-browser"),
    },
    callbacks: {
      setStatus: vi.fn(),
      closeMenu: vi.fn(),
      openModal: vi.fn(),
      closeModal: vi.fn(),
      setToolHintsEnabled: vi.fn(),
      schedulePixelOverlay: vi.fn(),
      applyLanguagePreference: vi.fn(),
    },
  });

  await controller.openSettingsModal();
  await new Promise((resolve) => setTimeout(resolve, 0));
  return {
    dataRoot: document.getElementById("settings-data-root"),
    logDir: document.getElementById("settings-log-dir"),
  };
}

describe("settings effective paths", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
    document.body.innerHTML = "";
  });

  it("shows the resolved path for a field left blank", async () => {
    const { dataRoot, logDir } = await mount({
      config: { data: { root: "" }, logging: { dir: "" } },
      defaults: {},
      effective: EFFECTIVE,
      path: "C:\\config.json",
    });

    expect(dataRoot.placeholder).toBe(EFFECTIVE.data.root);
    expect(logDir.placeholder).toBe(EFFECTIVE.logging.dir);
  });

  it("leaves the value blank so saving does not pin the path", async () => {
    // The whole point of the placeholder: the field still submits nothing, so
    // the config keeps saying "work it out" instead of freezing this location.
    const { dataRoot, logDir } = await mount({
      config: { data: { root: "" }, logging: { dir: "" } },
      defaults: {},
      effective: EFFECTIVE,
      path: "C:\\config.json",
    });

    expect(dataRoot.value).toBe("");
    expect(logDir.value).toBe("");
  });

  it("explains the placeholder on hover", async () => {
    const { dataRoot } = await mount({
      config: { data: { root: "" }, logging: { dir: "" } },
      defaults: {},
      effective: EFFECTIVE,
      path: "C:\\config.json",
    });

    expect(dataRoot.title).toContain(EFFECTIVE.data.root);
    expect(dataRoot.title).not.toContain("settings.path");
  });

  it("keeps an explicitly configured path as the value", async () => {
    const { dataRoot } = await mount({
      config: { data: { root: "./data" }, logging: { dir: "" } },
      defaults: {},
      effective: { data: { root: "/repo/data" }, logging: { dir: "/repo/logs" } },
      path: "/repo/albis.config.json",
    });

    expect(dataRoot.value).toBe("./data");
    // Still worth showing where a relative path lands, but never instead of it.
    expect(dataRoot.placeholder).toBe("/repo/data");
  });

  it("adds no placeholder when the backend reports nothing", async () => {
    // An older backend has no `effective` key; an empty box beats the string
    // "undefined" sitting in it.
    const { dataRoot, logDir } = await mount({
      config: { data: { root: "" }, logging: { dir: "" } },
      defaults: {},
      path: "/repo/albis.config.json",
    });

    expect(dataRoot.hasAttribute("placeholder")).toBe(false);
    expect(logDir.hasAttribute("placeholder")).toBe(false);
  });
});
