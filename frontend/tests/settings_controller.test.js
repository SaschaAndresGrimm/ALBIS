import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";

function readLocale(language) {
  const filePath = path.join(process.cwd(), "frontend", "locales", `${language}.json`);
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function renderSettingsShell() {
  document.body.innerHTML = `
    <div id="settings-modal" class="modal" aria-hidden="true">
      <button id="settings-close" type="button">x</button>
      <button id="settings-save" type="button">save</button>
      <button id="settings-save-close" type="button">save close</button>
      <code id="settings-config-path"></code>
      <div id="settings-message"></div>
      <label>
        <input id="settings-server-external" type="checkbox" />
        <span id="settings-server-external-label" data-i18n="settings.server.external_access">Allow external connections from other machines</span>
      </label>
      <div id="settings-server-external-warning" class="is-hidden" data-i18n="settings.server.external_warning" aria-hidden="true"></div>
      <input id="settings-server-port" type="number" />
      <input id="settings-server-reload" type="checkbox" />
      <input id="settings-startup-timeout" type="number" />
      <input id="settings-open-browser" type="checkbox" />
      <input id="settings-tool-hints" type="checkbox" />
      <select id="settings-language"><option value="en">English</option></select>
      <input id="settings-pixel-label-min" type="number" />
      <input id="settings-pixel-label-max" type="number" />
      <select id="settings-pixel-label-format"><option value="auto">Auto</option></select>
      <input id="settings-pixel-label-drag" type="checkbox" />
      <input id="settings-data-root" type="text" />
      <input id="settings-allow-abs" type="checkbox" />
      <input id="settings-scan-cache" type="number" />
      <input id="settings-max-scan-depth" type="number" />
      <input id="settings-max-upload" type="number" />
      <select id="settings-log-level"><option value="INFO">INFO</option></select>
      <input id="settings-log-dir" type="text" />
    </div>
  `;
}

function buildFetchMock({ initialConfig, savedConfigs }) {
  return vi.fn(async (url, init) => {
    const requestUrl = String(url);
    if (requestUrl.includes("locales/")) {
      const match = requestUrl.match(/locales\/([^/]+)\.json/);
      const language = match ? decodeURIComponent(match[1]) : "en";
      return {
        ok: true,
        json: async () => readLocale(language),
      };
    }
    if (requestUrl.endsWith("/api/settings") && (!init || !init.method || init.method === "GET")) {
      return {
        ok: true,
        json: async () => ({ config: initialConfig, path: "/tmp/albis.config.json" }),
      };
    }
    if (requestUrl.endsWith("/api/settings") && init?.method === "POST") {
      const payload = JSON.parse(String(init.body || "{}"));
      savedConfigs.push(payload.config);
      return {
        ok: true,
        json: async () => ({ config: payload.config, path: "/tmp/albis.config.json" }),
      };
    }
    throw new Error(`Unexpected fetch URL: ${requestUrl}`);
  });
}

async function initializeController({ initialConfig }) {
  vi.resetModules();
  renderSettingsShell();
  localStorage.clear();

  const savedConfigs = [];
  global.fetch = buildFetchMock({ initialConfig, savedConfigs });

  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });

  const { createSettingsController } = await import("../modules/settings_controller.js");

  const controller = createSettingsController({
    apiBase: "/api",
    state: {
      toolHintsEnabled: false,
      language: "en",
      pixelLabelMinCellPx: 18,
      pixelLabelMaxLabels: 4000,
      pixelLabelFormat: "auto",
      pixelLabelShowDuringDrag: false,
    },
    constants: {
      pixelLabelDefaultMinCellPx: 18,
      pixelLabelDefaultMaxLabels: 4000,
    },
    elements: {
      settingsModal: document.getElementById("settings-modal"),
      settingsClose: document.getElementById("settings-close"),
      settingsSave: document.getElementById("settings-save"),
      settingsSaveClose: document.getElementById("settings-save-close"),
      settingsConfigPath: document.getElementById("settings-config-path"),
      settingsMessage: document.getElementById("settings-message"),
      settingsServerExternal: document.getElementById("settings-server-external"),
      settingsServerExternalLabel: document.getElementById("settings-server-external-label"),
      settingsServerExternalWarning: document.getElementById("settings-server-external-warning"),
      settingsServerPort: document.getElementById("settings-server-port"),
      settingsServerReload: document.getElementById("settings-server-reload"),
      settingsStartupTimeout: document.getElementById("settings-startup-timeout"),
      settingsOpenBrowser: document.getElementById("settings-open-browser"),
      settingsToolHints: document.getElementById("settings-tool-hints"),
      settingsLanguage: document.getElementById("settings-language"),
      settingsPixelLabelMin: document.getElementById("settings-pixel-label-min"),
      settingsPixelLabelMax: document.getElementById("settings-pixel-label-max"),
      settingsPixelLabelFormat: document.getElementById("settings-pixel-label-format"),
      settingsPixelLabelDrag: document.getElementById("settings-pixel-label-drag"),
      settingsDataRoot: document.getElementById("settings-data-root"),
      settingsAllowAbs: document.getElementById("settings-allow-abs"),
      settingsScanCache: document.getElementById("settings-scan-cache"),
      settingsMaxScanDepth: document.getElementById("settings-max-scan-depth"),
      settingsMaxUpload: document.getElementById("settings-max-upload"),
      settingsLogLevel: document.getElementById("settings-log-level"),
      settingsLogDir: document.getElementById("settings-log-dir"),
    },
    callbacks: {
      setToolHintsEnabled: vi.fn(),
      openModal: vi.fn(),
      closeModal: vi.fn(),
      closeMenu: vi.fn(),
      setStatus: vi.fn(),
      schedulePixelOverlay: vi.fn(),
      applyLanguagePreference: vi.fn((language) => language),
    },
  });

  return { controller, savedConfigs, locale: readLocale("en") };
}

describe("settings controller external access warning", () => {
  beforeEach(() => {
    window.confirm = vi.fn(() => true);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("shows warning text and requires confirmation before enabling external access", async () => {
    const { controller, locale } = await initializeController({
      initialConfig: {
        server: { host: "127.0.0.1", port: 0, reload: false },
        launcher: { startup_timeout_sec: 10, open_browser: true },
        data: { root: "", allow_abs_paths: true, scan_cache_sec: 2, max_scan_depth: -1, max_upload_mb: 0 },
        logging: { level: "INFO", dir: "" },
        ui: { tool_hints: false, pixel_label_min_cell_px: 18, pixel_label_max_labels: 4000, pixel_label_format: "auto", pixel_label_show_during_drag: false, language: "en" },
      },
    });

    await controller.openSettingsModal();

    const toggle = document.getElementById("settings-server-external");
    const label = document.getElementById("settings-server-external-label");
    const warning = document.getElementById("settings-server-external-warning");

    expect(toggle?.checked).toBe(false);
    expect(label?.textContent).toBe(locale["settings.server.external_access"]);
    expect(warning?.classList.contains("is-hidden")).toBe(true);

    toggle.checked = true;
    toggle.dispatchEvent(new Event("change", { bubbles: true }));

    expect(window.confirm).toHaveBeenCalledWith(locale["settings.server.external_confirm"]);
    expect(label?.textContent).toBe(locale["settings.server.external_access_enabled"]);
    expect(warning?.textContent).toBe(locale["settings.server.external_warning"]);
    expect(warning?.classList.contains("is-hidden")).toBe(false);
  });

  it("reverts the toggle when the confirmation is declined and preserves local-only save output", async () => {
    window.confirm = vi.fn(() => false);
    const { controller, savedConfigs, locale } = await initializeController({
      initialConfig: {
        server: { host: "127.0.0.1", port: 8000, reload: false },
        launcher: { startup_timeout_sec: 10, open_browser: true },
        data: { root: "", allow_abs_paths: true, scan_cache_sec: 2, max_scan_depth: -1, max_upload_mb: 0 },
        logging: { level: "INFO", dir: "" },
        ui: { tool_hints: false, pixel_label_min_cell_px: 18, pixel_label_max_labels: 4000, pixel_label_format: "auto", pixel_label_show_during_drag: false, language: "en" },
      },
    });

    await controller.openSettingsModal();

    const toggle = document.getElementById("settings-server-external");
    const label = document.getElementById("settings-server-external-label");
    const warning = document.getElementById("settings-server-external-warning");

    toggle.checked = true;
    toggle.dispatchEvent(new Event("change", { bubbles: true }));

    expect(toggle.checked).toBe(false);
    expect(label?.textContent).toBe(locale["settings.server.external_access"]);
    expect(warning?.classList.contains("is-hidden")).toBe(true);

    await controller.saveSettingsFromModal();

    expect(savedConfigs).toHaveLength(1);
    expect(savedConfigs[0]?.server?.host).toBe("127.0.0.1");
  });
});
