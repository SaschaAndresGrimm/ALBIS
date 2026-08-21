import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";

const { showConfirmDialogMock } = vi.hoisted(() => ({ showConfirmDialogMock: vi.fn() }));
vi.mock("../modules/dialogs.js", () => ({
  showConfirmDialog: showConfirmDialogMock,
  showPromptDialog: vi.fn(),
}));

function readLocale(language) {
  const filePath = path.join(process.cwd(), "frontend", "locales", `${language}.json`);
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function renderSettingsShell() {
  document.body.innerHTML = `
    <div id="settings-modal" class="modal" aria-hidden="true">
      <button id="settings-close" type="button">x</button>
      <button id="settings-save" type="button">save</button>
      <code id="settings-config-path"></code>
      <div class="panel-tabs settings-tabs">
        <button class="panel-tab is-active" data-settings-tab="viewer" aria-selected="true">Viewer</button>
        <button class="panel-tab" data-settings-tab="connection" aria-selected="false">Connection</button>
      </div>
      <div class="settings-tabpage is-active" data-settings-tab="viewer"></div>
      <div class="settings-tabpage" data-settings-tab="connection" hidden></div>
      <div id="settings-restart-note" class="is-hidden" aria-hidden="true"></div>
      <div id="settings-env-note" class="is-hidden" aria-hidden="true"></div>
      <div id="settings-message"></div>
      <label>
        <input id="settings-server-external" type="checkbox" />
        <span id="settings-server-external-label" data-i18n="settings.server.external_access">Allow external connections from other machines</span>
      </label>
      <div id="settings-server-external-warning" class="is-hidden" data-i18n="settings.server.external_warning" aria-hidden="true"></div>
      <label><span>Port</span><input id="settings-server-port" type="number" /></label>
      <label><span>Allowed hosts</span><input id="settings-allowed-hosts" type="text" /></label>
      <label><span>Response compression</span><select id="settings-compression">
        <option value="auto">Auto</option><option value="on">On</option><option value="off">Off</option>
      </select></label>
      <input id="settings-startup-health-timeout" type="number" />
      <input id="settings-server-reload" type="checkbox" />
      <input id="settings-startup-timeout" type="number" />
      <input id="settings-open-browser" type="checkbox" />
      <input id="settings-auto-check-updates" type="checkbox" />
      <input id="settings-tool-hints" type="checkbox" />
      <select id="settings-language"><option value="en">English</option></select>
      <input id="settings-pixel-label-min" type="number" />
      <input id="settings-pixel-label-max" type="number" />
      <select id="settings-pixel-label-format"><option value="auto">Auto</option></select>
      <input id="settings-pixel-label-drag" type="checkbox" />
      <input id="settings-frame-cache" type="number" />
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

function buildFetchMock({ initialConfig, savedConfigs, envOverrides = [] }) {
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
        json: async () => ({
          config: initialConfig,
          path: "/tmp/albis.config.json",
          env_overrides: envOverrides,
        }),
      };
    }
    if (requestUrl.endsWith("/api/settings") && init?.method === "POST") {
      const payload = JSON.parse(String(init.body || "{}"));
      savedConfigs.push(payload.config);
      return {
        ok: true,
        json: async () => ({
          config: payload.config,
          path: "/tmp/albis.config.json",
          env_overrides: envOverrides,
        }),
      };
    }
    throw new Error(`Unexpected fetch URL: ${requestUrl}`);
  });
}

async function initializeController({ initialConfig, envOverrides = [] }) {
  vi.resetModules();
  renderSettingsShell();
  localStorage.clear();

  const savedConfigs = [];
  global.fetch = buildFetchMock({ initialConfig, savedConfigs, envOverrides });

  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });

  const { createSettingsController } = await import("../modules/settings_controller.js");

  const state = {
      toolHintsEnabled: false,
      autoCheckUpdates: true,
      language: "en",
      pixelLabelMinCellPx: 18,
      pixelLabelMaxLabels: 4000,
      pixelLabelFormat: "auto",
      pixelLabelShowDuringDrag: false,
  };
  const controller = createSettingsController({
    apiBase: "/api",
    state,
    constants: {
      pixelLabelDefaultMinCellPx: 18,
      pixelLabelDefaultMaxLabels: 4000,
    },
    elements: {
      settingsModal: document.getElementById("settings-modal"),
      settingsClose: document.getElementById("settings-close"),
      settingsSave: document.getElementById("settings-save"),
      settingsTabs: document.querySelector(".settings-tabs"),
      settingsRestartNote: document.getElementById("settings-restart-note"),
      settingsEnvNote: document.getElementById("settings-env-note"),
      settingsConfigPath: document.getElementById("settings-config-path"),
      settingsMessage: document.getElementById("settings-message"),
      settingsServerExternal: document.getElementById("settings-server-external"),
      settingsServerExternalWarning: document.getElementById("settings-server-external-warning"),
      settingsServerPort: document.getElementById("settings-server-port"),
      settingsAllowedHosts: document.getElementById("settings-allowed-hosts"),
      settingsCompression: document.getElementById("settings-compression"),
      settingsStartupHealthTimeout: document.getElementById("settings-startup-health-timeout"),
      settingsServerReload: document.getElementById("settings-server-reload"),
      settingsStartupTimeout: document.getElementById("settings-startup-timeout"),
      settingsOpenBrowser: document.getElementById("settings-open-browser"),
      settingsAutoCheckUpdates: document.getElementById("settings-auto-check-updates"),
      settingsToolHints: document.getElementById("settings-tool-hints"),
      settingsLanguage: document.getElementById("settings-language"),
      settingsPixelLabelMin: document.getElementById("settings-pixel-label-min"),
      settingsPixelLabelMax: document.getElementById("settings-pixel-label-max"),
      settingsPixelLabelFormat: document.getElementById("settings-pixel-label-format"),
      settingsPixelLabelDrag: document.getElementById("settings-pixel-label-drag"),
      settingsFrameCache: document.getElementById("settings-frame-cache"),
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

  return { controller, savedConfigs, state, locale: readLocale("en") };
}

describe("settings controller external access warning", () => {
  beforeEach(() => {
    showConfirmDialogMock.mockReset();
    showConfirmDialogMock.mockResolvedValue(true);
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
        ui: { tool_hints: false, auto_check_updates: true, pixel_label_min_cell_px: 18, pixel_label_max_labels: 4000, pixel_label_format: "auto", pixel_label_show_during_drag: false, language: "en" },
      },
    });

    await controller.openSettingsModal();

    const toggle = document.getElementById("settings-server-external");
    const label = document.getElementById("settings-server-external")?.parentElement?.querySelector("[data-i18n='settings.server.external_access']");
    const warning = document.getElementById("settings-server-external-warning");

    expect(toggle?.checked).toBe(false);
    expect(label?.textContent).toBe(locale["settings.server.external_access"]);
    expect(warning?.classList.contains("is-hidden")).toBe(true);
    const labelBeforeEnabling = label?.textContent;

    toggle.checked = true;
    toggle.dispatchEvent(new Event("change", { bubbles: true }));

    await vi.waitFor(() => expect(showConfirmDialogMock).toHaveBeenCalled());
    expect(showConfirmDialogMock.mock.calls[0][0]).toMatchObject({
      message: locale["settings.server.external_confirm"],
    });
    await vi.waitFor(() => expect(warning?.classList.contains("is-hidden")).toBe(false));
    expect(warning?.textContent).toBe(locale["settings.server.external_warning"]);
    // The label keeps describing what the checkbox does. It used to be replaced
    // by the warning, which left the control unlabelled once ticked and said the
    // same thing as the box directly below it.
    expect(label?.textContent).toBe(labelBeforeEnabling);
  });

  it("reverts the toggle when the confirmation is declined and preserves local-only save output", async () => {
    showConfirmDialogMock.mockResolvedValue(false);
    const { controller, savedConfigs, locale } = await initializeController({
      initialConfig: {
        server: { host: "127.0.0.1", port: 8000, reload: false },
        launcher: { startup_timeout_sec: 10, open_browser: true },
        data: { root: "", allow_abs_paths: true, scan_cache_sec: 2, max_scan_depth: -1, max_upload_mb: 0 },
        logging: { level: "INFO", dir: "" },
        ui: { tool_hints: false, auto_check_updates: true, pixel_label_min_cell_px: 18, pixel_label_max_labels: 4000, pixel_label_format: "auto", pixel_label_show_during_drag: false, language: "en" },
      },
    });

    await controller.openSettingsModal();

    const toggle = document.getElementById("settings-server-external");
    const label = document.getElementById("settings-server-external")?.parentElement?.querySelector("[data-i18n='settings.server.external_access']");
    const warning = document.getElementById("settings-server-external-warning");

    toggle.checked = true;
    toggle.dispatchEvent(new Event("change", { bubbles: true }));

    await vi.waitFor(() => expect(toggle.checked).toBe(false));
    expect(label?.textContent).toBe(locale["settings.server.external_access"]);
    expect(warning?.classList.contains("is-hidden")).toBe(true);

    await controller.saveSettingsFromModal();

    expect(savedConfigs).toHaveLength(1);
    expect(savedConfigs[0]?.server?.host).toBe("127.0.0.1");
  });

  it("round-trips the startup update-check preference", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: {
        server: { host: "127.0.0.1", port: 8000, reload: false },
        launcher: { startup_timeout_sec: 10, open_browser: true },
        data: { root: "", allow_abs_paths: true, scan_cache_sec: 2, max_scan_depth: -1, max_upload_mb: 0 },
        logging: { level: "INFO", dir: "" },
        ui: { tool_hints: false, auto_check_updates: true, pixel_label_min_cell_px: 18, pixel_label_max_labels: 4000, pixel_label_format: "auto", pixel_label_show_during_drag: false, language: "en" },
      },
    });

    await controller.openSettingsModal();

    const toggle = document.getElementById("settings-auto-check-updates");
    expect(toggle?.checked).toBe(true);

    toggle.checked = false;
    await controller.saveSettingsFromModal();

    expect(savedConfigs).toHaveLength(1);
    expect(savedConfigs[0]?.ui?.auto_check_updates).toBe(false);
  });
});

describe("settings controller preserves config it has no form control for", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  // Saving rebuilds each config section from the modal's form controls, so any
  // key without a control was silently dropped and then reset to its default by
  // the backend's normalization. Opening settings and pressing Save would quietly
  // undo a hand-edited albis.config.json.
  const configWithFormlessKeys = {
    server: {
      host: "127.0.0.1",
      port: 8000,
      reload: false,
      compression: "on",
      allowed_hosts: ["albis.lab"],
    },
    launcher: { startup_timeout_sec: 10, open_browser: true },
    data: { root: "", allow_abs_paths: true, scan_cache_sec: 2, max_scan_depth: -1, max_upload_mb: 0 },
    logging: { level: "INFO", dir: "" },
    ui: {
      tool_hints: false,
      auto_check_updates: true,
      pixel_label_min_cell_px: 18,
      pixel_label_max_labels: 4000,
      frame_cache_mb: 1024,
      pixel_label_format: "auto",
      pixel_label_show_during_drag: false,
      language: "en",
    },
  };

  it("keeps server.compression across a save", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWithFormlessKeys,
    });

    await controller.openSettingsModal();
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.server?.compression).toBe("on");
  });

  it("keeps server.allowed_hosts across a save", async () => {
    // This one decides which Host headers the backend answers, so dropping it
    // would not just reset a preference -- it would re-close a reverse proxy's
    // access, or silently widen what a deployment accepts.
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWithFormlessKeys,
    });

    await controller.openSettingsModal();
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.server?.allowed_hosts).toEqual(["albis.lab"]);
  });

  it("keeps ui.frame_cache_mb across a save", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWithFormlessKeys,
    });

    await controller.openSettingsModal();
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.ui?.frame_cache_mb).toBe(1024);
  });

  it("still lets a form control win over the loaded value", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWithFormlessKeys,
    });

    await controller.openSettingsModal();
    document.getElementById("settings-auto-check-updates").checked = false;
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.ui?.auto_check_updates).toBe(false);
    expect(savedConfigs[0]?.ui?.frame_cache_mb).toBe(1024);
  });

  it("round-trips the frame cache size through its dialog field", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWithFormlessKeys,
    });

    await controller.openSettingsModal();
    const field = document.getElementById("settings-frame-cache");
    expect(field?.value).toBe("1024");

    field.value = "512";
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.ui?.frame_cache_mb).toBe(512);
  });

  it("clamps a frame cache size outside the supported range", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWithFormlessKeys,
    });

    await controller.openSettingsModal();
    document.getElementById("settings-frame-cache").value = "999999";
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.ui?.frame_cache_mb).toBe(4096);
  });

  it("accepts 0 as an explicit way to disable the frame cache", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWithFormlessKeys,
    });

    await controller.openSettingsModal();
    document.getElementById("settings-frame-cache").value = "0";
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.ui?.frame_cache_mb).toBe(0);
  });

  it("applies frame_cache_mb to state so the cache honours it", async () => {
    const { controller, state } = await initializeController({
      initialConfig: configWithFormlessKeys,
    });

    await controller.openSettingsModal();

    expect(state.frameCacheMb).toBe(1024);
  });
});

describe("settings controller fields added for reverse-proxy deployments", () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  const configWith = (server) => ({
    server: { host: "127.0.0.1", port: 8000, reload: false, ...server },
    launcher: { startup_timeout_sec: 10, startup_health_timeout_sec: 15, open_browser: true },
    data: { root: "", allow_abs_paths: true, scan_cache_sec: 2, max_scan_depth: -1, max_upload_mb: 0 },
    logging: { level: "INFO", dir: "" },
    ui: {
      tool_hints: false, auto_check_updates: true, pixel_label_min_cell_px: 18,
      pixel_label_max_labels: 4000, pixel_label_format: "auto",
      pixel_label_show_during_drag: false, language: "en",
    },
  });

  it("shows the stored allowed hosts as a comma separated list", async () => {
    const { controller } = await initializeController({
      initialConfig: configWith({ allowed_hosts: ["albis.lab", "192.168.1.20"] }),
    });

    await controller.openSettingsModal();

    expect(document.getElementById("settings-allowed-hosts").value).toBe("albis.lab, 192.168.1.20");
  });

  it("saves allowed hosts as a list, however the user separated them", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWith({ allowed_hosts: [] }),
    });
    await controller.openSettingsModal();

    // Commas, spaces, a trailing comma and stray whitespace all appear in a
    // pasted list; none of them should reach the backend as an entry.
    document.getElementById("settings-allowed-hosts").value = " albis.lab,  192.168.1.20 ,\nproxy.internal, ";
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.server?.allowed_hosts).toEqual([
      "albis.lab",
      "192.168.1.20",
      "proxy.internal",
    ]);
  });

  it("saves an empty field as an empty list rather than a blank entry", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWith({ allowed_hosts: ["albis.lab"] }),
    });
    await controller.openSettingsModal();

    document.getElementById("settings-allowed-hosts").value = "   ";
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.server?.allowed_hosts).toEqual([]);
  });

  it("round-trips compression and the health check timeout", async () => {
    const { controller, savedConfigs } = await initializeController({
      initialConfig: configWith({ compression: "on" }),
    });
    await controller.openSettingsModal();

    expect(document.getElementById("settings-compression").value).toBe("on");
    expect(document.getElementById("settings-startup-health-timeout").value).toBe("15");

    document.getElementById("settings-compression").value = "off";
    document.getElementById("settings-startup-health-timeout").value = "22.5";
    await controller.saveSettingsFromModal();

    expect(savedConfigs[0]?.server?.compression).toBe("off");
    expect(savedConfigs[0]?.launcher?.startup_health_timeout_sec).toBe(22.5);
  });
});

describe("settings controller tabs and the restart notice", () => {
  beforeEach(() => localStorage.clear());
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  const baseConfig = {
    server: { host: "127.0.0.1", port: 8000, reload: false, compression: "auto", allowed_hosts: [] },
    launcher: { startup_timeout_sec: 10, startup_health_timeout_sec: 15, open_browser: true },
    data: { root: "", allow_abs_paths: true, scan_cache_sec: 2, max_scan_depth: -1, max_upload_mb: 0 },
    logging: { level: "INFO", dir: "" },
    ui: {
      tool_hints: false, auto_check_updates: true, pixel_label_min_cell_px: 18,
      pixel_label_max_labels: 4000, pixel_label_format: "auto",
      pixel_label_show_during_drag: false, language: "en",
    },
  };

  const note = () => document.getElementById("settings-restart-note");
  const noteHidden = () => note().classList.contains("is-hidden");

  it("shows one tab at a time and always opens on the first", async () => {
    const { controller } = await initializeController({ initialConfig: baseConfig });
    await controller.openSettingsModal();

    const viewer = document.querySelector('.settings-tabpage[data-settings-tab="viewer"]');
    const connection = document.querySelector('.settings-tabpage[data-settings-tab="connection"]');
    expect(viewer.hidden).toBe(false);
    expect(connection.hidden).toBe(true);

    controller.showSettingsTab("connection");
    expect(viewer.hidden).toBe(true);
    expect(connection.hidden).toBe(false);
    expect(
      document.querySelector('.panel-tab[data-settings-tab="connection"]').getAttribute("aria-selected"),
    ).toBe("true");

    // Reopening should not strand the user on whichever tab they left.
    await controller.openSettingsModal();
    expect(viewer.hidden).toBe(false);
  });

  it("says nothing about restarting until a restart-scoped field changes", async () => {
    const { controller } = await initializeController({ initialConfig: baseConfig });
    await controller.openSettingsModal();

    expect(noteHidden()).toBe(true);

    document.getElementById("settings-server-port").value = "9001";
    document.getElementById("settings-server-port").dispatchEvent(new Event("input", { bubbles: true }));

    expect(noteHidden()).toBe(false);
    expect(note().textContent).toContain("Port");
  });

  it("names every changed field, and only the changed ones", async () => {
    const { controller } = await initializeController({ initialConfig: baseConfig });
    await controller.openSettingsModal();

    const port = document.getElementById("settings-server-port");
    const compression = document.getElementById("settings-compression");
    port.value = "9001";
    port.dispatchEvent(new Event("input", { bubbles: true }));
    compression.value = "on";
    compression.dispatchEvent(new Event("change", { bubbles: true }));

    expect(note().textContent).toContain("Port");
    expect(note().textContent).toContain("Response compression");
  });

  it("stays quiet for a field that applies immediately", async () => {
    // allowed_hosts is picked up by the running server, so promising a restart
    // would be wrong -- and would teach users to ignore the notice.
    const { controller } = await initializeController({ initialConfig: baseConfig });
    await controller.openSettingsModal();

    const hosts = document.getElementById("settings-allowed-hosts");
    hosts.value = "albis.lab";
    hosts.dispatchEvent(new Event("input", { bubbles: true }));

    expect(noteHidden()).toBe(true);
  });

  it("clears the notice once the change has been saved", async () => {
    const { controller } = await initializeController({ initialConfig: baseConfig });
    await controller.openSettingsModal();

    const port = document.getElementById("settings-server-port");
    port.value = "9001";
    port.dispatchEvent(new Event("input", { bubbles: true }));
    expect(noteHidden()).toBe(false);

    await controller.saveSettingsFromModal();

    expect(noteHidden()).toBe(true);
  });
});

describe("settings controller environment overrides", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("says which settings the environment decides", async () => {
    const { controller, locale } = await initializeController({
      initialConfig: { data: { root: "/mnt/beamline" } },
      envOverrides: ["data.root", "ui.language"],
    });

    await controller.openSettingsModal();

    const note = document.getElementById("settings-env-note");
    expect(note.classList.contains("is-hidden")).toBe(false);
    expect(note.getAttribute("aria-hidden")).toBe("false");
    expect(note.textContent).toContain("data.root");
    expect(note.textContent).toContain("ui.language");
    expect(note.textContent).toContain(
      locale["settings.env_override_note"].replace(" {{keys}}", "").replace("{{keys}}", "").trim(),
    );
  });

  it("disables the fields it cannot change, and only those", async () => {
    const { controller } = await initializeController({
      initialConfig: { data: { root: "/mnt/beamline" } },
      envOverrides: ["data.root", "server.port"],
    });

    await controller.openSettingsModal();

    expect(document.getElementById("settings-data-root").disabled).toBe(true);
    expect(document.getElementById("settings-server-port").disabled).toBe(true);
    expect(document.getElementById("settings-log-dir").disabled).toBe(false);
    expect(document.getElementById("settings-max-upload").disabled).toBe(false);
  });

  it("says nothing and locks nothing when the environment is silent", async () => {
    const { controller } = await initializeController({
      initialConfig: { data: { root: "" } },
      envOverrides: [],
    });

    await controller.openSettingsModal();

    const note = document.getElementById("settings-env-note");
    expect(note.classList.contains("is-hidden")).toBe(true);
    expect(note.textContent).toBe("");
    expect(document.getElementById("settings-data-root").disabled).toBe(false);
  });

  it("keeps the fields locked after a save", async () => {
    const { controller } = await initializeController({
      initialConfig: { data: { root: "/mnt/beamline" } },
      envOverrides: ["data.root"],
    });

    await controller.openSettingsModal();
    await controller.saveSettingsFromModal();

    expect(document.getElementById("settings-data-root").disabled).toBe(true);
    expect(document.getElementById("settings-env-note").classList.contains("is-hidden")).toBe(false);
  });
});
