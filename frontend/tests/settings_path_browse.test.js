import { afterEach, describe, expect, it, vi } from "vitest";

/**
 * The Browse buttons beside the settings path fields.
 *
 * Typing a path into a settings field means getting it exactly right with no
 * feedback until the next restart, and these two decide where ALBIS reads data
 * from and writes logs to. They use the operating system's own folder chooser,
 * the same `/api/choose-folder` the image and export paths already use.
 */

async function mount({ chooseFolder }) {
  vi.resetModules();
  document.body.innerHTML = `
    <div id="settings-modal">
      <div class="inline">
        <input id="settings-data-root" type="text" value="/old/data" />
        <button id="settings-data-root-browse" type="button">Browse</button>
      </div>
      <div class="inline">
        <input id="settings-log-dir" type="text" value="/old/logs" />
        <button id="settings-log-dir-browse" type="button">Browse</button>
      </div>
    </div>`;

  const catalogue = JSON.parse(
    // The real English catalogue, so a renamed status key fails here.
    require("node:fs").readFileSync("frontend/locales/en.json", "utf8")
  );
  global.fetch = vi.fn(async (url) => {
    if (String(url).includes("/choose-folder")) return chooseFolder();
    return { ok: true, json: async () => catalogue };
  });

  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createSettingsController } = await import("../modules/settings_controller.js");

  const setStatus = vi.fn();
  const dataRoot = document.getElementById("settings-data-root");
  const logDir = document.getElementById("settings-log-dir");
  createSettingsController({
    apiBase: "/api",
    state: {},
    constants: { pixelLabelDefaultMinCellPx: 18, pixelLabelDefaultMaxLabels: 4000 },
    elements: {
      settingsDataRoot: dataRoot,
      settingsLogDir: logDir,
      settingsDataRootBrowse: document.getElementById("settings-data-root-browse"),
      settingsLogDirBrowse: document.getElementById("settings-log-dir-browse"),
    },
    callbacks: { setStatus },
  });
  return { dataRoot, logDir, setStatus, catalogue };
}

const picked = (path) => ({ status: 200, ok: true, json: async () => ({ path }) });
const cancelled = () => ({ status: 204, ok: true, json: async () => ({}) });

async function click(id) {
  document.getElementById(id).click();
  // Let the fetch promise settle.
  await new Promise((resolve) => setTimeout(resolve, 0));
}

describe("settings path Browse buttons", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
    document.body.innerHTML = "";
  });

  it("puts the chosen folder in the data root field", async () => {
    const { dataRoot } = await mount({ chooseFolder: () => picked("/beamline/data") });
    await click("settings-data-root-browse");
    expect(dataRoot.value).toBe("/beamline/data");
  });

  it("puts the chosen folder in the log directory field", async () => {
    const { logDir } = await mount({ chooseFolder: () => picked("/var/log/albis") });
    await click("settings-log-dir-browse");
    expect(logDir.value).toBe("/var/log/albis");
  });

  it("does not cross the two fields", async () => {
    const { dataRoot, logDir } = await mount({ chooseFolder: () => picked("/picked") });
    await click("settings-log-dir-browse");
    expect(logDir.value).toBe("/picked");
    expect(dataRoot.value).toBe("/old/data");
  });

  it.each([
    ["/data/runs/", "/data/runs"],
    ["/data/runs//", "/data/runs"],
    ["C:\\\\data\\\\runs\\\\", "C:\\\\data\\\\runs"],
    ["/data/runs", "/data/runs"],
    ["/", ""],
  ])("strips the trailing separator from %s", async (chosen, expected) => {
    // The other pickers store paths without one; two spellings of the same
    // folder in one config is a needless way to look inconsistent.
    const { dataRoot } = await mount({ chooseFolder: () => picked(chosen) });
    await click("settings-data-root-browse");
    expect(dataRoot.value).toBe(expected);
  });

  it("leaves the field alone when the chooser is dismissed", async () => {
    const { dataRoot, setStatus } = await mount({ chooseFolder: cancelled });
    await click("settings-data-root-browse");
    // 204 is a dismissal, not a failure: no change and no complaint.
    expect(dataRoot.value).toBe("/old/data");
    expect(setStatus).not.toHaveBeenCalled();
  });

  it("leaves the field alone when the chooser returns nothing", async () => {
    const { dataRoot } = await mount({ chooseFolder: () => picked("") });
    await click("settings-data-root-browse");
    expect(dataRoot.value).toBe("/old/data");
  });

  it("says so when the chooser cannot be opened", async () => {
    const { dataRoot, setStatus, catalogue } = await mount({
      chooseFolder: () => ({ status: 500, ok: false, json: async () => ({}) }),
    });
    await click("settings-data-root-browse");
    expect(dataRoot.value).toBe("/old/data");
    expect(setStatus).toHaveBeenCalledWith(catalogue["status.settings.browse_failed"], {
      tone: "error",
    });
  });

  it("announces the change, since assigning a value fires nothing itself", async () => {
    const { dataRoot } = await mount({ chooseFolder: () => picked("/picked") });
    const seen = [];
    dataRoot.addEventListener("input", () => seen.push("input"));
    dataRoot.addEventListener("change", () => seen.push("change"));
    await click("settings-data-root-browse");
    expect(seen).toEqual(["input", "change"]);
  });

  it("does nothing for a disabled field", async () => {
    // An env-override locks a field; the button must respect that.
    const { dataRoot } = await mount({ chooseFolder: () => picked("/picked") });
    dataRoot.disabled = true;
    await click("settings-data-root-browse");
    expect(dataRoot.value).toBe("/old/data");
    expect(global.fetch).not.toHaveBeenCalledWith(expect.stringContaining("/choose-folder"));
  });
});
