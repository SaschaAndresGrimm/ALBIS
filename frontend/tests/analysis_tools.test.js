import { afterEach, describe, expect, it, vi } from "vitest";

function renderTab() {
  document.body.innerHTML = `
    <section class="panel-section" data-section="roi"></section>
    <section class="panel-section" data-section="resolution-rings"><input id="rings-toggle" type="checkbox" checked /></section>
    <section class="panel-section" data-section="peak-finder"><input id="peaks-enable" type="checkbox" /></section>
    <section class="panel-section" data-section="flatfield"><input id="flat-toggle" type="checkbox" checked /></section>
    <section class="panel-section" data-section="inpainting"><input id="inpaint-toggle" type="checkbox" /></section>
    <section class="panel-section" data-section="series-summing"></section>
    <div id="hidden-line" hidden><span id="hidden-text"></span><button id="customise">Customise…</button></div>
  `;
}

async function setup(hiddenTools) {
  vi.resetModules();
  globalThis.fetch = vi.fn(async () => ({ ok: true, json: async () => ({ "tools.hidden_summary": "Hidden tools: {{count}}" }) }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const mod = await import("../modules/analysis_tools.js");
  renderTab();
  const state = {};
  const openToolSettings = vi.fn();
  const controller = mod.createAnalysisToolsController({
    state,
    elements: {
      summary: document.getElementById("hidden-line"),
      summaryText: document.getElementById("hidden-text"),
      customiseBtn: document.getElementById("customise"),
    },
    callbacks: { openToolSettings },
  });
  if (hiddenTools) controller.apply(hiddenTools);
  return { mod, state, controller, openToolSettings };
}

const section = (name) => document.querySelector(`[data-section="${name}"]`);

describe("analysis tools", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete globalThis.fetch;
  });

  it("keeps only known tools, in a stable order", async () => {
    const { mod } = await setup();
    expect(mod.normalizeHiddenTools(["combine", "ROI", "rings", "rings"])).toEqual(["rings", "combine"]);
    expect(mod.normalizeHiddenTools("peaks, flatfield")).toEqual(["peaks", "flatfield"]);
    expect(mod.normalizeHiddenTools(null)).toEqual([]);
  });

  it("hides the sections of hidden tools and never Statistics and ROI", async () => {
    const { state } = await setup(["rings", "flatfield", "roi"]);
    expect(state.hiddenTools).toEqual(["rings", "flatfield"]);
    expect(section("resolution-rings").hidden).toBe(true);
    expect(section("flatfield").hidden).toBe(true);
    expect(section("roi").hidden).toBe(false);
    expect(section("peak-finder").hidden).toBe(false);
  });

  it("switches a hidden tool off through its own switch", async () => {
    renderTab();
    const changes = [];
    const { controller } = await setup();
    document.getElementById("flat-toggle").addEventListener("change", (e) => changes.push(e.target.checked));
    controller.apply(["flatfield"]);
    expect(document.getElementById("flat-toggle").checked).toBe(false);
    expect(changes).toEqual([false]);
    // Showing it again does not switch it back on.
    controller.apply([]);
    expect(section("flatfield").hidden).toBe(false);
    expect(document.getElementById("flat-toggle").checked).toBe(false);
  });

  it("says how many tools are hidden and opens Settings from there", async () => {
    const { controller, openToolSettings } = await setup(["rings", "peaks"]);
    const line = document.getElementById("hidden-line");
    expect(line.hidden).toBe(false);
    expect(document.getElementById("hidden-text").textContent).toBe("Hidden tools: 2");
    document.getElementById("customise").click();
    expect(openToolSettings).toHaveBeenCalled();
    controller.apply([]);
    expect(line.hidden).toBe(true);
  });

  it("drops Combine Frames commands from the palette with the tool, but keeps Cancel", async () => {
    const { buildCommandPaletteCommands } = await import("../modules/command_palette_commands.js");
    const actions = new Proxy({}, { get: () => vi.fn() });
    const build = (hiddenTools) =>
      buildCommandPaletteCommands({
        state: {
          file: "",
          hasFrame: false,
          seriesFiles: [],
          dataset: "",
          autoload: { mode: "file" },
          thresholdCount: 1,
          seriesSum: { running: false, jobId: "", openTarget: "out.tiff" },
          frameCount: 1,
          hiddenTools,
        },
        panelTabState: "view",
        platformShortcutLabel: () => "",
        isHdfFile: () => false,
        getThresholdIndexAtOffset: () => 0,
        actions,
      })
        .filter((command) => command.when === undefined || command.when)
        .map((command) => command.id);
    expect(build([])).toContain("series-open-output");
    expect(build(["combine"])).not.toContain("series-open-output");
    expect(build(["combine"])).toContain("customise-tools");
  });
});
