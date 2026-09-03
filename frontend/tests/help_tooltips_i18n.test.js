import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";
import path from "node:path";

function readLocale(language) {
  const filePath = path.join(process.cwd(), "frontend", "locales", `${language}.json`);
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

function buildFetchMock(dictionaries) {
  return vi.fn(async (url) => {
    const match = String(url).match(/locales\/([^/]+)\.json/);
    const language = match ? decodeURIComponent(match[1]) : "en";
    return {
      ok: true,
      json: async () => dictionaries[language] || {},
    };
  });
}

describe("help tooltip i18n refresh", () => {
  beforeEach(() => {
    document.body.innerHTML = `
      <button id="btn-prev">◀</button>
      <div class="dropdown-item" data-action="command-palette">
        <span>Command Palette…</span>
      </div>
    `;
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
    document.querySelectorAll(".help-tooltip").forEach((el) => el.remove());
  });

  it("updates managed help strings after language change", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: readLocale("en"),
      "zh-CN": readLocale("zh-CN"),
      ja: readLocale("ja"),
    });

    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });

    const { createHelpTooltipController } = await import("../modules/help_tooltips.js");
    const state = { toolHintsEnabled: true };
    const controller = createHelpTooltipController({
      state,
      platformShortcutLabel: () => "⌘K",
      roiCanvases: [],
    });

    controller.initHelpTooltips();

    const prevButton = document.getElementById("btn-prev");
    const commandPaletteItem = document.querySelector('.dropdown-item[data-action="command-palette"]');

    expect(prevButton?.dataset.help).toBe(readLocale("en")["hint.frame.previous"]);
    expect(commandPaletteItem?.dataset.help).toBe("Command Palette (⌘K)");

    i18n.setLanguage("ja", { persist: false, applyDom: false });
    controller.refreshHelpTooltips();

    expect(prevButton?.dataset.help).toBe(readLocale("ja")["hint.frame.previous"]);
    expect(commandPaletteItem?.dataset.help).toBe(readLocale("ja")["hint.command.palette_shortcut"].replace("{{shortcut}}", "⌘K"));
  });

  it("keeps an unavailability reason that a managed hint would otherwise bury", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({ en: readLocale("en") });

    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });

    const { createHelpTooltipController } = await import("../modules/help_tooltips.js");
    const { setControlAvailability } = await import("../modules/control_availability.js");

    // A control that carries a managed hint of its own, which is what buried
    // the reason when it was written to `title`: data-help outranks title, and
    // applyHelpMap then strips title from anything it maps.
    document.body.innerHTML = '<button id="roi-export-csv">Export CSV</button>';
    const button = document.getElementById("roi-export-csv");
    setControlAvailability(button, "No ROI data to export");

    const controller = createHelpTooltipController({
      state: { toolHintsEnabled: true },
      platformShortcutLabel: () => "",
      roiCanvases: [],
    });
    controller.initHelpTooltips();
    controller.refreshHelpTooltips();

    expect(button.dataset.help).toBe(readLocale("en")["hint.roi.export_csv"]);
    expect(button.dataset.helpReason).toBe("No ROI data to export");
    // Marked unavailable, but still able to be hovered and clicked.
    expect(button.classList.contains("is-disabled")).toBe(true);
    expect(button.disabled).toBe(false);

    setControlAvailability(button, "");
    expect(button.dataset.helpReason).toBeUndefined();
    expect(button.hasAttribute("aria-disabled")).toBe(false);
  });
});
