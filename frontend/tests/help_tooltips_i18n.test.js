import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fs from "node:fs";

function readLocale(language) {
  const url = new URL(`../locales/${language}.json`, import.meta.url);
  return JSON.parse(fs.readFileSync(url, "utf8"));
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
});
