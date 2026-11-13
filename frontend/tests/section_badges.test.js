import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock(dictionaries) {
  return vi.fn(async (url) => {
    const match = String(url).match(/locales\/([^/]+)\.json/);
    const language = match ? decodeURIComponent(match[1]) : "en";
    return { ok: true, json: async () => dictionaries[language] || {} };
  });
}

const DICTIONARIES = {
  en: {
    "data_source.choose_image_source_to_begin": "Choose an image source to begin.",
    "status.data.image_loaded": "Image loaded",
    "summary.off": "Off",
  },
  de: {
    "data_source.choose_image_source_to_begin": "Bildquelle wählen, um zu beginnen.",
    "status.data.image_loaded": "Bild geladen",
    "summary.off": "Aus",
  },
};

async function loadModules() {
  vi.resetModules();
  global.fetch = buildFetchMock(DICTIONARIES);
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const badges = await import("../modules/section_badges.js");
  return { i18n, ...badges };
}

describe("section badges", () => {
  beforeEach(() => {
    // As in index.html: the start-up text comes with its own key.
    document.body.innerHTML = `
      <span id="state" class="is-empty" data-i18n="data_source.choose_image_source_to_begin">Choose an image source to begin.</span>
      <span id="chip" data-i18n="summary.off">Off</span>
    `;
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("keeps a state set as text when the page is translated again", async () => {
    const { i18n, setSectionBadgeState } = await loadModules();
    const badge = document.getElementById("state");

    setSectionBadgeState(badge, "active", "Image loaded");
    i18n.applyI18nToDom(document);

    // Before: the start-up key put "Choose an image source to begin." back
    // while the badge stayed blue.
    expect(badge.textContent).toBe("Image loaded");
    expect(badge.classList.contains("is-active")).toBe(true);
    expect(badge.classList.contains("is-empty")).toBe(false);
  });

  it("follows a language switch when the state is given as a key", async () => {
    const { i18n, setSectionBadgeState } = await loadModules();
    const badge = document.getElementById("state");

    setSectionBadgeState(badge, "active", { key: "status.data.image_loaded" });
    expect(badge.textContent).toBe("Image loaded");

    await i18n.setLanguage("de", { persist: false, applyDom: true });
    expect(badge.textContent).toBe("Bild geladen");
  });

  it("falls back to the empty tone for an unknown one", async () => {
    const { setSectionBadgeState } = await loadModules();
    const badge = document.getElementById("state");

    setSectionBadgeState(badge, "active", "x");
    setSectionBadgeState(badge, "bogus", "y");

    expect([...badge.classList]).toEqual(["is-empty"]);
  });

  it("keeps a summary chip's text and tone through a re-translation", async () => {
    const { i18n, setSummaryChip } = await loadModules();
    const chip = document.getElementById("chip");

    setSummaryChip(chip, "3 rings", "active");
    i18n.applyI18nToDom(document);

    expect(chip.textContent).toBe("3 rings");
    expect(chip.classList.contains("is-active")).toBe(true);

    setSummaryChip(chip, { key: "summary.off" }, "off");
    expect(chip.textContent).toBe("Off");
    expect(chip.classList.contains("is-active")).toBe(false);
    expect(chip.classList.contains("is-off")).toBe(true);
  });
});
