import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock(dictionaries) {
  return vi.fn(async (url) => {
    const match = String(url).match(/locales\/([^/]+)\.json/);
    const language = match ? decodeURIComponent(match[1]) : "en";
    const payload = dictionaries[language] || {};
    return {
      ok: true,
      json: async () => payload,
    };
  });
}

async function loadI18nModule(dictionaries) {
  vi.resetModules();
  global.fetch = buildFetchMock(dictionaries);
  return import("../modules/i18n.js");
}

beforeEach(() => {
  document.body.innerHTML = "";
  localStorage.clear();
});

afterEach(() => {
  vi.restoreAllMocks();
  delete global.fetch;
});

describe("i18n runtime", () => {
  it("resolves keys with interpolation and plural tokens", async () => {
    const mod = await loadI18nModule({
      en: {
        "greet.user": "Hello {{name}}",
        "count.items": "{{count}} item{{plural:count||s}}",
      },
      "zh-CN": {},
      ja: {},
    });

    await mod.initializeI18n({ backendLanguage: "en" });

    expect(mod.t("greet.user", { name: "ALBIS" })).toBe("Hello ALBIS");
    expect(mod.t("count.items", { count: 1 })).toBe("1 item");
    expect(mod.t("count.items", { count: 3 })).toBe("3 items");
  });

  it("normalizes supported language aliases to canonical codes", async () => {
    const mod = await loadI18nModule({
      en: {},
      "zh-CN": {},
      ja: {},
      fr: {},
      es: {},
      it: {},
      pt: {},
      rm: {},
      de: {},
      sv: {},
      da: {},
      mi: {},
      gsw: {},
    });

    expect(mod.normalizeLanguage("fr-FR")).toBe("fr");
    expect(mod.normalizeLanguage("es-ES")).toBe("es");
    expect(mod.normalizeLanguage("it-IT")).toBe("it");
    expect(mod.normalizeLanguage("pt-BR")).toBe("pt");
    expect(mod.normalizeLanguage("pt-PT")).toBe("pt");
    expect(mod.normalizeLanguage("rm-CH")).toBe("rm");
    expect(mod.normalizeLanguage("de-DE")).toBe("de");
    expect(mod.normalizeLanguage("sv-SE")).toBe("sv");
    expect(mod.normalizeLanguage("da-DK")).toBe("da");
    expect(mod.normalizeLanguage("mi-NZ")).toBe("mi");
    expect(mod.normalizeLanguage("gsw-CH")).toBe("gsw");
  });

  it("uses fallback chain and warns once for missing keys", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const mod = await loadI18nModule({
      en: {
        "status.ready": "Ready",
      },
      "zh-CN": {},
      ja: {},
    });

    await mod.initializeI18n({ backendLanguage: "zh-CN" });

    expect(mod.getLanguage()).toBe("zh-CN");
    expect(mod.t("status.ready")).toBe("Ready");
    expect(mod.t("missing.key")).toBe("missing.key");
    expect(mod.t("missing.key")).toBe("missing.key");
    expect(warn).toHaveBeenCalledTimes(1);
  });

  it("applies data-i18n attributes and updates on language change", async () => {
    const mod = await loadI18nModule({
      en: {
        "label.greeting": "Hello",
        "label.hint": "Type value",
        "label.action": "Open panel",
        "label.title": "Panel control",
      },
      "zh-CN": {
        "label.greeting": "你好",
        "label.hint": "输入数值",
        "label.action": "打开面板",
        "label.title": "面板控件",
      },
      ja: {
        "label.greeting": "こんにちは",
        "label.hint": "値を入力",
        "label.action": "パネルを開く",
        "label.title": "パネルコントロール",
      },
    });

    document.body.innerHTML = `
      <div id="root">
        <span id="greeting" data-i18n="label.greeting"></span>
        <input id="field" data-i18n-placeholder="label.hint" />
        <button id="action" data-i18n-aria-label="label.action" data-i18n-title="label.title"></button>
      </div>
    `;

    await mod.initializeI18n({ backendLanguage: "en" });
    mod.applyI18nToDom(document);
    expect(document.getElementById("greeting")?.textContent).toBe("Hello");
    expect(document.getElementById("field")?.getAttribute("placeholder")).toBe("Type value");
    expect(document.getElementById("action")?.getAttribute("aria-label")).toBe("Open panel");
    expect(document.getElementById("action")?.getAttribute("title")).toBe("Panel control");

    const listener = vi.fn();
    mod.onLanguageChange(listener);
    mod.setLanguage("zh-CN", { persist: false, applyDom: true });

    expect(listener).toHaveBeenCalledWith("zh-CN");
    expect(document.getElementById("greeting")?.textContent).toBe("你好");
    expect(document.getElementById("field")?.getAttribute("placeholder")).toBe("输入数值");
    expect(document.getElementById("action")?.getAttribute("aria-label")).toBe("打开面板");
    expect(document.getElementById("action")?.getAttribute("title")).toBe("面板控件");
    expect(document.documentElement.lang).toBe("zh-CN");
  });

  it("resolves startup precedence localStorage > backend > browser", async () => {
    localStorage.setItem("albis.ui.language", "zh-CN");

    const mod = await loadI18nModule({ en: {}, "zh-CN": {}, ja: {} });
    await mod.initializeI18n({ backendLanguage: "en" });

    expect(mod.getLanguage()).toBe("zh-CN");
  });

  it("does not persist inferred startup language before an explicit preference exists", async () => {
    const mod = await loadI18nModule({
      en: {},
      "zh-CN": {},
      ja: {},
      fr: {},
      es: {},
      it: {},
      pt: {},
      rm: {},
      de: {},
      sv: {},
      da: {},
      mi: {},
      gsw: {},
    });

    await mod.initializeI18n();

    expect(mod.getLanguage()).toBe("en");
    expect(localStorage.getItem("albis.ui.language")).toBeNull();
    expect(mod.hasStoredLanguagePreference()).toBe(false);
  });
});
