import fs from "node:fs";
import path from "node:path";

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const EN = JSON.parse(fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8"));

async function setup() {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  document.body.innerHTML = `
    <label class="field">
      <span class="field-label-row"><span>Normalization</span>
        <button type="button" class="info-tip" data-info-key="info.series.normalization"
          aria-label="More information"></button></span>
      <select><option>None</option></select>
    </label>
    <p id="elsewhere">elsewhere</p>`;
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createInfoTips } = await import("../modules/info_tips.js");
  const tips = createInfoTips();
  const button = document.querySelector(".info-tip");
  const bubble = () => document.getElementById("info-tip-bubble");
  return { tips, button, bubble };
}

describe("info tips", () => {
  beforeEach(() => {
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
    document.getElementById("info-tip-bubble")?.remove();
  });

  it("explains on hover, and goes away when the pointer leaves", async () => {
    const { button, bubble } = await setup();

    button.dispatchEvent(new Event("mouseenter"));
    expect(bubble().textContent).toBe(EN["info.series.normalization"]);
    expect(bubble().classList.contains("is-visible")).toBe(true);
    expect(button.getAttribute("aria-describedby")).toBe("info-tip-bubble");

    button.dispatchEvent(new Event("mouseleave"));
    expect(bubble().classList.contains("is-visible")).toBe(false);
  });

  it("stays after a click or tap until dismissed", async () => {
    const { button, bubble } = await setup();

    button.click();
    button.dispatchEvent(new Event("mouseleave"));
    expect(bubble().classList.contains("is-visible")).toBe(true);

    document.getElementById("elsewhere").click();
    expect(bubble().classList.contains("is-visible")).toBe(false);
  });

  it("closes on Escape", async () => {
    const { button, bubble } = await setup();
    button.click();

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));

    expect(bubble().classList.contains("is-visible")).toBe(false);
    expect(button.getAttribute("aria-expanded")).toBe("false");
  });

  it("does not open the control its label belongs to", async () => {
    const { button } = await setup();
    const select = document.querySelector("select");
    const focus = vi.spyOn(select, "focus");

    const event = new MouseEvent("click", { bubbles: true, cancelable: true });
    button.dispatchEvent(event);

    expect(event.defaultPrevented).toBe(true);
    expect(focus).not.toHaveBeenCalled();
  });

  it("has an explanation for every info key the page uses, in every language", () => {
    const html = fs.readFileSync(path.join(process.cwd(), "frontend", "index.html"), "utf8");
    const keys = [...html.matchAll(/data-info-key="([^"]+)"/g)].map((match) => match[1]);
    expect(keys.length).toBeGreaterThanOrEqual(5);
    const locales = fs.readdirSync(path.join(process.cwd(), "frontend", "locales"));
    for (const file of locales) {
      const dict = JSON.parse(fs.readFileSync(path.join(process.cwd(), "frontend", "locales", file), "utf8"));
      for (const key of keys) expect(dict[key], `${file} ${key}`).toBeTruthy();
    }
  });
  it("adds an untranslated detail, and stays open when a live panel rebuilds its button", async () => {
    const { tips, button, bubble } = await setup();
    button.dataset.infoDetail = "SIMPLON: detector/config/count_time";
    button.click();
    expect(bubble().textContent).toBe(`${EN["info.series.normalization"]}\n\nSIMPLON: detector/config/count_time`);
    // The row is rebuilt: same explanation, new button.
    const twin = button.cloneNode(true);
    delete twin.dataset.infoBound;
    button.replaceWith(twin);
    tips.refresh();
    expect(bubble().classList.contains("is-visible")).toBe(true);
    expect(twin.getAttribute("aria-expanded")).toBe("true");
    // Pinned still: a click elsewhere closes it.
    document.getElementById("elsewhere").click();
    expect(bubble().classList.contains("is-visible")).toBe(false);
  });
});
