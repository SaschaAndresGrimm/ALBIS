import { readFileSync } from "node:fs";
import path from "node:path";

import { beforeAll, describe, expect, it } from "vitest";

// Parse the real index.html so the ARIA menu contract is verified against what
// actually ships, not a fixture. Vitest runs from the repo root.
let doc;

beforeAll(() => {
  const indexPath = path.join(process.cwd(), "frontend", "index.html");
  const html = readFileSync(indexPath, "utf8");
  doc = new DOMParser().parseFromString(html, "text/html");
});

describe("menu ARIA structure", () => {
  it("exposes the menu bar as a labelled menubar", () => {
    const menubar = doc.querySelector('[role="menubar"]');
    expect(menubar).not.toBeNull();
    expect(menubar.getAttribute("aria-label")).toBeTruthy();
  });

  it("wires each menu button as a menuitem with a popup that points at a real menu", () => {
    const buttons = doc.querySelectorAll(".menu-item[data-menu]");
    expect(buttons.length).toBeGreaterThan(0);
    buttons.forEach((btn) => {
      expect(btn.getAttribute("role")).toBe("menuitem");
      expect(btn.getAttribute("aria-haspopup")).toBe("true");
      expect(btn.getAttribute("aria-expanded")).toBe("false"); // collapsed at rest
      const controlled = doc.getElementById(btn.getAttribute("aria-controls"));
      expect(controlled, `aria-controls target for ${btn.dataset.menu}`).not.toBeNull();
      expect(controlled.getAttribute("role")).toBe("menu");
    });
  });

  it("marks every dropdown panel as a labelled menu", () => {
    const panels = doc.querySelectorAll(".dropdown-panel");
    expect(panels.length).toBeGreaterThan(0);
    panels.forEach((panel) => {
      expect(panel.getAttribute("role")).toBe("menu");
      expect(panel.getAttribute("aria-label")).toBeTruthy();
    });
  });

  it("marks every dropdown item (including the submenu parent) as a menuitem", () => {
    const items = doc.querySelectorAll(".dropdown-item");
    expect(items.length).toBeGreaterThan(0);
    items.forEach((item) => {
      expect(item.getAttribute("role")).toBe("menuitem");
    });
  });

  it("exposes the submenu parent as a focusable popup and its container as a menu", () => {
    const parent = doc.querySelector(".dropdown-submenu-parent");
    expect(parent.getAttribute("role")).toBe("menuitem");
    expect(parent.getAttribute("aria-haspopup")).toBe("true");
    expect(parent.getAttribute("aria-expanded")).toBe("false");
    expect(parent.getAttribute("tabindex")).toBe("0");

    const submenu = parent.querySelector(".dropdown-submenu");
    expect(submenu.getAttribute("role")).toBe("menu");
    expect(submenu.getAttribute("aria-label")).toBeTruthy();
  });
});
