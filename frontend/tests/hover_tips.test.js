import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { createHoverTips } from "../modules/hover_tips.js";

/**
 * One controller for the file: init() attaches its listeners to `document`,
 * and those outlive `document.body.innerHTML = ""`.
 */

let tip;
let tips;

beforeAll(() => {
  tips = createHoverTips({ delayMs: 500, warmMs: 400 });
  tips.init();
  tip = document.getElementById("help-tooltip");
  if (!tip) throw new Error("init() did not create #help-tooltip");
});

beforeEach(() => {
  vi.useFakeTimers();
  // Past the warm window of whatever the previous test showed.
  vi.advanceTimersByTime(5000);
  document.body.querySelectorAll(":scope > :not(#help-tooltip)").forEach((el) => el.remove());
});

afterEach(() => {
  tips.hide();
  vi.useRealTimers();
});

function add(html) {
  const wrap = document.createElement("div");
  wrap.innerHTML = html;
  document.body.appendChild(wrap);
  return wrap.firstElementChild;
}

function over(el) {
  el.dispatchEvent(new MouseEvent("mouseover", { bubbles: true }));
}

function leave(el) {
  el.dispatchEvent(new MouseEvent("mouseout", { bubbles: true, relatedTarget: document.body }));
}

const visible = () => tip.classList.contains("is-visible");
const text = () => tip.querySelector(".help-tooltip-text").textContent;
const key = () => tip.querySelector(".help-tooltip-key");

describe("what the tooltip shows", () => {
  it("names an icon-only button after half a second, with its shortcut", () => {
    const button = add('<button aria-label="Previous frame" aria-keyshortcuts="ArrowLeft">◀</button>');
    over(button);
    vi.advanceTimersByTime(499);
    expect(visible()).toBe(false);
    vi.advanceTimersByTime(1);
    expect(visible()).toBe(true);
    expect(text()).toBe("Previous frame");
    expect(key().textContent).toBe("←");
    expect(key().hidden).toBe(false);
  });

  it("shows nothing over a labelled dropdown", () => {
    // The screenshot that retired the old hints: "Operation" over "Sum".
    const label = add('<label class="field"><span>Operation</span><select id="op"><option>Sum</option></select></label>');
    over(label.querySelector("select"));
    vi.advanceTimersByTime(2000);
    expect(visible()).toBe(false);
  });

  it("shows nothing over a button or checkbox that says it already", () => {
    const button = add('<button aria-label="Toggle full screen">Full screen</button>');
    const check = add('<label class="checkbox"><input type="checkbox" /><span>Apply mask</span></label>');
    over(button);
    vi.advanceTimersByTime(2000);
    expect(visible()).toBe(false);
    over(check.querySelector("input"));
    vi.advanceTimersByTime(2000);
    expect(visible()).toBe(false);
  });

  it("always says why a control is unavailable, without its shortcut", () => {
    const button = add(
      '<button class="is-disabled" aria-disabled="true" data-help-reason="Draw an ROI first" aria-keyshortcuts="E" title="Export CSV">Export CSV</button>',
    );
    over(button);
    vi.advanceTimersByTime(500);
    expect(text()).toBe("Draw an ROI first");
    expect(key().hidden).toBe(true);
  });

  it("leaves the ? buttons to their own bubble", () => {
    const info = add('<button class="info-tip" aria-label="More information"></button>');
    over(info);
    vi.advanceTimersByTime(2000);
    expect(visible()).toBe(false);
  });

  it("shows the next control at once while moving along a toolbar", () => {
    const first = add('<button aria-label="Previous frame">◀</button>');
    const second = add('<button aria-label="Next frame">▶</button>');
    over(first);
    vi.advanceTimersByTime(500);
    over(second);
    expect(visible()).toBe(true);
    expect(text()).toBe("Next frame");
  });

  it("goes when the pointer leaves, and on Escape", () => {
    const button = add('<button aria-label="Play or pause">⏯</button>');
    over(button);
    vi.advanceTimersByTime(500);
    leave(button);
    expect(visible()).toBe(false);

    vi.advanceTimersByTime(5000);
    over(button);
    vi.advanceTimersByTime(500);
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    expect(visible()).toBe(false);
  });

  it("does not open on touch", () => {
    const button = add('<button aria-label="Play or pause">⏯</button>');
    const event = new MouseEvent("pointerover", { bubbles: true });
    Object.defineProperty(event, "pointerType", { value: "touch" });
    button.dispatchEvent(event);
    vi.advanceTimersByTime(2000);
    expect(visible()).toBe(false);
  });

  it("opens at once on keyboard focus but not on a click", () => {
    const button = add('<button aria-label="Full screen" aria-keyshortcuts="F">⛶</button>');
    document.dispatchEvent(new MouseEvent("pointerdown", { bubbles: true }));
    button.focus();
    expect(visible()).toBe(false);
    button.blur();

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Tab", bubbles: true }));
    button.focus();
    expect(visible()).toBe(true);
    expect(text()).toBe("Full screen");
  });
});

describe("the browser's own title tooltip", () => {
  it("is taken away on hover, before the browser can paint it, and put back", () => {
    const fab = add('<button title="Open side menu">Open menu ▾</button>');
    over(fab);
    expect(fab.hasAttribute("title")).toBe(false);
    vi.advanceTimersByTime(500);
    expect(text()).toBe("Open side menu");
    leave(fab);
    expect(fab.getAttribute("title")).toBe("Open side menu");
  });

  it("goes back to a control the pointer moved away from without leaving it", () => {
    const fab = add('<button title="Open side menu">Open menu ▾</button>');
    const other = add('<button title="Browse folder">…</button>');
    over(fab);
    over(other);
    expect(fab.getAttribute("title")).toBe("Open side menu");
    expect(other.hasAttribute("title")).toBe(false);
  });

  it("does not overwrite a title re-set during the hover", () => {
    const fab = add('<button title="Open side menu">Open menu ▾</button>');
    over(fab);
    fab.setAttribute("title", "Seitenmenü öffnen");
    leave(fab);
    expect(fab.getAttribute("title")).toBe("Seitenmenü öffnen");
    expect(fab.dataset.tipNativeTitle).toBeUndefined();
  });
});

describe("assistive technology", () => {
  it("names the tooltip as the description only while it is shown, and keeps one already there", () => {
    add('<p id="extra">More</p>');
    const button = add('<button aria-label="Next frame" aria-describedby="extra">▶</button>');
    over(button);
    expect(button.getAttribute("aria-describedby")).toBe("extra");
    vi.advanceTimersByTime(500);
    expect(button.getAttribute("aria-describedby")).toBe("extra help-tooltip");
    leave(button);
    expect(button.getAttribute("aria-describedby")).toBe("extra");
  });
});

describe("a language change", () => {
  it("re-reads the open tooltip's control", () => {
    const button = add('<button aria-label="Next frame">▶</button>');
    over(button);
    vi.advanceTimersByTime(500);
    button.setAttribute("aria-label", "Nächstes Bild");
    tips.refresh();
    expect(text()).toBe("Nächstes Bild");
  });
});
