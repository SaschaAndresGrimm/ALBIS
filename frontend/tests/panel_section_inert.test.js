import { beforeEach, describe, expect, it, vi } from "vitest";

import { createPanelLayoutController } from "../modules/panel_layout_controller.js";

/**
 * A collapsed panel section hides its content with `max-height: 0` and an
 * inherited `overflow: hidden` — no `display: none`, no `visibility: hidden` —
 * so every control inside stayed in the tab order. At the same time
 * `syncSectionA11y` set `aria-hidden="true"` on that wrapper, telling
 * assistive technology the controls were not there. Focus could therefore land
 * on a node a screen reader had been told to ignore, inside a zero-height
 * clipped region with nothing visible to indicate where focus had gone.
 *
 * Asserted through the attribute rather than the IDL property: jsdom accepts
 * `element.inert = true` but does not reflect it, so a property assertion
 * would pass whether or not anything reached the DOM.
 */

function build() {
  const section = document.createElement("section");
  section.className = "panel-section";
  section.dataset.section = "roi";

  const title = document.createElement("button");
  title.className = "section-title";
  section.appendChild(title);

  const content = document.createElement("div");
  content.className = "section-content";
  const inner = document.createElement("button");
  inner.textContent = "Reset ROI";
  content.appendChild(inner);
  section.appendChild(content);

  document.body.appendChild(section);

  const controller = createPanelLayoutController({
    state: {},
    constants: { mobilePanelSnapPoints: [0.4, 0.6, 0.9] },
    elements: {
      coarsePointerQuery: { matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() },
      toolsPanel: document.createElement("div"),
      appLayout: document.createElement("div"),
      panelBody: document.createElement("div"),
      panelFab: document.createElement("button"),
      panelCollapseBtn: document.createElement("button"),
      panelSheetHandle: document.createElement("div"),
      panelTabs: [],
      panelTabContents: [],
    },
    callbacks: {
      scheduleOverview: vi.fn(),
      scheduleHistogram: vi.fn(),
      schedulePixelOverlay: vi.fn(),
      getSectionStateStore: () => ({}),
      setSectionStateStore: vi.fn(),
      persistPanelWidth: vi.fn(),
      onPanelToggled: vi.fn(),
    },
  });

  return { controller, section, content, title, inner };
}

describe("a collapsed panel section is inert", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  it("marks the content inert when the section collapses", () => {
    const { controller, section, content } = build();

    controller.setSectionState(section, true, false);

    expect(content.hasAttribute("inert")).toBe(true);
    expect(content.getAttribute("aria-hidden")).toBe("true");
  });

  it("clears inert when the section expands again", () => {
    const { controller, section, content } = build();

    controller.setSectionState(section, true, false);
    controller.setSectionState(section, false, false);

    expect(content.hasAttribute("inert")).toBe(false);
    expect(content.getAttribute("aria-hidden")).toBe("false");
  });

  it("never leaves aria-hidden set on content that is still focusable", () => {
    // The pairing is the whole point: aria-hidden without inert is the state
    // that hid the controls from screen readers while leaving them tabbable.
    const { controller, section, content } = build();

    for (const collapsed of [true, false, true]) {
      controller.setSectionState(section, collapsed, false);
      const hidden = content.getAttribute("aria-hidden") === "true";
      expect(content.hasAttribute("inert")).toBe(hidden);
    }
  });
});
