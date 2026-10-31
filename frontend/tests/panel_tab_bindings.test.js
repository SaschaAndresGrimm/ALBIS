import { describe, expect, it, vi } from "vitest";
import { bindPanelAndSectionInteractions } from "../modules/panel_tab_bindings.js";

function setup({ detectorHidden }) {
  document.body.innerHTML = `
    <div role="tablist">
      <button class="panel-tab" data-panel-tab="view"></button>
      <button class="panel-tab" data-panel-tab="data"></button>
      <button class="panel-tab" data-panel-tab="analysis"></button>
      <button class="panel-tab" data-panel-tab="detector" ${detectorHidden ? "hidden" : ""}></button>
    </div>`;
  const panelTabs = document.querySelectorAll(".panel-tab");
  const setPanelTab = vi.fn();
  bindPanelAndSectionInteractions({
    panelTabs,
    sectionToggles: [],
    sectionSwitches: [],
    callbacks: {
      initializeSectionContentWrappers: () => {},
      initializePanelTabA11y: () => {},
      setPanelTab,
      toggleSection: () => {},
      loadStoredPanelTab: () => "",
      getSectionStateStore: () => ({}),
      setSectionStateStore: () => {},
    },
    core: {
      panelLayout: { setSectionState: () => {} },
    },
  });
  setPanelTab.mockClear();
  const press = (tab, key) => tab.dispatchEvent(new KeyboardEvent("keydown", { key, bubbles: true }));
  return { tabs: Array.from(panelTabs), setPanelTab, press };
}

describe("panel tab arrow keys", () => {
  it("skip the Detector tab while it is hidden", () => {
    const { tabs, setPanelTab, press } = setup({ detectorHidden: true });
    press(tabs[2], "ArrowRight");
    expect(setPanelTab).toHaveBeenLastCalledWith("view");
    press(tabs[0], "ArrowLeft");
    expect(setPanelTab).toHaveBeenLastCalledWith("analysis");
    press(tabs[0], "End");
    expect(setPanelTab).toHaveBeenLastCalledWith("analysis");
  });

  it("reach the Detector tab once it is shown", () => {
    const { tabs, setPanelTab, press } = setup({ detectorHidden: false });
    press(tabs[2], "ArrowRight");
    expect(setPanelTab).toHaveBeenLastCalledWith("detector");
  });
});
