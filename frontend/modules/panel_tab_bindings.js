/**
 * Panel tab and collapsible section bindings.
 */

export function bindPanelAndSectionInteractions({
  panelTabs,
  sectionToggles,
  sectionSwitches,
  callbacks,
  core,
}) {
  const {
    initializeSectionContentWrappers,
    initializePanelTabA11y,
    setPanelTab,
    toggleSection,
    loadStoredPanelTab,
    getSectionStateStore,
    setSectionStateStore,
  } = callbacks;

  initializeSectionContentWrappers();
  initializePanelTabA11y();

  panelTabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      const target = tab.dataset.panelTab;
      if (!target) return;
      setPanelTab(target);
    });
    tab.addEventListener("keydown", (event) => {
      // Only the tabs on screen: the Detector tab stays hidden until a
      // detector is connected, and the arrows went on to its empty panel.
      const tabs = Array.from(panelTabs).filter((item) => !item.hidden);
      const currentIndex = tabs.indexOf(tab);
      if (currentIndex < 0) return;
      let nextIndex = null;
      if (event.key === "ArrowRight") {
        nextIndex = (currentIndex + 1) % tabs.length;
      } else if (event.key === "ArrowLeft") {
        nextIndex = (currentIndex - 1 + tabs.length) % tabs.length;
      } else if (event.key === "Home") {
        nextIndex = 0;
      } else if (event.key === "End") {
        nextIndex = tabs.length - 1;
      }
      if (nextIndex === null) return;
      event.preventDefault();
      const nextTab = tabs[nextIndex];
      const target = nextTab?.dataset.panelTab;
      if (!target) return;
      setPanelTab(target);
      nextTab.focus();
    });
  });

  sectionToggles.forEach((btn) => {
    btn.addEventListener("click", toggleSection);
  });

  sectionSwitches.forEach((toggle) => {
    ["mousedown", "click", "dblclick"].forEach((eventName) => {
      toggle.addEventListener(eventName, (event) => {
        event.stopPropagation();
      });
    });
  });

  let sectionStateStore = getSectionStateStore();
  try {
    const storedSections = localStorage.getItem("albis.sectionStates");
    if (storedSections) {
      sectionStateStore = JSON.parse(storedSections) || {};
    }
  } catch {
    sectionStateStore = {};
  }
  setSectionStateStore(sectionStateStore);

  sectionToggles.forEach((btn) => {
    const section = btn.closest(".panel-section");
    const id = section?.dataset.section;
    if (id && Object.prototype.hasOwnProperty.call(sectionStateStore, id)) {
      core.panelLayout?.setSectionState(section, Boolean(sectionStateStore[id]), false);
    }
  });

  const storedPanelTab = loadStoredPanelTab("albis.panelTab");
  if (storedPanelTab) {
    const normalized = storedPanelTab === "tools" ? "view" : storedPanelTab;
    setPanelTab(normalized, false);
  } else {
    setPanelTab("view", false);
  }
}
