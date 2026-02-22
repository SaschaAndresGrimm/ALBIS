export function applyPanelTab({
  tabId,
  panelTabs,
  panelTabContents,
  persist = true,
  persistKey = "albis.panelTab",
  onAfterChange = null,
}) {
  panelTabs.forEach((tab) => {
    const isActive = tab.dataset.panelTab === tabId;
    tab.classList.toggle("is-active", isActive);
    tab.setAttribute("aria-selected", isActive ? "true" : "false");
    tab.setAttribute("tabindex", isActive ? "0" : "-1");
  });
  panelTabContents.forEach((panel) => {
    const isActive = panel.dataset.panelTab === tabId;
    panel.classList.toggle("is-active", isActive);
    panel.setAttribute("aria-hidden", isActive ? "false" : "true");
  });
  if (persist) {
    try {
      localStorage.setItem(persistKey, tabId);
    } catch {
      // ignore storage errors
    }
  }
  if (typeof onAfterChange === "function") onAfterChange(tabId);
}

export function loadStoredPanelTab(persistKey = "albis.panelTab") {
  try {
    return localStorage.getItem(persistKey) || "";
  } catch {
    return "";
  }
}
