export function applyPanelTab({
  tabId,
  panelTabs,
  panelTabContents,
  persist = true,
  persistKey = "albis.panelTab",
  onAfterChange = null,
}) {
  panelTabs.forEach((tab) => {
    tab.classList.toggle("is-active", tab.dataset.panelTab === tabId);
  });
  panelTabContents.forEach((panel) => {
    panel.classList.toggle("is-active", panel.dataset.panelTab === tabId);
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
