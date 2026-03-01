/**
 * Side-panel layout, section state, and mobile drag interactions.
 */

export function createPanelLayoutController({
  state,
  constants,
  elements,
  callbacks,
}) {
  const {
    mobilePanelSnapPoints,
    initialMobilePanelSnap = 0.6,
  } = constants;

  const {
    coarsePointerQuery,
    toolsPanel,
    appLayout,
    panelBody,
    panelFab,
    panelCollapseBtn,
    panelSheetHandle,
    panelTabs,
    panelTabContents,
  } = elements;

  const {
    syncToolbarMoreControls,
    scheduleOverview,
    scheduleHistogram,
    schedulePixelOverlay,
    updateUiIdleAndAnchors,
    getSectionStateStore,
    setSectionStateStore,
  } = callbacks;

  let sectionA11yCounter = 0;
  let mobilePanelSnap = initialMobilePanelSnap;
  let mobilePanelDragActive = false;
  let mobilePanelDragPointer = null;
  let mobilePanelDragStartY = 0;
  let mobilePanelDragStartSnap = mobilePanelSnap;

  function getMaxPanelWidth() {
    return Math.max(220, Math.min(900, window.innerWidth - 24));
  }

  function isPhonePanelLayout() {
    return Boolean(coarsePointerQuery?.matches) && window.innerWidth < 768;
  }

  function clampMobilePanelSnap(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return mobilePanelSnapPoints[0];
    return Math.max(0.52, Math.min(1, numeric));
  }

  function nearestMobilePanelSnap(value) {
    const clamped = clampMobilePanelSnap(value);
    return mobilePanelSnapPoints.reduce((closest, candidate) =>
      Math.abs(candidate - clamped) < Math.abs(closest - clamped) ? candidate : closest
    );
  }

  function setMobilePanelSnap(value, snap = true, persist = false) {
    mobilePanelSnap = snap ? nearestMobilePanelSnap(value) : clampMobilePanelSnap(value);
    if (!toolsPanel) return;
    const heightPct = `${Math.round(mobilePanelSnap * 100)}vh`;
    toolsPanel.style.setProperty("--mobile-panel-height", heightPct);
    toolsPanel.dataset.mobileSnap = mobilePanelSnap >= 0.95 ? "full" : "mid";
    if (persist) {
      try {
        localStorage.setItem("albis.mobilePanelSnap", String(mobilePanelSnap));
      } catch {
        // ignore storage errors
      }
    }
  }

  function applyPanelState() {
    if (!toolsPanel || !appLayout) return;

    const isPhone = isPhonePanelLayout();
    document.body.classList.toggle("panel-collapsed", state.panelCollapsed);
    toolsPanel.classList.toggle("is-collapsed", state.panelCollapsed);
    toolsPanel.classList.toggle("is-mobile-sheet", isPhone);
    if (panelBody) {
      panelBody.setAttribute("aria-hidden", state.panelCollapsed ? "true" : "false");
    }

    if (isPhone) {
      setMobilePanelSnap(mobilePanelSnap, false);
      toolsPanel.classList.toggle("is-visible", !state.panelCollapsed);
      appLayout.style.removeProperty("--panel-width");
      document.documentElement.style.removeProperty("--panel-width");
    } else {
      toolsPanel.classList.remove("is-visible");
      toolsPanel.style.removeProperty("--mobile-panel-height");
      delete toolsPanel.dataset.mobileSnap;
      const maxPanelWidth = getMaxPanelWidth();
      const targetWidth = Math.max(220, Math.min(maxPanelWidth, state.panelWidth));
      const width = state.panelCollapsed ? 34 : targetWidth;
      appLayout.style.setProperty("--panel-width", `${width}px`);
      document.documentElement.style.setProperty("--panel-width", `${width}px`);
    }

    if (panelFab) {
      panelFab.classList.toggle("is-collapsed", state.panelCollapsed);
      panelFab.classList.toggle("is-open", !state.panelCollapsed);
      panelFab.dataset.state = state.panelCollapsed ? "collapsed" : "expanded";
      panelFab.textContent = state.panelCollapsed ? "Open menu ▾" : "Close menu ▴";
      panelFab.setAttribute("aria-label", state.panelCollapsed ? "Open panel" : "Collapse panel");
      panelFab.setAttribute("aria-expanded", state.panelCollapsed ? "false" : "true");
      panelFab.setAttribute("aria-keyshortcuts", "M");
      panelFab.title = state.panelCollapsed ? "Open side menu (M)" : "Close side menu (M)";
    }
    if (panelCollapseBtn) {
      panelCollapseBtn.disabled = state.panelCollapsed;
      panelCollapseBtn.setAttribute("aria-hidden", state.panelCollapsed ? "true" : "false");
      panelCollapseBtn.tabIndex = state.panelCollapsed ? -1 : 0;
    }
    syncToolbarMoreControls();
    scheduleOverview();
    scheduleHistogram();
    updateUiIdleAndAnchors();
  }

  function togglePanel() {
    const wasCollapsed = state.panelCollapsed;
    state.panelCollapsed = !state.panelCollapsed;
    if (isPhonePanelLayout() && wasCollapsed && !state.panelCollapsed) {
      setMobilePanelSnap(mobilePanelSnapPoints[0], true, true);
    }
    applyPanelState();
    try {
      localStorage.setItem("albis.panelCollapsed", String(state.panelCollapsed));
      localStorage.setItem("albis.panelWidth", String(state.panelWidth));
    } catch {
      // ignore storage errors
    }
  }

  function sanitizeIdFragment(value, fallbackPrefix = "section") {
    const normalized = String(value || "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9_-]+/g, "-")
      .replace(/^-+|-+$/g, "");
    if (normalized) return normalized;
    sectionA11yCounter += 1;
    return `${fallbackPrefix}-${sectionA11yCounter}`;
  }

  function syncSectionA11y(section) {
    if (!section) return;
    const title = section.querySelector(":scope > .section-title");
    const content = section.querySelector(":scope > .section-content");
    if (!title || !content) return;
    if (!content.id) {
      const sectionId = sanitizeIdFragment(section.dataset.section, "section");
      content.id = `section-content-${sectionId}`;
    }
    const collapsed = section.classList.contains("is-collapsed");
    title.setAttribute("aria-controls", content.id);
    title.setAttribute("aria-expanded", collapsed ? "false" : "true");
    content.setAttribute("aria-hidden", collapsed ? "true" : "false");
  }

  function initializePanelTabA11y() {
    document.querySelector(".panel-tabs[role='tablist']")?.setAttribute("aria-orientation", "horizontal");
    panelTabs.forEach((tab) => {
      tab.setAttribute("role", "tab");
      const tabId = sanitizeIdFragment(tab.dataset.panelTab, "panel-tab");
      if (!tab.id) {
        tab.id = `panel-tab-${tabId}`;
      }
      const panel = Array.from(panelTabContents).find((item) => item.dataset.panelTab === tab.dataset.panelTab);
      if (!panel) return;
      panel.setAttribute("role", "tabpanel");
      if (!panel.id) {
        panel.id = `panel-content-${tabId}`;
      }
      panel.setAttribute("aria-labelledby", tab.id);
      tab.setAttribute("aria-controls", panel.id);
    });
  }

  function toggleSection(event) {
    const button = event.currentTarget;
    const section = button.closest(".panel-section");
    if (!section) return;
    setSectionState(section, !section.classList.contains("is-collapsed"));
    scheduleOverview();
    scheduleHistogram();
    schedulePixelOverlay();
  }

  function setSectionState(section, collapsed, persist = true) {
    section.classList.toggle("is-collapsed", collapsed);
    syncSectionA11y(section);
    const id = section.dataset.section;
    if (persist && id) {
      const sectionStateStore = getSectionStateStore() || {};
      sectionStateStore[id] = collapsed;
      setSectionStateStore(sectionStateStore);
      try {
        localStorage.setItem("albis.sectionStates", JSON.stringify(sectionStateStore));
      } catch {
        // ignore storage errors
      }
    }
  }

  function initializeSectionContentWrappers() {
    const sections = document.querySelectorAll(".panel-section");
    sections.forEach((section) => {
      const title = section.querySelector(":scope > .section-title");
      if (!title) return;
      let wrapper = section.querySelector(":scope > .section-content");
      if (!wrapper) {
        wrapper = document.createElement("div");
        wrapper.className = "section-content";
        let node = title.nextSibling;
        while (node) {
          const next = node.nextSibling;
          wrapper.appendChild(node);
          node = next;
        }
        section.appendChild(wrapper);
      }
      syncSectionA11y(section);
    });
  }

  function setPanelWidth(width) {
    if (isPhonePanelLayout()) return;
    const maxPanelWidth = getMaxPanelWidth();
    const clamped = Math.max(220, Math.min(maxPanelWidth, Math.round(width)));
    state.panelWidth = clamped;
    state.panelCollapsed = false;
    applyPanelState();
    scheduleHistogram();
    try {
      localStorage.setItem("albis.panelWidth", String(state.panelWidth));
    } catch {
      // ignore storage errors
    }
  }

  function startMobilePanelDrag(event) {
    if (!isPhonePanelLayout() || state.panelCollapsed || !toolsPanel) return;
    mobilePanelDragActive = true;
    mobilePanelDragPointer = event.pointerId;
    mobilePanelDragStartY = event.clientY;
    mobilePanelDragStartSnap = mobilePanelSnap;
    toolsPanel.classList.add("is-dragging");
    if (panelSheetHandle?.setPointerCapture && Number.isInteger(event.pointerId)) {
      panelSheetHandle.setPointerCapture(event.pointerId);
    }
    event.preventDefault();
  }

  function updateMobilePanelDrag(event) {
    if (!mobilePanelDragActive) return;
    if (!Number.isInteger(mobilePanelDragPointer) || event.pointerId === mobilePanelDragPointer) {
      const viewportHeight = Math.max(320, window.innerHeight || 1);
      const delta = mobilePanelDragStartY - event.clientY;
      const next = mobilePanelDragStartSnap + delta / viewportHeight;
      setMobilePanelSnap(next, false);
      event.preventDefault();
    }
  }

  function stopMobilePanelDrag(event, cancelled = false) {
    if (!mobilePanelDragActive || !toolsPanel) return;
    if (!Number.isInteger(mobilePanelDragPointer) || event.pointerId === mobilePanelDragPointer || cancelled) {
      const dragDown = (event.clientY || mobilePanelDragStartY) - mobilePanelDragStartY;
      mobilePanelDragActive = false;
      mobilePanelDragPointer = null;
      toolsPanel.classList.remove("is-dragging");
      if (panelSheetHandle?.releasePointerCapture && Number.isInteger(event.pointerId)) {
        try {
          panelSheetHandle.releasePointerCapture(event.pointerId);
        } catch {
          // ignore release errors
        }
      }
      if (!cancelled && dragDown > 120 && mobilePanelSnap <= mobilePanelSnapPoints[0] + 0.05) {
        state.panelCollapsed = true;
        applyPanelState();
        try {
          localStorage.setItem("albis.panelCollapsed", String(state.panelCollapsed));
        } catch {
          // ignore storage errors
        }
        return;
      }
      setMobilePanelSnap(mobilePanelSnap, true, true);
      applyPanelState();
    }
  }

  return {
    getMaxPanelWidth,
    isPhonePanelLayout,
    nearestMobilePanelSnap,
    setMobilePanelSnap,
    applyPanelState,
    togglePanel,
    initializePanelTabA11y,
    toggleSection,
    setSectionState,
    initializeSectionContentWrappers,
    setPanelWidth,
    startMobilePanelDrag,
    updateMobilePanelDrag,
    stopMobilePanelDrag,
  };
}
