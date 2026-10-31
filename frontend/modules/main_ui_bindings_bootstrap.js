/**
 * Main UI binding bootstrap orchestration.
 */

import { bindMenuAndGlobalInteractions } from "./menu_bindings.js";
import { bindInspectorInteractions } from "./inspector_bindings.js";
import { bindFileIngress } from "./file_ingress_bindings.js";
import { bindChromeUiInteractions } from "./chrome_bindings.js";
import { bindDataControlInteractions } from "./data_controls_bindings.js";

export function initializeMainUiBindings({
  state,
  elements,
  callbacks,
  core,
}) {
  const {
    menuButtons,
    submenuParents,
    dropdown,
    menuActions,
    inspectorTree,
    inspectorSearchInput,
    inspectorSearchClear,
    inspectorResults,
    inspectorStateEl,
    fileInput,
    canvasShell,
    aboutClose,
    aboutModal,
    settingsClose,
    settingsCancel,
    settingsSave,
    settingsModal,
    commandInput,
    commandModal,
    fileSelect,
    datasetSelect,
    thresholdSelect,
    toolbarThresholdSelect,
    frameRange,
    frameIndex,
    frameStep,
    fpsSelect,
  } = elements;

  const {
    applyPlatformShortcutLabels,
    cancelClose,
    scheduleClose,
    isCoarsePointerDevice,
    openMenu,
    closeMenu,
    closeSubmenus,
    closeToolbarPlaybackPopover,
    closeToolbarMorePopover,
    closeFooterVersionPopover,
    registerChromeActivity,
    isMenuActive,
    trapModalFocus,
    isCommandPaletteOpen,
    handleCommandPaletteKeydown,
    closeTopModal,
    getTopOpenModal,
    handleNavShortcut,
    handleMenuAction,
    handleShortcut,
    selectInspectorRow,
    renderInspectorLink,
    setSectionBadgeState,
    renderSkeletonBlock,
    renderInspectorTree,
    clearInspectorSearch,
    isDocumentDropEnabled,
    showDocumentDropDisabledStatus,
    closeAboutModal,
    closeSettingsModal,
    setCommandPaletteIndex,
    renderCommandPalette,
    isHdfFile,
    loadDatasets,
    recordRecentFile,
    loadImageSeries,
    requestFrame,
    setFrameStep,
    setFps,
  } = callbacks;

  applyPlatformShortcutLabels();

  bindMenuAndGlobalInteractions({
    menuButtons,
    submenuParents,
    dropdown,
    menuActions,
    callbacks: {
      cancelClose,
      scheduleClose,
      isCoarsePointerDevice,
      openMenu,
      closeMenu,
      closeSubmenus,
      closeToolbarPlaybackPopover,
      closeToolbarMorePopover,
      closeFooterVersionPopover,
      registerChromeActivity,
      isMenuActive,
      trapModalFocus,
      isCommandPaletteOpen,
      handleCommandPaletteKeydown,
      closeTopModal,
      getTopOpenModal,
      handleNavShortcut,
      handleMenuAction,
      handleShortcut,
    },
  });

  bindInspectorInteractions({
    core,
    inspectorTree,
    inspectorSearchInput,
    inspectorSearchClear,
    inspectorResults,
    inspectorStateEl,
    callbacks: {
      selectInspectorRow,
      renderInspectorLink,
      setSectionBadgeState,
      renderSkeletonBlock,
      renderInspectorTree,
      clearInspectorSearch,
    },
  });

  bindFileIngress({
    fileInput,
    canvasShell,
    onFilesSelected: (selectedFiles) => core.uploadFlow.uploadAndOpenSelectedFiles(selectedFiles),
    allowDocumentDrop: typeof isDocumentDropEnabled === "function" ? isDocumentDropEnabled() : true,
    onDocumentDropDisabled: showDocumentDropDisabledStatus,
  });

  bindChromeUiInteractions({
    core,
    elements: {
      aboutClose,
      aboutModal,
      settingsClose,
      settingsCancel,
      settingsSave,
      settingsModal,
      commandInput,
      commandModal,
    },
    callbacks: {
      closeAboutModal,
      closeSettingsModal,
      setCommandPaletteIndex,
      renderCommandPalette,
    },
  });

  bindDataControlInteractions({
    core,
    state,
    elements: {
      fileSelect,
      datasetSelect,
      thresholdSelect,
      toolbarThresholdSelect,
      frameRange,
      frameIndex,
      frameStep,
      fpsSelect,
    },
    callbacks: {
      isHdfFile,
      loadDatasets,
      loadImageSeries,
      requestFrame,
      setFrameStep,
      closeToolbarPlaybackPopover,
      setFps,
      recordRecentFile,
    },
  });
}
