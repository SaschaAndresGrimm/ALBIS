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
    fetchInspectorTree,
    renderInspectorTree,
    showInspectorNode,
    clearInspectorSearch,
    runInspectorSearch,
    uploadAndOpenSelectedFiles,
    isDocumentDropEnabled,
    showDocumentDropDisabledStatus,
    closeAboutModal,
    closeSettingsModal,
    saveSettingsFromModal,
    setCommandPaletteIndex,
    renderCommandPalette,
    closeCommandPalette,
    ensureFileMode,
    syncSeriesSumOutputPath,
    stopPlayback,
    isHdfFile,
    loadDatasets,
    recordRecentFile,
    loadImageSeries,
    loadMetadata,
    setThresholdIndex,
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
      fetchInspectorTree,
      renderInspectorTree,
      showInspectorNode,
      clearInspectorSearch,
      runInspectorSearch,
    },
  });

  bindFileIngress({
    fileInput,
    canvasShell,
    onFilesSelected: uploadAndOpenSelectedFiles,
    allowDocumentDrop: typeof isDocumentDropEnabled === "function" ? isDocumentDropEnabled() : true,
    onDocumentDropDisabled: showDocumentDropDisabledStatus,
  });

  bindChromeUiInteractions({
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
      saveSettingsFromModal,
      setCommandPaletteIndex,
      renderCommandPalette,
      closeCommandPalette,
    },
  });

  bindDataControlInteractions({
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
      ensureFileMode,
      syncSeriesSumOutputPath,
      stopPlayback,
      isHdfFile,
      loadDatasets,
      loadImageSeries,
      loadMetadata,
      setThresholdIndex,
      requestFrame,
      setFrameStep,
      closeToolbarPlaybackPopover,
      setFps,
      recordRecentFile,
    },
  });
}
