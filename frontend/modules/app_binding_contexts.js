/**
 * Context factories for app-level binding/bootstrap orchestration.
 */

/**
 * Build the exact element map expected by `initializeMainUiBindings`.
 * Keep this key set aligned with `frontend/modules/main_ui_bindings_bootstrap.js`.
 */
export function createMainUiBindingsElements(elements) {
  return {
    menuButtons: elements.menuButtons,
    submenuParents: elements.submenuParents,
    dropdown: elements.dropdown,
    menuActions: elements.menuActions,
    inspectorTree: elements.inspectorTree,
    inspectorSearchInput: elements.inspectorSearchInput,
    inspectorSearchClear: elements.inspectorSearchClear,
    inspectorResults: elements.inspectorResults,
    inspectorStateEl: elements.inspectorStateEl,
    fileInput: elements.fileInput,
    canvasShell: elements.canvasShell,
    aboutClose: elements.aboutClose,
    aboutModal: elements.aboutModal,
    settingsClose: elements.settingsClose,
    settingsCancel: elements.settingsCancel,
    settingsSave: elements.settingsSave,
    settingsSaveClose: elements.settingsSaveClose,
    settingsModal: elements.settingsModal,
    commandInput: elements.commandInput,
    commandModal: elements.commandModal,
    fileSelect: elements.fileSelect,
    datasetSelect: elements.datasetSelect,
    thresholdSelect: elements.thresholdSelect,
    toolbarThresholdSelect: elements.toolbarThresholdSelect,
    frameRange: elements.frameRange,
    frameIndex: elements.frameIndex,
    frameStep: elements.frameStep,
    fpsSelect: elements.fpsSelect,
  };
}

/**
 * Build the exact callback map expected by `initializeMainUiBindings`.
 * This factory intentionally performs explicit key mapping so missing callbacks
 * are visible during refactors instead of being silently spread through.
 */
export function createMainUiBindingsCallbacks(callbacks) {
  return {
    applyPlatformShortcutLabels: callbacks.applyPlatformShortcutLabels,
    cancelClose: callbacks.cancelClose,
    scheduleClose: callbacks.scheduleClose,
    isCoarsePointerDevice: callbacks.isCoarsePointerDevice,
    openMenu: callbacks.openMenu,
    closeMenu: callbacks.closeMenu,
    closeSubmenus: callbacks.closeSubmenus,
    closeToolbarPlaybackPopover: callbacks.closeToolbarPlaybackPopover,
    closeToolbarMorePopover: callbacks.closeToolbarMorePopover,
    closeFooterVersionPopover: callbacks.closeFooterVersionPopover,
    registerChromeActivity: callbacks.registerChromeActivity,
    trapModalFocus: callbacks.trapModalFocus,
    isCommandPaletteOpen: callbacks.isCommandPaletteOpen,
    handleCommandPaletteKeydown: callbacks.handleCommandPaletteKeydown,
    closeTopModal: callbacks.closeTopModal,
    getTopOpenModal: callbacks.getTopOpenModal,
    handleNavShortcut: callbacks.handleNavShortcut,
    handleMenuAction: callbacks.handleMenuAction,
    handleShortcut: callbacks.handleShortcut,
    selectInspectorRow: callbacks.selectInspectorRow,
    renderInspectorLink: callbacks.renderInspectorLink,
    setSectionBadgeState: callbacks.setSectionBadgeState,
    renderSkeletonBlock: callbacks.renderSkeletonBlock,
    fetchInspectorTree: callbacks.fetchInspectorTree,
    renderInspectorTree: callbacks.renderInspectorTree,
    showInspectorNode: callbacks.showInspectorNode,
    clearInspectorSearch: callbacks.clearInspectorSearch,
    runInspectorSearch: callbacks.runInspectorSearch,
    uploadAndOpenSelectedFiles: callbacks.uploadAndOpenSelectedFiles,
    isDocumentDropEnabled: callbacks.isDocumentDropEnabled,
    showDocumentDropDisabledStatus: callbacks.showDocumentDropDisabledStatus,
    closeAboutModal: callbacks.closeAboutModal,
    closeSettingsModal: callbacks.closeSettingsModal,
    saveSettingsFromModal: callbacks.saveSettingsFromModal,
    renderCommandPalette: callbacks.renderCommandPalette,
    closeCommandPalette: callbacks.closeCommandPalette,
    ensureFileMode: callbacks.ensureFileMode,
    syncSeriesSumOutputPath: callbacks.syncSeriesSumOutputPath,
    stopPlayback: callbacks.stopPlayback,
    isHdfFile: callbacks.isHdfFile,
    loadDatasets: callbacks.loadDatasets,
    loadImageSeries: callbacks.loadImageSeries,
    loadMetadata: callbacks.loadMetadata,
    setThresholdIndex: callbacks.setThresholdIndex,
    requestFrame: callbacks.requestFrame,
    setFrameStep: callbacks.setFrameStep,
    setFps: callbacks.setFps,
  };
}

/**
 * Build the exact element map expected by `initializePostFilePickerBindings`.
 * Keep this key set aligned with `frontend/modules/post_file_picker_bindings.js`.
 */
export function createPostFilePickerBindingsElements(elements) {
  return {
    autoloadMode: elements.autoloadMode,
    autoloadWatchEnabled: elements.autoloadWatchEnabled,
    autoloadDir: elements.autoloadDir,
    autoloadInterval: elements.autoloadInterval,
    remoteIntervalInput: elements.remoteIntervalInput,
    jfjochIntervalInput: elements.jfjochIntervalInput,
    remoteSourceInput: elements.remoteSourceInput,
    jfjochSourceInput: elements.jfjochSourceInput,
    jfjochEndpointInput: elements.jfjochEndpointInput,
    jfjochTopicInput: elements.jfjochTopicInput,
    jfjochChannelInput: elements.jfjochChannelInput,
    autoloadTypeHdf5: elements.autoloadTypeHdf5,
    autoloadTypeTiff: elements.autoloadTypeTiff,
    autoloadTypeCbf: elements.autoloadTypeCbf,
    autoloadPattern: elements.autoloadPattern,
    autoloadBrowse: elements.autoloadBrowse,
    autoloadSelectFile: elements.autoloadSelectFile,
    filesystemMode: elements.filesystemMode,
    simplonUrl: elements.simplonUrl,
    simplonVersion: elements.simplonVersion,
    simplonTimeout: elements.simplonTimeout,
    simplonEnable: elements.simplonEnable,
    colormapSelect: elements.colormapSelect,
    autoScaleToggle: elements.autoScaleToggle,
    minInput: elements.minInput,
    maxInput: elements.maxInput,
    maskToggle: elements.maskToggle,
    maskSaturatedToggle: elements.maskSaturatedToggle,
    autoContrastBtn: elements.autoContrastBtn,
    invertToggle: elements.invertToggle,
    histLogX: elements.histLogX,
    histLogY: elements.histLogY,
    zoomRange: elements.zoomRange,
    zoomValue: elements.zoomValue,
    resetView: elements.resetView,
    prevBtn: elements.prevBtn,
    nextBtn: elements.nextBtn,
    playBtn: elements.playBtn,
    toolbarPlaybackToggle: elements.toolbarPlaybackToggle,
    toolbarMoreToggle: elements.toolbarMoreToggle,
    toolbarMoreStep: elements.toolbarMoreStep,
    toolbarMoreFps: elements.toolbarMoreFps,
    toolbarMoreThreshold: elements.toolbarMoreThreshold,
    toolbarMorePanelToggle: elements.toolbarMorePanelToggle,
    toolbarMoreFullscreen: elements.toolbarMoreFullscreen,
    fullscreenToggle: elements.fullscreenToggle,
    splashOpenFileBtn: elements.splashOpenFileBtn,
    footerVersionToggleEl: elements.footerVersionToggleEl,
    panelFab: elements.panelFab,
    panelCollapseBtn: elements.panelCollapseBtn,
    panelSheetHandle: elements.panelSheetHandle,
    canvasWrap: elements.canvasWrap,
    roiEnableToggle: elements.roiEnableToggle,
    roiModeSelect: elements.roiModeSelect,
    roiHistogramToggle: elements.roiHistogramToggle,
    roiHistBinsAuto: elements.roiHistBinsAuto,
    roiHistBinCount: elements.roiHistBinCount,
    roiHistBinChip: elements.roiHistBinChip,
    roiHistBinManualRow: elements.roiHistBinManualRow,
    roiHistBinPresetBtns: elements.roiHistBinPresetBtns,
    roiClearBtn: elements.roiClearBtn,
    roiExportCsvBtn: elements.roiExportCsvBtn,
    roiRadiusInput: elements.roiRadiusInput,
    roiInnerInput: elements.roiInnerInput,
    roiOuterInput: elements.roiOuterInput,
    roiCenterXInput: elements.roiCenterXInput,
    roiCenterYInput: elements.roiCenterYInput,
    roiCenterSnapBtn: elements.roiCenterSnapBtn,
    panelTabs: elements.panelTabs,
    sectionToggles: elements.sectionToggles,
    sectionSwitches: elements.sectionSwitches,
    panelResizer: elements.panelResizer,
    appLayout: elements.appLayout,
    toolsPanel: elements.toolsPanel,
    roiLineCanvas: elements.roiLineCanvas,
    roiXCanvas: elements.roiXCanvas,
    roiYCanvas: elements.roiYCanvas,
    roiHistCanvas: elements.roiHistCanvas,
    roiLinePlot: elements.roiLinePlot,
    roiBoxPlotX: elements.roiBoxPlotX,
    roiBoxPlotY: elements.roiBoxPlotY,
    roiHistogramPlot: elements.roiHistogramPlot,
    overviewCanvas: elements.overviewCanvas,
    histCanvas: elements.histCanvas,
    exportBtn: elements.exportBtn,
  };
}

/**
 * Build the exact callback map expected by `initializePostFilePickerBindings`.
 * Includes state-ref callbacks (`setRoiDragging`, section-state getters/setters)
 * because post-picker bindings consume them as part of the callback contract.
 */
export function createPostFilePickerBindingsCallbacks(callbacks) {
  return {
    stopAutoload: callbacks.stopAutoload,
    startAutoload: callbacks.startAutoload,
    updateAutoloadUI: callbacks.updateAutoloadUI,
    persistAutoloadSettings: callbacks.persistAutoloadSettings,
    loadFiles: callbacks.loadFiles,
    autoloadTick: callbacks.autoloadTick,
    updateRemoteMetaUI: callbacks.updateRemoteMetaUI,
    updateJfjochMetaUI: callbacks.updateJfjochMetaUI,
    schedulePeakOverlay: callbacks.schedulePeakOverlay,
    setSimplonMode: callbacks.setSimplonMode,
    setAutoloadStatus: callbacks.setAutoloadStatus,
    openFileBrowser: callbacks.openFileBrowser,
    openFileModal: callbacks.openFileModal,
    handleLocalFileSelection: callbacks.handleLocalFileSelection,
    redraw: callbacks.redraw,
    scheduleHistogram: callbacks.scheduleHistogram,
    handleContrastChanged: callbacks.handleContrastChanged,
    computeAutoLevels: callbacks.computeAutoLevels,
    formatValue: callbacks.formatValue,
    updateGlobalStats: callbacks.updateGlobalStats,
    scheduleRoiUpdate: callbacks.scheduleRoiUpdate,
    handleRoiChanged: callbacks.handleRoiChanged,
    redrawRoiPlots: callbacks.redrawRoiPlots,
    schedulePeakFinder: callbacks.schedulePeakFinder,
    chooseHistogramBins: callbacks.chooseHistogramBins,
    computeHistogram: callbacks.computeHistogram,
    snapHistogramValue: callbacks.snapHistogramValue,
    deferViewportInteraction: callbacks.deferViewportInteraction,
    setZoom: callbacks.setZoom,
    scheduleOverview: callbacks.scheduleOverview,
    zoomAt: callbacks.zoomAt,
    fitImageToView: callbacks.fitImageToView,
    stopPlayback: callbacks.stopPlayback,
    requestFrame: callbacks.requestFrame,
    startPlayback: callbacks.startPlayback,
    toggleToolbarPlaybackPopover: callbacks.toggleToolbarPlaybackPopover,
    toggleToolbarMorePopover: callbacks.toggleToolbarMorePopover,
    setFrameStep: callbacks.setFrameStep,
    setFps: callbacks.setFps,
    setThresholdIndex: callbacks.setThresholdIndex,
    togglePanel: callbacks.togglePanel,
    closeToolbarMorePopover: callbacks.closeToolbarMorePopover,
    toggleFullscreen: callbacks.toggleFullscreen,
    toggleFooterVersionPopover: callbacks.toggleFooterVersionPopover,
    registerChromeActivity: callbacks.registerChromeActivity,
    updateFullscreenUi: callbacks.updateFullscreenUi,
    startMobilePanelDrag: callbacks.startMobilePanelDrag,
    updateMobilePanelDrag: callbacks.updateMobilePanelDrag,
    stopMobilePanelDrag: callbacks.stopMobilePanelDrag,
    stopRoiEdit: callbacks.stopRoiEdit,
    updateRoiModeUI: callbacks.updateRoiModeUI,
    scheduleRoiOverlay: callbacks.scheduleRoiOverlay,
    clearRoi: callbacks.clearRoi,
    setStatus: callbacks.setStatus,
    exportRoiCsv: callbacks.exportRoiCsv,
    applyRoiCenterFromInputs: callbacks.applyRoiCenterFromInputs,
    updateRoiCenterInputs: callbacks.updateRoiCenterInputs,
    getRingParams: callbacks.getRingParams,
    initializeSectionContentWrappers: callbacks.initializeSectionContentWrappers,
    initializePanelTabA11y: callbacks.initializePanelTabA11y,
    setPanelTab: callbacks.setPanelTab,
    toggleSection: callbacks.toggleSection,
    loadStoredPanelTab: callbacks.loadStoredPanelTab,
    setSectionState: callbacks.setSectionState,
    applyPanelState: callbacks.applyPanelState,
    setPanelWidth: callbacks.setPanelWidth,
    normalizeWheelDelta: callbacks.normalizeWheelDelta,
    queueWheelZoom: callbacks.queueWheelZoom,
    schedulePixelOverlay: callbacks.schedulePixelOverlay,
    scheduleResolutionOverlay: callbacks.scheduleResolutionOverlay,
    startTouchGesture: callbacks.startTouchGesture,
    updateTouchGesture: callbacks.updateTouchGesture,
    stopTouchGesture: callbacks.stopTouchGesture,
    isTouchGestureActive: callbacks.isTouchGestureActive,
    getEffectiveScrollLeft: callbacks.getEffectiveScrollLeft,
    getEffectiveScrollTop: callbacks.getEffectiveScrollTop,
    setEffectiveScroll: callbacks.setEffectiveScroll,
    getImagePointFromEvent: callbacks.getImagePointFromEvent,
    getRoiHandleAt: callbacks.getRoiHandleAt,
    isPointInRoi: callbacks.isPointInRoi,
    startRoiEdit: callbacks.startRoiEdit,
    updateCursorOverlay: callbacks.updateCursorOverlay,
    isRoiEditing: callbacks.isRoiEditing,
    applyRoiEdit: callbacks.applyRoiEdit,
    getRingHandleAt: callbacks.getRingHandleAt,
    startRingEdit: callbacks.startRingEdit,
    applyRingEdit: callbacks.applyRingEdit,
    stopRingEdit: callbacks.stopRingEdit,
    isRingEditing: callbacks.isRingEditing,
    updateRingHover: callbacks.updateRingHover,
    clearRingHover: callbacks.clearRingHover,
    hideCursorOverlay: callbacks.hideCursorOverlay,
    getMinZoom: callbacks.getMinZoom,
    updateRoiTooltip: callbacks.updateRoiTooltip,
    hideRoiTooltip: callbacks.hideRoiTooltip,
    getRoiPlotKey: callbacks.getRoiPlotKey,
    getRoiPlotLimits: callbacks.getRoiPlotLimits,
    getRoiPlotLog: callbacks.getRoiPlotLog,
    setRoiPlotLog: callbacks.setRoiPlotLog,
    setRoiPlotAxisLimits: callbacks.setRoiPlotAxisLimits,
    syncRoiPlotLimitControls: callbacks.syncRoiPlotLimitControls,
    clearRoiPlotLimitsForKey: callbacks.clearRoiPlotLimitsForKey,
    hasManualRoiPlotLimits: callbacks.hasManualRoiPlotLimits,
    hasAnyManualRoiPlotLimits: callbacks.hasAnyManualRoiPlotLimits,
    overviewEventToImage: callbacks.overviewEventToImage,
    overviewEventToOverview: callbacks.overviewEventToOverview,
    getViewRect: callbacks.getViewRect,
    getOverviewHandleAt: callbacks.getOverviewHandleAt,
    getAnchorForHandle: callbacks.getAnchorForHandle,
    resizeViewFromHandle: callbacks.resizeViewFromHandle,
    panToImageCenter: callbacks.panToImageCenter,
    histogramValueToX: callbacks.histogramValueToX,
    histogramXToValue: callbacks.histogramXToValue,
    getHistTooltipPosition: callbacks.getHistTooltipPosition,
    showHistTooltip: callbacks.showHistTooltip,
    hideHistTooltip: callbacks.hideHistTooltip,
    exportFullImage: callbacks.exportFullImage,
    updateToolbar: callbacks.updateToolbar,
    drawHistogram: callbacks.drawHistogram,
    drawSplash: callbacks.drawSplash,
    isHdfFile: callbacks.isHdfFile,
    loadDatasets: callbacks.loadDatasets,
    loadImageSeries: callbacks.loadImageSeries,
    loadMetadata: callbacks.loadMetadata,
    setRoiDragging: callbacks.setRoiDragging,
    getRoiDragging: callbacks.getRoiDragging,
    getSectionStateStore: callbacks.getSectionStateStore,
    setSectionStateStore: callbacks.setSectionStateStore,
  };
}

/**
 * Build the argument object for `createFileBrowserController`.
 *
 * `onPathSelected` behavior is centralized here to keep `app.js` compact while
 * preserving the exact autoload/series-sum side effects expected by the UI.
 */
export function createFileBrowserControllerContext({
  apiBase,
  elements,
  callbacks,
}) {
  return {
    apiBase,
    browseModal: elements.browseModal,
    browseTitle: elements.browseTitle,
    browseBreadcrumb: elements.browseBreadcrumb,
    browseUpBtn: elements.browseUp,
    browseSearchInput: elements.browseSearchInput,
    browseSearchClearBtn: elements.browseSearchClear,
    browseFormatField: elements.browseFormatField,
    browseFormatSelect: elements.browseFormat,
    browseSortSelect: elements.browseSort,
    browseSeriesModeSelect: elements.browseSeriesMode,
    browseSeriesField: elements.browseSeriesField,
    browseViewModeSelect: elements.browseViewMode,
    browseViewField: elements.browseViewField,
    browseContent: elements.browseContent,
    browseSplitter: elements.browseSplitter,
    browseFoldersList: elements.browseFoldersList,
    browseFilesList: elements.browseFilesList,
    browsePathInput: elements.browsePathInput,
    browseStatus: elements.browseStatus,
    browseSelectBtn: elements.browseSelectBtn,
    browseCancelBtn: elements.browseCancelBtn,
    browseCloseBtn: elements.browseCloseBtn,
    filesystemModeEl: elements.filesystemMode,
    openModal: callbacks.openModal,
    closeModal: callbacks.closeModal,
    setStatus: callbacks.setStatus,
    onPathSelected: ({ mode, selectedPath }) => {
      if (mode === "autoload") {
        if (elements.autoloadDir) elements.autoloadDir.value = selectedPath;
        const autoloadState = callbacks.getAutoloadState();
        autoloadState.dir = selectedPath;
        callbacks.persistAutoloadSettings();
        if (autoloadState.mode === "file") {
          callbacks.loadFiles().catch((err) => console.error(err));
        }
        if (autoloadState.running && autoloadState.mode === "file" && autoloadState.watchEnabled) {
          callbacks.autoloadTick();
        }
      } else if (mode === "series-sum") {
        const picked = selectedPath.replace(/[\\/]$/, "");
        if (elements.seriesSumOutput) {
          elements.seriesSumOutput.value = picked ? `${picked}/series_sum` : "series_sum";
        }
      }
    },
  };
}

/**
 * Compose main-ui binding context with derived helpers that depend on runtime state.
 */
export function createMainUiBindingsContext({
  state,
  elements,
  callbacks,
  activeMenuRef,
  commandPaletteController,
}) {
  return {
    state,
    elements,
    callbacks: {
      ...callbacks,
      isMenuActive: (menuId) => activeMenuRef() === menuId,
      setCommandPaletteIndex: (next) => {
        commandPaletteController.setIndex(next);
      },
    },
  };
}

/**
 * Compose post-picker binding context and attach state-ref helpers.
 *
 * Backward compatibility:
 * - accepts explicit `stateRefs`
 * - falls back to state-ref callbacks already present on `callbacks`
 */
export function createPostFilePickerBindingsContext({
  apiBase,
  state,
  analysisState,
  roiState,
  backendIsLocal,
  overviewState,
  constants,
  elements,
  callbacks,
  stateRefs,
}) {
  const resolvedStateRefs = stateRefs || {
    setRoiDragging: callbacks.setRoiDragging,
    getRoiDragging: callbacks.getRoiDragging,
    getSectionStateStore: callbacks.getSectionStateStore,
    setSectionStateStore: callbacks.setSectionStateStore,
  };

  const {
    setRoiDragging,
    getRoiDragging,
    getSectionStateStore,
    setSectionStateStore,
  } = resolvedStateRefs;

  return {
    apiBase,
    state,
    analysisState,
    roiState,
    backendIsLocal,
    overviewState,
    constants,
    elements,
    callbacks: {
      ...callbacks,
      setRoiDragging,
      getRoiDragging,
      getSectionStateStore,
      setSectionStateStore,
    },
  };
}

/**
 * Compose runtime-bootstrap context and inject mutable state helpers.
 */
export function createRuntimeBootstrapContext({
  state,
  callbacks,
  stateRefs,
}) {
  return {
    state,
    callbacks: {
      ...callbacks,
      setMobilePanelSnap: stateRefs.setMobilePanelSnap,
    },
  };
}
