/**
 * Context factories for app-level binding/bootstrap orchestration.
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
  const {
    setRoiDragging,
    getRoiDragging,
    getSectionStateStore,
    setSectionStateStore,
  } = stateRefs;

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
