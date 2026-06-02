export function createRoiState() {
  return {
    // Active ROI geometry and derived plot configuration.
    enabled: true,
    mode: "line",
    start: null,
    end: null,
    active: false,
    log: false,
    histogramEnabled: false,
    stats: null,
    lineProfile: null,
    xProjection: null,
    yProjection: null,
    histogramDistribution: null,
    innerRadius: 0,
    outerRadius: 0,
    plotLimits: {
      autoscale: true,
      line: { xMin: null, xMax: null, yMin: null, yMax: null },
      x: { xMin: null, xMax: null, yMin: null, yMax: null },
      y: { xMin: null, xMax: null, yMin: null, yMax: null },
      hist: { xMin: null, xMax: null, yMin: null, yMax: null },
    },
  };
}

export function createAnalysisState() {
  return {
    // Analysis overlays rendered on top of the current frame.
    ringsEnabled: false,
    distanceMm: null,
    pixelSizeUm: null,
    energyEv: null,
    centerX: null,
    centerY: null,
    ringMode: "planar",
    ringGeometry: null,
    ringGeometrySource: "",
    ringGeometryKey: "",
    geometryOverridePath: "",
    geometryOverrideScopeKey: "",
    geometryOverrideActive: false,
    geometryManualKey: "",
    geometryDistanceManual: false,
    geometryCenterXManual: false,
    geometryCenterYManual: false,
    rings: [1, 3.67, 11.01],
    ringCount: 3,
    peaksEnabled: false,
    peakCount: 25,
    peaks: [],
    externalPeakSets: [],
    selectedPeaks: [],
    peakSelectionAnchor: null,
  };
}

function createSourceState() {
  return {
    file: "",
    dataset: "",
    shape: [],
    dtype: "",
    frameCount: 1,
    frameIndex: 0,
    thresholdCount: 1,
    thresholdIndex: 0,
    thresholdEnergies: [],
    seriesFiles: [],
    seriesLabel: "",
    imageHeaderFile: "",
    imageHeaderText: "",
  };
}

function createUiPreferencesState() {
  return {
    backendAlive: false,
    backendVersion: "",
    language: "en",
    toolHintsEnabled: false,
    pixelLabelMinCellPx: 18,
    pixelLabelMaxLabels: 4000,
    pixelLabelFormat: "auto",
    pixelLabelShowDuringDrag: false,
    panelWidth: 640,
    panelCollapsed: true,
  };
}

function createTransientFrameLoadState() {
  return {
    isLoading: false,
    pendingFrame: null,
    playing: false,
    playTimer: null,
    fps: 1,
    step: 1,
  };
}

function createViewportState() {
  return {
    autoScale: true,
    min: 0,
    max: 1,
    colormap: "albulaHdr",
    invert: false,
    zoom: 1,
    renderOffsetX: 0,
    renderOffsetY: 0,
    panOffsetX: 0,
    panOffsetY: 0,
  };
}

function createFrameDataState() {
  return {
    dataRaw: null,
    dataFloat: null,
    histogram: null,
    stats: null,
    histLogX: true,
    histLogY: true,
    pixelLabels: true,
    hasFrame: false,
    width: 0,
    height: 0,
    globalStats: null,
  };
}

function createMaskState() {
  return {
    maskRaw: null,
    maskShape: null,
    maskAvailable: false,
    maskEnabled: false,
    maskSaturatedEnabled: true,
    maskAuto: true,
    maskFile: "",
    maskPath: "",
  };
}

function createAutoloadState() {
  return {
    mode: "file",
    watchEnabled: false,
    dir: "",
    interval: 1000,
    types: {
      hdf5: true,
      tiff: true,
      cbf: true,
      edf: true,
    },
    pattern: "",
    simplonUrl: "",
    simplonVersion: "1.8.0",
    simplonTimeout: 500,
    simplonEnable: true,
    remoteSourceId: "default",
    jfjochEndpoint: "",
    jfjochSourceId: "jungfraujoch",
    jfjochTopic: "",
    jfjochChannel: "",
    jfjochInterval: 250,
    remoteSeq: 0,
    remoteMeta: {},
    jfjochMeta: {},
    jfjochStatus: {},
    autoStart: false,
    running: false,
    timer: null,
    busy: false,
    lastFile: "",
    lastMtime: 0,
    lastUpdate: 0,
    lastPoll: 0,
    lastMonitorSig: "",
    lastRemoteSeq: 0,
    lastJfjochSeq: 0,
    lastMaskAttempt: 0,
    simplonMeta: {},
    historyEntries: [],
    historyCapacity: 8,
    historyCursor: 0,
    followingLatest: true,
    livePaused: false,
    pendingNewFrames: 0,
  };
}

function createSeriesSumState() {
  return {
    running: false,
    cancelling: false,
    jobId: "",
    progress: 0,
    message: "Idle",
    outputs: [],
    openTarget: "",
    autoOutputPath: "",
  };
}

function createDataExportState() {
  return {
    running: false,
    cancelling: false,
    jobId: "",
    progress: 0,
    message: "Idle",
    outputs: [],
    autoOutputDir: "",
    autoOutputPrefix: "",
  };
}

export function createAppState() {
  return {
    // Global view/data state used across renderer + UI controls.
    ...createSourceState(),
    ...createUiPreferencesState(),
    ...createTransientFrameLoadState(),
    ...createViewportState(),
    ...createFrameDataState(),
    ...createMaskState(),
    autoload: createAutoloadState(),
    seriesSum: createSeriesSumState(),
    dataExport: createDataExportState(),
  };
}
