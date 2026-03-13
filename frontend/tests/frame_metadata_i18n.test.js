import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

function buildFetchMock(dictionaries) {
  return vi.fn(async (url) => {
    const match = String(url).match(/locales\/([^/]+)\.json/);
    const language = match ? decodeURIComponent(match[1]) : "en";
    const payload = dictionaries[language] || {};
    return {
      ok: true,
      json: async () => payload,
    };
  });
}

describe("frame metadata i18n", () => {
  beforeEach(() => {
    document.body.innerHTML = `
      <input id="autoload-dir" value="" />
      <datalist id="autoload-dir-list"></datalist>
      <select id="file-select"></select>
    `;
    localStorage.clear();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("retranslates the file placeholder option on language change", async () => {
    vi.resetModules();
    global.fetch = buildFetchMock({
      en: {
        "files.select_placeholder": "Select a file",
        "status.files.loading": "Loading file list...",
        "status.frame.select_file_to_begin": "Select a file to begin",
      },
      ja: {
        "files.select_placeholder": "ファイルを選択",
        "status.files.loading": "ファイル一覧を読み込み中...",
        "status.frame.select_file_to_begin": "開始するファイルを選択",
      },
      rm: {
        "files.select_placeholder": "Tscherna ina datoteca",
        "status.files.loading": "Chargiar glista da datotecas...",
        "status.frame.select_file_to_begin": "Tscherna ina datoteca per cumenzar",
      },
      "zh-CN": {},
      fr: {},
      es: {},
      it: {},
      pt: {},
    });

    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "ja" });

    const { createFrameMetadataController } = await import("../modules/frame_metadata_controller.js");
    const fileSelect = document.getElementById("file-select");
    const autoloadDir = document.getElementById("autoload-dir");
    const autoloadDirList = document.getElementById("autoload-dir-list");

    const controller = createFrameMetadataController({
      apiBase: "/api",
      state: {
        autoload: { dir: "" },
        file: "",
        dataset: "",
      },
      analysisState: {},
      elements: {
        autoloadDir,
        autoloadDirList,
        fileSelect,
        metaShape: null,
        metaDtype: null,
        ringsDistance: null,
        ringsPixel: null,
        ringsEnergy: null,
        ringsCenterX: null,
        ringsCenterY: null,
        ringInputs: [],
      },
      callbacks: {
        fetchJSON: vi.fn(async (url) => {
          if (String(url).includes("/folders")) {
            return { folders: [] };
          }
          return { files: ["sample.h5"] };
        }),
        option: (label, value) => {
          const opt = document.createElement("option");
          opt.value = value;
          opt.textContent = label;
          return opt;
        },
        fileLabel: (value) => String(value || ""),
        setDataControlsForHdf5: () => {},
        setDataSourceSectionState: () => {},
        setStatus: () => {},
        updateToolbar: () => {},
        showSplash: () => {},
        setSplashStatus: () => {},
        setLoading: () => {},
        showProcessingProgress: () => {},
        hideProcessingProgress: () => {},
        getDefaultThresholdIndex: () => 0,
        syncSeriesSumOutputPath: () => {},
        updateFrameControls: () => {},
        updateThresholdOptions: () => {},
        loadMask: async () => {},
        loadFrame: async () => {},
        isHdf5File: () => false,
        getDefaultCenter: () => ({ x: 0, y: 0 }),
        scheduleResolutionOverlay: () => {},
      },
    });

    await controller.loadFiles();

    expect(fileSelect?.options[0]?.textContent).toBe("ファイルを選択");

    i18n.setLanguage("rm", { persist: false, applyDom: true });

    expect(fileSelect?.options[0]?.textContent).toBe("Tscherna ina datoteca");
  });
});
