import fs from "node:fs";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";

// The real English catalogue, so a renamed status key fails here instead of
// quietly degrading to a raw key in the UI.
const EN = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8")
);

const isHdfFile = (file) => /\.(h5|hdf5)$/i.test(String(file || ""));

function makeState(overrides = {}) {
  return {
    file: "",
    dataset: "",
    frameCount: 1,
    thresholdCount: 1,
    dataExport: {
      running: false,
      cancelling: false,
      jobId: "",
      progress: 0,
      message: "",
      outputs: [],
      openTarget: "",
      sourceKey: "",
      autoOutputDir: "",
      autoOutputPrefix: "",
    },
    ...overrides,
  };
}

async function build(state) {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createDataExportController } = await import("../modules/data_export_controller.js");
  const setStatus = vi.fn();
  const openModal = vi.fn();
  const controller = createDataExportController({
    apiBase: "/api",
    state,
    elements: {},
    callbacks: { isHdfFile, openModal, closeModal: vi.fn(), setStatus },
  });
  return { controller, setStatus, openModal };
}

describe("data_export_controller", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("refuses with no file open with a toast rather than silently", async () => {
    const { controller, setStatus, openModal } = await build(makeState());
    controller.openDialog();
    expect(openModal).not.toHaveBeenCalled();
    expect(setStatus).toHaveBeenCalledWith(EN["status.data_export.no_file"], { tone: "warning" });
  });

  it("refuses an HDF5 file with no dataset with a toast rather than silently", async () => {
    const { controller, setStatus, openModal } = await build(
      makeState({ file: "/data/stack.h5" })
    );
    controller.openDialog();
    expect(openModal).not.toHaveBeenCalled();
    expect(setStatus).toHaveBeenCalledWith(EN["status.data_export.no_dataset"], {
      tone: "warning",
    });
  });

  it("counts and names what a file export will write, and says why the flatfield is unavailable", async () => {
    vi.resetModules();
    global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createDataExportController } = await import("../modules/data_export_controller.js");
    document.body.innerHTML = `
      <div id="source"></div>
      <select id="format"><option value="tiff">TIFF</option><option value="cbf">CBF</option></select>
      <select id="dtype"><option value="auto">A</option><option value="uint16">16</option></select><span id="hint"></span>
      <select id="frames"><option value="all">All</option><option value="current">Current</option><option value="range">Range</option></select>
      <input id="start" value="1" /><input id="end" value="10" />
      <input id="prefix" /><input id="flat" type="checkbox" /><span id="flat-label"></span>
      <div id="text"></div>`;
    const $ = (id) => document.getElementById(id);
    const state = makeState({ file: "/data/img_th2_01999.tif", frameCount: 2000, frameIndex: 1999, width: 1000, height: 1000 });
    const controller = createDataExportController({
      apiBase: "/api",
      state,
      elements: {
        dataExportSource: $("source"), dataExportFormat: $("format"), dataExportDataType: $("dtype"), dataExportDataTypeHint: $("hint"),
        dataExportFrameMode: $("frames"), dataExportRangeStart: $("start"), dataExportRangeEnd: $("end"),
        dataExportPrefix: $("prefix"), dataExportFlatfield: $("flat"), dataExportFlatfieldLabel: $("flat-label"), dataExportProgressText: $("text"),
      },
      callbacks: { isHdfFile, openModal: vi.fn(), closeModal: vi.fn(), setStatus: vi.fn() },
    });
    controller.openDialog();
    expect($("source").textContent).toBe("/data/img_th2_01999.tif · frame 2000 of 2000");
    expect($("text").textContent).toBe("Writes 2000 TIFF files · Signed 32-bit · ≈ 8.0 GB · img_th2_?????.tiff");
    $("dtype").value = "uint16";
    $("frames").value = "range";
    $("dtype").dispatchEvent(new Event("change"));
    expect($("text").textContent).toBe("Writes 10 TIFF files · Unsigned 16-bit · ≈ 20.0 MB · img_th2_?????.tiff");
    $("format").value = "cbf";
    $("format").dispatchEvent(new Event("change"));
    expect($("text").textContent).toBe("Writes 10 CBF files · Signed 32-bit · img_th2_?????.cbf");
    expect($("flat-label").textContent).toBe(EN["data_export.flatfield_none"]);
    expect($("flat").disabled).toBe(true);
    localStorage.clear();
  });

  it("opens the dialog for an exportable source without a warning", async () => {
    const { controller, setStatus, openModal } = await build(
      makeState({ file: "/data/frame.cbf" })
    );
    controller.openDialog();
    expect(openModal).toHaveBeenCalledTimes(1);
    expect(setStatus).not.toHaveBeenCalled();
  });
});

describe("data_export_controller, live sources", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
    document.body.innerHTML = "";
  });

  const entry = (image, values) => ({
    sourceKey: "simplon:http://1.2.3.4",
    sourceKind: "simplon",
    label: `SIMPLON monitor (1.2.3.4) S189 Img${image}`,
    data: new Uint32Array(values),
    shape: [2, 2],
    dtype: "<u4",
    snapshot: {
      sourceKind: "simplon",
      analysis: { distanceMm: 150, pixelSizeUm: null, energyEv: 8040, centerX: 512.5, centerY: 600 },
      simplonMeta: { series: "189", image: String(image), date: "2026-10-12T10:41:14", thresholdEv: 4020, wavelengthA: 1.5421 },
    },
  });

  function liveState(entries, cursor = entries.length - 1) {
    return makeState({
      file: entries[cursor]?.label || "",
      autoload: { mode: "simplon", historyEntries: entries, historyCursor: cursor },
    });
  }

  function dialogElements() {
    document.body.innerHTML = `
      <div id="source"></div>
      <select id="format"><option value="tiff">TIFF</option><option value="cbf">CBF</option></select>
      <select id="dtype"><option value="auto">Automatic</option><option value="uint16">16</option><option value="uint32">32</option></select>
      <span id="dtype-hint"></span>
      <select id="frames">
        <option value="all">All frames</option>
        <option value="current">Current frame</option>
        <option value="range">Range</option>
      </select>
      <input id="dir" /><input id="prefix" /><input id="overwrite" type="checkbox" />
      <input id="inpaint" type="checkbox" /><span id="inpaint-label"></span>
      <button id="start"></button><button id="cancel"></button>
      <div id="progress"><div id="fill"></div><div id="text"></div></div>`;
    const $ = (id) => document.getElementById(id);
    return {
      dataExportSource: $("source"),
      dataExportFormat: $("format"),
      dataExportDataType: $("dtype"),
      dataExportDataTypeHint: $("dtype-hint"),
      dataExportFrameMode: $("frames"),
      dataExportOutputDir: $("dir"),
      dataExportPrefix: $("prefix"),
      dataExportOverwrite: $("overwrite"),
      dataExportInpaint: $("inpaint"),
      dataExportInpaintLabel: $("inpaint-label"),
      dataExportStart: $("start"),
      dataExportCancel: $("cancel"),
      dataExportProgress: $("progress"),
      dataExportProgressFill: $("fill"),
      dataExportProgressText: $("text"),
    };
  }

  async function buildLive(state, upload) {
    vi.resetModules();
    global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const mod = await import("../modules/data_export_controller.js");
    const elements = dialogElements();
    const setStatus = vi.fn();
    const openModal = vi.fn();
    const sent = [];
    const fetchJSONWithInit = vi.fn(async (url, init) => {
      const meta = JSON.parse(init.body.get("meta"));
      const pixels = init.body.get("pixels");
      const mask = init.body.get("mask");
      sent.push({ url, meta, bytes: pixels.size, maskBytes: mask ? mask.size : 0 });
      return upload ? upload(meta) : { path: `/out/${meta.name}.${meta.format === "cbf" ? "cbf" : "tiff"}` };
    });
    const controller = mod.createDataExportController({
      apiBase: "/api",
      state,
      elements,
      callbacks: { isHdfFile, openModal, closeModal: vi.fn(), setStatus, fetchJSONWithInit },
    });
    return { mod, controller, elements, setStatus, openModal, sent };
  }

  it("offers a data type for TIFF, says what it does, and remembers it", async () => {
    localStorage.clear();
    const state = liveState([entry(9, [1, 2, 3, 4])]);
    const { controller, elements, sent } = await buildLive(state);
    controller.openDialog();
    const select = elements.dataExportDataType;
    const hint = elements.dataExportDataTypeHint;
    expect(select.value).toBe("auto");
    expect(hint.textContent).toBe("Signed 32-bit · gaps −1, bad or saturated pixels −2");
    // In place of "Idle": what Start Export will do.
    expect(elements.dataExportProgressText.textContent).toBe(
      "Writes 1 TIFF file · Signed 32-bit · ≈ 1 kB · simplon_monitor_s189_00009.tiff",
    );
    // 16 bits for 32-bit data: what will be clipped, said before it is.
    select.value = "uint16";
    select.dispatchEvent(new Event("change"));
    expect(hint.textContent).toBe(
      "This data is 32-bit: counts above 65,534 will be clipped · gaps, bad and saturated pixels → 65,535",
    );
    expect(hint.classList.contains("is-warning")).toBe(true);
    expect(localStorage.getItem("albis.dataExport.dataType")).toBe("uint16");
    await controller.startExport();
    expect(sent[0].meta.data_type).toBe("uint16");
    // CBF is signed 32-bit: the choice is kept, but not sent.
    elements.dataExportFormat.value = "cbf";
    elements.dataExportFormat.dispatchEvent(new Event("change"));
    expect(select.disabled).toBe(true);
    expect(hint.textContent).toBe(EN["data_export.data_type.hint_cbf"]);
    state.dataExport.openTarget = "";
    await controller.startExport();
    expect(sent[1].meta.data_type).toBe("auto");
    expect(select.value).toBe("uint16");
    // The next dialog starts with the type chosen last.
    const again = await buildLive(liveState([entry(9, [1, 2, 3, 4])]));
    expect(again.elements.dataExportDataType.value).toBe("uint16");
    localStorage.clear();
  });

  it("says in the note what Inpaint fills instead of marking it", async () => {
    const state = liveState([entry(9, [1, 2, 3, 4])]);
    state.inpaintEnabled = true;
    const { controller, elements } = await buildLive(state);
    controller.openDialog();
    expect(elements.dataExportDataTypeHint.textContent).toBe("Signed 32-bit · gaps and defective pixels filled, saturated −2");
    state.inpaintSaturated = true;
    elements.dataExportInpaint.dispatchEvent(new Event("change"));
    expect(elements.dataExportDataTypeHint.textContent).toBe("Signed 32-bit · gaps, defective and saturated pixels filled");
  });

  it("names the output folder after the series, not the frame open", async () => {
    const { mod } = await buildLive(liveState([entry(1, [0, 0, 0, 0])]));
    expect(mod.defaultOutputDir("/data/img_th2_01999.tif")).toBe("/data/img_th2_export");
    expect(mod.defaultOutputDir("/data/sample.tif")).toBe("/data/sample_export");
    expect(mod.defaultOutputDir("/data/series_190_master.h5", "/entry/data/data")).toBe("/data/series_190_master_export");
  });

  it("says how many pixels were clipped to fit the data type", async () => {
    const state = liveState([entry(8, [1, 2, 3, 4]), entry(9, [5, 6, 7, 8])]);
    const { controller, elements, setStatus } = await buildLive(state, (meta) => ({
      path: `/out/${meta.name}.tiff`,
      clipped: meta.name.endsWith("9") ? 3 : 0,
    }));
    controller.openDialog();
    elements.dataExportFrameMode.value = "all";
    await controller.startExport();
    const text = "Export complete: wrote 2 files · 3 pixels clipped to fit, in 1 file";
    expect(setStatus).toHaveBeenLastCalledWith(text, { tone: "warning" });
    expect(elements.dataExportProgressText.textContent).toBe(text);
  });

  it("reads the bits of a dtype", async () => {
    const { mod } = await buildLive(liveState([entry(1, [0, 0, 0, 0])]));
    expect(mod.dtypeBits("<u2")).toBe(16);
    expect(mod.dtypeBits("<i4")).toBe(32);
    expect(mod.dtypeBits("|u1")).toBe(8);
    expect(mod.dtypeBits("<f8")).toBe(64);
    expect(mod.dtypeBits("")).toBeNull();
  });

  it("states what a live frame's header can say, under the backend's keys", async () => {
    const { mod } = await buildLive(liveState([entry(9, [1, 2, 3, 4])]));
    expect(mod.liveFrameMetadata(entry(9, [1, 2, 3, 4]))).toEqual({
      source_name: "SIMPLON monitor (1.2.3.4) S189 Img9",
      incident_energy_ev: 8040,
      wavelength_a: 1.5421,
      detector_distance_m: 0.15,
      beam_center_x_px: 512.5,
      beam_center_y_px: 600,
      threshold_energy_ev: 4020,
      series_number: 189,
      image_number: 9,
      image_datetime: "2026-10-12T10:41:14",
    });
    expect(mod.liveFrameName("simplon_monitor", entry(9, [0, 0, 0, 0]), 0)).toBe("simplon_monitor_s189_00009");
    // Zeros are the snapshot's "not known": left out, as is a 0,0 beam centre.
    const unknown = { ...entry(1, [0, 0, 0, 0]), snapshot: { sourceKind: "simplon", analysis: { distanceMm: 0, energyEv: 0, centerX: 0, centerY: 0, pixelSizeUm: 0 }, simplonMeta: {} } };
    expect(mod.liveFrameMetadata(unknown)).toEqual({ source_name: "SIMPLON monitor (1.2.3.4) S189 Img1" });
    const anonymous = { ...entry(1, [0, 0, 0, 0]), snapshot: { analysis: {} } };
    expect(mod.liveFrameName("remote stream", anonymous, 2)).toBe("remote_stream_00003");
  });

  it("defaults to the name an export's files share, the number left for last", async () => {
    const { mod } = await buildLive(liveState([entry(1, [0, 0, 0, 0])]));
    expect(mod.defaultOutputPrefix("/data/series_190_master.h5", "/entry/data/data")).toBe("series_190");
    expect(mod.defaultOutputPrefix("/data/series_190_master.h5", "/entry/data/data_000001")).toBe("series_190_entry_data_data_000001");
    expect(mod.defaultOutputPrefix("/data/image_00007.cbf", "")).toBe("image");
    expect(mod.defaultOutputPrefix("/data/scan.cbf.gz", "")).toBe("scan");
  });

  it("refuses a live source with no image yet, with a reason", async () => {
    const { controller, setStatus, openModal } = await buildLive(liveState([]));
    controller.openDialog();
    expect(openModal).not.toHaveBeenCalled();
    expect(setStatus).toHaveBeenCalledWith(EN["status.data_export.no_live_image"], { tone: "warning" });
  });

  it("offers the current image or the live history, names the source, and defaults to live_export", async () => {
    const state = liveState([entry(8, [0, 0, 0, 0]), entry(9, [0, 0, 0, 0])]);
    const { controller, elements, openModal } = await buildLive(state);
    controller.openDialog();
    expect(openModal).toHaveBeenCalledTimes(1);
    expect(elements.dataExportSource.textContent).toBe("SIMPLON monitor (1.2.3.4) S189 Img9 · 2 images in the live history");
    const options = Object.fromEntries([...elements.dataExportFrameMode.options].map((o) => [o.value, o]));
    expect(options.current.textContent).toBe("Current image");
    expect(options.all.textContent).toBe("All images in the live history (2)");
    expect(options.range.hidden).toBe(true);
    expect(elements.dataExportFrameMode.value).toBe("current");
    expect(elements.dataExportOutputDir.value).toBe("live_export");
    expect(elements.dataExportPrefix.value).toBe("simplon_monitor");
    expect(document.querySelector(".data-export-live-note").textContent).toBe(EN["data_export.live_note"]);
  });

  it("sends each chosen frame with its pixels, type, name and metadata", async () => {
    const state = liveState([entry(8, [1, 2, 3, 4]), entry(9, [5, 6, 7, 8])], 0);
    const { controller, elements, setStatus, sent } = await buildLive(state);
    controller.openDialog();
    elements.dataExportFormat.value = "cbf";
    // The current image: the one on screen, not the newest.
    await controller.startExport();
    expect(sent).toHaveLength(1);
    expect(sent[0].url).toBe("/api/export/data/frame");
    expect(sent[0].meta).toMatchObject({ dtype: "<u4", shape: [2, 2], name: "simplon_monitor_s189_00008", format: "cbf", output_dir: "live_export" });
    expect(sent[0].meta.metadata.image_number).toBe(8);
    expect(sent[0].bytes).toBe(16);
    expect(state.dataExport.openTarget).toBe("/out/simplon_monitor_s189_00008.cbf");
    // The whole history.
    elements.dataExportFrameMode.value = "all";
    state.dataExport.openTarget = "";
    await controller.startExport();
    expect(sent.slice(1).map((s) => s.meta.name)).toEqual(["simplon_monitor_s189_00008", "simplon_monitor_s189_00009"]);
    expect(setStatus).toHaveBeenLastCalledWith("Export complete: wrote 2 files", { tone: "success" });
  });

  it("stops at a refusal and shows the backend's reason", async () => {
    const state = liveState([entry(8, [0, 0, 0, 0]), entry(9, [0, 0, 0, 0])]);
    const { controller, elements, setStatus, sent } = await buildLive(state, () => {
      const err = new Error("HTTP 400");
      err.detail = "Output directory is outside data directory";
      throw err;
    });
    controller.openDialog();
    elements.dataExportFrameMode.value = "all";
    await controller.startExport();
    expect(sent).toHaveLength(1);
    expect(elements.dataExportProgressText.textContent).toBe("Failed: Output directory is outside data directory");
    expect(setStatus).toHaveBeenLastCalledWith(EN["status.data_export.failed"], { tone: "error" });
    expect(state.dataExport.running).toBe(false);
  });

  it("inpaints as on screen: ticked while inpainting is on, with the source's mask for a live frame", async () => {
    const state = liveState([entry(8, [1, 2, 3, 4])], 0);
    Object.assign(state, {
      inpaintEnabled: true,
      inpaintSaturated: true,
      inpaintMargin: 2,
      inpaintMethod: "fixed",
      inpaintValue: 12,
      maskAvailable: true,
      maskRaw: new Uint32Array([0, 1, 0, 0]),
      maskShape: [2, 2],
    });
    const { controller, elements, sent } = await buildLive(state);
    controller.openDialog();
    expect(elements.dataExportInpaint.checked).toBe(true);
    expect(elements.dataExportInpaintLabel.textContent).toBe(
      EN["data_export.inpaint_saturated"] +
        EN["data_export.inpaint_margin"].replace("{{margin}}", "2") +
        EN["data_export.inpaint_fixed"].replace("{{value}}", "12"),
    );
    await controller.startExport();
    expect(sent[0].meta.inpaint).toBe("saturated");
    expect(sent[0].meta.inpaint_margin).toBe(2);
    expect(sent[0].meta).toMatchObject({ inpaint_method: "fixed", inpaint_value: 12 });
    expect(sent[0].maskBytes).toBe(16);
    // Unticked here: written as measured, no mask sent.
    elements.dataExportInpaint.checked = false;
    state.dataExport.openTarget = "";
    await controller.startExport();
    expect(sent[1].meta.inpaint).toBe("off");
    expect(sent[1].meta.inpaint_margin).toBe(0);
    expect(sent[1].maskBytes).toBe(0);
  });
});
