import fs from "node:fs";
import path from "node:path";

import { afterEach, describe, expect, it, vi } from "vitest";
import { frameSumExportCount, frameSumNameTag } from "../modules/frame_sum_utils.js";

// Export Data writes what the view's Sum shows. With Sum 10 on, Current frame
// wrote the single frame while the screen showed ten added up.

const EN = JSON.parse(fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8"));

describe("what an export with the sum on writes", () => {
  it("writes one file per window, the current view as one", () => {
    expect(frameSumExportCount("current", 1, 10)).toBe(1);
    expect(frameSumExportCount("all", 2000, 10)).toBe(200);
    expect(frameSumExportCount("range", 25, 10)).toBe(3);
    expect(frameSumExportCount("all", 2000, 1)).toBe(2000);
  });

  it("names a sum so it is not taken for a frame", () => {
    expect(frameSumNameTag(10, "sum")).toBe("_sum10");
    expect(frameSumNameTag(10, "mean")).toBe("_mean10");
    expect(frameSumNameTag(1, "sum")).toBe("");
  });
});

describe("the Export Data dialog with the sum on", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
    localStorage.clear();
  });

  async function build(stateOverrides = {}) {
    vi.resetModules();
    global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
    const i18n = await import("../modules/i18n.js");
    await i18n.initializeI18n({ backendLanguage: "en" });
    const { createDataExportController } = await import("../modules/data_export_controller.js");
    document.body.innerHTML = `
      <div id="source"></div>
      <select id="format"><option value="tiff">TIFF</option><option value="cbf">CBF</option></select>
      <select id="frames"><option value="all">All frames</option><option value="current">Current frame</option><option value="range">Range</option></select>
      <input id="start" value="1" /><input id="end" value="25" />
      <input id="prefix" /><span id="names"></span><div id="text"></div>`;
    const $ = (id) => document.getElementById(id);
    const state = {
      file: "/data/scan_00001.cbf",
      dataset: "",
      frameCount: 2000,
      frameIndex: 120,
      thresholdCount: 1,
      width: 100,
      height: 100,
      frameSum: 10,
      frameSumOp: "sum",
      seriesFiles: ["/data/scan_00001.cbf"],
      autoload: { mode: "file" },
      dataExport: { running: false, outputs: [], openTarget: "", sourceKey: "", autoOutputDir: "", autoOutputPrefix: "" },
      ...stateOverrides,
    };
    const sent = [];
    const fetchJSONWithInit = vi.fn(async (url, init) => {
      sent.push(JSON.parse(init.body));
      return { job_id: "job", status: "queued" };
    });
    const controller = createDataExportController({
      apiBase: "/api",
      state,
      elements: {
        dataExportSource: $("source"),
        dataExportFormat: $("format"),
        dataExportFrameMode: $("frames"),
        dataExportRangeStart: $("start"),
        dataExportRangeEnd: $("end"),
        dataExportPrefix: $("prefix"),
        dataExportNames: $("names"),
        dataExportProgressText: $("text"),
      },
      callbacks: { isHdfFile: () => false, openModal: vi.fn(), closeModal: vi.fn(), setStatus: vi.fn(), fetchJSONWithInit, fetchJSON: vi.fn() },
    });
    return { controller, $, sent };
  }

  it("says the files are windows, counts them and names them", async () => {
    const { controller, $ } = await build();
    controller.openDialog();
    const label = (value) => $("frames").querySelector(`option[value="${value}"]`).textContent;
    expect(label("current")).toBe("Current view (frames 121–130)");
    expect(label("all")).toBe("All frames, one file per 10");
    expect(label("range")).toBe("Range, one file per 10 frames");
    expect(document.querySelector(".data-export-live-note").textContent).toBe(
      "Sum 10 is on: each file is 10 frames added together. For single frames, set the Sum menu to 1 frame."
    );
    expect($("text").textContent).toMatch(/^Writes 200 TIFF files/);
    expect($("names").textContent).toBe("scan_sum10_?????.tiff");
    // The source line says what is on screen, as the toolbar does.
    expect($("source").textContent).toBe("/data/scan_00001.cbf · frames 121–130 of 2000");
  });

  it("asks the backend for the windows", async () => {
    const { controller, $, sent } = await build({ frameSumOp: "mean" });
    controller.openDialog();
    $("frames").value = "current";
    await controller.startExport();
    expect(sent[0]).toMatchObject({ frame_mode: "current", frame_start: 121, combine_count: 10, combine_op: "mean" });
  });

  it("is the plain export with the sum off", async () => {
    const { controller, $, sent } = await build({ frameSum: 1 });
    controller.openDialog();
    expect($("frames").querySelector('option[value="current"]').textContent).toBe("Current frame");
    expect(document.querySelector(".data-export-live-note")).toBeNull();
    await controller.startExport();
    expect(sent[0].combine_count).toBe(1);
  });
});
