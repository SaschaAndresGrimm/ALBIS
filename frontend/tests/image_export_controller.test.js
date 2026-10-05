import fs from "node:fs";
import path from "node:path";
import zlib from "node:zlib";

import { afterEach, describe, expect, it, vi } from "vitest";

import { readPngDpi } from "../modules/image_export.js";

const EN = JSON.parse(fs.readFileSync(path.join(process.cwd(), "frontend", "locales", "en.json"), "utf8"));

function pngBlob() {
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(1, 0);
  ihdr.writeUInt32BE(1, 4);
  ihdr[8] = 8;
  ihdr[9] = 6;
  const chunk = (type, data) => {
    const out = Buffer.alloc(12 + data.length);
    out.writeUInt32BE(data.length, 0);
    out.write(type, 4, "ascii");
    Buffer.from(data).copy(out, 8);
    out.writeUInt32BE(zlib.crc32(out.subarray(4, 8 + data.length)), 8 + data.length);
    return out;
  };
  const bytes = Buffer.concat([
    Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]),
    chunk("IHDR", ihdr),
    chunk("IDAT", zlib.deflateSync(Buffer.from([0, 0, 0, 0, 255]))),
    chunk("IEND", Buffer.alloc(0)),
  ]);
  return new Blob([bytes], { type: "image/png" });
}

async function setup({
  overlays = {},
  labels = { enabled: false, minCellPx: 18 },
  zoom = 1,
  visible = { x: 100, y: 0, width: 400, height: 96 },
} = {}) {
  vi.resetModules();
  global.fetch = vi.fn(async () => ({ ok: true, json: async () => EN }));
  const i18n = await import("../modules/i18n.js");
  await i18n.initializeI18n({ backendLanguage: "en" });
  const { createImageExportController } = await import("../modules/image_export_controller.js");
  document.body.innerHTML = `
    <div id="m"></div>
    <select id="region"><option value="full">Full</option><option value="visible">Visible</option></select>
    <select id="scale"></select><select id="dpi"></select>
    <label id="of"><input id="ov" type="checkbox" checked /></label>
    <label id="pf"><input id="pv" type="checkbox" checked /></label>
    <div id="sum"></div><button id="go"></button>`;
  const $ = (id) => document.getElementById(id);
  const drawn = { smoothing: null, drawImage: null, text: [] };
  const target = {
    set imageSmoothingEnabled(value) {
      drawn.smoothing = value;
    },
    drawImage: (...args) => {
      drawn.drawImage = args;
    },
    fillText: (text, x, y) => drawn.text.push([text, x, y]),
    measureText: () => ({ width: 10 }),
  };
  const ctx = new Proxy(target, {
    get: (obj, key) => (key in obj ? obj[key] : () => {}),
    set: (obj, key, value) => {
      if (key === "imageSmoothingEnabled") obj.imageSmoothingEnabled = value;
      return true;
    },
  });
  const canvases = [];
  const state = { hasFrame: true, dataRaw: new Uint32Array(1544 * 96), width: 1544, height: 96, pixelAspect: 1, zoom, file: "/data/pollux_00001.tif" };
  state.dataRaw.forEach((_, i) => {
    state.dataRaw[i] = i;
  });
  const saveBlobAs = vi.fn(async (name, produce) => ({ name, blob: await produce() }));
  const controller = createImageExportController({
    state,
    elements: {
      modal: $("m"),
      regionSelect: $("region"),
      scaleSelect: $("scale"),
      dpiSelect: $("dpi"),
      overlaysCheckbox: $("ov"),
      overlaysField: $("of"),
      pixelValuesCheckbox: $("pv"),
      pixelValuesField: $("pf"),
      summary: $("sum"),
      startBtn: $("go"),
    },
    callbacks: {
      getVisibleRegion: () => visible,
      renderRegionToCanvas: (region) => ({ native: true, region }),
      canvasToBlob: async () => pngBlob(),
      saveBlobAs,
      defaultExportName: (kind) => (kind === "view" ? "pollux_00001_view_1.png" : "pollux_00001_frame_1.png"),
      getOverlaySnapshot: () => ({ ringParams: null, peaks: [], roi: null, outerRadius: 0, ...overlays }),
      getPixelLabelSettings: () => labels,
      pixelLabelsForFrame: (frame) => ({ float: false, labelAt: (idx) => String(frame.data[idx]) }),
      createCanvas: (width, height) => {
        const canvas = { width, height, getContext: () => ctx };
        canvases.push(canvas);
        return canvas;
      },
      openModal: vi.fn(),
      closeModal: vi.fn(),
      setStatus: vi.fn(),
    },
  });
  return { controller, $, state, drawn, canvases, saveBlobAs };
}

describe("image export dialog", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    delete global.fetch;
  });

  it("lists each size with its pixels and picks one that fills a slide", async () => {
    const { controller, $ } = await setup();

    controller.openDialog();

    const options = [...$("scale").options];
    expect(options.map((o) => o.textContent)).toEqual([
      "1× — 1544 × 96 px",
      "2× — 3088 × 192 px",
      "4× — 6176 × 384 px",
      "8× — 12352 × 768 px",
      "As on screen (1×) — 1544 × 96 px",
    ]);
    expect($("scale").value).toBe("2");
    expect($("dpi").value).toBe("300");
    expect($("sum").textContent).toBe("3088 × 192 px · 26.1 × 1.6 cm at 300 dpi");
  });

  it("enlarges without smoothing, at the chosen size, and stamps the resolution", async () => {
    const { controller, $, drawn, canvases, saveBlobAs } = await setup();
    controller.openDialog();
    $("scale").value = "4";

    await controller.startExport();

    const [name] = saveBlobAs.mock.calls[0];
    const { blob } = await saveBlobAs.mock.results[0].value;
    expect(name).toBe("pollux_00001_frame_1_4x.png");
    expect(canvases[0]).toMatchObject({ width: 6176, height: 384 });
    expect(drawn.smoothing).toBe(false);
    expect(drawn.drawImage.slice(1)).toEqual([0, 0, 6176, 384]);
    expect(readPngDpi(new Uint8Array(await blob.arrayBuffer()))).toBe(300);
  });

  it("stretches the image by the pixel aspect", async () => {
    const { controller, state, canvases, saveBlobAs } = await setup();
    state.pixelAspect = 3;
    controller.openDialog();

    await controller.startExport();
    await saveBlobAs.mock.results[0].value;

    expect(canvases[0].height).toBe(Math.round(96 * 3 * Number(document.getElementById("scale").value)));
  });

  it("offers overlays only when the viewer shows some", async () => {
    const without = await setup();
    without.controller.openDialog();
    expect(without.$("ov").disabled).toBe(true);
    expect(without.$("ov").checked).toBe(false);

    const withRoi = await setup({ overlays: { roi: { mode: "box", start: { x: 1, y: 1 }, end: { x: 5, y: 5 } } } });
    withRoi.controller.openDialog();
    expect(withRoi.$("ov").disabled).toBe(false);
  });

  it("names a visible-area export after the view", async () => {
    const { controller, $, saveBlobAs } = await setup();
    controller.openDialog();
    $("region").value = "visible";
    $("region").dispatchEvent(new Event("change"));

    await controller.startExport();

    expect(saveBlobAs.mock.calls[0][0]).toMatch(/^pollux_00001_view_1(_\dx)?\.png$/);
  });

  it("exports a zoomed-in view as it looks, pixel values included", async () => {
    const view = { x: 10, y: 20, width: 3, height: 2 };
    const { controller, $, drawn, canvases, saveBlobAs } = await setup({
      labels: { enabled: true, minCellPx: 18 },
      zoom: 24,
      visible: view,
    });
    controller.openDialog();
    $("region").value = "visible";
    $("region").dispatchEvent(new Event("change"));
    $("scale").value = "screen";
    $("scale").dispatchEvent(new Event("change"));
    expect($("pv").disabled).toBe(false);
    expect($("pv").checked).toBe(true);

    await controller.startExport();
    await saveBlobAs.mock.results[0].value;

    expect(saveBlobAs.mock.calls[0][0]).toBe("pollux_00001_view_1_24x.png");
    expect(canvases[0]).toMatchObject({ width: 72, height: 48 });
    // One label per detector pixel, centred in its 24 px cell.
    expect(drawn.text).toHaveLength(6);
    expect(drawn.text[0]).toEqual([String(20 * 1544 + 10), 12, 12]);
    expect(drawn.text[5]).toEqual([String(21 * 1544 + 12), 60, 36]);
  });

  it("offers pixel values only at a size they fit, and says why not", async () => {
    const { controller, $ } = await setup({ labels: { enabled: true, minCellPx: 18 }, zoom: 24 });
    controller.openDialog();
    $("scale").value = "8";
    $("scale").dispatchEvent(new Event("change"));
    expect($("pv").disabled).toBe(true);
    expect($("pv").checked).toBe(false);
    expect($("pf").title).toBe(EN["export.pixel_values.too_small"].replace("{{min}}", "18"));

    // Back at a size that fits, the earlier choice returns.
    $("scale").value = "screen";
    $("scale").dispatchEvent(new Event("change"));
    expect($("pv").checked).toBe(true);
  });

  it("does not offer pixel values the viewer is not showing", async () => {
    const { controller, $, drawn, saveBlobAs } = await setup({ zoom: 24 });
    controller.openDialog();
    $("scale").value = "screen";
    $("scale").dispatchEvent(new Event("change"));
    expect($("pv").disabled).toBe(true);
    expect($("pf").title).toBe(EN["export.pixel_values.off"]);

    await controller.startExport();
    await saveBlobAs.mock.results[0].value;
    expect(drawn.text).toEqual([]);
  });
});
