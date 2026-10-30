#!/usr/bin/env node
/**
 * The pictures in docs/USER_GUIDE.md, made again from the running code.
 *
 * A hand-made screenshot is out of date the week after it is taken, and
 * nobody notices until a reader cannot find the button it shows. So every
 * picture in the guide is a scene here: this script starts its own ALBIS on
 * the insulin series in testdata/ (a PILATUS 300K, ten frames of 0.1°), puts
 * the interface into the state the section describes, and crops to the part
 * that matters. Change a window the guide shows, run this, and look at the
 * diff.
 *
 *   npm run guide:screenshots                      # all scenes
 *   npm run guide:screenshots -- --only roi,rings  # scenes whose name starts so
 *
 * Everything runs in a temporary folder: its own config (betas off, no update
 * check), a copy of the test data, its own known-detectors list, a fresh
 * Chrome profile, and a simulated detector (test_scripts/fake_simplon_dcu.py)
 * for the live and Detector-tab scenes. Your settings and testdata/ are not
 * touched; the only output is the pictures in --out.
 *
 * Options:
 *   --out     folder for the pictures (WebP) (default docs/images/user-guide)
 *   --only    comma-separated scene-name prefixes
 *   --python  Python to run ALBIS with (default .venv/bin/python)
 *   --chrome  Chrome or Chromium binary (default: $CHROME, else the usual
 *             install path for this platform)
 *
 * Needs Node 22+ (built-in WebSocket), Chrome, and the dev setup.
 */

import { spawn } from "node:child_process";
import { copyFileSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const DEFAULT_CHROME = {
  darwin: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  win32: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  linux: "google-chrome",
};

const outDir = path.resolve(ROOT, arg("out", "docs/images/user-guide"));
const only = arg("only", "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const python = path.resolve(ROOT, arg("python", process.platform === "win32" ? ".venv/Scripts/python.exe" : ".venv/bin/python"));
const chromePath = arg("chrome", process.env.CHROME || DEFAULT_CHROME[process.platform] || "google-chrome");

// One size for every picture, so the interface is drawn the same way in all
// of them. Wide enough for the side panel to sit beside the image.
const WIDTH = 1440;
const HEIGHT = 900;
// The insulin series: 487 x 619 pixels, beam centre (244, 308).
const SERIES = "in16c_010001.cbf";
const BEAM = { x: 244, y: 308 };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function freePort() {
  return new Promise((resolve, reject) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
    server.on("error", reject);
  });
}

async function waitForHttp(url, what, timeoutMs = 30000) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    try {
      const res = await fetch(url);
      if (res.status < 500) return;
    } catch {
      // Not up yet.
    }
    await sleep(250);
  }
  throw new Error(`${what} did not start (${url})`);
}

// ---------------------------------------------------------------------------
// The isolated ALBIS, simulated detector and Chrome.

const work = mkdtempSync(path.join(os.tmpdir(), "albis-guide-"));
const dataDir = path.join(work, "data");
mkdirSync(dataDir);
for (const name of readdirSync(path.join(ROOT, "testdata"))) {
  if (/^in16c_\d+\.cbf$/.test(name)) copyFileSync(path.join(ROOT, "testdata", name), path.join(dataDir, name));
}
const configPath = path.join(work, "albis.config.json");
function writeConfig(ui = {}) {
  writeFileSync(
    configPath,
    JSON.stringify({
      launcher: { idle_shutdown_min: 0 },
      // In the file rather than ALBIS_DATA_ROOT: a key the environment sets
      // is announced in the Settings dialog, which is in one of the pictures.
      data: { root: dataDir },
      logging: { dir: path.join(work, "logs") },
      ui: { auto_check_updates: false, known_detectors_file: path.join(work, "known_detectors.json"), ...ui },
    }),
  );
}
writeConfig();

const children = [];
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, () => {
    for (const child of children) child.kill();
    rmSync(work, { recursive: true, force: true });
    process.exit(130);
  });
}
function start(command, args, env = {}) {
  const child = spawn(command, args, { cwd: ROOT, env: { ...process.env, ...env }, stdio: "ignore" });
  children.push(child);
  return child;
}

const albisPort = await freePort();
// The port the guide names for the simulated detector, so its address in the
// pictures does not change from one run to the next; another if it is taken.
const dcuPort = await new Promise((resolve) => {
  const probe = net.createServer();
  probe.once("error", () => resolve(freePort()));
  probe.listen(8100, "127.0.0.1", () => probe.close(() => resolve(8100)));
});
const cdpPort = await freePort();
const albisUrl = `http://127.0.0.1:${albisPort}`;
const dcuUrl = `127.0.0.1:${dcuPort}`;

start(python, ["-m", "uvicorn", "backend.app:app", "--port", String(albisPort)], { ALBIS_CONFIG: configPath });
start(python, ["test_scripts/fake_simplon_dcu.py", "--port", String(dcuPort), "--init-delay", "0.5"]);
const profile = path.join(work, "chrome");
start(chromePath, [
  "--headless=new",
  "--disable-gpu",
  "--hide-scrollbars",
  `--remote-debugging-port=${cdpPort}`,
  `--user-data-dir=${profile}`,
  `--window-size=${WIDTH},${HEIGHT}`,
  "about:blank",
]);

// ---------------------------------------------------------------------------
// DevTools protocol.

let socket;
let nextId = 0;
const pending = new Map();

function send(method, params = {}) {
  return new Promise((resolve, reject) => {
    const id = ++nextId;
    pending.set(id, (message) => (message.error ? reject(new Error(`${method}: ${message.error.message}`)) : resolve(message.result)));
    socket.send(JSON.stringify({ id, method, params }));
  });
}

async function evaluate(expression) {
  const reply = await send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
  if (reply.exceptionDetails) throw new Error(`in page: ${reply.exceptionDetails.exception?.description || reply.exceptionDetails.text}`);
  return reply.result.value;
}

/** Run `fn` in the page with JSON-able `args`. */
const page = (fn, ...args) => evaluate(`(${fn})(...${JSON.stringify(args)})`);

async function waitFor(fn, what, timeoutMs = 15000, ...args) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    if (await page(fn, ...args)) return;
    await sleep(150);
  }
  throw new Error(`timed out waiting for ${what}`);
}

const click = (selector) =>
  page((sel) => {
    const el = document.querySelector(sel);
    if (!el) throw new Error(`no element ${sel}`);
    el.click();
  }, selector);

const setValue = (selector, value) =>
  page(
    (sel, v) => {
      const el = document.querySelector(sel);
      if (!el) throw new Error(`no element ${sel}`);
      if (el.type === "checkbox") el.checked = Boolean(v);
      else el.value = v;
      el.dispatchEvent(new Event("input", { bubbles: true }));
      el.dispatchEvent(new Event("change", { bubbles: true }));
    },
    selector,
    value,
  );

const BUTTONS = { left: 1, right: 2, middle: 4 };
async function mouse(type, x, y, { button = "none", modifiers = 0, clickCount = 0 } = {}) {
  await send("Input.dispatchMouseEvent", {
    type,
    x,
    y,
    button,
    buttons: type === "mouseReleased" ? 0 : BUTTONS[button] || 0,
    modifiers,
    clickCount,
  });
}

async function drag(from, to, button = "left") {
  await mouse("mouseMoved", from.x, from.y);
  await mouse("mousePressed", from.x, from.y, { button, clickCount: 1 });
  for (let i = 1; i <= 8; i += 1) {
    await mouse("mouseMoved", from.x + ((to.x - from.x) * i) / 8, from.y + ((to.y - from.y) * i) / 8, { button });
    await sleep(20);
  }
  await mouse("mouseReleased", to.x, to.y, { button, clickCount: 1 });
}

async function key(type, keyName, code, text) {
  await send("Input.dispatchKeyEvent", { type, key: keyName, code, text, windowsVirtualKeyCode: keyName.toUpperCase().charCodeAt(0) });
}

/** Screen position of a detector pixel, from where the image canvas is drawn. */
const screenOf = (ix, iy) =>
  page(
    (x, y) => {
      const c = document.getElementById("image-canvas");
      const r = c.getBoundingClientRect();
      return { x: r.x + ((x + 0.5) * r.width) / c.width, y: r.y + ((y + 0.5) * r.height) / c.height };
    },
    ix,
    iy,
  );

const rectOf = (selectors) =>
  page((sels) => {
    const rects = sels
      .map((s) => document.querySelector(s))
      .filter((el) => el && el.offsetParent !== null)
      .map((el) => el.getBoundingClientRect());
    if (!rects.length) throw new Error(`nothing visible for ${sels.join(", ")}`);
    const x = Math.min(...rects.map((r) => r.left));
    const y = Math.min(...rects.map((r) => r.top));
    return { x, y, width: Math.max(...rects.map((r) => r.right)) - x, height: Math.max(...rects.map((r) => r.bottom)) - y };
  }, [].concat(selectors));

const written = [];
/**
 * Save a picture: the whole window, the box around `selector` (one or a
 * list) grown by `pad`, or an explicit `clip`.
 */
async function shot(name, { selector, clip, pad = 8 } = {}) {
  let area = clip;
  if (selector) {
    const r = await rectOf(selector);
    area = { x: r.x - pad, y: r.y - pad, width: r.width + 2 * pad, height: r.height + 2 * pad };
  }
  if (area) {
    const x = Math.max(0, Math.floor(area.x));
    const y = Math.max(0, Math.floor(area.y));
    area = { x, y, width: Math.min(WIDTH - x, Math.ceil(area.width)), height: Math.min(HEIGHT - y, Math.ceil(area.height)), scale: 1 };
  }
  // WebP at 90: text stays crisp, and a full window is about 130 KB where a
  // PNG is 500 KB -- the pictures live in the repository for good.
  const reply = await send("Page.captureScreenshot", { format: "webp", quality: 90, ...(area ? { clip: area } : {}) });
  const file = path.join(outDir, `${name}.webp`);
  writeFileSync(file, Buffer.from(reply.data, "base64"));
  written.push(file);
  process.stdout.write(`${path.relative(ROOT, file)}\n`);
}

/**
 * Numbered markers drawn over the page, for the picture that names the parts
 * of the window. `spots` are [x, y] in window pixels.
 */
const callouts = (spots) =>
  page((list) => {
    document.getElementById("guide-callouts")?.remove();
    const layer = document.createElement("div");
    layer.id = "guide-callouts";
    layer.style.cssText = "position:fixed;inset:0;pointer-events:none;z-index:2147483647";
    list.forEach(([x, y], i) => {
      const dot = document.createElement("div");
      dot.textContent = String(i + 1);
      dot.style.cssText = `position:absolute;left:${x - 15}px;top:${y - 15}px;width:30px;height:30px;border-radius:50%;
        background:#f5a623;color:#111;font:700 17px/30px system-ui,sans-serif;text-align:center;
        box-shadow:0 0 0 3px #111,0 2px 8px rgba(0,0,0,.6)`;
      layer.append(dot);
    });
    document.body.append(layer);
  }, spots);
const clearCallouts = () => page(() => document.getElementById("guide-callouts")?.remove());

// ---------------------------------------------------------------------------
// Getting the interface into a known state.

async function openApp({ panel = true } = {}) {
  // The interface keeps folded sections, tabs and tools in the browser's
  // storage; cleared, every scene starts as a first visit does.
  await send("Storage.clearDataForOrigin", { origin: albisUrl, storageTypes: "local_storage,session_storage,indexeddb" });
  await send("Page.navigate", { url: `${albisUrl}/` });
  await waitFor(() => document.getElementById("file-select")?.options.length > 2, "the Data tab's file list", 30000);
  // The side panel starts folded on a window this size.
  if (panel) {
    await page(() => {
      const fab = document.getElementById("panel-fab");
      if (fab?.classList.contains("is-collapsed")) fab.click();
    });
  }
  await sleep(800);
}

/** Move the mouse off the image, so no cursor readout is left in the picture. */
async function parkMouse() {
  await mouse("mouseMoved", WIDTH - 4, 4);
  await sleep(300);
}

async function loadSeries(name = SERIES) {
  await setValue("#file-select", name);
  await waitFor(() => /487/.test(document.getElementById("meta-shape")?.textContent || ""), `${name} to load`, 20000);
  await sleep(1500);
}

async function tab(name) {
  await click(`.panel-tab[data-panel-tab="${name}"]`);
  await sleep(400);
}

/** Expand a panel section (and switch it on, if it has a switch and `on`). */
async function section(name, { on } = {}) {
  await page(
    (n, enable) => {
      const s = document.querySelector(`section[data-section="${n}"]`);
      if (!s) throw new Error(`no section ${n}`);
      if (s.classList.contains("is-collapsed")) s.querySelector("[data-section-toggle]").click();
      const sw = s.querySelector(".section-switch input");
      if (enable !== undefined && sw && sw.checked !== enable) sw.click();
    },
    name,
    on,
  );
  await sleep(600);
}

async function collapseAll() {
  await page(() => {
    document.querySelectorAll("section.panel-section:not(.is-collapsed) > [data-section-toggle]").forEach((b) => b.click());
  });
  await sleep(300);
}

async function fitToWindow() {
  await click("#reset-view");
  await sleep(600);
}

/** Zoom so detector pixel (ix, iy) sits at the centre of the image area at `zoom`. */
async function zoomTo(ix, iy, zoom) {
  await fitToWindow();
  for (let i = 0; i < 40; i += 1) {
    const current = await page(() => parseFloat(document.getElementById("zoom-value")?.value || "1"));
    if (current >= zoom) break;
    const p = await screenOf(ix, iy);
    await send("Input.dispatchMouseEvent", { type: "mouseWheel", x: p.x, y: p.y, deltaX: 0, deltaY: -120 });
    await sleep(200);
  }
  await parkMouse();
}

async function closeAll() {
  await page(() => {
    document.querySelectorAll(".modal.is-open .modal-close").forEach((b) => b.click());
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    document.body.click();
  });
  await sleep(400);
}

async function menuAction(menu, action) {
  await click(`.menu-item[data-menu="${menu}"]`);
  await sleep(300);
  await click(`[data-action="${action}"]`);
  await sleep(1200);
}

async function saveSettings(ui) {
  writeConfig(ui);
  const current = await (await fetch(`${albisUrl}/api/settings`)).json();
  const config = { ...current.config, ui: { ...current.config.ui, ...ui } };
  const res = await fetch(`${albisUrl}/api/settings`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ config }),
  });
  if (!res.ok) throw new Error(`saving settings: ${res.status} ${await res.text()}`);
}

// ---------------------------------------------------------------------------
// The scenes, in the order the guide shows them. Each starts from a freshly
// loaded page, so one scene's state cannot leak into the next.

const scenes = [
  {
    name: "start-window",
    panel: false,
    async run() {
      await shot("start-window");
    },
  },
  {
    name: "window-overview",
    async run() {
      await loadSeries();
      await fitToWindow();
      await parkMouse();
      // Each marker sits in free space just past the part it names.
      const spots = await page(() => {
        const r = (sel) => document.querySelector(sel).getBoundingClientRect();
        const menus = [...document.querySelectorAll(".menu-bar .menu-item")].map((m) => m.getBoundingClientRect());
        const menu = menus[menus.length - 1];
        const toolbar = r(".toolbar");
        const image = r("#image-canvas");
        const tabs = r(".panel-tabs");
        const hist = r('section[data-section="histogram"]');
        const pills = [...document.querySelectorAll(".viewer-footer button, .viewer-footer .footer-pill, .viewer-footer [class*=pill]")]
          .map((b) => b.getBoundingClientRect())
          .filter((b) => b.width && b.left < window.innerWidth / 2);
        const footerRight = Math.max(...pills.map((b) => b.right));
        const footer = r(".viewer-footer");
        return [
          [menu.right + 22, menu.top + menu.height / 2],
          [toolbar.right + 24, toolbar.top + toolbar.height / 2],
          [image.right - 40, image.top + image.height * 0.3],
          [tabs.left - 14, tabs.top + tabs.height / 2],
          [hist.left - 14, hist.top + 18],
          [footerRight + 26, footer.top + footer.height / 2],
        ];
      });
      await callouts(spots);
      await shot("window-overview");
      await clearCallouts();
    },
  },
  {
    name: "data-tab",
    async run() {
      await loadSeries();
      await tab("data");
      await collapseAll();
      await section("data-source");
      await shot("data-tab", { selector: "#side-panel" });
    },
  },
  {
    name: "toolbar",
    async run() {
      await loadSeries();
      await setValue("#frame-range", 4);
      await sleep(1500);
      await shot("toolbar", { selector: ".toolbar", pad: 6 });
    },
  },
  {
    name: "histogram",
    async run() {
      await loadSeries();
      await tab("view");
      await section("histogram");
      await shot("histogram", { selector: 'section[data-section="histogram"]', pad: 4 });
    },
  },
  {
    name: "magnifier",
    async run() {
      await loadSeries();
      await fitToWindow();
      const p = await screenOf(300, 250);
      await mouse("mouseMoved", p.x, p.y);
      await key("keyDown", "z", "KeyZ", "z");
      await sleep(800);
      await shot("magnifier", { clip: { x: p.x - 200, y: p.y - 150, width: 440, height: 300 } });
      await key("keyUp", "z", "KeyZ");
    },
  },
  {
    name: "roi",
    async run() {
      await loadSeries();
      await fitToWindow();
      await tab("analysis");
      await collapseAll();
      await section("roi", { on: true });
      await setValue("#roi-mode", "line");
      await drag(await screenOf(BEAM.x + 20, BEAM.y + 15), await screenOf(BEAM.x + 200, BEAM.y + 150), "right");
      await sleep(1500);
      await parkMouse();
      await shot("roi");
    },
  },
  {
    name: "inpainting",
    async run() {
      await loadSeries();
      // Where the upper module gap crosses the ring of reflections.
      await zoomTo(150, 203, 4);
      const p = await screenOf(150, 203);
      const clip = { x: p.x - 200, y: p.y - 140, width: 400, height: 280 };
      await shot("inpainting-before", { clip });
      await tab("analysis");
      await section("inpainting", { on: true });
      // The method the guide recommends for a gap this wide (17 px), with
      // noise so the filled gap reads like the measured pixels around it.
      await setValue("#inpaint-method", "edges");
      await setValue("#inpaint-noise", true);
      await sleep(2000);
      await shot("inpainting-after", { clip });
      await shot("inpainting-section", { selector: 'section[data-section="inpainting"]', pad: 4 });
    },
  },
  {
    name: "flatfield",
    async run() {
      await loadSeries();
      await tab("analysis");
      await collapseAll();
      await section("flatfield");
      await shot("flatfield-section", { selector: 'section[data-section="flatfield"]', pad: 4 });
    },
  },
  {
    name: "geometry",
    async run() {
      await loadSeries();
      await tab("data");
      await collapseAll();
      await section("detector-geometry");
      await shot("geometry-section", { selector: "#detector-geometry-section", pad: 4 });
    },
  },
  {
    name: "rings",
    async run() {
      await loadSeries();
      await fitToWindow();
      await tab("analysis");
      await collapseAll();
      await section("resolution-rings", { on: true });
      // Rings that fall on this detector, from low to high resolution.
      await click("#ring-clear");
      for (const d of [10, 5, 3.3, 2.5]) {
        await click("#ring-add");
        await page((v) => {
          const inputs = document.querySelectorAll("#ring-list input");
          const input = inputs[inputs.length - 1];
          input.value = v;
          input.dispatchEvent(new Event("input", { bubbles: true }));
          input.dispatchEvent(new Event("change", { bubbles: true }));
          input.dispatchEvent(new FocusEvent("focusout", { bubbles: true }));
        }, d);
        await sleep(200);
      }
      await section("peak-finder", { on: true });
      await setValue("#peaks-count", 30);
      await sleep(2500);
      await parkMouse();
      await shot("rings-peaks");
    },
  },
  {
    name: "combine",
    async run() {
      await loadSeries();
      await tab("analysis");
      await collapseAll();
      await section("series-summing");
      await shot("combine-frames", { selector: "#series-sum-section", pad: 4 });
    },
  },
  {
    name: "settings-tools",
    async run() {
      await click('[data-action="settings-open"]');
      await sleep(1000);
      await click("#settings-tab-tools");
      // The config file's path is this run's temporary folder.
      await page(() => {
        document.getElementById("settings-config-path").closest("div").style.display = "none";
      });
      await sleep(600);
      await shot("settings-tools", { selector: "#settings-modal .modal-card" });
    },
  },
  {
    name: "export",
    async run() {
      await loadSeries();
      await menuAction("file", "export-image");
      await shot("export-image", { selector: ".modal.is-open .modal-card" });
      await closeAll();
      await menuAction("file", "export-data");
      await shot("convert-dataset", { selector: ".modal.is-open .modal-card" });
    },
  },
  {
    name: "link",
    async run() {
      await loadSeries();
      await click("#viewer-sync-options-toggle");
      await sleep(600);
      await shot("link-popover", { selector: [".toolbar", "#viewer-sync-popover"], pad: 6 });
    },
  },
  {
    name: "command-palette",
    async run() {
      await loadSeries();
      await click('[data-action="command-palette"]');
      await sleep(800);
      await shot("command-palette", { selector: "#command-modal .modal-card" });
    },
  },
  {
    name: "simplon",
    async run() {
      await tab("data");
      await collapseAll();
      await section("data-source");
      await setValue("#autoload-mode", "simplon");
      await sleep(500);
      await setValue("#simplon-url", dcuUrl);
      await click("#simplon-test");
      await waitFor(() => /PILATUS|EIGER|S\/N|serial/i.test(document.getElementById("simplon-probe-message")?.textContent || ""), "the SIMPLON test", 15000).catch(async (e) => {
        throw new Error(`${e.message}: "${await page(() => document.getElementById("simplon-probe-message")?.textContent)}"`);
      });
      await sleep(500);
      await shot("simplon-test", { selector: "#data-section", pad: 4 });
    },
  },
  {
    name: "mask-edit",
    betas: true,
    async run() {
      await loadSeries();
      await fitToWindow();
      await tab("data");
      await collapseAll();
      await section("pixel-mask");
      await click("#pixel-mask-edit");
      await sleep(800);
      await key("keyDown", "r", "KeyR", "r");
      await key("keyUp", "r", "KeyR");
      await sleep(300);
      await drag(await screenOf(40, 440), await screenOf(150, 540), "left");
      await sleep(800);
      await parkMouse();
      await shot("mask-edit");
    },
  },
  {
    name: "detector",
    betas: true,
    async run() {
      await tab("detector");
      await setValue("#detector-url", dcuUrl);
      await click("#detector-connect");
      await sleep(3000);
      await shot("detector-tab", { selector: "#side-panel" });
    },
  },
];

// ---------------------------------------------------------------------------

async function main() {
  await waitForHttp(`${albisUrl}/api/health`, "ALBIS");
  await waitForHttp(`http://${dcuUrl}/detector/api/version/`, "the simulated detector");
  // A detector answers its configuration only once initialized, as a real one
  // after power-up; the scenes want one that is ready.
  await fetch(`http://${dcuUrl}/detector/api/1.8.0/command/initialize`, { method: "PUT" });
  await waitForHttp(`http://127.0.0.1:${cdpPort}/json/version`, `Chrome (${chromePath}; pass --chrome or set CHROME)`);
  const targets = (await (await fetch(`http://127.0.0.1:${cdpPort}/json/list`)).json()).filter((t) => t.type === "page");
  socket = new WebSocket(targets[0].webSocketDebuggerUrl);
  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    if (message.id && pending.has(message.id)) {
      pending.get(message.id)(message);
      pending.delete(message.id);
    } else if (message.method === "Page.javascriptDialogOpening") {
      // Leaving a page with unsaved mask edits asks first; a headless
      // browser has nobody to answer, and would wait for ever.
      send("Page.handleJavaScriptDialog", { accept: true });
    }
  };
  await new Promise((resolve, reject) => {
    socket.onopen = resolve;
    socket.onerror = reject;
  });
  await send("Page.enable");
  await send("Runtime.enable");
  await send("Emulation.setDeviceMetricsOverride", { width: WIDTH, height: HEIGHT, deviceScaleFactor: 1, mobile: false });
  mkdirSync(outDir, { recursive: true });

  let betas = false;
  const failed = [];
  for (const scene of scenes) {
    if (only.length && !only.some((prefix) => scene.name.startsWith(prefix))) continue;
    if (Boolean(scene.betas) !== betas) {
      betas = Boolean(scene.betas);
      await saveSettings({ detector_control: betas, pixel_mask: betas });
    }
    const started = Date.now();
    let timer;
    try {
      await Promise.race([
        (async () => {
          await openApp({ panel: scene.panel !== false });
          await scene.run();
        })(),
        new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error("took longer than 90 s")), 90000);
        }),
      ]);
    } catch (error) {
      failed.push(scene.name);
      process.stderr.write(`${scene.name}: ${error.message}\n`);
    }
    clearTimeout(timer);
    process.stdout.write(`  ${scene.name}: ${((Date.now() - started) / 1000).toFixed(1)} s\n`);
  }
  if (failed.length) {
    process.stderr.write(`Failed: ${failed.join(", ")}\n`);
    process.exitCode = 1;
  }
}

try {
  await main();
} catch (error) {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
} finally {
  socket?.close();
  for (const child of children) child.kill();
  await sleep(500);
  rmSync(work, { recursive: true, force: true });
}
