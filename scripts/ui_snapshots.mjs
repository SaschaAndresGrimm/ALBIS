#!/usr/bin/env node
/**
 * Screenshots of every window ALBIS has, for checking a UI change by eye:
 * the main view, each Settings tab, About, Check for Updates, the backend log,
 * the command palette, the file browser, the menus and toolbar popovers, and
 * -- with an image loaded -- the export dialogs.
 *
 * It drives a headless Chrome over the DevTools protocol against an ALBIS
 * that is already running, and opens the image from the data folder through
 * the Data tab's file list, never by upload (an upload would leave a copy in
 * the data folder).
 *
 *   .venv/bin/python -m uvicorn backend.app:app --port 8731 &
 *   npm run ui:snapshots -- --out /tmp/albis-ui [--file Image_master.h5]
 *
 * Options:
 *   --url     ALBIS to open (default http://127.0.0.1:8731/)
 *   --out     folder for the PNGs (default ./ui-snapshots)
 *   --file    file in the data folder to open, as listed in the Data tab
 *             (default: the first one listed)
 *   --width, --height   viewport (default 1400 x 1000; below about 1500
 *             wide the side panel starts collapsed)
 *   --chrome  Chrome or Chromium binary (default: $CHROME, else the usual
 *             install path for this platform)
 *
 * Needs Node 22+ (built-in WebSocket) and Chrome. Writes only into --out.
 */

import { spawn } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const DEFAULT_CHROME = {
  darwin: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  win32: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
  linux: "google-chrome",
};

const url = arg("url", "http://127.0.0.1:8731/");
const outDir = path.resolve(arg("out", "ui-snapshots"));
const wantedFile = arg("file", "");
const width = Number(arg("width", 1400));
const height = Number(arg("height", 1000));
const chromePath = arg("chrome", process.env.CHROME || DEFAULT_CHROME[process.platform] || "google-chrome");
const port = 9300 + Math.floor(Math.random() * 600);

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

mkdirSync(outDir, { recursive: true });
const profile = mkdtempSync(path.join(os.tmpdir(), "albis-ui-snapshots-"));
const chrome = spawn(
  chromePath,
  [
    "--headless=new",
    "--disable-gpu",
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${profile}`,
    `--window-size=${width},${height}`,
    "about:blank",
  ],
  { stdio: "ignore" },
);

let socket;
let nextId = 0;
const pending = new Map();

function send(method, params = {}) {
  return new Promise((resolve) => {
    const id = ++nextId;
    pending.set(id, resolve);
    socket.send(JSON.stringify({ id, method, params }));
  });
}

async function evaluate(expression) {
  const reply = await send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
  return reply.result?.result?.value;
}

let shotCount = 0;
async function shot(name) {
  const reply = await send("Page.captureScreenshot", { format: "png" });
  shotCount += 1;
  const file = path.join(outDir, `${String(shotCount).padStart(2, "0")}-${name}.png`);
  writeFileSync(file, Buffer.from(reply.result.data, "base64"));
  const shown = path.relative(process.cwd(), file);
  process.stdout.write(`${shown.startsWith("..") ? file : shown}\n`);
}

// Page-side helpers, run in the page.
const clickAction = (action) =>
  `(() => { const el = document.querySelector('[data-action="${action}"]'); if (!el || el.disabled) return false; el.click(); return true; })()`;
const openMenu = (menu) => `document.querySelector('.menu-item[data-menu="${menu}"]')?.click()`;
const closeAll = `
  document.querySelectorAll('.modal.is-open .modal-close').forEach((b) => b.click());
  document.dispatchEvent(new KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
  document.body.click();`;

async function main() {
  let targets = [];
  for (let i = 0; i < 60 && !targets.length; i += 1) {
    try {
      targets = (await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()).filter((t) => t.type === "page");
    } catch {
      // Chrome is still starting.
    }
    if (!targets.length) await sleep(250);
  }
  if (!targets.length) throw new Error(`Chrome did not start (${chromePath}); pass --chrome or set CHROME.`);

  socket = new WebSocket(targets[0].webSocketDebuggerUrl);
  socket.onmessage = (event) => {
    const message = JSON.parse(event.data);
    if (message.id && pending.has(message.id)) {
      pending.get(message.id)(message);
      pending.delete(message.id);
    }
  };
  await new Promise((resolve, reject) => {
    socket.onopen = resolve;
    socket.onerror = reject;
  });
  await send("Page.enable");
  await send("Runtime.enable");
  await send("Emulation.setDeviceMetricsOverride", { width, height, deviceScaleFactor: 1, mobile: false });
  await send("Page.navigate", { url });
  await sleep(5000);
  await shot("main");

  // Settings, one tab after another.
  await evaluate(clickAction("settings-open"));
  await sleep(1200);
  for (const tab of ["viewer", "tools", "connection", "data", "logging"]) {
    await evaluate(`document.getElementById('settings-tab-${tab}')?.click()`);
    await sleep(500);
    await shot(`settings-${tab}`);
  }
  await evaluate(closeAll);
  await sleep(500);

  for (const action of ["help-about", "help-check-updates", "help-log", "command-palette"]) {
    if (await evaluate(clickAction(action))) {
      await sleep(1500);
      await shot(action);
    }
    await evaluate(closeAll);
    await sleep(500);
  }

  // The file browser, from the command palette (offered when the backend is local).
  await evaluate(clickAction("command-palette"));
  await sleep(700);
  const browsed = await evaluate(`(() => {
    const item = [...document.querySelectorAll('#command-modal button, #command-modal [role=option]')]
      .find((b) => /Browse Files/.test(b.textContent));
    if (!item) return false;
    item.click();
    return true;
  })()`);
  if (browsed) {
    await sleep(2000);
    await shot("file-browser");
  }
  await evaluate(closeAll);
  await sleep(500);

  // An image from the data folder, through the Data tab's file list.
  const opened = await evaluate(`(async () => {
    const select = document.getElementById('file-select');
    for (let i = 0; i < 40 && select.options.length < 2; i += 1) await new Promise((r) => setTimeout(r, 250));
    const wanted = ${JSON.stringify(wantedFile)};
    const option = [...select.options].find((o) => o.value && (!wanted || o.value === wanted || o.textContent.trim() === wanted));
    if (!option) return '';
    select.value = option.value;
    select.dispatchEvent(new Event('change', { bubbles: true }));
    return option.value;
  })()`);
  if (!opened) {
    process.stderr.write(`No file${wantedFile ? ` "${wantedFile}"` : ""} in the data folder; skipping the windows that need one.\n`);
  } else {
    process.stdout.write(`opened ${opened}\n`);
    await sleep(8000);
    await shot("loaded");
    for (const action of ["export-image", "export-animation", "export-data"]) {
      // Opening the File menu brings its items up to date with the file.
      await evaluate(openMenu("file"));
      await sleep(400);
      if (await evaluate(clickAction(action))) {
        await sleep(1500);
        await shot(action);
      }
      await evaluate(closeAll);
      await sleep(500);
    }
  }

  for (const menu of ["file", "settings", "help"]) {
    await evaluate(openMenu(menu));
    await sleep(500);
    await shot(`menu-${menu}`);
    await evaluate(closeAll);
    await sleep(400);
  }
  for (const [name, selector] of [
    ["playback", "#toolbar-playback-toggle"],
    ["sync", "#viewer-sync-options-toggle"],
    ["versions", "#footer-version-toggle"],
  ]) {
    if (await evaluate(`(() => { const b = document.querySelector('${selector}'); if (!b || b.offsetParent === null) return false; b.click(); return true; })()`)) {
      await sleep(600);
      await shot(`popover-${name}`);
    }
    await evaluate(closeAll);
    await sleep(400);
  }
}

try {
  await main();
} catch (error) {
  process.stderr.write(`${error.message}\n`);
  process.exitCode = 1;
} finally {
  chrome.kill();
  await sleep(300);
  rmSync(profile, { recursive: true, force: true });
}
