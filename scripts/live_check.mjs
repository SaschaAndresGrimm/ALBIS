#!/usr/bin/env node
/**
 * Drive ALBIS through its data sources in a real browser and say what the
 * viewer showed: a file series and frame stepping, playback, an HDF5 stack,
 * folder watch, the Remote Stream API, a SIMPLON monitor, a JUNGFRAUJOCH
 * preview stream, a handoff job, a launch target and Duplicate Window.
 *
 * The unit tests drive each controller with stand-ins; this is the check that
 * the wiring in app.js still connects them. Everything runs locally: ALBIS on
 * a scratch copy of testdata/, the simulated detector
 * (test_scripts/fake_simplon_dcu.py) and the simulated JUNGFRAUJOCH publisher
 * (test_scripts/fake_jfjoch_publisher.py), started and stopped by this script.
 *
 *   npm run ui:live                                  # checks, and the fingerprint to ./live-check.json
 *   npm run ui:live -- --out /tmp/after.json --compare /tmp/before.json
 *
 * Each step checks what must be true (a live frame arrived, the frame counter
 * moved) and records what the viewer showed. --compare lists every recorded
 * value that differs from an earlier run -- run it on main first, then on a
 * change. Values that differ between any two runs (times, image numbers of a
 * running stream) are not recorded, only checked.
 *
 * Needs Node 22+, Chrome, and the project's .venv. Exits 1 if a check fails or
 * a recorded value differs.
 */

import { spawn } from "node:child_process";
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";

function arg(name, fallback) {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 && process.argv[i + 1] ? process.argv[i + 1] : fallback;
}

const ROOT = path.resolve(path.dirname(new URL(import.meta.url).pathname), "..");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const outFile = path.resolve(arg("out", "live-check.json"));
const compareFile = arg("compare", "");
const chromePath = arg("chrome", process.env.CHROME || {
  darwin: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  win32: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
}[process.platform] || "google-chrome");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function freePort() {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      server.close(() => resolve(port));
    });
  });
}

const children = [];
function start(command, args, env = {}) {
  const child = spawn(command, args, { cwd: ROOT, env: { ...process.env, ...env }, stdio: "ignore" });
  children.push(child);
  return child;
}

async function waitFor(check, timeoutMs, what) {
  const until = Date.now() + timeoutMs;
  while (Date.now() < until) {
    try {
      if (await check()) return true;
    } catch {
      // not up yet
    }
    await sleep(250);
  }
  throw new Error(`timed out waiting for ${what}`);
}

// --- the scratch data folder ---------------------------------------------
const data = mkdtempSync(path.join(os.tmpdir(), "albis-live-"));
const series = ["in16c_010001.cbf", "in16c_010002.cbf", "in16c_010003.cbf", "in16c_010004.cbf", "in16c_010005.cbf"];
for (const name of [...series, "compressed_stack.h5"]) copyFileSync(path.join(ROOT, "testdata", name), path.join(data, name));
mkdirSync(path.join(data, "watch"));
writeFileSync(
  path.join(data, "manifest.json"),
  JSON.stringify({ run_id: "live-check", outputs: [{ kind: "image", path: path.join(data, "in16c_010004.cbf") }] }),
);

// --- the browser ----------------------------------------------------------
let socket;
let nextId = 0;
const pending = new Map();
const pageErrors = [];
function send(method, params = {}, ws = socket) {
  return new Promise((resolve) => {
    const id = ++nextId;
    pending.set(id, resolve);
    ws.send(JSON.stringify({ id, method, params }));
  });
}
async function connect(target) {
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  ws.onmessage = (event) => {
    const message = JSON.parse(event.data);
    if (message.id && pending.has(message.id)) {
      pending.get(message.id)(message);
      pending.delete(message.id);
    }
    if (message.method === "Runtime.exceptionThrown") {
      pageErrors.push(message.params.exceptionDetails?.exception?.description || message.params.exceptionDetails?.text || "exception");
    }
  };
  await new Promise((resolve, reject) => {
    ws.onopen = resolve;
    ws.onerror = reject;
  });
  await send("Page.enable", {}, ws);
  await send("Runtime.enable", {}, ws);
  return ws;
}

// Runs fn in the page. Its arguments go over the DevTools protocol as values,
// never spliced into the source, so a file name or id cannot become code.
async function call(fn, args = [], ws = socket) {
  const global = await send("Runtime.evaluate", { expression: "globalThis" }, ws);
  const reply = await send(
    "Runtime.callFunctionOn",
    {
      functionDeclaration: fn.toString(),
      objectId: global.result?.result?.objectId,
      arguments: args.map((value) => ({ value })),
      awaitPromise: true,
      returnByValue: true,
    },
    ws,
  );
  return reply.result?.result?.value;
}

// Functions that run in the page through call(): self-contained, values as
// arguments.
const inPage = {
  text: (id) => (document.getElementById(id)?.textContent || "").replace(/\s+/g, " ").trim(),
  value: (id) => document.getElementById(id)?.value ?? "",
  setValue: (id, v) => {
    const e = document.getElementById(id);
    if (!e) return false;
    e.value = v;
    e.dispatchEvent(new Event("input", { bubbles: true }));
    e.dispatchEvent(new Event("change", { bubbles: true }));
    return true;
  },
  setChecked: (id, on) => {
    const e = document.getElementById(id);
    if (!e) return false;
    if (e.checked !== on) {
      e.checked = on;
      e.dispatchEvent(new Event("change", { bubbles: true }));
    }
    return true;
  },
  click: (selector) => {
    const e = document.querySelector(selector);
    if (!e) return false;
    e.click();
    return true;
  },
  // The Data tab's file list holds the names the backend gave; pick by file name.
  findFile: (name) => {
    const options = [...(document.getElementById("file-select")?.options || [])];
    return options.find((o) => o.value === name || o.value.endsWith(`/${name}`))?.value ?? "";
  },
  fileOptions: () => document.getElementById("file-select")?.options.length ?? 0,
  viewer: () => {
    const text = (id) => (document.getElementById(id)?.textContent || "").replace(/\s+/g, " ").trim();
    return {
      file: text("footer-file"),
      frame: document.getElementById("frame-index")?.value ?? "",
      range: text("meta-range"),
      shape: text("meta-shape"),
      dtype: text("meta-dtype"),
    };
  },
  hash: () => location.hash,
};
const text = (id, ws = socket) => call(inPage.text, [id], ws);
const value = (id) => call(inPage.value, [id]);
const setValue = (id, v) => call(inPage.setValue, [id, v]);
const setChecked = (id, on) => call(inPage.setChecked, [id, on]);
const click = (selector) => call(inPage.click, [selector]);
const viewer = () => call(inPage.viewer);
async function openFile(name) {
  const option = await call(inPage.findFile, [name]);
  return Boolean(option) && setValue("file-select", option);
}

const results = [];
const recorded = {};
// Page text in a detail is printed: no control characters reach the terminal.
const printable = (detail) =>
  String(detail ?? "")
    .replace(/\r|\n/g, " ")
    .replace(/[\u0000-\u001f\u007f]/g, " ");
function check(step, ok, detail) {
  results.push({ step, ok: Boolean(ok), detail: printable(detail) });
  console.info(`${ok ? "ok  " : "FAIL"} ${step}${detail ? `: ${printable(detail)}` : ""}`);
}
async function until(predicate, timeoutMs = 15000) {
  const end = Date.now() + timeoutMs;
  while (Date.now() < end) {
    if (await predicate()) return true;
    await sleep(250);
  }
  return false;
}
const footerHas = (needle) => async () => (await text("footer-file")).includes(needle);
const filled = (id) => async () => !["", "-"].includes(await text(id));

async function main() {
  const albisPort = await freePort();
  const dcuPort = await freePort();
  const jfjochPort = await freePort();
  const chromePort = await freePort();
  const url = `http://127.0.0.1:${albisPort}/`;

  start(PYTHON, ["-m", "uvicorn", "backend.app:app", "--port", String(albisPort)], { ALBIS_DATA_ROOT: data });
  start(PYTHON, ["test_scripts/fake_simplon_dcu.py", "--port", String(dcuPort), "--init-delay", "0.3", "--max-series", "6"], {
    PYTHONPATH: ROOT,
  });
  start(PYTHON, ["test_scripts/fake_jfjoch_publisher.py", "--port", String(jfjochPort), "--rate", "4"]);
  const profile = mkdtempSync(path.join(os.tmpdir(), "albis-live-chrome-"));
  start(chromePath, ["--headless=new", "--disable-gpu", "--disable-popup-blocking", `--remote-debugging-port=${chromePort}`, `--user-data-dir=${profile}`, "--window-size=1400,1000", "about:blank"]);
  await waitFor(async () => (await fetch(`${url}api/health`)).ok, 30000, "ALBIS");
  await waitFor(async () => (await fetch(`http://127.0.0.1:${dcuPort}/detector/api/1.8.0/status/state`)).ok, 15000, "the simulated detector");
  let targets = [];
  await waitFor(async () => {
    targets = (await (await fetch(`http://127.0.0.1:${chromePort}/json/list`)).json()).filter((t) => t.type === "page");
    return targets.length > 0;
  }, 15000, "Chrome");
  socket = await connect(targets[0]);
  await send("Emulation.setDeviceMetricsOverride", { width: 1400, height: 1000, deviceScaleFactor: 1, mobile: false });
  await send("Page.navigate", { url });
  await until(async () => (await call(inPage.fileOptions)) > 2, 20000);

  // 1. A file series: open, step, play.
  check("the file list offers the series", await openFile(series[0]));
  check(
    "open a CBF series",
    await until(async () => (await text("footer-file")).includes("in16c_010001.cbf 1 / 5") && (await text("meta-shape")) !== "-"),
    await text("footer-file"),
  );
  recorded.series_open = await viewer();
  // A click while a frame is still loading is dropped, as it is for a user;
  // step until the frame is reached, clicking again if one was dropped.
  async function stepTo(frame, total) {
    for (let attempt = 0; attempt < 4; attempt += 1) {
      await click("#btn-next");
      const reached = async () => (await value("frame-index")) === String(frame) && (await text("footer-file")).includes(`${frame} / ${total}`);
      if (await until(reached, 3000)) return true;
    }
    return false;
  }
  check("step two frames forward", (await stepTo(2, 5)) && (await stepTo(3, 5)), `frame ${await value("frame-index")}`);
  await sleep(800);
  recorded.series_step = await viewer();
  const beforePlay = Number(await value("frame-index"));
  await click("#btn-play");
  await sleep(2500);
  await click("#btn-play");
  const afterPlay = Number(await value("frame-index"));
  check("playback moves the frame", afterPlay !== beforePlay, `frame ${beforePlay} -> ${afterPlay}`);

  // 2. An HDF5 stack.
  check("the file list offers the HDF5 stack", await openFile("compressed_stack.h5"));
  check(
    "open an HDF5 stack",
    await until(async () => (await text("footer-file")).includes("compressed_stack.h5") && (await value("dataset-select")) !== ""),
    await value("dataset-select"),
  );
  await sleep(1500);
  recorded.hdf5_open = { ...(await viewer()), dataset: await value("dataset-select") };
  await click("#btn-next");
  check("step an HDF5 frame", await until(async () => (await value("frame-index")) === "2"), `frame ${await value("frame-index")}`);
  await sleep(800);
  recorded.hdf5_step = await viewer();

  // 3. Folder watch: a file written into the folder is opened.
  await setValue("autoload-mode", "file");
  await setValue("autoload-dir", path.join(data, "watch"));
  await setValue("autoload-interval", "500");
  await setChecked("autoload-watch-enabled", true);
  await sleep(1500);
  copyFileSync(path.join(data, series[1]), path.join(data, "watch", "watched_0001.cbf"));
  check("folder watch opens a new file", await until(footerHas("watched_0001.cbf"), 20000), await text("footer-file"));
  await sleep(800);
  recorded.watch_first = await viewer();
  await sleep(1200);
  copyFileSync(path.join(data, series[2]), path.join(data, "watch", "watched_0002.cbf"));
  check("folder watch follows the next file", await until(footerHas("watched_0002.cbf"), 20000), await text("footer-file"));
  await setChecked("autoload-watch-enabled", false);

  // 4. Remote Stream API: frames pushed by another program.
  await setValue("autoload-mode", "remote");
  await sleep(1000);
  for (const image of [42, 43]) {
    const pixels = new Uint16Array(64 * 64).map((_, i) => (i * 7 + image) % 1000);
    const form = new FormData();
    form.append("meta", JSON.stringify({ format: "raw", dtype: "<u2", shape: [64, 64], display_name: "live check", series_number: 1, image_number: image }));
    form.append("image", new Blob([pixels.buffer]), "frame.raw");
    const res = await fetch(`${url}api/remote/v1/frame?source_id=default`, { method: "POST", body: form });
    check(`push remote frame ${image}`, res.ok, `HTTP ${res.status}`);
    check(`viewer shows remote frame ${image}`, await until(async () => (await text("remote-image")) === String(image), 15000), await text("remote-image"));
  }
  await sleep(800);
  recorded.remote = { ...(await viewer()), series: await text("remote-series"), image: await text("remote-image") };

  // 5. SIMPLON monitor of the simulated detector.
  const dcu = `http://127.0.0.1:${dcuPort}`;
  const put = (p) => fetch(`${dcu}${p}`, { method: "PUT", headers: { "Content-Type": "application/json" }, body: "{}" });
  await put("/detector/api/1.8.0/command/initialize");
  await waitFor(async () => (await (await fetch(`${dcu}/detector/api/1.8.0/status/state`)).json()).value === "idle", 15000, "the detector to initialize");
  await setValue("simplon-url", dcu);
  await setChecked("simplon-enable", true);
  await setValue("autoload-mode", "simplon");
  await sleep(1000);
  await put("/detector/api/1.8.0/command/arm");
  put("/detector/api/1.8.0/command/trigger"); // answers when the series ends
  check("SIMPLON monitor shows a frame", await until(filled("simplon-series"), 20000), `series ${await text("simplon-series")}`);
  await sleep(800);
  const simplon = await viewer();
  recorded.simplon = { shape: simplon.shape, dtype: simplon.dtype, series: await text("simplon-series") };

  // 6. JUNGFRAUJOCH preview stream from the simulated publisher.
  await setValue("jfjoch-preview-endpoint", `tcp://127.0.0.1:${jfjochPort}`);
  await setValue("autoload-mode", "jungfraujoch");
  check("JUNGFRAUJOCH stream shows a frame", await until(filled("jfjoch-seq"), 20000), `seq ${await text("jfjoch-seq")}`);
  const firstSeq = Number(await text("jfjoch-seq"));
  check("JUNGFRAUJOCH stream keeps coming", await until(async () => Number(await text("jfjoch-seq")) > firstSeq, 10000), `seq ${await text("jfjoch-seq")}`);
  check("JUNGFRAUJOCH spots arrive with the frames", await until(async () => (await text("jfjoch-reflections")) === "2", 10000), await text("jfjoch-reflections"));
  const jfjoch = await viewer();
  recorded.jfjoch = { shape: jfjoch.shape, dtype: jfjoch.dtype, series: await text("jfjoch-series") };

  // 7. Back to a file from a live source.
  await setValue("autoload-mode", "file");
  await setValue("autoload-dir", data);
  await until(async () => Boolean(await call(inPage.findFile, [series[4]])), 10000);
  check("the file list offers a file again", await openFile(series[4]));
  check("open a file after live sources", await until(footerHas("in16c_010005.cbf")), await text("footer-file"));
  await sleep(800);
  recorded.back_to_file = await viewer();

  // 8. A handoff job, a launch target, Duplicate Window.
  const queued = await fetch(`${url}api/handoff/v1/jobs`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ manifest_path: path.join(data, "manifest.json") }) });
  check("queue a handoff job", queued.ok, `HTTP ${queued.status}`);
  check("the handoff job is opened", await until(footerHas("in16c_010004.cbf")), await text("footer-file"));
  await send("Page.navigate", { url: "about:blank" });
  await sleep(300);
  await send("Page.navigate", { url: `${url}#albis-open=${encodeURIComponent(path.join(data, series[2]))}` });
  check("a launch target is opened", await until(footerHas("in16c_010003.cbf"), 20000), await text("footer-file"));
  check("the launch target leaves the address", (await call(inPage.hash)) === "", await call(inPage.hash));
  const before = (await (await fetch(`http://127.0.0.1:${chromePort}/json/list`)).json()).filter((t) => t.type === "page");
  await click('.menu-item[data-menu="file"]');
  await sleep(400);
  await click('[data-action="duplicate-window"]');
  let clone;
  await waitFor(async () => {
    const now = (await (await fetch(`http://127.0.0.1:${chromePort}/json/list`)).json()).filter((t) => t.type === "page");
    clone = now.find((t) => !before.some((b) => b.id === t.id));
    return Boolean(clone);
  }, 10000, "the duplicated window").catch(() => {});
  if (clone) {
    const cloneSocket = await connect(clone);
    const end = Date.now() + 15000;
    let cloneFile = "";
    while (Date.now() < end && !cloneFile.includes("in16c_010003.cbf")) {
      cloneFile = (await text("footer-file", cloneSocket)) || "";
      await sleep(300);
    }
    check("Duplicate Window opens the same file", cloneFile.includes("in16c_010003.cbf"), cloneFile);
    cloneSocket.close();
  } else {
    check("Duplicate Window opens the same file", false, "no new window");
  }

  check("no uncaught errors in the page", pageErrors.length === 0, pageErrors.slice(0, 3).join(" | "));
}

let failed = false;
try {
  await main();
} catch (error) {
  console.error(error.message);
  failed = true;
} finally {
  for (const child of children.reverse()) child.kill();
  await sleep(500);
  rmSync(data, { recursive: true, force: true });
}

writeFileSync(outFile, `${JSON.stringify({ checks: results, recorded }, null, 2)}\n`);
failed ||= results.some((r) => !r.ok);
if (compareFile && existsSync(compareFile)) {
  const earlier = JSON.parse(readFileSync(compareFile, "utf8")).recorded || {};
  const keys = new Set([...Object.keys(earlier), ...Object.keys(recorded)]);
  let differences = 0;
  for (const key of keys) {
    const a = JSON.stringify(earlier[key] ?? null);
    const b = JSON.stringify(recorded[key] ?? null);
    if (a !== b) {
      differences += 1;
      console.info(`DIFF ${key}\n  before ${a}\n  after  ${b}`);
    }
  }
  console.info(differences ? `${differences} recorded values differ from ${compareFile}` : `recorded values match ${compareFile}`);
  failed ||= differences > 0;
}
console.info(`\n${results.filter((r) => r.ok).length}/${results.length} checks passed; fingerprint in ${outFile}`);
process.exit(failed ? 1 : 0);
