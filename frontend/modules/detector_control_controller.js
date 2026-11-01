/**
 * Detector tab (beta): control a DECTRIS detector over the SIMPLON API.
 *
 * Every field is built from what the detector says about the parameter --
 * value, type, unit, limits, allowed values, access -- so each detector model
 * gets its own correct form. The backend (/api/detector/*) proxies the
 * detector; it answers 404 unless Settings -> Viewer -> Beta: detector control
 * is on, which is also what shows this tab.
 *
 * SIMPLON reports no image counter while a series runs, so progress is the
 * elapsed time against images x triggers x frame time, and is labelled as an
 * estimate. Anything the detector sends (its description, file names) is put
 * on the page as text, never as markup.
 */

import { t } from "./i18n.js";
import { attachDetectorPicker } from "./detector_picker.js";
import { knownDetectors } from "./known_detectors.js";
import { fetchResponse } from "./http.js";
import { describeSimplonFailure } from "./simplon_diagnostics.js";
import { normalizeSimplonBaseUrl } from "./simplon_url_utils.js";
import { createDetectorAdvancedController } from "./detector_advanced_controller.js";
import { createDetectorCommandsController } from "./detector_commands_controller.js";
import { el } from "./detector_control_dom.js";
import {
  readFollow,
  readOpen,
  readQuick,
  readStored,
  writeFollow,
  writeOpen,
  writeStored,
} from "./detector_control_storage.js";
import {
  BUSY_STATES,
  CONTINUOUS_HOURS,
  detectorPage,
  displayValue,
  formatDuration,
  validateInput,
  withKnownUnits,
} from "./detector_control_utils.js";
import { createDetectorOutputsController } from "./detector_outputs_controller.js";
import { createDetectorParamsController } from "./detector_params_controller.js";
import { createDetectorQuickController } from "./detector_quick_controller.js";
import { createDetectorStateController } from "./detector_state_controller.js";

const POLL_ACTIVE_MS = 1000;
// The SIMPLON version a new detector is first asked in; its answer says
// which one it speaks.
const DEFAULT_API_VERSION = "1.8.0";
const POLL_IDLE_MS = 3000;
const LOG_LIMIT = 12;
// The progress bar runs on the clock, not on the status polls: a poll to a
// real detector takes a variable time, and the bar jumped with it.
const PROGRESS_TICK_MS = 100;

export function createDetectorControlController({ apiBase, elements, callbacks = {} }) {
  const {
    tab,
    content,
    urlInput,
    connectBtn,
    addressHost,
    message,
    summary,
    live,
    model,
    serial,
    statePill,
    stateText,
    progress,
    progressBar,
    progressLeft,
    progressRight,
    confirm,
    confirmText,
    confirmYes,
    confirmNo,
    primaryBtn,
    stopBtn,
    followToggle,
    sensors,
    notice,
    noticeText,
    noticeDismiss,
    sections,
    paramsHost,
    lockNote,
    seriesSummary,
    outputSummary,
    outputsHost,
    filesHost,
    logHost,
    advancedHost,
    commandsHost,
  } = elements;
  const { getPanelTab, setPanelTab, watchLive, refreshInfoTips, openPath, onChange } = callbacks;

  let enabled = false;
  let url = "";
  let version = DEFAULT_API_VERSION;
  let params = { detector: {}, monitor: {}, filewriter: {}, stream: {} };
  let status = null;
  let lastModes = null;
  let pollTimer = null;
  let series = null;
  let seriesStarted = 0;
  let acquiring = false;
  let confirmAction = null;
  let files = [];
  let followedBefore = false;
  // Internal enable: images triggered since arm, and the exposure being taken.
  let triggered = 0;
  let triggerExposure = 0;
  // The series last taken here, for the result line; Continuous while it runs.
  let lastResult = null;
  let armedPrefix = "";
  // Stop was confirmed for the running series: its result says so, rather
  // than the image count it was set to.
  let stopRequested = false;
  let continuous = false;
  // A quick action from its first temporary change to its last restore. Its
  // own writes pass through states the panel would otherwise react to -- the
  // file writer and stream briefly off, the detector idle between steps -- so
  // the buttons and the pre-flight line hold still until it is done.
  let quickRunning = false;
  // Stop pressed while a quick action was still setting up or arming: it
  // must not go on to take its series.
  let quickCancelled = false;
  let quick = readQuick();
  let progressTimer = null;

  // The panel's state, for its parts in their own modules (settings, Data
  // output, Advanced, commands, the state view, quick actions): read and
  // written through here, so there is one copy of each.
  const ctx = {
    get url() { return url; },
    get version() { return version; },
    get params() { return params; },
    get status() { return status; },
    get lastModes() { return lastModes; },
    set lastModes(value) { lastModes = value; },
    get series() { return series; },
    get seriesStarted() { return seriesStarted; },
    get acquiring() { return acquiring; },
    get files() { return files; },
    set files(value) { files = value; },
    get triggered() { return triggered; },
    get triggerExposure() { return triggerExposure; },
    get lastResult() { return lastResult; },
    get stopRequested() { return stopRequested; },
    set stopRequested(value) { stopRequested = value; },
    get continuous() { return continuous; },
    set continuous(value) { continuous = value; },
    get quickRunning() { return quickRunning; },
    set quickRunning(value) { quickRunning = value; },
    get quickCancelled() { return quickCancelled; },
    set quickCancelled(value) { quickCancelled = value; },
    get quick() { return quick; },
    set quick(value) { quick = value; },
    get writeSeq() { return writeSeq; },
    set writeSeq(value) { writeSeq = value; },
  };
  // Internal enable: each Trigger sends its own exposure, chosen here, next to
  // the button, while the detector is armed.
  const exposureBox = primaryBtn ? el("label", "detector-exposure") : null;
  const exposureInput = exposureBox ? el("input") : null;
  const exposureHint = exposureBox ? el("span", "detector-hint is-error") : null;
  if (exposureBox) {
    exposureInput.type = "text";
    exposureInput.inputMode = "decimal";
    exposureInput.autocomplete = "off";
    exposureInput.classList.add("is-number");
    const field = el("span", "detector-field");
    field.append(exposureInput, el("span", "detector-unit", "s"));
    exposureBox.append(el("span", "", t("detector.exposure.label")), field, exposureHint);
    exposureBox.hidden = true;
    primaryBtn.before?.(exposureBox);
    exposureInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && primaryBtn.dataset.action === "trigger") {
        event.preventDefault();
        void trigger();
      }
    });
  }

  // Beside Acquire: Snap (one image now) and Continuous (until Stop), one
  // block of equal widths; while anything runs, Stop takes exactly their
  // place, so Acquire never changes size or position. Under the buttons, the
  // pre-flight check before a series and the result after.
  const actionsRow = primaryBtn?.parentElement || null;
  const quickGroup = actionsRow ? el("div", "detector-side") : null;
  const snapBtn = actionsRow ? el("button", "btn btn-secondary detector-quick", t("detector.action.snap")) : null;
  const continuousBtn = actionsRow ? el("button", "btn btn-secondary detector-quick", t("detector.action.continuous")) : null;
  const slot = actionsRow ? el("div", "detector-slot") : null;
  const preflightHost = actionsRow ? el("div", "detector-preflight") : null;
  const resultHost = actionsRow ? el("div", "detector-result") : null;
  if (actionsRow) {
    snapBtn.type = "button";
    continuousBtn.type = "button";
    labelQuickButtons();
    snapBtn.disabled = true;
    continuousBtn.disabled = true;
    quickGroup.append(snapBtn, continuousBtn);
    if (stopBtn) quickGroup.append(stopBtn);
    primaryBtn.after(quickGroup);
    // One slot under the buttons, at least a line high even while a series
    // is armed and nothing shows: a question or notice, the check before a
    // series, its progress while it runs, the result after. Nothing above
    // the buttons comes and goes, so they stay where they are.
    if (confirm) slot.append(confirm);
    if (notice) slot.append(notice);
    slot.append(preflightHost, resultHost);
    if (progress) slot.append(progress);
    actionsRow.after(slot);
    snapBtn.addEventListener("click", () => void snap());
    continuousBtn.addEventListener("click", () => void runContinuous());
  }
  // Their settings -- the exposure, the rate, whether images show in the
  // viewer -- fold away behind ⚙ at the end of the row: the defaults suit
  // most people, and the buttons' tooltips say what they will do.
  const { snap, runContinuous, quickSettings } = createDetectorQuickController({
    ctx,
    sendParam: (...args) => sendParam(...args),
    acquire,
    renderState: () => renderState(),
    poll,
    log,
    labelQuickButtons,
  });
  const quickBox = actionsRow ? quickSettings() : null;
  if (quickBox) {
    const followRow = followToggle?.closest?.(".detector-follow");
    if (followRow) quickBox.prepend(followRow);
    slot.after(quickBox);
    quickBox.id = "detector-quick-settings";
    const gear = el("button", "btn btn-secondary detector-quick-toggle", "⚙\uFE0E");
    gear.type = "button";
    gear.setAttribute("aria-controls", quickBox.id);
    const showQuick = (open) => {
      quickBox.hidden = !open;
      gear.classList.toggle("is-open", open);
      gear.setAttribute("aria-expanded", String(open));
    };
    showQuick(readOpen("quick"));
    gear.addEventListener("click", () => {
      const open = quickBox.hidden;
      showQuick(open);
      writeOpen("quick", open);
    });
    gear.title = t("detector.quick.options");
    gear.setAttribute("aria-label", gear.title);
    quickGroup.after(gear);
  }

  // Plain names on the buttons, to keep the row narrow; the timings are in
  // their tooltips and in the settings behind ⚙.
  function labelQuickButtons() {
    if (!snapBtn) return;
    const exposure = formatDuration(quick.snapExposure);
    const rate = `${+quick.continuousRate.toPrecision(3)} Hz`;
    snapBtn.textContent = t("detector.action.snap");
    continuousBtn.textContent = t("detector.action.continuous");
    snapBtn.title = t("detector.snap.hint", { exposure });
    continuousBtn.title = t("detector.continuous.hint", { rate, hours: CONTINUOUS_HOURS });
  }

  // Once connected, the address shrinks to a link beside the serial number,
  // with Change to bring the field back: the name already says it connected.
  // "D029661 · 192.168.20.191 ↗ · Change", on the name's line.
  const whereLine = model ? el("span", "detector-where") : null;
  if (whereLine && serial?.before) {
    const side = el("span", "detector-ident-side");
    serial.before(side);
    side.append(serial, whereLine);
  } else if (whereLine) {
    model.parentElement?.after?.(whereLine);
  }

  // Whether the panel is connected: a detector answered, and its view shows.
  const isConnected = () => Boolean(url) && live?.hidden === false;

  // Beside Connect while the address of a connected detector is being
  // changed: back to that detector without connecting anywhere.
  const cancelBtn = connectBtn ? el("button", "btn btn-secondary", t("detector.action.cancel")) : null;
  if (cancelBtn) {
    cancelBtn.type = "button";
    cancelBtn.hidden = true;
    cancelBtn.addEventListener("click", () => cancelChange());
    connectBtn.after?.(cancelBtn);
  }

  // The detectors connected before, under the address: picking one connects.
  attachDetectorPicker({ input: urlInput, onPick: () => void connect() });

  // Beside the detector's name: keep it in that list (☆ -> ★).
  const keepBtn = model ? el("button", "linkish detector-keep") : null;
  if (keepBtn) {
    keepBtn.type = "button";
    keepBtn.addEventListener("click", () => {
      const entry = knownDetectors.find(url);
      if (entry) knownDetectors.setKept(url, !entry.kept);
    });
    // With the name on one side of the line, the serial and address on the other.
    const main = el("span", "detector-ident-main");
    model.before(main);
    main.append(model, keepBtn);
    knownDetectors.subscribe(() => renderKeep());
  }

  function renderKeep() {
    if (!keepBtn) return;
    const entry = url ? knownDetectors.find(url) : null;
    keepBtn.hidden = !entry;
    const kept = Boolean(entry?.kept);
    keepBtn.textContent = kept ? "★" : "☆";
    keepBtn.classList.toggle("is-kept", kept);
    keepBtn.setAttribute("aria-pressed", kept ? "true" : "false");
    keepBtn.title = kept ? t("known_detectors.unkeep") : t("known_detectors.keep");
    keepBtn.setAttribute("aria-label", keepBtn.title);
  }

  function showAddress(show) {
    if (addressHost) addressHost.hidden = !show;
    if (whereLine) whereLine.hidden = show;
    if (cancelBtn) cancelBtn.hidden = !(show && isConnected());
  }

  function renderWhere() {
    if (!whereLine) return;
    whereLine.replaceChildren();
    const change = el("button", "linkish", t("detector.action.change"));
    change.type = "button";
    change.addEventListener("click", () => {
      showAddress(true);
      urlInput?.focus?.();
    });
    // The address opens the control unit's own web interface, served at its
    // root by every DCU (an EIGER1's: "EIGER Detector Control Unit").
    const shown = url.replace(/^https?:\/\//, "").replace(/\/+$/, "");
    const webUi = detectorPage(url);
    let address;
    if (webUi) {
      address = el("a", "linkish detector-external", `${shown} ↗`);
      address.href = webUi;
      address.target = "_blank";
      address.rel = "noopener";
      address.title = t("detector.where.web_ui");
    } else {
      address = el("span", "", shown);
    }
    whereLine.append(address, " · ", change);
  }

  // Recovery hints sit right under the sensors, in the detector card.
  const recoverHost = sensors ? el("div", "detector-recover") : null;
  sensors?.after?.(recoverHost);

  // ---------- small pieces ----------
  function setMessage(text, variant = "") {
    if (!message) return;
    message.textContent = text || "";
    message.classList.toggle("is-hidden", !text);
    message.classList.toggle("is-busy", variant === "busy");
    message.classList.toggle("is-ok", variant === "ok");
    message.classList.toggle("is-error", variant === "error");
  }

  function log(text, kind = "") {
    if (!logHost) return;
    const li = el("li", kind ? `is-${kind}` : "");
    li.append(el("time", "", new Date().toLocaleTimeString([], { hour12: false })), el("span", "", text));
    logHost.prepend(li);
    while (logHost.children.length > LOG_LIMIT) logHost.lastChild.remove();
  }

  function ask(text, yesLabel, noLabel, onYes) {
    if (!confirm) return;
    confirmText.textContent = text;
    confirmYes.textContent = yesLabel;
    confirmNo.textContent = noLabel;
    confirmAction = onYes;
    confirm.hidden = false;
    // The question stands in for the check: "Acquire anyway?" repeats it.
    if (preflightHost) preflightHost.hidden = true;
    confirmYes.focus?.();
    // Asked from Data output, further down the tab.
    confirm.scrollIntoView?.({ block: "nearest", behavior: "smooth" });
  }

  function closeConfirm() {
    confirm.hidden = true;
    if (preflightHost) preflightHost.hidden = false;
    const action = confirmAction;
    confirmAction = null;
    return action;
  }

  function reason(detail) {
    if (detail && typeof detail === "object") {
      // The detector's own explanation says more than the status code.
      if (detail.detector_message) return t("detector.failure.detector_says", { message: detail.detector_message });
      return detail.code ? describeSimplonFailure({ api_version: version, ...detail }) : detail.summary || "";
    }
    return String(detail || "");
  }

  async function request(path, init) {
    let res;
    try {
      res = await fetchResponse(`${apiBase}${path}`, init);
    } catch (err) {
      if (!(err?.status >= 400)) throw err;
      // http.js keeps a structured detail as detailData, a sentence as detail.
      throw Object.assign(new Error(reason(err.detailData ?? err.detail) || err.message), { status: err.status });
    }
    // A command may answer with an empty body, which is a success.
    return res.json().catch(() => null);
  }

  const query = () => `url=${encodeURIComponent(url)}&version=${encodeURIComponent(version)}`;

  function detectorState() {
    return String(status?.detector?.state || "").toLowerCase();
  }

  function isBusy() {
    return BUSY_STATES.has(detectorState()) || Boolean(status?.command?.running);
  }

  // ---------- the panel's parts ----------
  const { paramRow, sendParam, renderParams, coreKeys } = createDetectorParamsController({
    ctx,
    content,
    paramsHost,
    refreshInfoTips,
    request,
    log,
    ask,
    renderOutputs: (...args) => renderOutputs(...args),
    renderAdvanced: () => renderAdvanced(),
    renderState: () => renderState(),
  });
  const commands = createDetectorCommandsController({
    ctx,
    recoverHost,
    commandsHost,
    refreshInfoTips,
    reason,
    runCommand,
    poll,
    refreshFiles: () => refreshFiles(),
    refreshDescription,
    initialize,
    log,
    renderOutputs: (...args) => renderOutputs(...args),
    isBusy,
    rememberArmed,
    trigger,
  });
  const { renderRecovery, renderCommands } = commands;
  const { renderOutputs, setMode, refreshFiles, renderFiles } = createDetectorOutputsController({
    ctx,
    apiBase,
    outputsHost,
    filesHost,
    refreshInfoTips,
    request,
    query,
    ask,
    log,
    renderState: () => renderState(),
    paramRow,
    sendParam,
    feedback: commands.feedback,
    track: commands.track,
    commandButton: commands.commandButton,
    feedbackNote: commands.feedbackNote,
    deleteFiles: commands.deleteFiles,
    deleteArmedText: commands.deleteArmedText,
    resetStream: commands.resetStream,
    recoveryButton: commands.recoveryButton,
    initializeOutput: commands.initializeOutput,
    clearMonitor: commands.clearMonitor,
  });
  const { renderAdvanced } = createDetectorAdvancedController({
    ctx,
    advancedHost,
    paramRow,
    coreKeys,
    refreshInfoTips,
    renderState: () => renderState(),
  });
  const { preflight, renderState, renderProgress } = createDetectorStateController({
    ctx,
    content,
    summary,
    statePill,
    stateText,
    progress,
    progressBar,
    progressLeft,
    progressRight,
    primaryBtn,
    stopBtn,
    sensors,
    lockNote,
    seriesSummary,
    outputSummary,
    exposureBox,
    exposureInput,
    quickGroup,
    snapBtn,
    continuousBtn,
    preflightHost,
    resultHost,
    quickBox,
    setMessage,
    request,
    log,
    openPath,
    onChange,
    detectorState,
    isBusy,
    renderRecovery,
  });

  // ---------- connecting ----------
  // The new address is tried on its own, with its own requests: the panel
  // takes it over only once it answers. Until then the detector already
  // connected stays connected and polled -- it may be in the middle of a
  // series, and a typo in the address must not take its Stop button away.
  // A first connect that fails leaves the panel in its starting state, with
  // nothing polled in the background.
  async function connect() {
    const normalized = normalizeSimplonBaseUrl(urlInput?.value || "");
    if (!normalized) {
      setMessage(t("simplon.probe.enter_address"), "error");
      urlInput?.focus?.();
      return;
    }
    if (urlInput) urlInput.value = normalized;
    setMessage(t("detector.message.connecting", { url: normalized }), "busy");
    if (connectBtn) connectBtn.disabled = true;
    let described;
    let answered;
    try {
      // Not the monitor's connection test: that reads a monitor setting, and
      // before initialize a detector answers only its state ("Before
      // initializing the detector only the detector state is available!") --
      // a freshly switched-on detector would look like no detector at all.
      // Asked in the default version; the answer says which it speaks.
      const at = (path, apiVersion) => `${path}?url=${encodeURIComponent(normalized)}&version=${encodeURIComponent(apiVersion)}`;
      described = await request(at("/detector/describe", DEFAULT_API_VERSION));
      answered = await request(at("/detector/status", String(described?.api_version || DEFAULT_API_VERSION)));
    } catch (err) {
      setMessage(err.message || t("simplon.probe.request_failed"), "error");
      // The address stays open with what was typed, to correct it.
      showAddress(true);
      if (!isConnected()) {
        url = "";
        stopPolling();
      }
      return;
    } finally {
      if (connectBtn) connectBtn.disabled = false;
    }
    // It answered: from here on this is the detector.
    stopPolling();
    url = normalized;
    writeStored(url);
    status = answered;
    lastModes = null;
    files = [];
    lastResult = null;
    applyDescription(described);
    renderState();
    const name = displayValue(params.detector.description) || url;
    knownDetectors.record({
      address: url,
      model: displayValue(params.detector.description),
      serial: displayValue(params.detector.detector_number),
    });
    renderKeep();
    setMessage("");
    renderWhere();
    showAddress(false);
    log(t("detector.log.connected", { detector: name }));
    if (live) live.hidden = false;
    sections?.forEach((section) => { section.hidden = false; });
    onChange?.();
    schedulePoll();
    await refreshFiles();
  }

  // Back to the detector already connected, the address as it was.
  function cancelChange() {
    if (urlInput) urlInput.value = url;
    setMessage("");
    showAddress(false);
  }

  async function refreshDescription() {
    applyDescription(await request(`/detector/describe?${query()}`));
  }

  function applyDescription(payload) {
    // The detector says which SIMPLON it speaks (an EIGER1: 1.6.0); every
    // request from here on uses that.
    version = payload?.api_version ? String(payload.api_version) : DEFAULT_API_VERSION;
    params = withKnownUnits({ detector: {}, monitor: {}, filewriter: {}, stream: {}, ...(payload?.params || {}) });
    if (model) model.textContent = displayValue(params.detector.description) || t("detector.section.detector");
    if (serial) serial.textContent = displayValue(params.detector.detector_number);
    renderParams();
    renderOutputs();
    renderAdvanced();
    renderCommands();
  }

  // ---------- polling ----------
  function stopPolling() {
    if (pollTimer) window.clearTimeout(pollTimer);
    pollTimer = null;
  }

  function schedulePoll() {
    stopPolling();
    if (!enabled || !url) return;
    pollTimer = window.setTimeout(() => {
      void poll();
    }, isBusy() || acquiring ? POLL_ACTIVE_MS : POLL_IDLE_MS);
  }

  // Counts writes started and finished. A status answer whose request
  // overlapped a write may predate it: taken as news, a switch just flipped
  // on would read as "changed by another program" and show as off.
  let writeSeq = 0;

  async function poll() {
    if (!enabled || !url) return;
    try {
      const seq = writeSeq;
      const next = await request(`/detector/status?${query()}`);
      // The next round brings the state after the write.
      if (seq !== writeSeq) return;
      const before = detectorState();
      status = next;
      noticeExternalChanges();
      // Leaving "na" means initialize finished, possibly started elsewhere:
      // only now are the parameters readable.
      if (before === "na" && detectorState() !== "na") await refreshDescription();
      renderState();
      renderOutputs({ fromPoll: true });
    } catch (err) {
      if (stateText) stateText.textContent = err.message;
      if (statePill) {
        statePill.dataset.tone = "error";
        statePill.textContent = t("detector.state.unreachable");
      }
    } finally {
      schedulePoll();
    }
  }

  function noticeExternalChanges() {
    const modes = {
      filewriter: status?.filewriter?.mode,
      stream: status?.stream?.mode,
      monitor: status?.monitor?.mode,
    };
    const job = status?.command;
    // A stream reset switches the stream off and on again: not "elsewhere".
    if (job?.running && job.subsystem === "stream") return;
    if (lastModes) {
      for (const [name, mode] of Object.entries(modes)) {
        if (mode && lastModes[name] && mode !== lastModes[name]) {
          if (params[name]?.mode) params[name].mode.value = mode;
          log(t("detector.log.elsewhere", { what: t(`detector.output.${name}`) }), "elsewhere");
          if (notice) {
            noticeText.textContent = t("detector.notice.elsewhere");
            notice.hidden = false;
          }
        }
      }
    }
    lastModes = modes;
  }

  async function waitForCommand(command, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      await new Promise((resolve) => window.setTimeout(resolve, 400));
      const next = await request(`/detector/status?${query()}`);
      status = next;
      renderState();
      const job = next?.command;
      if (job && job.command === command && !job.running) return job;
    }
    return null;
  }

  // ---------- commands ----------
  // "initialize" alone would read as the detector's; name other subsystems.
  const commandName = (subsystem, command) => (subsystem === "detector" ? command : `${subsystem} ${command}`);

  async function sendCommand(subsystem, command, { quiet = false, value = undefined } = {}) {
    if (!quiet) log(t("detector.log.sent", { command: commandName(subsystem, command) }));
    try {
      return await request("/detector/command", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(value === undefined ? { url, version, subsystem, command } : { url, version, subsystem, command, value }),
      });
    } catch (err) {
      log(t("detector.log.failed", { command: commandName(subsystem, command), reason: err.message }), "error");
      throw err;
    }
  }

  async function runCommand(subsystem, command, timeoutMs = 60000, value = undefined) {
    const sent = await sendCommand(subsystem, command, { value });
    schedulePoll();
    // Disarm, cancel or abort sent while a trigger runs is carried out at
    // once and answered with its result; the status keeps showing the
    // trigger, so waiting for it there would wait until the timeout.
    const job = sent && sent.command === command && sent.running === false ? sent : await waitForCommand(command, timeoutMs);
    if (!job) return null;
    const name = commandName(subsystem, command);
    if (job.ok) log(t("detector.log.done", { command: name }));
    else log(t("detector.log.failed", { command: name, reason: reason(job.error) }), "error");
    return job;
  }

  async function initialize() {
    renderState();
    const job = await runCommand("detector", "initialize", 260000);
    if (job?.ok) await refreshDescription();
    await poll();
    return job;
  }

  function startAcquire() {
    const { issues } = preflight();
    if (issues.length) {
      ask(`${issues.join(" ")} ${t("detector.preflight.ask")}`, t("detector.confirm.acquire_anyway"), t("detector.action.cancel"), () => acquire());
      return;
    }
    void acquire();
  }

  async function acquire({ forceFollow = false } = {}) {
    acquiring = true;
    lastResult = null;
    try {
      const arm = await runCommand("detector", "arm", 130000);
      if (!arm?.ok) return;
      // Stopped while arming: disarm rather than start the series.
      if (quickRunning && quickCancelled) {
        await sendCommand("detector", "abort");
        return;
      }
      rememberArmed(arm);
      if (forceFollow || followToggle?.checked) await follow();
      const mode = String(params.detector.trigger_mode?.value || "");
      // Internal series starts at once; internal enable waits for the first
      // Trigger, so its exposure can be chosen too.
      if (mode === "ints") await trigger();
      else {
        await poll();
        if (mode === "inte") exposureInput?.focus?.();
      }
    } finally {
      acquiring = false;
      schedulePoll();
    }
  }

  // The series an arm opened: its number, and the files it writes (the name
  // pattern with $id as that number). Also for an Arm from Commands, so a
  // Trigger after it reports "Series 12 done", not "Series - done".
  function rememberArmed(job) {
    series = job.result?.["sequence id"] ?? job.result?.sequence_id ?? null;
    triggered = 0;
    armedPrefix = params.filewriter?.mode?.value === "enabled"
      ? String(params.filewriter.name_pattern?.value ?? "").replace("$id", String(series ?? ""))
      : "";
  }

  // Show the series as it is taken: switch the viewer to this detector's
  // monitor, switching the monitor on first if needed. Only after a
  // successful arm, so a refused series leaves the open image alone.
  async function follow() {
    if (!params.monitor?.mode) return;
    if (params.monitor.mode.value !== "enabled") await setMode("monitor", "enabled");
    if (params.monitor.mode.value !== "enabled") return;
    watchLive?.(url, version);
    if (!followedBefore) {
      followedBefore = true;
      log(t("detector.log.following"));
    }
  }

  async function trigger() {
    const mode = String(params.detector.trigger_mode?.value || "");
    let value;
    if (mode === "inte" && exposureInput) {
      // The exposure goes with the trigger, checked against the count time's limits.
      const checked = validateInput(params.detector.count_time || { value_type: "float" }, exposureInput.value);
      exposureInput.setAttribute("aria-invalid", String(!checked.ok));
      exposureHint.textContent = checked.ok ? "" : checked.error;
      if (!checked.ok) return;
      value = checked.value;
      triggerExposure = value;
    }
    seriesStarted = Date.now();
    const startedAt = seriesStarted;
    acquiring = true;
    stopRequested = false;
    progressTimer = window.setInterval(() => renderProgress(detectorState()), PROGRESS_TICK_MS);
    let job;
    try {
      job = await runCommand("detector", "trigger", 24 * 3600 * 1000, value);
    } finally {
      window.clearInterval(progressTimer);
      progressTimer = null;
      acquiring = false;
      seriesStarted = 0;
    }
    await poll();
    if (job?.ok && mode === "inte") {
      triggered += 1;
      const total = Number(params.detector.ntrigger?.value || 1);
      log(t("detector.log.image_done", { n: triggered, total, exposure: formatDuration(value) }));
      // The detector ends the series by itself after the last trigger.
      if (detectorState() !== "idle") {
        renderState();
        exposureInput?.focus?.();
        return;
      }
    }
    const seconds = (Date.now() - startedAt) / 1000;
    const stopped = stopRequested;
    stopRequested = false;
    if (job?.ok && !stopped) log(t("detector.log.series_done", { series: series ?? "-" }));
    await refreshFiles();
    if ((job?.ok || stopped) && !continuous) {
      const det = params.detector;
      lastResult = {
        series,
        count: Number(det.nimages?.value || 1) * Number(det.ntrigger?.value || 1),
        seconds: mode === "ints" || stopped ? seconds : null,
        prefix: armedPrefix,
        stopped,
      };
      renderState();
    }
  }

  function stop() {
    // A quick action saves nothing Stop could lose: no question. Before it
    // arms, the flag alone stops it; once armed, the detector is told.
    if (quickRunning) {
      quickCancelled = true;
      if (acquiring) void sendCommand("detector", "abort").finally(() => poll());
      return;
    }
    ask(t("detector.confirm.abort"), t("detector.action.abort"), t("detector.action.keep_running"), async () => {
      stopRequested = Boolean(seriesStarted);
      try {
        await sendCommand("detector", "abort");
      } finally {
        await poll();
      }
    });
  }

  // ---------- wiring ----------
  primaryBtn?.addEventListener("click", () => {
    const action = primaryBtn.dataset.action;
    if (action === "initialize") void initialize();
    else if (action === "acquire") startAcquire();
    else if (action === "trigger") void trigger();
  });
  stopBtn?.addEventListener("click", stop);
  if (followToggle) {
    followToggle.checked = readFollow();
    followToggle.addEventListener("change", () => writeFollow(followToggle.checked));
  }
  connectBtn?.addEventListener("click", () => void connect());
  urlInput?.addEventListener("keydown", (event) => {
    // Enter on a highlighted known detector was taken by the list.
    if (event.key === "Enter" && !event.defaultPrevented) {
      event.preventDefault();
      void connect();
    }
  });
  confirmNo?.addEventListener("click", () => {
    closeConfirm();
  });
  confirmYes?.addEventListener("click", () => {
    const action = closeConfirm();
    void action?.();
  });
  noticeDismiss?.addEventListener("click", () => {
    notice.hidden = true;
  });
  if (urlInput && !urlInput.value) urlInput.value = readStored();

  /** Show or hide the whole feature; a hidden tab cannot stay open. */
  function setEnabled(next) {
    enabled = Boolean(next);
    if (tab) tab.hidden = !enabled;
    if (!enabled) {
      stopPolling();
      if (getPanelTab?.() === "detector") setPanelTab?.("view");
    } else if (url) {
      schedulePoll();
    }
    onChange?.();
  }

  /** The detector this tab is connected to, for the Pixel Mask section; null if none. */
  function getConnection() {
    if (!enabled || !isConnected()) return null;
    return {
      url,
      version,
      serial: displayValue(params.detector.detector_number),
      model: displayValue(params.detector.description),
      locked: isBusy() || acquiring || quickRunning,
    };
  }

  return {
    setEnabled,
    getConnection,
    log,
    connect,
    poll,
    refreshDescription,
    get params() {
      return params;
    },
    get status() {
      return status;
    },
    _setConnection(nextUrl, nextVersion = "1.8.0") {
      url = nextUrl;
      version = nextVersion;
    },
    _renderAll() {
      renderParams();
      renderOutputs();
      renderAdvanced();
      renderCommands();
    },
    _setStatus(next) {
      status = next;
      renderState();
      renderOutputs();
    },
    _setParams(next) {
      params = withKnownUnits({ detector: {}, monitor: {}, filewriter: {}, stream: {}, ...next });
    },
    _pollRender(next) {
      status = next;
      renderState();
      renderOutputs({ fromPoll: true });
    },
    _setFiles(next) {
      files = next;
      renderFiles();
    },
  };
}
