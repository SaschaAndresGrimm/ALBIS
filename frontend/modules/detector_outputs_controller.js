/**
 * Detector tab (beta): Data output. The file writer, stream and monitor
 * with their switches, states, main settings and closed More settings, and
 * the file writer's files on the detector.
 */

import { t } from "./i18n.js";
import { disclosure, el, infoTip } from "./detector_control_dom.js";
import { detectorDataPage, formatBytes, OUTPUT_MAIN, OUTPUT_STATES } from "./detector_control_utils.js";

// Below this much free detector storage, offer to clear it even if SIMPLON
// does not (yet) flag buffer_free as critical.
const LOW_STORAGE_BYTES = 1024 ** 3;

export function createDetectorOutputsController({
  ctx,
  apiBase,
  outputsHost,
  filesHost,
  refreshInfoTips,
  request,
  query,
  ask,
  renderState,
  paramRow,
  sendParam,
  feedback,
  track,
  commandButton,
  feedbackNote,
  deleteFiles,
  deleteArmedText,
  resetStream,
  recoveryButton,
  initializeOutput,
  clearMonitor,
  log,
}) {
  let filesError = "";

  function storageLow() {
    const fw = ctx.status?.filewriter || {};
    if (Array.isArray(fw.critical) && fw.critical.includes("buffer_free")) return true;
    return fw.buffer_free !== null && fw.buffer_free !== undefined && Number(fw.buffer_free) < LOW_STORAGE_BYTES;
  }

  // ---------- rendering: data output ----------
  function outputState(name) {
    const st = ctx.status?.[name] || {};
    const mode = st.mode ?? ctx.params[name]?.mode?.value;
    if (mode !== "enabled") return { tone: "", text: t("detector.output.state.disabled") };
    const value = String(st.state || "ready");
    const text = OUTPUT_STATES.has(value) ? t(`detector.output.state.${value}`) : value;
    const tone = value === "acquire" ? "busy" : value === "error" || value === "overflow" ? "error" : "ok";
    return { tone, text };
  }

  // What Data output shows, to skip a poll's re-render when nothing in it
  // changed: a rebuild would pull a field from under the cursor.
  let outputsKey = "";

  function renderOutputs({ fromPoll = false } = {}) {
    if (!outputsHost) return;
    const key = JSON.stringify([
      ["filewriter", "stream", "monitor"].map((name) => [ctx.params[name], ctx.status?.[name]]),
      ctx.params.detector.compression,
      ctx.series,
      storageLow(),
    ]);
    if (fromPoll) {
      if (key === outputsKey) return;
      // Typing in a field: the next poll after it is left catches up.
      if (outputsHost.contains(document.activeElement) && document.activeElement.matches?.("input, select")) return;
    }
    outputsKey = key;
    outputsHost.replaceChildren();
    for (const name of ["filewriter", "stream", "monitor"]) {
      const mode = ctx.params[name]?.mode;
      if (!mode) continue;
      const on = mode.value === "enabled";
      const block = el("div", "detector-output");
      block.dataset.output = name;
      block.append(outputHead(name, mode));
      if (on) {
        for (const key of OUTPUT_MAIN[name]) {
          if (!ctx.params[name][key]) continue;
          block.append(paramRow(name, key, ctx.params[name][key]));
          if (name === "filewriter" && key === "name_pattern") block.append(...nextFileNote());
        }
        if (name === "stream") block.append(...streamNotes());
        if (name === "filewriter" && storageLow()) {
          const warning = el("div", "detector-warning", t("detector.output.storage_low"));
          const armed = feedback["filewriter.clear"]?.state === "armed";
          warning.append(" ", recoveryButton(armed ? deleteArmedText() : t("detector.action.delete_files"), deleteFiles));
          block.append(warning);
        }
      }
      // The files stay listed with the writer off: they are still there.
      if (name === "filewriter" && filesHost) block.append(filesHost);
      if (on) {
        const more = moreSettings(name);
        if (more) block.append(more);
      }
      outputsHost.append(block);
    }
    renderFiles();
    refreshInfoTips?.();
    renderState();
  }

  // Switch, name and "?", and the state.
  function outputHead(name, mode) {
    const head = el("div", "detector-output-head");
    const switchLabel = el("label", "detector-switch");
    const toggle = el("input");
    toggle.type = "checkbox";
    toggle.checked = mode.value === "enabled";
    toggle.dataset.paramInput = "";
    toggle.setAttribute("aria-label", t(`detector.output.${name}`));
    switchLabel.append(toggle, el("span"));
    const title = el("div", "detector-param-name");
    const titleLine = el("span", "detector-param-label");
    titleLine.append(el("strong", "", t(`detector.output.${name}`)), infoTip(`detector.help.output.${name}`, `${name}/config/mode`));
    title.append(titleLine);
    const state = outputState(name);
    const dot = el("span", "detector-dot", state.text);
    dot.dataset.tone = state.tone;
    head.append(switchLabel, title, dot);
    // The DCU serves the files it wrote at /data/ (SIMPLON reference), which
    // is also where its own web interface lists them. The address is typed
    // text: only an http(s) one becomes a link, never `javascript:` and the like.
    toggle.addEventListener("change", () => {
      const next = toggle.checked ? "enabled" : "disabled";
      if (name === "stream" && next === "disabled") {
        toggle.checked = true;
        ask(t("detector.confirm.stream_off"), t("detector.confirm.turn_off"), t("detector.confirm.keep_on"), () => setMode(name, next));
        return;
      }
      void setMode(name, next);
    });
    return head;
  }

  // Under the name pattern: the file the next series writes, or why the
  // pattern overwrites. The number is known only once a series was armed
  // here; SIMPLON numbers them in turn. Until then $id stands in, as the "?"
  // explains.
  function nextFileNote() {
    const pattern = String(ctx.params.filewriter.name_pattern?.value ?? "");
    if (!pattern.includes("$id")) return [el("div", "detector-warning", t("detector.output.no_id"))];
    const known = ctx.series !== null && Number.isFinite(Number(ctx.series));
    const next = known ? pattern.replace("$id", String(Number(ctx.series) + 1)) : pattern;
    return [el("div", "detector-meta detector-output-note", t("detector.output.next_file", { name: `${next}_master.h5` }))];
  }

  function streamNotes() {
    const out = [];
    // SIMPLON counts images nobody picked up and resets the count at each
    // arm: with no receiver, a non-zero count is expected, not a fault -- the
    // short line says what, its "?" why.
    if (ctx.status?.stream?.dropped) {
      const dropped = el("div", "detector-meta detector-output-note", t("detector.output.dropped", { count: ctx.status.stream.dropped }));
      dropped.append(" ", infoTip("detector.help.dropped", "stream/status/dropped"));
      out.push(dropped);
    }
    if (String(ctx.status?.stream?.state || "") === "error") {
      const warning = el("div", "detector-warning", t("detector.output.stream_error"));
      warning.append(" ", recoveryButton(t("detector.action.reset_stream"), resetStream));
      out.push(warning);
    }
    return out;
  }

  // Everything else the output has, closed until wanted: its other settings
  // (for the file writer also the detector's compression, which the stream
  // uses too) and its SIMPLON commands. One home each: Reset stream and
  // Delete files are not repeated under Commands.
  function moreSettings(name) {
    const rows = [];
    if (name === "filewriter" && ctx.params.detector.compression) rows.push(paramRow("detector", "compression", ctx.params.detector.compression));
    for (const key of Object.keys(ctx.params[name]).sort()) {
      if (key === "mode" || OUTPUT_MAIN[name].includes(key)) continue;
      rows.push(paramRow(name, key, ctx.params[name][key]));
    }
    const commands = el("div", "detector-output-commands");
    const ids = [];
    const command = (id, label, title, onClick) => {
      ids.push(id);
      commands.append(commandButton(id, label, onClick, { title }));
    };
    if (name === "filewriter") command("filewriter.initialize", t("detector.action.initialize_filewriter"), t("detector.fix.initialize_filewriter"), () => initializeOutput("filewriter"));
    if (name === "stream") command("stream.initialize", t("detector.action.reset_stream"), t("detector.fix.stream"), resetStream);
    if (name === "monitor") {
      command("monitor.clear", t("detector.action.clear_monitor"), t("detector.fix.clear_monitor"), clearMonitor);
      command("monitor.initialize", t("detector.action.initialize_monitor"), t("detector.fix.initialize_monitor"), () => initializeOutput("monitor"));
    }
    commands.append(feedbackNote(ids));
    return disclosure(`more.${name}`, t("detector.output.more"), [...rows, commands]);
  }

  async function setMode(name, value) {
    const descriptor = ctx.params[name]?.mode;
    if (!descriptor) return;
    const row = el("div");
    const hint = el("div");
    await sendParam(name, "mode", value, { value: "" }, hint, row);
    if (hint.className.includes("is-error")) log(hint.textContent, "error");
    ctx.lastModes = { ...(ctx.lastModes || {}), [name]: value };
  }

  // ---------- files ----------
  async function refreshFiles() {
    if (!ctx.params.filewriter?.mode) return;
    try {
      const payload = await request(`/detector/files?${query()}`);
      ctx.files = Array.isArray(payload?.files) ? payload.files : [];
      filesError = "";
    } catch (err) {
      ctx.files = [];
      filesError = err.message || t("simplon.probe.request_failed");
    }
    renderFiles();
  }

  // The file writer's files, in a closed section of its own: newest first,
  // to download one by one, and Delete all files as a button.
  function renderFiles() {
    if (!filesHost) return;
    filesHost.replaceChildren();
    if (!ctx.params.filewriter?.mode) return;
    const refreshTracked = () => void track("files.refresh", async () => {
      await refreshFiles();
      return { ok: !filesError, error: filesError };
    });
    const refresh = commandButton("files.refresh", t("detector.action.refresh_files"), refreshTracked);
    if (!filesError && !ctx.files.length) {
      // Nothing to list: one line, no section to open.
      const link = el("button", "linkish", t("detector.action.refresh_files"));
      link.type = "button";
      link.addEventListener("click", refreshTracked);
      const line = el("div", "detector-meta detector-files-line", t("detector.files.none_inline"));
      line.append(" · ", link);
      const page = dataPageLink();
      if (page) line.append(" ", page);
      // Also where "Done" lands after deleting them all.
      const note = feedbackNote(["files.refresh", "filewriter.clear"]);
      if (note.dataset.state) line.append(" ", note);
      filesHost.append(line);
      return;
    }
    const children = [];
    if (filesError) {
      children.push(el("p", "detector-warning", t("detector.files.error", { reason: filesError })));
    } else {
      const list = el("ul", "detector-files");
      for (const file of ctx.files.slice(-12).reverse()) {
        const li = el("li");
        const link = el("a", "", file.name);
        link.href = `${apiBase}/detector/files/download?url=${encodeURIComponent(ctx.url)}&name=${encodeURIComponent(file.name)}`;
        link.download = file.name.split("/").pop();
        link.title = t("detector.action.download");
        li.append(link, el("span", "", formatBytes(file.size)));
        list.append(li);
      }
      children.push(list);
      if (ctx.files.length > 12) children.push(el("p", "detector-note", t("detector.files.more", { count: ctx.files.length - 12 })));
    }
    const actions = el("div", "detector-output-commands");
    actions.append(refresh);
    if (ctx.files.length) actions.append(commandButton("filewriter.clear", t("detector.action.delete_files"), deleteFiles, { title: t("detector.fix.files"), danger: true }));
    actions.append(feedbackNote(["files.refresh", "filewriter.clear"]));
    children.push(actions);
    const title = filesError ? t("detector.files.title") : t("detector.files.summary", { count: ctx.files.length });
    const page = dataPageLink();
    filesHost.append(disclosure("files", page ? [title, page] : title, children, "detector-files-section"));
  }

  // The detector's own list of these files, as a small ↗ beside their
  // heading. The DCU serves the files it wrote at /data/ (SIMPLON
  // reference), which is also where its web interface lists them. The
  // address is typed text: only an http(s) one becomes a link, never
  // `javascript:` and the like.
  function dataPageLink() {
    const href = detectorDataPage(ctx.url);
    if (!href) return null;
    const link = el("a", "detector-data-page", "↗");
    link.href = href;
    link.target = "_blank";
    link.rel = "noopener";
    link.title = t("detector.output.data_page");
    link.setAttribute("aria-label", t("detector.output.data_page"));
    // Opens the page; the section it sits in stays as it was.
    link.addEventListener("click", (event) => event.stopPropagation());
    return link;
  }

  return {
    renderOutputs,
    setMode,
    refreshFiles,
    renderFiles,
  };
}
