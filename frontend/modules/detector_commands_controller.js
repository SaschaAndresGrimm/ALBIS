/**
 * Detector tab (beta): the commands sent by hand. Each button's feedback
 * where it was clicked, the recovery steps (re-initialize, stream reset,
 * deleting the files with a second click), the hint under the sensors, and
 * the Commands section.
 */

import { t } from "./i18n.js";
import { el, infoTip } from "./detector_control_dom.js";

export function createDetectorCommandsController({
  ctx,
  recoverHost,
  commandsHost,
  refreshInfoTips,
  reason,
  runCommand,
  poll,
  refreshFiles,
  refreshDescription,
  initialize,
  log,
  renderOutputs,
  isBusy,
  rememberArmed,
  trigger,
}) {
  // ---------- recovery ----------
  // Initialize, stream reset and deleting the files are recovery steps, not
  // everyday ones: each is offered next to what it fixes when the detector
  // reports that problem, and under Commands (the detector's) or with its
  // output in Data output (the stream's and the files').
  // ---------- command feedback ----------
  // What a command button started, shown where it was clicked: a spinner on
  // the button while it runs, then "Done" for a moment or why it failed until
  // the next try. Kept here, not on the button: a re-render can replace the
  // button while its command is still running.
  const FEEDBACK_DONE_MS = 4000;
  const feedback = {};
  // Order of the commands' starts: a row's note speaks for its latest.
  let feedbackSeq = 0;

  function renderFeedback() {
    renderOutputs();
    renderCommands();
  }

  async function track(id, work) {
    feedbackSeq += 1;
    const seq = feedbackSeq;
    feedback[id] = { state: "busy", seq };
    renderFeedback();
    try {
      const job = await work();
      feedback[id] = job && job.ok === false ? { state: "error", text: reason(job.error) || "", seq } : { state: "done", seq };
    } catch (err) {
      feedback[id] = { state: "error", text: err.message || "", seq };
    }
    renderFeedback();
    if (feedback[id].state === "done") {
      window.setTimeout(() => {
        if (feedback[id]?.state !== "done") return;
        delete feedback[id];
        renderFeedback();
      }, FEEDBACK_DONE_MS);
    }
  }

  // A button that shows its command's state.
  function commandButton(id, label, onClick, { title = "", danger = false } = {}) {
    const button = el("button", `btn btn-secondary${danger ? " is-danger" : ""}`, label);
    button.type = "button";
    button.dataset.command = id;
    if (title) button.title = title;
    if (feedback[id]?.state === "busy") {
      button.disabled = true;
      button.classList.add("is-busy");
      button.setAttribute("aria-busy", "true");
    }
    if (feedback[id]?.state === "armed") button.classList.add("is-armed");
    button.addEventListener("click", onClick);
    return button;
  }

  // Beside a row of command buttons: what the latest of them did.
  function feedbackNote(ids) {
    const note = el("span", "detector-feedback");
    note.setAttribute("role", "status");
    const entry = ids.map((each) => feedback[each]).filter(Boolean).sort((a, b) => b.seq - a.seq)[0] || null;
    if (entry?.state === "busy") note.textContent = t("detector.feedback.working");
    else if (entry?.state === "done") note.textContent = `✓ ${t("detector.feedback.done")}`;
    else if (entry?.state === "error") note.textContent = `✗ ${entry.text || t("simplon.probe.request_failed")}`;
    else if (entry?.state === "armed") note.textContent = deleteArmedText();
    if (entry) note.dataset.state = entry.state;
    return note;
  }

  // The recovery commands run at once, with their progress beside the
  // button: no question at the top of the tab, far from where they were
  // clicked. Only deleting the files, which cannot be undone, takes a second
  // click on the same spot (see deleteFiles).
  function reinitialize() {
    void track("detector.initialize", initialize);
  }

  function resetStream() {
    void track("stream.initialize", async () => {
      const job = await runCommand("stream", "initialize", 60000);
      if (job?.ok) {
        log(t("detector.log.stream_reset"));
        const mode = job.result?.mode;
        if (mode && ctx.params.stream?.mode) ctx.params.stream.mode.value = mode;
        ctx.lastModes = { ...(ctx.lastModes || {}), stream: mode || ctx.lastModes?.stream };
      }
      await poll();
      return job;
    });
  }

  // The first click arms it for a few seconds ("Click again to delete 35
  // files"), the second deletes: the one command that loses data.
  const DELETE_ARMED_MS = 4000;

  function deleteFiles() {
    const id = "filewriter.clear";
    if (feedback[id]?.state !== "armed") {
      feedbackSeq += 1;
      const seq = feedbackSeq;
      feedback[id] = { state: "armed", seq };
      renderFeedback();
      window.setTimeout(() => {
        if (feedback[id]?.seq !== seq || feedback[id].state !== "armed") return;
        delete feedback[id];
        renderFeedback();
      }, DELETE_ARMED_MS);
      return;
    }
    void track(id, async () => {
      const job = await runCommand("filewriter", "clear", 60000);
      if (job?.ok) log(t("detector.log.files_deleted"));
      await refreshFiles();
      await poll();
      return job;
    });
  }

  function deleteArmedText() {
    return ctx.files.length ? t("detector.action.delete_again", { count: ctx.files.length }) : t("detector.action.delete_again_unlisted");
  }

  function recoveryButton(label, onClick) {
    const button = el("button", "linkish", label);
    button.type = "button";
    button.addEventListener("click", onClick);
    return button;
  }

  // Under the sensors, only when something is wrong: high voltage not ready
  // or a failed command, with Re-initialize… next to the explanation. Not
  // while busy, and not in "na" or "error", where the main button already
  // says Initialize. Otherwise it lives under Commands.
  function renderRecovery(value) {
    if (!recoverHost) return;
    recoverHost.replaceChildren();
    if (!ctx.url || !ctx.status || value === "na" || value === "error" || isBusy() || ctx.acquiring) return;
    const job = ctx.status.command;
    const hv = String(ctx.status.detector?.["high_voltage/state"] || "");
    let text = "";
    if (job && job.ok === false && job.subsystem === "detector") {
      text = t("detector.recover.command_failed", { command: job.command });
    } else if (hv && hv.toUpperCase() !== "READY") {
      text = t("detector.recover.high_voltage", { state: hv });
    }
    if (!text) return;
    const button = recoveryButton(t("detector.action.reinitialize"), reinitialize);
    const note = el("div", "detector-warning is-caution", text);
    note.append(" ", button);
    recoverHost.append(note);
  }

  // Initialize resets an interface any other program may be using too, as
  // its tooltip says; the settings are re-read afterwards, since they may
  // have changed.
  function initializeOutput(name) {
    void track(`${name}.initialize`, async () => {
      const job = await runCommand(name, "initialize", 60000);
      if (job?.ok) await refreshDescription();
      await poll();
      return job;
    });
  }

  // Emptying the monitor's buffer loses nothing that is saved: no question.
  function clearMonitor() {
    void track("monitor.clear", async () => {
      const job = await runCommand("monitor", "clear", 30000);
      await poll();
      return job;
    });
  }

  // Every command the panel sends by hand: the series ones, then the
  // detector's, each group a row of buttons with a "?" each (what it does,
  // and its SIMPLON path) and one note for how the latest went. Re-initialize
  // is also offered under the sensors when something is wrong.
  function renderCommands() {
    if (!commandsHost) return;
    commandsHost.replaceChildren();
    const series = (command) => async () => {
      // Ending a running series early: its result says it stopped, as after Stop.
      if ((command === "disarm" || command === "cancel") && ctx.seriesStarted) ctx.stopRequested = true;
      const job = await runCommand("detector", command, 130000);
      if (command === "arm" && job?.ok) rememberArmed(job);
      await poll();
      return job;
    };
    const groups = [
      ["detector.group.series", [
        ["arm", "detector.action.arm", series("arm")],
        ["trigger", "detector.action.trigger", () => trigger()],
        ["disarm", "detector.action.disarm", series("disarm")],
        ["cancel", "detector.action.cancel_series", series("cancel")],
      ]],
      ["detector.section.detector", [
        ["refresh", "detector.action.refresh", async () => {
          await refreshDescription();
          log(t("detector.log.reread"));
        }],
        ["initialize", "detector.action.reinitialize", null],
      ]],
    ];
    for (const [heading, items] of groups) {
      commandsHost.append(el("div", "detector-group-label", t(heading)));
      const row = el("div", "detector-output-commands detector-command-row");
      const ids = [];
      for (const [command, label, work] of items) {
        const id = `detector.${command}`;
        ids.push(id);
        const pair = el("span", "detector-command");
        const onClick = work ? () => void track(id, work) : reinitialize;
        // Re-read settings is ALBIS's own: no SIMPLON command behind it.
        const path = command === "refresh" ? [] : [`detector/command/${command}`];
        pair.append(commandButton(id, t(label), onClick), infoTip(`detector.fix.${command}`, path));
        row.append(pair);
      }
      row.append(feedbackNote(ids));
      commandsHost.append(row);
    }
    refreshInfoTips?.();
  }

  return {
    feedback,
    track,
    commandButton,
    feedbackNote,
    reinitialize,
    resetStream,
    deleteFiles,
    deleteArmedText,
    recoveryButton,
    renderRecovery,
    initializeOutput,
    clearMonitor,
    renderCommands,
  };
}
