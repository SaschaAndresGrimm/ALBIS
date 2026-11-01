/**
 * Detector tab (beta): the settings. A row per parameter built from what the
 * detector says about it, writing a value and what it moved, the main view
 * (series, timing, energy and thresholds with their image switches), and
 * frame and count time linked at full duty cycle.
 */

import { t } from "./i18n.js";
import { chainIcon, el, infoTip, tipRight } from "./detector_control_dom.js";
import { readLinkTiming, writeLinkTiming } from "./detector_control_storage.js";
import {
  atFullDutyCycle,
  displayValue,
  formatDuration,
  helpKey,
  isBinary,
  isEnableMode,
  isOn,
  knownUnit,
  optionLabel,
  paramLabel,
  rangeText,
  readableValue,
  readoutTime,
  sameValue,
  thresholdRows,
  unitSymbol,
  validateInput,
} from "./detector_control_utils.js";

// The main view: what an acquisition needs, if the detector has it, ordered
// by how often it changes -- the series and its timing from one measurement to
// the next, the energy and thresholds once per experiment. On a wide panel the
// groups sit side by side, each a compact column. A row is a key; `oneOf` takes
// the first key the detector has (X-ray detectors call the energy
// photon_energy, electron-microscopy ones incident_energy); `thresholds` is one
// row per threshold energy, one to four, then the images the detector delivers.
const CORE_GROUPS = [
  { id: "series", rows: ["nimages", "ntrigger", "trigger_mode"] },
  // Frame time first: it sets the rate, and the count time fits inside it.
  { id: "timing", rows: ["frame_time", "count_time"] },
  { id: "energy", rows: [{ oneOf: ["photon_energy", "incident_energy"] }, { thresholds: true }] },
];

export function createDetectorParamsController({
  ctx,
  content,
  paramsHost,
  refreshInfoTips,
  request,
  log,
  ask,
  renderOutputs,
  renderAdvanced,
  renderState,
}) {
  // Wanted, not necessarily so: linked shows only while the detector is at
  // full duty cycle, whoever set the times.
  let linkTiming = readLinkTiming();

  // ---------- rendering: parameters ----------
  function labelFor(subsystem, key) {
    // With several thresholds, the first one is "Threshold 1", not "Threshold".
    if (subsystem === "detector" && key === "threshold_energy" && ctx.params.detector["threshold/2/energy"]) {
      return t("detector.param.threshold_n", { n: 1 });
    }
    // ...and a lone threshold is "Threshold", whichever name it goes by.
    if (subsystem === "detector" && key === "threshold/1/energy" && !ctx.params.detector["threshold/2/energy"]) {
      return t("detector.param.threshold_energy");
    }
    return paramLabel(key);
  }

  // A setting with an explanation shows its name and a "?" that explains it
  // and names its SIMPLON key; the others, all in Advanced and mostly without
  // a translated name, show the key itself.
  function paramRow(subsystem, key, descriptor, options = {}) {
    const row = buildParamRow(subsystem, key, descriptor, options);
    return options.info ? row : tipRight(row);
  }

  function buildParamRow(subsystem, key, descriptor, { help: helpOverride = "", alsoKeys = [], inline = false, info = false } = {}) {
    const row = el("div", "detector-param");
    row.dataset.key = `${subsystem}:${key}`;
    const id = `detector-p-${subsystem}-${key.replace(/[^a-z0-9]/gi, "_")}`;
    const name = el("div", "detector-param-name");
    const labelText = labelFor(subsystem, key);
    const label = el("label", "", labelText);
    label.htmlFor = id;
    // Every setting has a "?": what it does where known, else its SIMPLON
    // path and range. Not the information table.
    const help = helpOverride || helpKey(subsystem, key) || (info ? "" : "detector.help.adv.generic");
    if (help) {
      const line = el("span", "detector-param-label");
      // The range is in the "?" too, since the row shows it only while editing.
      const range = rangeText(descriptor);
      const paths = [key, ...alsoKeys].map((name) => `${subsystem}/config/${name}`);
      line.append(label, infoTip(help, paths, range ? t("detector.help.range", { range }) : ""));
      name.append(line);
    } else {
      name.append(label);
      // The SIMPLON key under a translated name; once is enough when they match.
      if (labelText !== key) name.append(el("code", "", key));
    }
    row.dataset.search = `${labelText} ${key}`.toLowerCase();
    row.append(name);
    const readonly = !String(descriptor.access_mode || "rw").includes("w");
    if (readonly) {
      const value = el("div", "detector-readonly", readableValue(descriptor));
      value.id = id;
      row.append(value);
      return row;
    }
    const field = el("div", "detector-field");
    if (isBinary(descriptor)) return switchRow(subsystem, key, descriptor, row, field, id);
    let input;
    const allowed = Array.isArray(descriptor.allowed_values) ? descriptor.allowed_values : null;
    if (descriptor.value_type === "bool") {
      input = el("select");
      for (const [value, text] of [["true", t("detector.value.on")], ["false", t("detector.value.off")]]) {
        const option = el("option", "", text);
        option.value = value;
        input.append(option);
      }
      input.value = String(Boolean(descriptor.value));
    } else if (allowed) {
      input = el("select");
      // A value outside the list (an empty test image mode) is still shown,
      // rather than a blank dropdown.
      const values = allowed.map(String).includes(String(descriptor.value)) ? allowed : [descriptor.value, ...allowed];
      for (const value of values) {
        const option = el("option", "", value === "" || value === null ? t("detector.value.none") : optionLabel(key, value));
        option.value = String(value ?? "");
        input.append(option);
      }
      input.value = String(descriptor.value ?? "");
    } else {
      input = el("input");
      input.type = "text";
      input.autocomplete = "off";
      input.inputMode = descriptor.value_type === "string" ? "text" : "decimal";
      input.value = displayValue(descriptor);
      const unit = unitSymbol(descriptor.unit);
      if (unit) {
        field.append(el("span", "detector-unit", unit));
        input.style.paddingRight = `${16 + unit.length * 7}px`;
      }
      if (descriptor.value_type === "string" && !inline) {
        // Free text (a name pattern) needs the whole row; in Advanced it
        // shares the line, as the other settings there do.
        row.classList.add("is-wide");
      } else {
        input.classList.add("is-number");
      }
    }
    input.id = id;
    input.dataset.paramInput = "";
    field.prepend(input);
    row.append(field);
    // A range says something about a number; a dropdown already shows its options.
    const numeric = !allowed && descriptor.value_type !== "bool" && descriptor.value_type !== "string";
    const hint = el("div", "detector-hint", numeric ? rangeText(descriptor) : "");
    hint.dataset.range = hint.textContent;
    row.append(hint);
    const commit = () => void writeParam(subsystem, key, input, hint, row);
    input.addEventListener("change", commit);
    if (input.tagName === "INPUT") {
      input.addEventListener("keydown", (event) => {
        if (event.key === "Enter") input.blur();
      });
    }
    return row;
  }

  // On/off as a switch: one click, and the same control as the data outputs.
  function switchRow(subsystem, key, descriptor, row, field, id) {
    const toggle = el("label", "detector-switch");
    const box = el("input");
    box.type = "checkbox";
    box.id = id;
    box.checked = isOn(descriptor);
    box.dataset.paramInput = "";
    toggle.append(box, el("span"));
    field.classList.add("is-switch");
    field.append(toggle);
    const hint = el("div", "detector-hint", "");
    hint.dataset.range = "";
    row.append(field, hint);
    box.addEventListener("change", async () => {
      const value = descriptor.value_type === "bool" ? box.checked : box.checked ? "enabled" : "disabled";
      await sendParam(subsystem, key, value, { value: "" }, hint, row);
      // Refused: the switch shows what the detector has.
      box.checked = isOn(ctx.params[subsystem]?.[key]);
    });
    return row;
  }

  async function writeParam(subsystem, key, input, hint, row) {
    const descriptor = ctx.params[subsystem]?.[key];
    if (!descriptor) return;
    const checked = validateInput(descriptor, input.value);
    input.setAttribute("aria-invalid", String(!checked.ok));
    if (!checked.ok) {
      hint.textContent = checked.error;
      hint.className = "detector-hint is-error";
      return;
    }
    if (checked.value === descriptor.value) {
      hint.textContent = rangeText(descriptor);
      hint.className = "detector-hint";
      return;
    }
    if (subsystem === "detector" && key === "trigger_mode" && isEnableMode(checked.value) && ctx.params.detector.nimages && Number(ctx.params.detector.nimages.value) !== 1) {
      // The detector refuses an enable mode otherwise.
      await sendParam("detector", "nimages", 1, { value: "" }, hint, row, { quiet: true });
      if (Number(ctx.params.detector.nimages?.value) !== 1) return;
      log(t("detector.log.enable_one_image"));
    }
    if (subsystem === "filewriter" && key === "name_pattern" && !String(checked.value).includes("$id")) {
      ask(t("detector.confirm.no_id"), t("detector.confirm.use_anyway"), t("detector.action.cancel"), () => sendParam(subsystem, key, checked.value, input, hint, row));
      input.value = displayValue(descriptor);
      return;
    }
    if (subsystem === "detector" && (key === "frame_time" || key === "count_time") && timingLinked()) {
      await writeLinkedTiming(key, checked.value, input, hint, row);
      return;
    }
    await sendParam(subsystem, key, checked.value, input, hint, row);
  }

  // ---------- frame and count time linked ----------
  function timingLinked() {
    return linkTiming && atFullDutyCycle(ctx.params.detector);
  }

  function timingRow(key) {
    return paramsHost?.querySelector(`[data-key="detector:${key}"]`) || null;
  }

  // Linked, one time sets both. A frame time takes the count time along by
  // itself (the detector fits the count time to it); a count time raises the
  // frame time only when it no longer fits, so the frame time follows here.
  // Whatever the detector did not do itself is sent after it.
  async function writeLinkedTiming(key, value, input, hint, row) {
    await sendParam("detector", key, value, input, hint, row);
    if (!sameValue(Number(ctx.params.detector[key]?.value), value) || atFullDutyCycle(ctx.params.detector)) return;
    await fillDutyCycle(key === "frame_time" ? "count_time" : "frame_time");
  }

  // Sets `key` so that the two times are at full duty cycle again, the other
  // one staying as it is.
  async function fillDutyCycle(key) {
    const det = ctx.params.detector;
    const readout = readoutTime(det);
    const target = key === "count_time" ? Number(det.frame_time.value) - readout : Number(det.count_time.value) + readout;
    const row = timingRow(key) || el("div");
    const hint = row.querySelector(".detector-hint") || el("div");
    await sendParam("detector", key, target, { value: "" }, hint, row, { quiet: true });
    if (!atFullDutyCycle(ctx.params.detector)) return false;
    log(t("detector.log.duty_set", { label: paramLabel(key), value: displayValue(ctx.params.detector[key]) }));
    flashRow("detector", key, t("detector.hint.duty"));
    return true;
  }

  async function toggleTimingLink() {
    if (timingLinked()) {
      linkTiming = false;
      writeLinkTiming(false);
      log(t("detector.log.timing_unlinked"));
      renderParams();
      return;
    }
    linkTiming = true;
    writeLinkTiming(true);
    if (atFullDutyCycle(ctx.params.detector)) {
      log(t("detector.log.timing_linked"));
      renderParams();
      return;
    }
    // The frame time, the rate, stays; the count time fills it. The write
    // redraws the panel, with the chain as it then is.
    if (await fillDutyCycle("count_time")) log(t("detector.log.timing_linked"));
  }

  // Between frame and count time: a chain, whole while they move together.
  function timingLink() {
    const linked = timingLinked();
    const line = el("div", "detector-timing-link");
    const button = el("button", `detector-link${linked ? " is-linked" : ""}`);
    button.type = "button";
    button.dataset.paramInput = "";
    button.setAttribute("aria-pressed", String(linked));
    button.title = t(linked ? "detector.timing.linked_hint" : "detector.timing.unlinked_hint");
    button.append(chainIcon(linked), el("span", "", t(linked ? "detector.timing.linked" : "detector.timing.unlinked")));
    button.addEventListener("click", () => void toggleTimingLink());
    line.append(button);
    return line;
  }

  async function sendParam(subsystem, key, value, input, hint, row, { quiet = false } = {}) {
    row.classList.add("is-pending");
    ctx.writeSeq += 1;
    try {
      const result = await request("/detector/config", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ url: ctx.url, version: ctx.version, subsystem, key, value }),
      });
      // SIMPLON lists what changed "or may have been changed"; flag only the
      // values that really moved.
      const before = {};
      for (const name of Object.keys(result?.params || {})) before[name] = ctx.params[subsystem][name]?.value;
      for (const [name, descriptor] of Object.entries(result?.params || {})) {
        ctx.params[subsystem][name] = knownUnit(name, descriptor);
      }
      const moved = Object.keys(result?.params || {}).filter(
        (name) =>
          name !== key &&
          JSON.stringify(before[name]) !== JSON.stringify(ctx.params[subsystem][name]?.value) &&
          !sameValue(before[name], ctx.params[subsystem][name]?.value),
      );
      // An output's mode reads as "File writer: On", not "mode set to enabled".
      const isMode = key === "mode" && subsystem !== "detector";
      const label = isMode ? t(`detector.output.${subsystem}`) : paramLabel(key);
      const shown = isMode || isBinary(ctx.params[subsystem][key])
        ? t(isOn(ctx.params[subsystem][key]) || value === "enabled" || value === true ? "detector.value.on" : "detector.value.off")
        : optionLabel(key, displayValue(ctx.params[subsystem][key]));
      if (!quiet) log(t("detector.log.set", { label, value: shown }));
      if (isMode && ctx.status?.[subsystem]) {
        // The status poll would say so only on its next round; until then the
        // row must not contradict the switch just flipped.
        ctx.status[subsystem].mode = value;
        ctx.status[subsystem].state =
          value !== "enabled" ? "disabled" : subsystem === "monitor" ? "normal" : "ready";
      }
      // A quick action's temporary writes are its own business, not news.
      if (!quiet) {
        for (const name of moved) {
          log(t("detector.log.adjusted", { label: labelFor(subsystem, name), value: displayValue(ctx.params[subsystem][name]) }));
        }
      }
      rerenderAfterWrite(subsystem, key, moved);
    } catch (err) {
      hint.textContent = err.message;
      hint.className = "detector-hint is-error";
      input.value = displayValue(ctx.params[subsystem]?.[key]);
    } finally {
      ctx.writeSeq += 1;
      row.classList.remove("is-pending");
    }
  }

  function rerenderAfterWrite(subsystem, key, changed) {
    if (subsystem === "detector") renderParams();
    renderOutputs();
    renderAdvanced();
    for (const name of changed) {
      if (name !== key) flashRow(subsystem, name, t("detector.hint.adjusted"));
    }
  }

  // A row whose value moved without being typed there: it lights up, and its
  // hint says why for a moment.
  function flashRow(subsystem, name, text) {
    const selector = `[data-key="${`${subsystem}:${name}`.replace(/["\\]/g, "\\$&")}"]`;
    const row = content?.querySelector(selector);
    if (!row) return;
    row.classList.add("is-flash");
    const hint = row.querySelector(".detector-hint");
    if (hint) {
      hint.textContent = text;
      hint.className = "detector-hint is-changed";
      window.setTimeout(() => {
        if (!hint.classList.contains("is-changed")) return;
        hint.textContent = hint.dataset.range || "";
        hint.className = "detector-hint";
      }, 3500);
    }
    window.setTimeout(() => row.classList.remove("is-flash"), 1600);
  }

  // The main view's rows for this detector: the groups, each with its rows
  // (a key, a threshold energy, or the images row), and every key they show.
  function coreLayout() {
    const det = ctx.params.detector;
    const groups = [];
    const keys = [];
    // In an enable mode the trigger sets the exposure: internal enable keeps
    // the count time as the default exposure, external enable needs neither
    // time. What does not apply moves to Advanced, still within reach.
    const mode = String(det.trigger_mode?.value || "");
    const unused = mode === "inte" ? new Set(["frame_time"]) : mode === "exte" ? new Set(["frame_time", "count_time"]) : new Set();
    const notes = { inte: "detector.enable.count_time_note", exte: "detector.enable.signal_note" };
    for (const group of CORE_GROUPS) {
      const rows = [];
      let groupImages = null;
      for (const entry of group.rows) {
        if (typeof entry === "string") {
          if (det[entry] && !unused.has(entry)) rows.push({ keys: [entry] });
        } else if (entry.oneOf) {
          const found = entry.oneOf.find((key) => det[key]);
          if (found) rows.push({ keys: [found] });
        } else if (entry.thresholds) {
          // Each threshold's energy, with the switch for its images beside it
          // when there is a choice; then the difference image on its own row.
          const { energies, images } = thresholdRows(det);
          const switchFor = (n) => images.find((image) => image.n === n)?.key || null;
          for (const { n, energy } of energies) {
            const mode = switchFor(n);
            rows.push({ keys: mode ? [energy, mode] : [energy], imageSwitch: mode });
          }
          const difference = images.find((image) => image.n === null);
          if (difference) rows.push({ keys: [difference.key], imageSwitch: difference.key, switchOnly: true });
          if (images.length) groupImages = images;
        }
      }
      const note = group.id === "timing" ? notes[mode] : "";
      if (rows.length || note) groups.push({ id: group.id, rows, note, images: groupImages });
      for (const row of rows) keys.push(...row.keys);
    }
    return { groups, keys };
  }

  function coreKeys() {
    return coreLayout().keys;
  }

  // The switch deciding whether a threshold's images (or the difference
  // image) are delivered. The energies stay in use either way: they decide
  // what is counted, and the difference image is calculated from them.
  function imageSwitch(key, row) {
    const descriptor = ctx.params.detector[key];
    const cell = el("div", "detector-image-switch");
    const toggle = el("label", "detector-switch");
    const box = el("input");
    box.type = "checkbox";
    box.checked = isOn(descriptor);
    box.dataset.paramInput = "";
    box.dataset.imageKey = key;
    if (!String(descriptor.access_mode || "rw").includes("w")) box.dataset.readonly = "true";
    box.setAttribute("aria-label", paramLabel(key));
    // No column heading: what the switch does is said on hover, and by the
    // tab's help.
    toggle.title = t("detector.help.images");
    toggle.append(box, el("span"));
    cell.append(toggle);
    box.addEventListener("change", async () => {
      const hint = row.querySelector(".detector-hint") || el("div");
      await sendParam("detector", key, box.checked ? "enabled" : "disabled", { value: "" }, hint, row);
      // Refused: the switch shows what the detector has.
      box.checked = isOn(ctx.params.detector[key]);
    });
    return cell;
  }

  // A row of the energy group: name, images switch, energy. The switch has a
  // column of its own left of the field, so every field in the tab keeps the
  // same width and the switches line up, the difference image's included.
  function switchedRow(row) {
    const key = row.keys[0];
    let line;
    if (row.switchOnly) {
      // The difference image: a name and a switch, no value of its own here.
      // "Difference", short enough for one line beside the switch when the
      // groups sit side by side; the switch and the log say "Difference image".
      line = el("div", "detector-param");
      line.dataset.key = `detector:${key}`;
      const name = el("div", "detector-param-name");
      const label = el("span", "detector-param-label");
      label.append(el("label", "", t("detector.param.difference_short")), infoTip("detector.help.difference_mode", `detector/config/${key}`));
      name.append(label);
      line.append(name, el("div", "detector-field is-empty"), el("div", "detector-hint"));
      tipRight(line);
    } else {
      line = paramRow("detector", key, ctx.params.detector[key], { alsoKeys: row.keys.slice(1) });
    }
    const field = line.querySelector(".detector-field");
    if (row.imageSwitch && field) {
      line.classList.add("has-switch");
      field.before(imageSwitch(row.imageSwitch, line));
    }
    return line;
  }

  // How fast a series runs, from frame and count time: "100 Hz · full duty
  // cycle", or "100 Hz · 5 ms between images · 50 % duty cycle". Makes plain
  // how the two times relate.
  function timingSummary(det) {
    const frame = Number(det.frame_time?.value);
    if (!(frame > 0)) return "";
    const hz = 1 / frame;
    const rate = hz >= 1 ? `${+hz.toPrecision(3)} Hz` : t("detector.timing.every", { interval: formatDuration(frame) });
    if (atFullDutyCycle(det)) return t("detector.timing.full_duty", { rate });
    const count = Number(det.count_time?.value);
    const gap = frame - count;
    if (!det.count_time || !(gap > 0)) return rate;
    const text = t("detector.timing.summary", { rate, gap: formatDuration(gap) });
    // Only where the readout is known: without it, a gap of 0.1 µs could be
    // all the detector can do, and "100 %" would say nothing.
    const duty = (100 * count) / frame;
    return readoutTime(det) !== null && duty < 99.95 ? `${text} · ${t("detector.timing.duty", { percent: +duty.toFixed(1) })}` : text;
  }

  // Images per trigger in an enable mode: one, fixed by the mode.
  function oneImageRow() {
    const row = paramRow("detector", "nimages", { ...ctx.params.detector.nimages, access_mode: "r" });
    const value = row.querySelector(".detector-readonly");
    if (value) value.textContent = t("detector.enable.one_per_trigger");
    return row;
  }

  function renderParams() {
    if (!paramsHost) return;
    paramsHost.replaceChildren();
    const columns = el("div", "detector-groups");
    for (const group of coreLayout().groups) {
      const column = el("div", "detector-group");
      column.dataset.group = group.id;
      const heading = el("div", "detector-group-label", t(`detector.group.${group.id}`));
      column.append(heading);
      if (group.images) column.classList.add("has-image-switches");
      // Frame and count time both here, and the readout known: the chain
      // that links them goes between the two.
      const linkable =
        group.id === "timing" &&
        group.rows.length === 2 &&
        readoutTime(ctx.params.detector) !== null &&
        group.rows.every((row) => String(ctx.params.detector[row.keys[0]]?.access_mode || "rw").includes("w"));
      for (const row of group.rows) {
        if (group.images) column.append(switchedRow(row));
        else if (row.keys[0] === "nimages" && isEnableMode(ctx.params.detector.trigger_mode?.value)) column.append(oneImageRow());
        else column.append(paramRow("detector", row.keys[0], ctx.params.detector[row.keys[0]]));
        if (linkable && row.keys[0] === "frame_time") column.append(timingLink());
      }
      if (group.images && !group.images.some((image) => isOn(ctx.params.detector[image.key]))) {
        column.append(el("div", "detector-warning", t("detector.images.none")));
      }
      if (group.note) column.append(el("p", "detector-note detector-group-note", t(group.note)));
      if (group.id === "timing" && !group.note) {
        const rate = timingSummary(ctx.params.detector);
        if (rate) column.append(el("p", "detector-note detector-group-note", rate));
      }
      columns.append(column);
    }
    paramsHost.append(columns);
    refreshInfoTips?.();
    renderState();
  }

  return {
    paramRow,
    sendParam,
    renderParams,
    coreKeys,
  };
}
