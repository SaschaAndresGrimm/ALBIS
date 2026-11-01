/**
 * Detector tab (beta): the quick actions. Snap (one image now) and
 * Continuous (an alignment view until Stop), each with its own settings for
 * the time it runs and put back afterwards, and their timings behind the
 * gear.
 */

import { t } from "./i18n.js";
import { el } from "./detector_control_dom.js";
import { writeQuick } from "./detector_control_storage.js";
import { CONTINUOUS_HOURS, paramLabel, QUICK_DEFAULTS, QUICK_LIMITS, rangeText, sameValue } from "./detector_control_utils.js";

export function createDetectorQuickController({
  ctx,
  sendParam,
  acquire,
  renderState,
  poll,
  log,
  labelQuickButtons,
}) {
  // ---------- quick actions ----------
  // Snap and Continuous change the series settings only for themselves and
  // put them back afterwards, also after Stop or an error. Applied in order,
  // put back in reverse: that keeps the detector's rule that an enable mode
  // needs images per trigger at 1 satisfied at every step.
  async function withTemporarySettings(changes, run) {
    // Recorded before the first change: writing one time can make the
    // detector adjust another (a frame time pulls the count time along), so
    // what to put back is what was there at the start, compared with a
    // tolerance for the detector's own rounding.
    const before = changes.map(([subsystem, key]) => [subsystem, key, ctx.params[subsystem]?.[key]?.value]);
    const setOne = async (subsystem, key, value) => {
      await sendParam(subsystem, key, value, { value: "" }, el("div"), el("div"), { quiet: true });
      // Our own switch, not "changed by another program".
      if (key === "mode") ctx.lastModes = { ...(ctx.lastModes || {}), [subsystem]: value };
      return sameValue(ctx.params[subsystem]?.[key]?.value, value);
    };
    try {
      for (const [subsystem, key, value] of changes) {
        if (ctx.quickCancelled) break;
        if (!ctx.params[subsystem]?.[key] || sameValue(ctx.params[subsystem][key].value, value)) continue;
        if (!(await setOne(subsystem, key, value))) {
          log(t("detector.log.failed", { command: paramLabel(key), reason: "" }), "error");
          return;
        }
      }
      if (!ctx.quickCancelled) await run();
    } finally {
      let changed = false;
      for (const [subsystem, key, value] of before.reverse()) {
        if (value === undefined || sameValue(ctx.params[subsystem]?.[key]?.value, value)) continue;
        changed = true;
        if (!(await setOne(subsystem, key, value))) {
          log(t("detector.log.restore_failed", { label: subsystem === "detector" ? paramLabel(key) : t(`detector.output.${subsystem}`) }), "error");
        }
      }
      if (changed) log(t("detector.log.restored"));
    }
  }

  // The timing for a quick action: frame time first, which the detector
  // follows with the count time, then the count time itself.
  const timingFor = (exposure) => [
    ["detector", "frame_time", exposure],
    ["detector", "count_time", exposure],
  ];

  // One image of quick.snapExposure seconds now, shown live.
  async function snap() {
    ctx.quickRunning = true;
    ctx.quickCancelled = false;
    renderState();
    log(t("detector.log.snap"));
    try {
      await withTemporarySettings(
        [["detector", "trigger_mode", "ints"], ...timingFor(ctx.quick.snapExposure), ["detector", "nimages", 1], ["detector", "ntrigger", 1]],
        () => acquire({ forceFollow: true }),
      );
    } finally {
      ctx.quickRunning = false;
      renderState();
      await poll();
    }
  }

  // Images one after another, shown live, until Stop: an alignment view. It
  // saves nothing: the file writer and the stream are off while it runs.
  async function runContinuous() {
    const wanted = Math.max(1, Math.round(ctx.quick.continuousRate * CONTINUOUS_HOURS * 3600));
    const most = Math.min(wanted, Number(ctx.params.detector.nimages?.max) || wanted);
    ctx.continuous = true;
    ctx.quickRunning = true;
    ctx.quickCancelled = false;
    renderState();
    log(t("detector.log.continuous"));
    try {
      await withTemporarySettings(
        [
          ["detector", "trigger_mode", "ints"],
          // Times before the image count: a long series at the old frame
          // time could pass the detector's one-week limit in between.
          ...timingFor(1 / ctx.quick.continuousRate),
          ["detector", "nimages", most],
          ["detector", "ntrigger", 1],
          ["filewriter", "mode", "disabled"],
          ["stream", "mode", "disabled"],
        ],
        () => acquire({ forceFollow: true }),
      );
    } finally {
      ctx.continuous = false;
      ctx.quickRunning = false;
      renderState();
      await poll();
    }
  }

  // One line under the buttons: Live view, and the timings of Snap and
  // Continuous right beside it, labelled with the buttons' own names. ALBIS
  // settings, not the detector's, remembered per browser.
  function quickSettings() {
    const box = el("div", "detector-quick-settings");
    // The two timings wrap together, under Live view, when the panel is narrow.
    const fields = el("div", "detector-quick-fields");
    box.append(fields);
    for (const key of Object.keys(QUICK_DEFAULTS)) {
      const { min, max, unit } = QUICK_LIMITS[key];
      const id = `detector-quick-${key}`;
      const row = el("div", "detector-quick-field");
      const label = el("label", "", t(key === "snapExposure" ? "detector.action.snap" : "detector.action.continuous"));
      label.htmlFor = id;
      const field = el("div", "detector-field");
      const input = el("input", "is-number");
      input.type = "text";
      input.inputMode = "decimal";
      input.id = id;
      input.value = String(+ctx.quick[key].toPrecision(6));
      // "Snap exposure", "Continuous rate", and the range on hover.
      input.setAttribute("aria-label", t(`detector.quick.${key}`));
      input.title = `${t(`detector.quick.${key}`)}: ${rangeText({ value_type: "float", min, max, unit: unit === "s" ? "s" : "" })}`;
      field.append(input, el("span", "detector-unit", unit));
      row.append(label, field);
      input.addEventListener("change", () => {
        const value = Number(String(input.value).replace(",", "."));
        const ok = Number.isFinite(value) && value >= min && value <= max;
        input.setAttribute("aria-invalid", String(!ok));
        if (!ok) return;
        ctx.quick = { ...ctx.quick, [key]: value };
        writeQuick(ctx.quick);
        labelQuickButtons();
      });
      fields.append(row);
    }
    return box;
  }

  return {
    snap,
    runContinuous,
    quickSettings,
  };
}
