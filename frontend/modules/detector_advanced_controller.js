/**
 * Detector tab (beta): Advanced. Every setting the main view and Data
 * output do not show, in closed groups, the goniometer's axes paired on one
 * row each, the read-only information, and a filter.
 */

import { t } from "./i18n.js";
import { disclosure, el, infoTip } from "./detector_control_dom.js";
import { readOpen } from "./detector_control_storage.js";

export function createDetectorAdvancedController({
  ctx,
  advancedHost,
  paramRow,
  coreKeys,
  refreshInfoTips,
  renderState,
}) {
  // ---------- advanced ----------
  // Everything else the detector documents, grouped so it can be found:
  // corrections, readout, geometry and metadata, test images, the rest; then
  // the data interfaces by name, then read-only information. A filter narrows
  // the list as you type.
  const ADVANCED_GROUPS = [
    // Counting mode decides what a pixel counts, as the corrections do.
    ["corrections", /correction|mask|auto_sum|virtual_pixel|counting_mode/],
    ["readout", /bit_depth|roi_|binning|compression|pixel_format|extg|nexpi|ntriggers_skipped|trigger_start_delay|sensor_movement|fast_arm|threshold\//],
    ["geometry", /beam_center|distance|orientation|translation|transformation|_start$|_increment$|_axis$|sample_name|source_name|instrument_name|element|flux|wavelength|energy/],
    ["test", /^test_image/],
  ];
  let advancedFilter = "";

  // Geometry and metadata, with the goniometer's start and increment of each
  // axis side by side on one row under "Start" and "Increment": five rows
  // instead of ten, mostly empty ones.
  function geometryRows(keys) {
    const axes = ["chi", "kappa", "omega", "phi", "two_theta"].filter((axis) => keys.includes(`${axis}_start`) && keys.includes(`${axis}_increment`));
    const paired = new Set(axes.flatMap((axis) => [`${axis}_start`, `${axis}_increment`]));
    const rows = keys.filter((key) => !paired.has(key)).sort().map((key) => paramRow("detector", key, ctx.params.detector[key], { inline: true }));
    if (!axes.length) return rows;
    const head = el("div", "detector-axis-head");
    head.append(el("span", "", t("detector.advanced.goniometer")), el("span", "", t("detector.advanced.start")), el("span", "", t("detector.advanced.increment")));
    const axisRows = axes.map((axis) => {
      const parts = [`${axis}_start`, `${axis}_increment`].map((key) => paramRow("detector", key, ctx.params.detector[key], { inline: true }));
      const row = el("div", "detector-param detector-axis-row has-tip");
      row.dataset.key = `detector:${axis}`;
      row.dataset.search = `${axis} ${axis}_start ${axis}_increment`;
      row.dataset.count = "2";
      const name = el("div", "detector-param-name");
      name.append(el("label", "", axis));
      // The fields keep their own writing and hints; only the row is shared.
      const fields = parts.map((part) => part.querySelector(":scope > .detector-field, :scope > .detector-readonly"));
      const hints = parts.map((part) => part.querySelector(":scope > .detector-hint")).filter(Boolean);
      const tip = el("div", "detector-tip");
      tip.append(infoTip("detector.help.adv.goniometer", [`detector/config/${axis}_start`, `detector/config/${axis}_increment`]));
      row.append(name, ...fields, tip, ...hints);
      return row;
    });
    // The goniometer after the other geometry, as one block.
    return [...rows, head, ...axisRows];
  }

  function renderAdvanced() {
    if (!advancedHost) return;
    advancedHost.replaceChildren();
    const filter = el("input", "detector-filter");
    filter.type = "search";
    filter.placeholder = t("detector.advanced.filter");
    filter.setAttribute("aria-label", t("detector.advanced.filter"));
    filter.value = advancedFilter;
    advancedHost.append(filter);

    const shown = new Set(coreKeys());
    if (shown.has("threshold_energy")) shown.add("threshold/1/energy");
    // With the file writer's settings in Data output, which the stream's
    // compression follows too. The outputs' own settings are all there.
    shown.add("compression");
    const buckets = new Map([...ADVANCED_GROUPS.map(([id]) => [id, []]), ["other", []]]);
    const info = [];
    for (const [key, descriptor] of Object.entries(ctx.params.detector)) {
      if (shown.has(key)) continue;
      if (!String(descriptor.access_mode || "rw").includes("w")) {
        info.push(key);
        continue;
      }
      const group = ADVANCED_GROUPS.find(([, pattern]) => pattern.test(key));
      buckets.get(group ? group[0] : "other").push(key);
    }
    // Each group a closed section with its count, open state remembered; the
    // filter opens the ones with matches while it holds text.
    const groups = [];
    const addGroup = (id, title, rows, className = "") => {
      if (!rows.length) return null;
      // Settings, not lines: an axis row is two, the goniometer's heads none.
      const count = rows.filter((row) => row.classList.contains("detector-param")).reduce((sum, row) => sum + (Number(row.dataset.count) || 1), 0);
      const box = disclosure(`adv.${id}`, `${title} (${count})`, rows, `detector-adv-group ${className}`.trim());
      advancedHost.append(box);
      groups.push(box);
      return box;
    };
    for (const [id, keys] of buckets) {
      const rows = id === "geometry" ? geometryRows(keys) : keys.sort().map((key) => paramRow("detector", key, ctx.params.detector[key], { inline: true }));
      addGroup(id, t(`detector.advanced.group.${id}`), rows);
    }
    // Reference, not settings: a dense table, two columns when there is room.
    addGroup("info", t("detector.advanced.info"), info.sort().map((key) => paramRow("detector", key, ctx.params.detector[key], { inline: true, info: true })), "is-info");
    const empty = el("p", "detector-note", t("detector.advanced.no_match"));
    advancedHost.append(empty);

    const setOpen = (box, open) => {
      if (box.open === open) return;
      box.dataset.auto = "1";
      box.open = open;
    };
    const applyFilter = () => {
      const needle = advancedFilter.trim().toLowerCase();
      let any = false;
      for (const box of groups) {
        let visible = 0;
        box.querySelectorAll(".detector-param").forEach((row) => {
          const match = !needle || (row.dataset.search || "").includes(needle);
          row.hidden = !match;
          if (match) visible += 1;
        });
        // The goniometer's column heads go with its rows.
        const head = box.querySelector(".detector-axis-head");
        if (head) head.hidden = !box.querySelector(".detector-axis-row:not([hidden])");
        box.hidden = visible === 0;
        setOpen(box, needle ? visible > 0 : readOpen(box.dataset.openId));
        any = any || visible > 0;
      }
      empty.hidden = any;
    };
    filter.addEventListener("input", () => {
      advancedFilter = filter.value;
      applyFilter();
    });
    applyFilter();
    refreshInfoTips?.();
    renderState();
  }

  return {
    renderAdvanced,
  };
}
