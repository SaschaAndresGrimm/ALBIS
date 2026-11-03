/**
 * The value preview under the inspector's details: the table or text, the
 * JSON and CSV links, and "Show in viewer".
 *
 * Split out of inspector_panel_controller.js, which passed 1,000 lines.
 */

import { copyText } from "./clipboard_copy.js";
import { formatStat } from "./format_utils.js";
import { t } from "./i18n.js";
import { outlineGeometry, outlinePointAt } from "./inspector_chart_utils.js";
import { inspectorValueText } from "./inspector_format_utils.js";

export function createInspectorPreviewRenderer({ apiBase, state, elements, callbacks }) {
  const { inspectorPreview, datasetSelect } = elements;
  const { formatInspectorValue } = callbacks;

  function formatInspectorCell(value) {
    if (value === null || value === undefined) return "-";
    if (typeof value === "number") {
      return Number.isFinite(value) ? String(value) : "-";
    }
    return String(value);
  }

  function buildInspectorTable1D(values) {
    const table = document.createElement("table");
    table.className = "inspector-table";
    const head = document.createElement("thead");
    const headRow = document.createElement("tr");
    [t("inspector.table.index"), t("inspector.table.value")].forEach((label) => {
      const th = document.createElement("th");
      th.textContent = label;
      headRow.appendChild(th);
    });
    head.appendChild(headRow);
    table.appendChild(head);
    const body = document.createElement("tbody");
    values.forEach((value, idx) => {
      const row = document.createElement("tr");
      const indexCell = document.createElement("td");
      indexCell.textContent = String(idx);
      const valueCell = document.createElement("td");
      valueCell.textContent = formatInspectorCell(value);
      row.appendChild(indexCell);
      row.appendChild(valueCell);
      body.appendChild(row);
    });
    table.appendChild(body);
    return table;
  }

  function buildInspectorTable2D(values) {
    const table = document.createElement("table");
    table.className = "inspector-table";
    const head = document.createElement("thead");
    const headRow = document.createElement("tr");
    const corner = document.createElement("th");
    corner.textContent = "";
    headRow.appendChild(corner);
    const cols = values.length ? values[0].length : 0;
    for (let c = 0; c < cols; c += 1) {
      const th = document.createElement("th");
      th.textContent = String(c);
      headRow.appendChild(th);
    }
    head.appendChild(headRow);
    table.appendChild(head);
    const body = document.createElement("tbody");
    values.forEach((rowValues, r) => {
      const row = document.createElement("tr");
      const indexCell = document.createElement("td");
      indexCell.textContent = String(r);
      row.appendChild(indexCell);
      rowValues.forEach((value) => {
        const cell = document.createElement("td");
        cell.textContent = formatInspectorCell(value);
        row.appendChild(cell);
      });
      body.appendChild(row);
    });
    table.appendChild(body);
    return table;
  }

  function datasetIsListed(path) {
    return Boolean(path) && [...(datasetSelect?.options || [])].some((option) => option.value === path);
  }

  /**
   * "Show in viewer" for a dataset the Dataset list offers, so an image found
   * by browsing opens without hunting for it in a second list. It goes through
   * the list's own change handler, so it loads exactly as picking it there
   * does. The dataset already on show says so instead.
   */
  function buildViewerAction(path) {
    if (!datasetIsListed(path)) return null;
    if (state.dataset === path) {
      const note = document.createElement("span");
      note.className = "inspector-preview-note inspector-viewer-action";
      note.textContent = t("inspector.shown_in_viewer");
      return note;
    }
    const button = document.createElement("button");
    button.type = "button";
    button.className = "inspector-show-in-viewer inspector-viewer-action";
    button.textContent = t("inspector.show_in_viewer");
    button.addEventListener("click", () => showDatasetInViewer(path));
    return button;
  }

  function showDatasetInViewer(path) {
    if (!datasetSelect || !datasetIsListed(path)) return false;
    datasetSelect.value = path;
    datasetSelect.dispatchEvent(new window.Event("change", { bubbles: true }));
    refreshViewerAction();
    return true;
  }

  // The dataset on show changed (here or in the Dataset list): the action
  // under the preview would otherwise claim the wrong one is shown.
  function refreshViewerAction() {
    const actions = inspectorPreview?.querySelector(".inspector-preview-actions");
    if (!actions) return;
    actions.querySelector(".inspector-viewer-action")?.remove();
    const next = buildViewerAction(actions.dataset.path || "");
    if (next) actions.prepend(next);
  }

  const SVG_NS = "http://www.w3.org/2000/svg";
  // The chart's own units; CSS stretches it to the panel's width.
  const CHART_W = 400;
  const CHART_H = 64;

  function statPart(key, value) {
    const part = document.createElement("span");
    part.className = "inspector-stat";
    const label = document.createElement("span");
    label.className = "inspector-stat-label";
    label.textContent = t(key);
    const shown = document.createElement("span");
    shown.textContent = value;
    part.appendChild(label);
    part.appendChild(shown);
    return part;
  }

  /** Min, max and mean over the whole dataset, not just the rows below. */
  function buildStats(stats) {
    if (!stats || stats.min === null || stats.min === undefined) return null;
    const line = document.createElement("div");
    line.className = "inspector-stats";
    line.appendChild(statPart("inspector.stats.min", formatStat(stats.min)));
    line.appendChild(statPart("inspector.stats.max", formatStat(stats.max)));
    line.appendChild(statPart("inspector.stats.mean", formatStat(stats.mean)));
    if (stats.non_finite) {
      const extra = document.createElement("span");
      extra.className = "inspector-stat inspector-stat-warning";
      extra.textContent = t("inspector.stats.non_finite", { count: stats.non_finite.toLocaleString() });
      line.appendChild(extra);
    }
    return line;
  }

  function hoverText(point) {
    if (!point) return t("inspector.chart.hint");
    const low = formatStat(point.low);
    const high = formatStat(point.high);
    if (point.from === point.to) return t("inspector.chart.point", { index: point.from, value: high });
    return t("inspector.chart.range", { from: point.from, to: point.to, low, high });
  }

  /**
   * A line chart of a 1D array, for what a table of its first rows hides: a
   * jump in a series of start times, a counter that resets, a single spike.
   * One series, so no legend; the exact values stay in the table below.
   */
  function buildChart(outline, stats) {
    const geometry = outline && outlineGeometry(outline, { width: CHART_W, height: CHART_H });
    if (!geometry) return null;
    const count = Number(stats?.count) || outline.high.length;
    const figure = document.createElement("figure");
    figure.className = "inspector-chart";
    const plot = document.createElement("div");
    plot.className = "inspector-chart-plot";
    const svg = document.createElementNS(SVG_NS, "svg");
    svg.setAttribute("viewBox", `0 0 ${CHART_W} ${CHART_H}`);
    svg.setAttribute("preserveAspectRatio", "none");
    svg.setAttribute("role", "img");
    svg.setAttribute(
      "aria-label",
      t("inspector.chart.label", {
        count: count.toLocaleString(),
        min: formatStat(geometry.yMin),
        max: formatStat(geometry.yMax),
      }),
    );
    const shape = document.createElementNS(SVG_NS, "path");
    shape.setAttribute("class", geometry.band ? "inspector-chart-band" : "inspector-chart-line");
    shape.setAttribute("d", geometry.band || geometry.line);
    shape.setAttribute("vector-effect", "non-scaling-stroke");
    const cursor = document.createElementNS(SVG_NS, "line");
    cursor.setAttribute("class", "inspector-chart-cursor");
    cursor.setAttribute("y1", "0");
    cursor.setAttribute("y2", String(CHART_H));
    cursor.setAttribute("vector-effect", "non-scaling-stroke");
    svg.appendChild(shape);
    svg.appendChild(cursor);
    const yAxis = document.createElement("div");
    yAxis.className = "inspector-chart-y";
    yAxis.setAttribute("aria-hidden", "true");
    [geometry.yMax, geometry.yMin].forEach((value) => {
      const tick = document.createElement("span");
      tick.textContent = formatStat(value);
      yAxis.appendChild(tick);
    });
    plot.appendChild(yAxis);
    plot.appendChild(svg);
    const axis = document.createElement("div");
    axis.className = "inspector-chart-x";
    axis.setAttribute("aria-hidden", "true");
    axis.innerHTML = "<span>0</span><span></span>";
    axis.lastChild.textContent = String(count - 1);
    const caption = document.createElement("figcaption");
    caption.textContent = hoverText(null);
    plot.appendChild(document.createElement("span"));
    plot.appendChild(axis);
    figure.appendChild(plot);
    figure.appendChild(caption);
    // The hit target is the whole plot, not the 2px line.
    plot.addEventListener("pointermove", (event) => {
      const box = svg.getBoundingClientRect();
      if (!box.width) return;
      const fraction = Math.min(1, Math.max(0, (event.clientX - box.left) / box.width));
      const point = outlinePointAt(outline, fraction, count);
      const n = outline.high.length;
      const x = n === 1 ? CHART_W / 2 : 2 + (point.index * (CHART_W - 4)) / (n - 1);
      cursor.setAttribute("x1", String(x));
      cursor.setAttribute("x2", String(x));
      figure.classList.add("is-hovering");
      caption.textContent = hoverText(point);
    });
    plot.addEventListener("pointerleave", () => {
      figure.classList.remove("is-hovering");
      caption.textContent = hoverText(null);
    });
    return figure;
  }

  /**
   * "Copy value" for a single value: a timestamp, a sample name or a stored
   * configuration is what one pastes elsewhere, and a long one is tedious to
   * select by hand inside a scrolling box.
   */
  function buildCopyValueAction(text) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "inspector-copy-value";
    button.textContent = t("inspector.copy_value");
    let timer = null;
    button.addEventListener("click", async () => {
      const copied = await copyText(text);
      button.textContent = copied ? t("inspector.value_copied") : t("inspector.copy_value_failed");
      if (timer) window.clearTimeout(timer);
      timer = window.setTimeout(() => {
        button.textContent = t("inspector.copy_value");
      }, 1500);
    });
    return button;
  }

  function renderInspectorPreview(data) {
    if (!inspectorPreview) return;
    inspectorPreview.innerHTML = "";
    if (!data || data.preview === null || data.preview === undefined) {
      return;
    }
    const actions = document.createElement("div");
    actions.className = "inspector-preview-actions";
    actions.dataset.path = data.path || "";
    const link = document.createElement("a");
    link.href = `${apiBase}/hdf5/value?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(
      data.path || "",
    )}&max_cells=65536`;
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = t("inspector.open_json");
    actions.appendChild(link);
    if (Array.isArray(data.shape) && data.shape.length > 0) {
      const csvLink = document.createElement("a");
      csvLink.href = `${apiBase}/hdf5/csv?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(
        data.path || "",
      )}&max_cells=65536`;
      csvLink.textContent = t("inspector.download_csv");
      actions.appendChild(csvLink);
    }
    // First: the row is as wide as the table under it, so anything pushed to
    // its far end can sit outside the visible part of the preview.
    const viewerAction = buildViewerAction(data.path || "");
    if (viewerAction) actions.prepend(viewerAction);
    inspectorPreview.appendChild(actions);
    const stats = buildStats(data.stats);
    if (stats) inspectorPreview.appendChild(stats);
    const chart = buildChart(data.outline, data.stats);
    if (chart) inspectorPreview.appendChild(chart);

    // Only the values scroll; the actions, statistics and chart above stay put.
    const body = document.createElement("div");
    body.className = "inspector-preview-body";
    const preview = data.preview;
    if (Array.isArray(preview)) {
      const table = Array.isArray(preview[0]) ? buildInspectorTable2D(preview) : buildInspectorTable1D(preview);
      body.appendChild(table);
    } else {
      const raw = formatInspectorValue(preview);
      const text = document.createElement("div");
      text.textContent = inspectorValueText(raw).text;
      body.appendChild(text);
      actions.appendChild(buildCopyValueAction(raw));
    }
    inspectorPreview.appendChild(body);

    if (data.preview_shape) {
      const note = document.createElement("div");
      note.className = "inspector-preview-note";
      const shapeText = data.preview_shape.join("×");
      note.textContent = t("inspector.preview", {
        shapeText,
        suffix: data.truncated ? t("inspector.preview.truncated_suffix") : "",
      });
      if (data.slice && Array.isArray(data.slice.lead) && data.slice.lead.length) {
        note.textContent += ` • ${t("inspector.preview.slice")} [${data.slice.lead.join(", ")}]`;
      }
      inspectorPreview.appendChild(note);
    }
  }

  return { renderInspectorPreview, showDatasetInViewer, refreshViewerAction };
}
