/**
 * The value preview under the inspector's details: the table or text, the
 * JSON and CSV links, and "Show in viewer".
 *
 * Split out of inspector_panel_controller.js, which passed 1,000 lines.
 */

import { t } from "./i18n.js";

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

    const preview = data.preview;
    if (Array.isArray(preview)) {
      const table = Array.isArray(preview[0]) ? buildInspectorTable2D(preview) : buildInspectorTable1D(preview);
      inspectorPreview.appendChild(table);
    } else {
      const text = document.createElement("div");
      text.textContent = formatInspectorValue(preview);
      inspectorPreview.appendChild(text);
    }

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
