/**
 * Detector tab (beta): small DOM builders the panel's parts share -- an
 * element with text (never markup), the "?" beside a setting and its
 * column, the chain between frame and count time, and a closed section
 * whose open state is remembered.
 */

import { t } from "./i18n.js";
import { readOpen, writeOpen } from "./detector_control_storage.js";

export function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}

export function infoTip(infoKey, simplonPaths, extra = "") {
  const button = el("button", "info-tip");
  button.type = "button";
  button.dataset.infoKey = infoKey;
  const lines = [].concat(simplonPaths).map((path) => `SIMPLON: ${path}`);
  if (extra) lines.push(extra);
  button.dataset.infoDetail = lines.join("\n");
  button.setAttribute("aria-label", t("info.button_label"));
  return button;
}

// The "?" has a column of its own right of the field: one aligned column
// down the section, whatever the length of the names, in Advanced too.
// Rows without one keep the column, empty, so the fields line up all the
// same. Advanced's Detector information is a dense table: no "?".
export function tipRight(row) {
  const value = row.querySelector(":scope > .detector-field, :scope > .detector-readonly");
  if (!value) return row;
  const cell = el("div", "detector-tip");
  const tip = row.querySelector(".detector-param-name .info-tip");
  if (tip) cell.append(tip);
  value.after(cell);
  row.classList.add("has-tip");
  return row;
}

// The chain between frame and count time, whole or broken (Lucide's link-2
// and unlink-2).
export function chainIcon(linked) {
  const ns = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(ns, "svg");
  svg.setAttribute("viewBox", "0 0 24 24");
  svg.setAttribute("aria-hidden", "true");
  const paths = linked
    ? ["M9 17H7A5 5 0 0 1 7 7h2", "M15 7h2a5 5 0 1 1 0 10h-2", "M8 12h8"]
    : ["M15 7h2a5 5 0 0 1 0 10h-2m-6 0H7A5 5 0 0 1 7 7h2"];
  for (const d of paths) {
    const path = document.createElementNS(ns, "path");
    path.setAttribute("d", d);
    svg.append(path);
  }
  return svg;
}

// A closed section in Data output, its open state remembered per browser.
export function disclosure(id, title, children, className = "") {
  const box = el("details", `advanced-details detector-disclosure ${className}`.trim());
  box.open = readOpen(id);
  box.dataset.openId = id;
  box.addEventListener("toggle", () => {
    // Opened or closed by the Advanced filter, not by hand: not remembered.
    if (box.dataset.auto) {
      delete box.dataset.auto;
      return;
    }
    writeOpen(id, box.open);
  });
  const summary = el("summary");
  summary.append(...[].concat(title));
  const inner = el("div", "advanced-inner");
  inner.append(...children);
  box.append(summary, inner);
  return box;
}
