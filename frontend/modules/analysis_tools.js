/**
 * The Analysis tab's optional tools, and which of them a user hides.
 *
 * Statistics and ROI is not listed: everyone uses it, so it is always shown.
 * The rest serve different work -- diffraction, imaging, processing -- and a
 * tab holding all of them is crowded for anyone doing only one. Settings ->
 * Tools hides them, stored as `ui.hidden_tools` (a hidden rather than a shown
 * list, so a tool added later appears unless someone hid it on purpose).
 *
 * A hidden tool is switched off: a flatfield or overlay that kept acting with
 * its section out of sight would change every image with nothing on screen to
 * say why. A new tool is one entry here plus its section.
 */

import { t } from "./i18n.js";

export const ANALYSIS_TOOLS = [
  { id: "rings", section: "resolution-rings", group: "diffraction", label: "analysis.rings.section_title", toggleId: "rings-toggle" },
  { id: "peaks", section: "peak-finder", group: "diffraction", label: "analysis.peaks.section_title", toggleId: "peaks-enable" },
  { id: "flatfield", section: "flatfield", group: "imaging", label: "flatfield.section_title", toggleId: "flat-toggle" },
  { id: "inpainting", section: "inpainting", group: "imaging", label: "inpaint.section_title", toggleId: "inpaint-toggle" },
  { id: "combine", section: "series-summing", group: "processing", label: "series.ui.section_title" },
];

const TOOL_IDS = ANALYSIS_TOOLS.map((tool) => tool.id);

/**
 * What the quick choices hide. Inpainting and Combine Frames stay in both:
 * diffraction fills module gaps too, and both kinds of work sum frames.
 */
export const TOOL_PRESETS = {
  diffraction: ["flatfield"],
  imaging: ["rings", "peaks"],
  everything: [],
};

/** Known tool ids only, in registry order -- as backend/config.py keeps them. */
export function normalizeHiddenTools(value) {
  const list = typeof value === "string" ? value.split(",") : Array.isArray(value) ? value : [];
  const wanted = new Set(list.map((entry) => String(entry || "").trim().toLowerCase()));
  return TOOL_IDS.filter((id) => wanted.has(id));
}

export function isToolHidden(state, id) {
  return Array.isArray(state?.hiddenTools) && state.hiddenTools.includes(id);
}

export function createAnalysisToolsController({ state, root = document, elements = {}, callbacks = {} }) {
  const { summary, summaryText, customiseBtn } = elements;
  const { openToolSettings } = callbacks;

  function sectionOf(tool) {
    return root.querySelector(`.panel-section[data-section="${tool.section}"]`);
  }

  function switchOff(tool) {
    if (!tool.toggleId) return;
    const toggle = root.getElementById ? root.getElementById(tool.toggleId) : root.querySelector(`#${tool.toggleId}`);
    if (!toggle || !toggle.checked) return;
    toggle.checked = false;
    toggle.dispatchEvent(new window.Event("change", { bubbles: true }));
  }

  function updateSummary() {
    if (!summary) return;
    const count = state.hiddenTools.length;
    summary.hidden = count === 0;
    if (summaryText) summaryText.textContent = count ? t("tools.hidden_summary", { count }) : "";
  }

  /** Show and hide the sections; switch off what was just hidden. */
  function apply(hiddenTools) {
    state.hiddenTools = normalizeHiddenTools(hiddenTools);
    ANALYSIS_TOOLS.forEach((tool) => {
      const hidden = state.hiddenTools.includes(tool.id);
      const section = sectionOf(tool);
      if (section) section.hidden = hidden;
      if (hidden) switchOff(tool);
    });
    updateSummary();
  }

  customiseBtn?.addEventListener("click", () => openToolSettings?.());
  if (!Array.isArray(state.hiddenTools)) state.hiddenTools = [];
  updateSummary();

  return { apply, refresh: updateSummary };
}
