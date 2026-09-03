/**
 * Contextual help tooltip controller.
 */

import { t } from "./i18n.js";

const HELP_SELECTORS = [
  "button",
  "input",
  "select",
  "textarea",
  "label.checkbox",
  ".menu-item",
  ".dropdown-item",
  ".panel-tab",
  ".section-title",
  ".panel-fab",
  ".panel-resizer",
  ".panel-sheet-handle",
  ".roi-resize-handle",
].join(",");

const HELP_DELAY_MS = 1000;

function getHelpLabelText(target) {
  if (!target) return "";
  const label = target.closest("label");
  if (label) {
    const span = label.querySelector("span");
    const labelText = (span ? span.textContent : label.textContent) || "";
    return labelText.replace(/\s+/g, " ").trim();
  }
  if (target.id) {
    const labelFor = document.querySelector(`label[for="${target.id}"]`);
    if (labelFor) {
      return (labelFor.textContent || "").replace(/\s+/g, " ").trim();
    }
  }
  return "";
}

function getHelpText(target) {
  if (!target) return "";
  // An unavailable control explains itself first: why it cannot be used right
  // now outranks a description of what it would otherwise do. Kept out of
  // `data-help` so that applyHelpMap's managed hints cannot overwrite it.
  const reason = target.dataset?.helpReason;
  if (reason) return reason;
  const dataHelp = target.dataset?.help;
  if (dataHelp) return dataHelp;
  const ariaLabel = target.getAttribute?.("aria-label");
  if (ariaLabel) return ariaLabel;
  const title = target.getAttribute?.("title");
  if (title) return title;
  if (target.classList?.contains("menu-item")) {
    const text = (target.textContent || "").replace(/\s+/g, " ").trim();
    if (text) return t("hint.menu.open", { menu: text });
  }
  if (target.classList?.contains("dropdown-item")) {
    const text = target.querySelector("span")?.textContent?.trim() || "";
    if (text) return text;
  }
  const labelText = getHelpLabelText(target);
  if (labelText) return labelText;
  const text = (target.textContent || "").replace(/\s+/g, " ").trim();
  if (text) return text;
  return "";
}

export function createHelpTooltipController({
  state,
  platformShortcutLabel,
  roiCanvases,
}) {
  let helpTooltip = null;
  let helpTimer = null;
  let helpTarget = null;
  let helpLastEvent = null;

  function setManagedHelp(element, text) {
    if (!element) return;
    element.dataset.help = text;
    element.dataset.helpManaged = "true";
  }

  function applyHelpMap() {
    const helpMap = {
      "btn-prev": "hint.frame.previous",
      "btn-next": "hint.frame.next",
      "btn-play": "hint.frame.play_pause",
      "frame-range": "hint.frame.scrub",
      "frame-index": "hint.frame.exact_number",
      "toolbar-playback-toggle": "hint.toolbar.playback_controls",
      "toolbar-more-toggle": "hint.toolbar.more_controls",
      "toolbar-more-panel-toggle": "hint.toolbar.toggle_side_menu",
      "toolbar-more-fullscreen": "hint.toolbar.fullscreen",
      "viewer-sync-toggle": "hint.toolbar.viewer_sync",
      "viewer-sync-options-toggle": "hint.toolbar.viewer_sync_options",
      "frame-step": "hint.frame.step",
      "fps-select": "hint.frame.fps",
      "toolbar-more-step": "hint.frame.step",
      "toolbar-more-fps": "hint.frame.fps",
      "toolbar-more-threshold": "hint.frame.threshold_channel",
      "toolbar-threshold": "hint.frame.threshold_channel",
      "zoom-range": "hint.view.zoom_slider",
      "zoom-value": "hint.view.zoom_input",
      "reset-view": "hint.view.fit",
      "canvas-wrap": "hint.canvas.controls",
      "pixel-label-toggle": "hint.overlay.pixel_labels",
      "mask-toggle": "hint.overlay.mask",
      "mask-saturated-toggle": "hint.overlay.mask_saturated",
      "colormap-select": "hint.overlay.colormap",
      "invert-color": "hint.overlay.invert",
      "roi-enable": "hint.roi.enable",
      "rings-toggle": "hint.rings.toggle",
      "roi-mode": "hint.roi.mode",
      "roi-histogram": "hint.roi.histogram",
      "roi-clear-btn": "hint.roi.clear",
      "roi-export-csv": "hint.roi.export_csv",
      "autoload-mode": "hint.autoload.mode",
      "filesystem-mode": "hint.autoload.filesystem_mode",
      "autoload-dir": "hint.autoload.directory",
      "autoload-watch-enabled": "hint.autoload.watch_enabled",
      "autoload-browse": "hint.autoload.browse",
      "autoload-select-file": "hint.autoload.select_file",
      "autoload-pattern": "hint.autoload.pattern",
      "autoload-interval": "hint.autoload.interval",
      "remote-source-id": "hint.remote.source_id",
      "remote-interval": "hint.remote.interval",
      "jfjoch-preview-endpoint": "hint.jfjoch.endpoint",
      "jfjoch-test": "hint.jfjoch.test",
      "jfjoch-source-id": "hint.jfjoch.source_id",
      "jfjoch-topic": "hint.jfjoch.topic",
      "jfjoch-channel": "hint.jfjoch.channel",
      "jfjoch-interval": "hint.jfjoch.interval",
      "simplon-url": "hint.simplon.url",
      "simplon-test": "hint.simplon.test",
      "simplon-timeout": "hint.simplon.timeout",
      "simplon-enable": "hint.simplon.enable",
      "series-sum-start": "hint.series.start",
      "series-sum-cancel": "hint.series.cancel",
      "settings-server-external": "hint.settings.server_external",
      "settings-server-port": "hint.settings.server_port",
      "panel-fab": "hint.panel.toggle",
      "panel-collapse-btn": "hint.panel.collapse",
      "panel-resizer": "hint.panel.resizer",
      "panel-sheet-handle": "hint.panel.sheet_handle",
      "fullscreen-toggle": "hint.toolbar.fullscreen",
      "inspector-search-input": "hint.inspector.search",
      "inspector-search-clear": "hint.inspector.clear",
      "command-input": "hint.command.search",
    };

    Object.entries(helpMap).forEach(([id, key]) => {
      const el = document.getElementById(id);
      setManagedHelp(el, t(key));
    });

    const commandMenuItem = document.querySelector('.dropdown-item[data-action="command-palette"]');
    if (commandMenuItem) {
      const shortcut = platformShortcutLabel("command-palette");
      setManagedHelp(
        commandMenuItem,
        shortcut ? t("hint.command.palette_shortcut", { shortcut }) : t("hint.command.palette"),
      );
    }

    document.querySelectorAll(".roi-resize-handle").forEach((el) => {
      setManagedHelp(el, t("hint.roi.resize_panel"));
    });

    roiCanvases.forEach((canvasEl) => {
      setManagedHelp(canvasEl, t("hint.roi.plot_canvas_controls"));
    });

    document.querySelectorAll("[data-help]").forEach((el) => {
      if (el.hasAttribute("title")) {
        el.removeAttribute("title");
      }
    });
  }

  function refreshHelpTooltips() {
    applyHelpMap();
    if (!helpTooltip || !helpTooltip.classList.contains("is-visible") || !helpTarget) return;
    const text = getHelpText(helpTarget);
    if (!text) return;
    helpTooltip.textContent = text;
    positionHelpTooltip(helpLastEvent);
  }

  function positionHelpTooltip(event) {
    if (!helpTooltip || !helpLastEvent) return;
    const evt = event || helpLastEvent;
    const padding = 12;
    const offset = 14;
    let x = evt.clientX + offset;
    let y = evt.clientY + offset;
    const rect = helpTooltip.getBoundingClientRect();
    const maxX = window.innerWidth - rect.width - padding;
    const maxY = window.innerHeight - rect.height - padding;
    if (x > maxX) x = Math.max(padding, evt.clientX - rect.width - offset);
    if (y > maxY) y = Math.max(padding, evt.clientY - rect.height - offset);
    helpTooltip.style.left = `${x}px`;
    helpTooltip.style.top = `${y}px`;
  }

  function hideHelp() {
    if (helpTimer) {
      clearTimeout(helpTimer);
      helpTimer = null;
    }
    if (helpTooltip) {
      helpTooltip.classList.remove("is-visible");
    }
    helpTarget = null;
  }

  function showHelp(target, event, immediate = false) {
    if (!helpTooltip || !state.toolHintsEnabled) return;
    const text = getHelpText(target);
    if (!text) return;
    helpTarget = target;
    helpLastEvent = event;
    if (helpTimer) {
      clearTimeout(helpTimer);
      helpTimer = null;
    }
    const reveal = () => {
      helpTooltip.textContent = text;
      helpTooltip.classList.add("is-visible");
      positionHelpTooltip(event);
    };
    if (immediate) {
      reveal();
    } else {
      helpTimer = setTimeout(reveal, HELP_DELAY_MS);
    }
  }

  function findHelpTarget(node) {
    if (!node) return null;
    if (node.closest(".help-tooltip")) return null;
    return node.closest(HELP_SELECTORS);
  }

  function initHelpTooltips() {
    if (helpTooltip) return;
    helpTooltip = document.createElement("div");
    helpTooltip.className = "help-tooltip";
    helpTooltip.setAttribute("role", "tooltip");
    document.body.appendChild(helpTooltip);
    refreshHelpTooltips();

    document.addEventListener(
      "pointerover",
      (event) => {
        const target = findHelpTarget(event.target);
        if (!target) return;
        if (target === helpTarget) return;
        showHelp(target, event, false);
      },
      true
    );

    document.addEventListener("mouseover", (event) => {
      const target = findHelpTarget(event.target);
      if (!target) return;
      if (target === helpTarget) return;
      showHelp(target, event, false);
    });

    document.addEventListener("pointermove", (event) => {
      if (!helpTooltip) return;
      if (helpTimer) {
        helpLastEvent = event;
        return;
      }
      if (!helpTooltip.classList.contains("is-visible")) return;
      helpLastEvent = event;
      positionHelpTooltip(event);
    });

    document.addEventListener(
      "pointerout",
      (event) => {
        if (!helpTarget) return;
        const related = event.relatedTarget;
        if (related && helpTarget.contains(related)) return;
        hideHelp();
      },
      true
    );

    document.addEventListener(
      "mouseout",
      (event) => {
        if (!helpTarget) return;
        const related = event.relatedTarget;
        if (related && helpTarget.contains(related)) return;
        hideHelp();
      },
      true
    );

    document.addEventListener(
      "focusin",
      (event) => {
        const target = findHelpTarget(event.target);
        if (!target) return;
        const rect = target.getBoundingClientRect();
        const fakeEvent = { clientX: rect.left + rect.width / 2, clientY: rect.top };
        showHelp(target, fakeEvent, true);
      },
      true
    );

    document.addEventListener("focusout", () => {
      hideHelp();
    });
  }

  function setToolHintsEnabled(enabled) {
    state.toolHintsEnabled = Boolean(enabled);
    if (!state.toolHintsEnabled) {
      hideHelp();
    }
  }

  return {
    initHelpTooltips,
    refreshHelpTooltips,
    setToolHintsEnabled,
  };
}
