/**
 * Contextual help tooltip controller.
 */

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
  const dataHelp = target.dataset?.help;
  if (dataHelp) return dataHelp;
  const ariaLabel = target.getAttribute?.("aria-label");
  if (ariaLabel) return ariaLabel;
  const title = target.getAttribute?.("title");
  if (title) return title;
  if (target.classList?.contains("menu-item")) {
    const text = (target.textContent || "").replace(/\s+/g, " ").trim();
    if (text) return `Open ${text} menu`;
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

  function applyHelpMap() {
    const helpMap = {
      "btn-prev": "Move to the previous frame in the loaded stack (Left Arrow).",
      "btn-next": "Move to the next frame in the loaded stack (Right Arrow).",
      "btn-play": "Start or pause frame playback using the selected FPS (Tab).",
      "frame-range": "Drag to scrub through frames.",
      "frame-index": "Enter an exact frame number.",
      "toolbar-playback-toggle": "Open playback controls for frame step and FPS.",
      "toolbar-more-toggle": "Open additional toolbar controls.",
      "toolbar-more-panel-toggle": "Open or close the side analysis menu.",
      "toolbar-more-fullscreen": "Enter or leave full-screen mode (F).",
      "frame-step": "Set how many frames each step advances.",
      "fps-select": "Set playback speed in frames per second.",
      "toolbar-more-step": "Set how many frames each step advances.",
      "toolbar-more-fps": "Set playback speed in frames per second.",
      "toolbar-more-threshold": "Choose the detector threshold channel (Up/Down Arrow).",
      "toolbar-threshold": "Choose the detector threshold channel (Up/Down Arrow).",
      "zoom-range": "Adjust image zoom level.",
      "reset-view": "Fit the full image into the viewport.",
      "canvas-wrap":
        "Wheel: zoom at cursor. Left-drag: pan. Shift+left-drag: contrast/brightness (horizontal/vertical). Right-drag: ROI.",
      "pixel-label-toggle": "Show pixel intensity labels at high zoom.",
      "mask-toggle": "Apply the detector pixel mask when available.",
      "mask-saturated-toggle": "Hide saturated pixels (datatype maximum).",
      "colormap-select": "Change the intensity-to-color mapping.",
      "invert-color": "Invert the active color map.",
      "roi-enable": "Enable ROI overlays and ROI statistics.",
      "rings-toggle": "Show or hide resolution ring overlays.",
      "roi-mode": "Choose ROI geometry (line, box, circle, annulus).",
      "roi-log": "Display ROI plots with logarithmic Y scale.",
      "roi-limits-enable": "Autoscale ROI plots; disable to keep manual zoom/pan.",
      "roi-clear-btn": "Clear the active ROI and reset ROI stats.",
      "roi-export-csv": "Export current ROI profile/projection data as CSV.",
      "autoload-mode": "Select where incoming images are read from.",
      "filesystem-mode": "Choose local filesystem source mode.",
      "autoload-dir": "Folder path to poll for new files.",
      "autoload-watch-enabled": "Automatically poll the selected folder for updates.",
      "autoload-browse": "Browse and select a source folder.",
      "autoload-select-file": "Open a file picker and load a specific image file.",
      "autoload-pattern": "Filename filter with wildcard support.",
      "autoload-interval": "Polling interval in milliseconds.",
      "remote-source-id": "Remote stream source identifier.",
      "remote-interval": "Remote polling interval in milliseconds.",
      "jfjoch-preview-endpoint": "ZeroMQ preview PUB endpoint for JUNGFRAUJOCH (for example tcp://host:31003).",
      "jfjoch-source-id": "ALBIS source identifier used to cache JUNGFRAUJOCH frames.",
      "jfjoch-topic": "Optional ZeroMQ topic prefix to subscribe to.",
      "jfjoch-channel": "Optional image channel to display from data map payload.",
      "jfjoch-interval": "Polling interval in milliseconds for updated preview frames.",
      "simplon-url": "Base URL for the SIMPLON monitor API.",
      "simplon-timeout": "Request timeout for monitor polling (ms).",
      "simplon-enable": "Enable or pause SIMPLON live monitoring.",
      "series-sum-start": "Start the configured series operation job.",
      "series-sum-cancel": "Cancel the currently running series operation.",
      "settings-server-external": "Allow connections from other machines (binds to all interfaces).",
      "settings-server-port": "Backend server port. Use 0 to auto-select a free port at startup.",
      "panel-fab": "Toggle the side panel open or closed (M).",
      "panel-collapse-btn": "Collapse the side panel (M).",
      "panel-resizer": "Drag to resize the side panel width.",
      "panel-sheet-handle": "Drag up/down to resize the mobile panel sheet.",
      "fullscreen-toggle": "Enter or leave full-screen mode (F).",
      "inspector-search-input": "Search datasets and nodes in the HDF5 tree.",
      "inspector-search-clear": "Clear the current inspector search query.",
      "command-input": "Search available commands and run one.",
    };

    Object.entries(helpMap).forEach(([id, text]) => {
      const el = document.getElementById(id);
      if (el && !el.dataset.help) {
        el.dataset.help = text;
      }
    });

    const commandMenuItem = document.querySelector('.dropdown-item[data-action="command-palette"]');
    if (commandMenuItem && !commandMenuItem.dataset.help) {
      commandMenuItem.dataset.help = `Command Palette (${platformShortcutLabel("command-palette")})`;
    }

    document.querySelectorAll(".roi-resize-handle").forEach((el) => {
      if (!el.dataset.help) {
        el.dataset.help = "Drag to change ROI plot panel height.";
      }
    });

    roiCanvases.forEach((canvasEl) => {
      if (canvasEl && !canvasEl.dataset.help) {
        canvasEl.dataset.help = "Drag to pan plot axes. Use wheel on axes to zoom. Double-click to reset.";
      }
    });

    document.querySelectorAll("[data-help]").forEach((el) => {
      if (el.hasAttribute("title")) {
        el.removeAttribute("title");
      }
    });
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
    applyHelpMap();

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
    setToolHintsEnabled,
  };
}
