/**
 * Toolbar, footer, and chrome-idle UI state controller.
 */

import { t } from "./i18n.js";

export function createChromeToolbarController({
  state,
  constants,
  elements,
  callbacks,
}) {
  const {
    frameStepOptions,
    chromeIdleDelayMs,
  } = constants;

  const {
    fpsSelect,
    toolbarMoreFps,
    toolbarMoreStep,
    toolbarMoreThreshold,
    toolbarMorePanelToggle,
    toolbarMoreFullscreen,
    toolbarPlaybackWrap,
    toolbarPlaybackToggle,
    toolbarPlaybackPopover,
    toolbarMoreWrap,
    toolbarMoreToggle,
    toolbarMorePopover,
    toolbarStepWrap,
    toolbarFpsWrap,
    footerVersionToggleEl,
    footerVersionPopoverEl,
    footerFileEl,
    footerZoomEl,
    footerVersionBuildEl,
    footerVersionUpdateEl,
    footerVersionStaleEl,
    footerVersionStaleTextEl,
    footerVersionReloadEl,
    footerVersionCopyEl,
    splash,
    dropdown,
    panelFab,
    toolbarPath,
    dataSourceSummaryEl,
  } = elements;

  const {
    middleTruncate,
    fileLabel,
    formatTimeStamp,
    setSummaryChip,
    estimateToolbarChars,
    updateSeriesSumUi,
    isPhonePanelLayout,
    isMenuOpen,
    onOpenUpdateCheck,
    onCopyBuildInfoUnavailable,
  } = callbacks;

  let footerVersionPopoverOpen = false;
  let chromeIdleTimer = null;
  let chromeIdleActive = false;
  let chromeActivityTs = 0;

  function updateFpsLabel() {
    if (fpsSelect) {
      fpsSelect.value = String(state.fps);
    }
    if (toolbarMoreFps) {
      toolbarMoreFps.value = String(state.fps);
    }
  }

  function syncToolbarPlaybackToggle() {
    if (!toolbarPlaybackWrap || !toolbarPlaybackToggle || !toolbarPlaybackPopover) return;
    const open = toolbarPlaybackWrap.classList.contains("is-open");
    toolbarPlaybackToggle.setAttribute("aria-expanded", open ? "true" : "false");
    toolbarPlaybackToggle.textContent = open ? t("toolbar.playback.open") : t("toolbar.playback.closed");
    toolbarPlaybackPopover.setAttribute("aria-hidden", open ? "false" : "true");
  }

  function setToolbarPlaybackPopoverOpen(open) {
    if (!toolbarPlaybackWrap || !toolbarPlaybackToggle || !toolbarPlaybackPopover) return;
    if (open) {
      closeToolbarMorePopover();
    }
    toolbarPlaybackWrap.classList.toggle("is-open", open);
    syncToolbarPlaybackToggle();
  }

  function closeToolbarPlaybackPopover() {
    setToolbarPlaybackPopoverOpen(false);
  }

  function toggleToolbarPlaybackPopover() {
    if (!toolbarPlaybackWrap || toolbarPlaybackWrap.classList.contains("is-hidden")) return;
    const hasStep = Boolean(toolbarStepWrap && !toolbarStepWrap.classList.contains("is-hidden"));
    const hasFps = Boolean(toolbarFpsWrap && !toolbarFpsWrap.classList.contains("is-hidden"));
    if (!hasStep && !hasFps) return;
    const willOpen = !toolbarPlaybackWrap.classList.contains("is-open");
    setToolbarPlaybackPopoverOpen(willOpen);
  }

  function setToolbarMorePopoverOpen(open) {
    if (!toolbarMoreWrap || !toolbarMoreToggle || !toolbarMorePopover) return;
    if (open) {
      closeToolbarPlaybackPopover();
    }
    toolbarMoreWrap.classList.toggle("is-open", open);
    toolbarMoreToggle.setAttribute("aria-expanded", open ? "true" : "false");
    toolbarMoreToggle.textContent = open ? t("toolbar.more.open") : t("toolbar.more.closed");
    toolbarMorePopover.setAttribute("aria-hidden", open ? "false" : "true");
  }

  function closeToolbarMorePopover() {
    setToolbarMorePopoverOpen(false);
  }

  function toggleToolbarMorePopover() {
    if (!toolbarMoreWrap || toolbarMoreWrap.classList.contains("is-hidden")) return;
    const willOpen = !toolbarMoreWrap.classList.contains("is-open");
    setToolbarMorePopoverOpen(willOpen);
  }

  function syncToolbarMoreControls() {
    if (toolbarMoreStep) {
      toolbarMoreStep.value = String(state.step || frameStepOptions[0]);
    }
    if (toolbarMoreFps) {
      toolbarMoreFps.value = String(state.fps || 1);
    }
    if (toolbarMoreThreshold) {
      toolbarMoreThreshold.value = String(state.thresholdIndex || 0);
    }
    if (toolbarMorePanelToggle) {
      const label = state.panelCollapsed ? t("toolbar.side_menu.open") : t("toolbar.side_menu.close");
      toolbarMorePanelToggle.textContent = label;
      toolbarMorePanelToggle.setAttribute("aria-label", label);
    }
    if (toolbarMoreFullscreen) {
      toolbarMoreFullscreen.textContent = document.fullscreenElement ? t("toolbar.fullscreen.exit") : t("toolbar.fullscreen.enter");
    }
  }

  function buildViewerSourceText(maxChars = 72) {
    if (!state.file) return t("toolbar.source.no_file");
    const fileName = fileLabel(state.file);
    let frameLabel = "";
    if (state.frameCount > 1) {
      frameLabel = `${state.frameIndex + 1} / ${state.frameCount}`;
    } else if (state.autoload.mode !== "file" && state.autoload.lastUpdate) {
      frameLabel = formatTimeStamp(state.autoload.lastUpdate);
    }
    const datasetRaw = state.dataset ? middleTruncate(state.dataset, 38) : "";
    const datasetLabel = datasetRaw ? ` ${datasetRaw}` : "";
    const suffix = frameLabel ? `  ${frameLabel}` : "";
    const reserved = datasetLabel.length + suffix.length;
    const fileBudget = Math.max(10, maxChars - reserved);
    const fileText = middleTruncate(fileName, fileBudget);
    return `${fileText}${datasetLabel}${suffix}`;
  }

  /**
   * What to paste into a bug report.
   *
   * Deliberately not translated. The person reading this is whoever is
   * diagnosing the problem, not the person running the app, and a report in a
   * language the maintainer cannot read is worse than one that is terse.
   */
  function buildInfoText() {
    const commit = state.backendCommit ? ` (${state.backendCommit})` : "";
    const lines = [
      `ALBIS ${state.backendVersion || "unknown"}${commit}`,
      `Server: ${state.backendAlive ? "online" : "offline"}`,
    ];
    if (state.serverBuildChanged) {
      lines.push(`Page loaded against build: ${state.buildStampAtLoad || "unknown"}`);
    }
    if (typeof navigator !== "undefined" && navigator.userAgent) {
      lines.push(`User agent: ${navigator.userAgent}`);
    }
    return lines.join("\n");
  }

  function updateFooterBuildRow() {
    if (!footerVersionBuildEl) return;
    const version = state.backendVersion || "-";
    const commit = state.backendCommit || "";
    // An unstamped build has no commit to show, and inventing one would be
    // worse than the version alone.
    footerVersionBuildEl.textContent = commit
      ? t("toolbar.footer.build", { version, commit })
      : t("toolbar.footer.build.unstamped", { version });
    footerVersionBuildEl.title = t("toolbar.footer.build.title");
  }

  function updateFooterUpdateRow() {
    if (!footerVersionUpdateEl) return;
    const status = String(state.updateStatus || "");
    const actionable = status === "update_available";
    let label = "";
    if (status === "up_to_date") {
      label = t("toolbar.footer.update.up_to_date");
    } else if (actionable) {
      label = t("toolbar.footer.update.available", { version: state.updateLatestVersion || "-" });
    } else if (status === "unavailable") {
      label = t("toolbar.footer.update.unavailable");
    } else if (status === "disabled") {
      label = t("toolbar.footer.update.disabled");
    }
    footerVersionUpdateEl.hidden = !label;
    footerVersionUpdateEl.textContent = label;
    // Only a pending update leads anywhere, so only then is it a control.
    footerVersionUpdateEl.disabled = !actionable;
    footerVersionUpdateEl.classList.toggle("is-actionable", actionable);
  }

  function updateFooterStaleRow() {
    if (!footerVersionStaleEl) return;
    const stale = state.serverBuildChanged === true;
    footerVersionStaleEl.hidden = !stale;
    if (stale && footerVersionStaleTextEl) {
      footerVersionStaleTextEl.textContent = t("toolbar.footer.stale", {
        version: state.backendVersion || "-",
        commit: state.backendCommit || "-",
      });
    }
  }

  function updateFooterVersions() {
    updateFooterBuildRow();
    updateFooterUpdateRow();
    updateFooterStaleRow();
  }

  function updateViewerFooter() {
    if (footerFileEl) {
      const hasFile = Boolean(state.file);
      footerFileEl.textContent = hasFile ? buildViewerSourceText(78) : "";
      footerFileEl.classList.toggle("is-empty", !hasFile);
    }
    if (footerZoomEl) {
      footerZoomEl.textContent = t("toolbar.footer.zoom", { zoom: (state.zoom || 1).toFixed(1) });
    }
    updateFooterVersions();
    scheduleChromeIdle();
  }

  function setFooterVersionPopoverOpen(open) {
    footerVersionPopoverOpen = Boolean(open);
    if (footerVersionToggleEl) {
      footerVersionToggleEl.setAttribute("aria-expanded", footerVersionPopoverOpen ? "true" : "false");
      footerVersionToggleEl.textContent = footerVersionPopoverOpen ? t("toolbar.footer.versions.open") : t("toolbar.footer.versions.closed");
    }
    if (footerVersionPopoverEl) {
      footerVersionPopoverEl.classList.toggle("is-open", footerVersionPopoverOpen);
      footerVersionPopoverEl.setAttribute("aria-hidden", footerVersionPopoverOpen ? "false" : "true");
    }
  }

  function closeFooterVersionPopover() {
    setFooterVersionPopoverOpen(false);
  }

  function toggleFooterVersionPopover() {
    setFooterVersionPopoverOpen(!footerVersionPopoverOpen);
  }

  function shouldEnableChromeIdle() {
    if (!document.body.classList.contains("canvas-first")) return false;
    if (!state.hasFrame || state.isLoading) return false;
    if (!splash?.classList.contains("is-hidden")) return false;
    if (isMenuOpen() && dropdown?.classList.contains("is-open")) return false;
    if (toolbarPlaybackWrap?.classList.contains("is-open")) return false;
    if (toolbarMoreWrap?.classList.contains("is-open")) return false;
    if (footerVersionPopoverOpen) return false;
    return true;
  }

  function setChromeIdle(active) {
    chromeIdleActive = Boolean(active);
    document.body.classList.toggle("chrome-idle", chromeIdleActive);
  }

  function clearChromeIdleTimer() {
    if (!chromeIdleTimer) return;
    window.clearTimeout(chromeIdleTimer);
    chromeIdleTimer = null;
  }

  function scheduleChromeIdle() {
    clearChromeIdleTimer();
    if (!shouldEnableChromeIdle()) {
      setChromeIdle(false);
      return;
    }
    chromeIdleTimer = window.setTimeout(() => {
      chromeIdleTimer = null;
      if (shouldEnableChromeIdle()) {
        setChromeIdle(true);
      }
    }, chromeIdleDelayMs);
  }

  function registerChromeActivity() {
    const now = performance.now();
    if (!chromeIdleActive && now - chromeActivityTs < 110) return;
    chromeActivityTs = now;
    setChromeIdle(false);
    scheduleChromeIdle();
  }

  function syncOverlayAnchors() {
    if (!document.body.classList.contains("canvas-first")) return;
    if (isPhonePanelLayout()) return;
    const toolbarEl = document.querySelector(".toolbar");
    if (!toolbarEl) return;
    const toolbarRect = toolbarEl.getBoundingClientRect();
    if (!Number.isFinite(toolbarRect.top)) return;
    const anchorTop = Math.max(0, Math.round(toolbarRect.top));
    document.documentElement.style.setProperty("--overlay-anchor-top", `${anchorTop}px`);
    const fabHeight = panelFab?.getBoundingClientRect().height || 46;
    const triggerTop = Math.max(0, Math.round(anchorTop + Math.max(0, (toolbarRect.height - fabHeight) * 0.5)));
    document.documentElement.style.setProperty("--overlay-panel-trigger-top", `${triggerTop}px`);
  }

  function updateUiIdleAndAnchors() {
    syncOverlayAnchors();
    scheduleChromeIdle();
  }

  function updateDataSourceSummary() {
    if (!dataSourceSummaryEl) return;
    const mode = (state.autoload.mode || "file").toLowerCase();
    if (mode === "file") {
      const hasFile = Boolean(state.file);
      const fileText = hasFile ? middleTruncate(fileLabel(state.file), 24) : t("toolbar.datasource.no_file");
      setSummaryChip(dataSourceSummaryEl, `${t("toolbar.datasource.file")} · ${fileText}`, hasFile ? "active" : "default");
      return;
    }

    const modeLabel =
      mode === "simplon"
        ? t("toolbar.datasource.simplon")
        : mode === "jungfraujoch"
          ? t("toolbar.datasource.jfjoch")
          : t("toolbar.datasource.remote");
    const running = Boolean(state.autoload.running);
    const age = Date.now() - (state.autoload.lastUpdate || 0);
    const paused = running && state.autoload.livePaused === true;
    const stale = !paused && running && (!state.autoload.lastUpdate || age > Math.max(1500, state.autoload.interval * 2));
    const streamState = !running
      ? t("toolbar.datasource.state.idle")
      : paused
        ? t("toolbar.datasource.state.paused")
      : stale
        ? t("toolbar.datasource.state.waiting")
        : t("toolbar.datasource.state.live");
    const tone = stale || paused ? "warning" : running ? "active" : "default";
    setSummaryChip(dataSourceSummaryEl, `${modeLabel} · ${streamState}`, tone);
  }

  let copyFeedbackTimer = null;

  /**
   * Copy without the Clipboard API, which needs a secure context.
   *
   * ALBIS is routinely reached over plain HTTP on a LAN -- that is a documented
   * way to use it -- so `navigator.clipboard` being absent is the ordinary case
   * for a remote session, not an edge case. A hidden textarea and
   * `execCommand` still work there, and a button that copies is worth more than
   * a modern API that does nothing.
   */
  function copyViaSelection(text) {
    if (typeof document === "undefined" || !document.body) return false;
    const scratch = document.createElement("textarea");
    scratch.value = text;
    // Off-screen rather than hidden: a display:none element cannot be selected.
    scratch.setAttribute("aria-hidden", "true");
    scratch.style.position = "fixed";
    scratch.style.top = "-1000px";
    scratch.style.opacity = "0";
    document.body.appendChild(scratch);
    let copied = false;
    try {
      scratch.select();
      copied = document.execCommand?.("copy") === true;
    } catch {
      copied = false;
    } finally {
      scratch.remove();
    }
    return copied;
  }

  async function copyBuildInfo() {
    if (!footerVersionCopyEl) return;
    const text = buildInfoText();
    let copied = false;
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(text);
        copied = true;
      }
    } catch {
      copied = false;
    }
    if (!copied) {
      copied = copyViaSelection(text);
    }
    if (!copied) {
      onCopyBuildInfoUnavailable?.(text);
      return;
    }
    footerVersionCopyEl.textContent = t("toolbar.footer.copy_build.copied");
    if (copyFeedbackTimer) window.clearTimeout(copyFeedbackTimer);
    copyFeedbackTimer = window.setTimeout(() => {
      copyFeedbackTimer = null;
      if (footerVersionCopyEl) {
        footerVersionCopyEl.textContent = t("toolbar.footer.copy_build");
      }
    }, 1500);
  }

  footerVersionCopyEl?.addEventListener("click", () => {
    void copyBuildInfo();
  });

  footerVersionUpdateEl?.addEventListener("click", () => {
    // Reuses the update dialog rather than duplicating its release link and
    // version rows in a popover.
    onOpenUpdateCheck?.();
  });

  footerVersionReloadEl?.addEventListener("click", () => {
    window.location.reload();
  });

  function updateToolbar() {
    syncToolbarPlaybackToggle();
    if (toolbarPath) {
      toolbarPath.textContent = buildViewerSourceText(estimateToolbarChars());
    }
    updateSeriesSumUi();
    updateDataSourceSummary();
    syncToolbarMoreControls();
    updateViewerFooter();
    syncOverlayAnchors();
  }

  return {
    updateFpsLabel,
    setToolbarPlaybackPopoverOpen,
    closeToolbarPlaybackPopover,
    toggleToolbarPlaybackPopover,
    setToolbarMorePopoverOpen,
    closeToolbarMorePopover,
    toggleToolbarMorePopover,
    syncToolbarMoreControls,
    buildViewerSourceText,
    updateFooterVersions,
    updateViewerFooter,
    setFooterVersionPopoverOpen,
    closeFooterVersionPopover,
    toggleFooterVersionPopover,
    scheduleChromeIdle,
    registerChromeActivity,
    syncOverlayAnchors,
    updateUiIdleAndAnchors,
    updateDataSourceSummary,
    updateToolbar,
  };
}
