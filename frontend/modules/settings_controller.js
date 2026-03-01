/**
 * Settings modal controller.
 */

export function createSettingsController({
  apiBase,
  state,
  constants,
  elements,
  callbacks,
}) {
  const {
    pixelLabelDefaultMinCellPx,
    pixelLabelDefaultMaxLabels,
  } = constants;

  const {
    settingsModal,
    settingsClose,
    settingsSave,
    settingsSaveClose,
    settingsConfigPath,
    settingsMessage,
    settingsServerExternal,
    settingsServerPort,
    settingsServerReload,
    settingsStartupTimeout,
    settingsOpenBrowser,
    settingsToolHints,
    settingsPixelLabelMin,
    settingsPixelLabelMax,
    settingsPixelLabelFormat,
    settingsPixelLabelDrag,
    settingsDataRoot,
    settingsAllowAbs,
    settingsScanCache,
    settingsMaxScanDepth,
    settingsMaxUpload,
    settingsLogLevel,
    settingsLogDir,
  } = elements;

  const {
    setToolHintsEnabled,
    openModal,
    closeModal,
    closeMenu,
    setStatus,
    schedulePixelOverlay,
  } = callbacks;

  let settingsModalBusy = false;
  let settingsRequestId = 0;

  function setSettingsMessage(text, isError = false, isBusy = false) {
    if (!settingsMessage) return;
    settingsMessage.textContent = text || "";
    settingsMessage.classList.toggle("is-error", Boolean(isError));
    settingsMessage.classList.toggle("is-busy", Boolean(isBusy));
  }

  function setSettingsModalBusy(isBusy) {
    settingsModalBusy = Boolean(isBusy);
    settingsModal?.setAttribute("aria-busy", settingsModalBusy ? "true" : "false");
    if (settingsSave) {
      settingsSave.disabled = settingsModalBusy;
    }
    if (settingsSaveClose) {
      settingsSaveClose.disabled = settingsModalBusy;
    }
  }

  function isLocalOnlyHost(hostValue) {
    const host = String(hostValue || "")
      .trim()
      .toLowerCase();
    if (!host) return true;
    return host === "127.0.0.1" || host === "localhost" || host === "::1";
  }

  function fillSettingsForm(config, configPath = "") {
    if (!config) return;
    if (settingsServerExternal) {
      const host = String(config?.server?.host ?? "127.0.0.1");
      settingsServerExternal.checked = !isLocalOnlyHost(host);
    }
    settingsServerPort.value = String(Number(config?.server?.port ?? 8000));
    settingsServerReload.checked = Boolean(config?.server?.reload);

    settingsStartupTimeout.value = String(Number(config?.launcher?.startup_timeout_sec ?? 5.0));
    settingsOpenBrowser.checked = Boolean(config?.launcher?.open_browser ?? true);
    if (settingsToolHints) {
      const toolHints = config?.ui?.tool_hints;
      settingsToolHints.checked = Boolean(toolHints ?? state.toolHintsEnabled);
    }
    if (settingsPixelLabelMin) {
      settingsPixelLabelMin.value = String(
        Number(config?.ui?.pixel_label_min_cell_px ?? state.pixelLabelMinCellPx ?? pixelLabelDefaultMinCellPx)
      );
    }
    if (settingsPixelLabelMax) {
      settingsPixelLabelMax.value = String(
        Number(config?.ui?.pixel_label_max_labels ?? state.pixelLabelMaxLabels ?? pixelLabelDefaultMaxLabels)
      );
    }
    if (settingsPixelLabelFormat) {
      settingsPixelLabelFormat.value = String(config?.ui?.pixel_label_format ?? state.pixelLabelFormat ?? "auto");
    }
    if (settingsPixelLabelDrag) {
      settingsPixelLabelDrag.checked = Boolean(
        config?.ui?.pixel_label_show_during_drag ?? state.pixelLabelShowDuringDrag
      );
    }

    settingsDataRoot.value = String(config?.data?.root ?? "");
    settingsAllowAbs.checked = Boolean(config?.data?.allow_abs_paths ?? true);
    settingsScanCache.value = String(Number(config?.data?.scan_cache_sec ?? 2.0));
    settingsMaxScanDepth.value = String(Number(config?.data?.max_scan_depth ?? -1));
    settingsMaxUpload.value = String(Number(config?.data?.max_upload_mb ?? 0));

    settingsLogLevel.value = String(config?.logging?.level ?? "INFO").toUpperCase();
    settingsLogDir.value = String(config?.logging?.dir ?? "");
    if (settingsConfigPath) {
      settingsConfigPath.textContent = configPath || "-";
    }
  }

  function collectSettingsForm() {
    const asInt = (value, fallback) => {
      const num = Number(value);
      if (!Number.isFinite(num)) return fallback;
      return Math.round(num);
    };
    const asFloat = (value, fallback) => {
      const num = Number(value);
      if (!Number.isFinite(num)) return fallback;
      return num;
    };

    return {
      server: {
        host: settingsServerExternal?.checked ? "0.0.0.0" : "127.0.0.1",
        port: Math.max(0, Math.min(65535, asInt(settingsServerPort?.value, 8000))),
        reload: Boolean(settingsServerReload?.checked),
      },
      launcher: {
        startup_timeout_sec: Math.max(0.1, asFloat(settingsStartupTimeout?.value, 5.0)),
        open_browser: Boolean(settingsOpenBrowser?.checked),
      },
      data: {
        root: (settingsDataRoot?.value || "").trim(),
        allow_abs_paths: Boolean(settingsAllowAbs?.checked),
        scan_cache_sec: Math.max(0, asFloat(settingsScanCache?.value, 2.0)),
        max_scan_depth: Math.max(-1, asInt(settingsMaxScanDepth?.value, -1)),
        max_upload_mb: Math.max(0, asInt(settingsMaxUpload?.value, 0)),
      },
      logging: {
        level: (settingsLogLevel?.value || "INFO").toUpperCase(),
        dir: (settingsLogDir?.value || "").trim(),
      },
      ui: {
        tool_hints: Boolean(settingsToolHints?.checked),
        pixel_label_min_cell_px: Math.max(
          8,
          Math.min(64, asInt(settingsPixelLabelMin?.value, state.pixelLabelMinCellPx || pixelLabelDefaultMinCellPx))
        ),
        pixel_label_max_labels: Math.max(
          100,
          Math.min(100000, asInt(settingsPixelLabelMax?.value, state.pixelLabelMaxLabels || pixelLabelDefaultMaxLabels))
        ),
        pixel_label_format: (() => {
          const format = String(settingsPixelLabelFormat?.value || "auto").toLowerCase();
          return format === "integer" || format === "scientific" ? format : "auto";
        })(),
        pixel_label_show_during_drag: Boolean(settingsPixelLabelDrag?.checked),
      },
    };
  }

  function applyUiSettings(uiConfig) {
    const cfg = uiConfig && typeof uiConfig === "object" ? uiConfig : {};
    if (typeof cfg.tool_hints !== "undefined") {
      setToolHintsEnabled(Boolean(cfg.tool_hints));
    }
    const minCell = Number(cfg.pixel_label_min_cell_px);
    if (Number.isFinite(minCell)) {
      state.pixelLabelMinCellPx = Math.max(8, Math.min(64, Math.round(minCell)));
    }
    const maxLabels = Number(cfg.pixel_label_max_labels);
    if (Number.isFinite(maxLabels)) {
      state.pixelLabelMaxLabels = Math.max(100, Math.min(100000, Math.round(maxLabels)));
    }
    const format = String(cfg.pixel_label_format || "").toLowerCase();
    if (format === "auto" || format === "integer" || format === "scientific") {
      state.pixelLabelFormat = format;
    }
    if (typeof cfg.pixel_label_show_during_drag !== "undefined") {
      state.pixelLabelShowDuringDrag = Boolean(cfg.pixel_label_show_during_drag);
    }
  }

  function closeSettingsModal({ restoreFocus = true } = {}) {
    settingsRequestId += 1;
    closeModal(settingsModal, { restoreFocus });
    setSettingsModalBusy(false);
    setSettingsMessage("");
  }

  async function openSettingsModal() {
    closeMenu();
    if (!settingsModal) return;
    const requestId = ++settingsRequestId;
    openModal(settingsModal, { focusTarget: settingsServerPort || settingsClose });
    setSettingsModalBusy(true);
    setSettingsMessage("Loading settings...", false, true);
    try {
      const res = await fetch(`${apiBase}/settings`);
      if (!res.ok) {
        throw new Error(`Settings request failed: ${res.status}`);
      }
      const payload = await res.json();
      if (requestId !== settingsRequestId) return;
      const config = payload?.config || {};
      fillSettingsForm(config, payload?.path || "");
      applyUiSettings(config?.ui);
      if (settingsToolHints) {
        settingsToolHints.checked = Boolean(config?.ui?.tool_hints ?? state.toolHintsEnabled);
      }
      setSettingsMessage("Edit values and click Save or Save & Close.");
    } catch (err) {
      if (requestId !== settingsRequestId) return;
      console.error(err);
      setSettingsMessage("Failed to load settings", true);
    } finally {
      if (requestId === settingsRequestId) {
        setSettingsModalBusy(false);
      }
    }
  }

  async function saveSettingsFromModal(closeAfter = false) {
    if (!settingsSave || settingsModalBusy) return;
    const config = collectSettingsForm();
    setSettingsModalBusy(true);
    setSettingsMessage("Saving settings...", false, true);
    try {
      const res = await fetch(`${apiBase}/settings`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ config }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        throw new Error(data?.detail || `Save failed (${res.status})`);
      }
      fillSettingsForm(data?.config || config, data?.path || "");
      applyUiSettings(data?.config?.ui || config?.ui);
      schedulePixelOverlay();
      setSettingsMessage("Saved. Restart ALBIS to apply all settings.");
      setStatus("Settings saved. Restart ALBIS to apply all settings.");
      if (closeAfter) {
        closeSettingsModal();
      }
    } catch (err) {
      console.error(err);
      setSettingsMessage(String(err?.message || "Failed to save settings"), true);
    } finally {
      setSettingsModalBusy(false);
    }
  }

  return {
    applyUiSettings,
    closeSettingsModal,
    openSettingsModal,
    saveSettingsFromModal,
  };
}
