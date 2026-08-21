/**
 * Settings modal controller.
 */

import { onLanguageChange, t } from "./i18n.js";
import { showConfirmDialog } from "./dialogs.js";

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

  // Mirrors ui.frame_cache_mb in backend/config.py. Only a fallback: the value
  // shown and saved comes from the loaded config whenever there is one.
  const FRAME_CACHE_DEFAULT_MB = 256;

  const {
    settingsModal,
    settingsClose,
    settingsSave,
    settingsTabs,
    settingsRestartNote,
    settingsConfigPath,
    settingsMessage,
    settingsServerExternal,
    settingsServerExternalWarning,
    settingsServerPort,
    settingsServerReload,
    settingsAllowedHosts,
    settingsCompression,
    settingsStartupTimeout,
    settingsStartupHealthTimeout,
    settingsOpenBrowser,
    settingsAutoCheckUpdates,
    settingsToolHints,
    settingsLanguage,
    settingsPixelLabelMin,
    settingsPixelLabelMax,
    settingsFrameCache,
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
    applyLanguagePreference,
  } = callbacks;

  // The config last loaded into the form. Saving rebuilds each section from the
  // form controls, so without this any key that has no control — server.compression
  // or ui.frame_cache_mb, say — would be dropped on save and silently reset to its
  // default by the backend's normalization.
  let loadedConfig = null;
  let settingsModalBusy = false;
  let settingsRequestId = 0;
  let syncingExternalAccessUi = false;

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
    if (settingsLanguage) {
      settingsLanguage.disabled = settingsModalBusy;
    }
  }

  function isLocalOnlyHost(hostValue) {
    const host = String(hostValue || "")
      .trim()
      .toLowerCase();
    if (!host) return true;
    return host === "127.0.0.1" || host === "localhost" || host === "::1";
  }

  function updateExternalAccessUi() {
    const enabled = Boolean(settingsServerExternal?.checked);
    // The label stays constant. It used to become "external connections are
    // enabled" when ticked, which left the control no longer describing what it
    // does -- and unticking it unlabelled -- while duplicating the warning box
    // directly below. The box is the right place for the warning.
    if (settingsServerExternalWarning) {
      settingsServerExternalWarning.textContent = t("settings.server.external_warning");
      settingsServerExternalWarning.classList.toggle("is-hidden", !enabled);
      settingsServerExternalWarning.setAttribute("aria-hidden", enabled ? "false" : "true");
    }
  }

  function setExternalAccessChecked(checked) {
    if (!settingsServerExternal) return;
    syncingExternalAccessUi = true;
    settingsServerExternal.checked = Boolean(checked);
    syncingExternalAccessUi = false;
    updateExternalAccessUi();
  }

  async function handleExternalAccessToggle() {
    if (!settingsServerExternal) return;
    if (!syncingExternalAccessUi && settingsServerExternal.checked) {
      const confirmed = await showConfirmDialog({
        title: t("settings.server.external_access"),
        message: t("settings.server.external_confirm"),
        confirmLabel: t("common.confirm"),
        danger: true,
      });
      if (!confirmed) {
        setExternalAccessChecked(false);
        return;
      }
    }
    updateExternalAccessUi();
  }

  // `server.allowed_hosts` is a list in the config and a single field in the
  // dialog. Split on commas and whitespace so a pasted list works whichever way
  // it is separated, and drop blanks so a trailing comma is harmless.
  function parseAllowedHosts(value) {
    return String(value || "")
      .split(/[\s,]+/)
      .map((entry) => entry.trim())
      .filter(Boolean);
  }

  function formatAllowedHosts(value) {
    if (Array.isArray(value)) return value.join(", ");
    return String(value || "");
  }

  // Fields whose value only reaches the running process at the next start. The
  // dialog used to badge each of these; naming them in the footer once one has
  // actually changed says more and shows nothing until it is relevant.
  const RESTART_SCOPED_IDS = [
    "settings-server-external",
    "settings-server-port",
    "settings-compression",
    "settings-startup-timeout",
    "settings-startup-health-timeout",
    "settings-server-reload",
    "settings-open-browser",
    "settings-data-root",
    "settings-log-level",
    "settings-log-dir",
  ];

  // Values as last loaded or saved, so the note reflects unsaved edits only.
  let restartBaseline = new Map();

  function readFieldValue(el) {
    if (!el) return null;
    return el.type === "checkbox" ? String(el.checked) : String(el.value);
  }

  /** The field's own visible label, so the note needs no second set of names. */
  function fieldLabel(el) {
    const span = el?.closest("label")?.querySelector("span");
    return (span?.textContent || el?.id || "").trim();
  }

  function captureRestartBaseline() {
    restartBaseline = new Map(
      RESTART_SCOPED_IDS.map((id) => [id, readFieldValue(document.getElementById(id))]),
    );
    updateRestartNote();
  }

  function updateRestartNote() {
    if (!settingsRestartNote) return;
    const changed = [];
    for (const [id, before] of restartBaseline) {
      const el = document.getElementById(id);
      if (!el) continue;
      if (readFieldValue(el) !== before) changed.push(fieldLabel(el));
    }
    if (!changed.length) {
      settingsRestartNote.textContent = "";
      settingsRestartNote.classList.add("is-hidden");
      settingsRestartNote.setAttribute("aria-hidden", "true");
      return;
    }
    settingsRestartNote.textContent = "";
    const icon = document.createElement("span");
    icon.textContent = "\u21bb";
    const text = document.createElement("span");
    text.textContent = t("settings.restart_note", { fields: changed.join(", ") });
    settingsRestartNote.append(icon, text);
    settingsRestartNote.classList.remove("is-hidden");
    settingsRestartNote.setAttribute("aria-hidden", "false");
  }

  function showSettingsTab(name) {
    if (!settingsModal) return;
    const target = String(name || "");
    settingsModal.querySelectorAll("[data-settings-tab]").forEach((el) => {
      const matches = el.dataset.settingsTab === target;
      if (el.classList.contains("panel-tab")) {
        el.classList.toggle("is-active", matches);
        el.setAttribute("aria-selected", matches ? "true" : "false");
      } else {
        el.classList.toggle("is-active", matches);
        el.hidden = !matches;
      }
    });
  }

  function fillSettingsForm(config, configPath = "") {
    if (!config) return;
    loadedConfig = config;
    if (settingsServerExternal) {
      const host = String(config?.server?.host ?? "127.0.0.1");
      setExternalAccessChecked(!isLocalOnlyHost(host));
    }
    settingsServerPort.value = String(Number(config?.server?.port ?? 0));
    settingsServerReload.checked = Boolean(config?.server?.reload);
    if (settingsAllowedHosts) {
      settingsAllowedHosts.value = formatAllowedHosts(config?.server?.allowed_hosts);
    }
    if (settingsCompression) {
      settingsCompression.value = String(config?.server?.compression ?? "auto");
    }

    settingsStartupTimeout.value = String(Number(config?.launcher?.startup_timeout_sec ?? 5.0));
    if (settingsStartupHealthTimeout) {
      settingsStartupHealthTimeout.value = String(
        Number(config?.launcher?.startup_health_timeout_sec ?? 15.0)
      );
    }
    settingsOpenBrowser.checked = Boolean(config?.launcher?.open_browser ?? true);
    if (settingsAutoCheckUpdates) {
      settingsAutoCheckUpdates.checked = Boolean(config?.ui?.auto_check_updates ?? state.autoCheckUpdates ?? true);
    }
    if (settingsToolHints) {
      const toolHints = config?.ui?.tool_hints;
      settingsToolHints.checked = Boolean(toolHints ?? state.toolHintsEnabled);
    }
    if (settingsLanguage) {
      settingsLanguage.value = String(config?.ui?.language ?? state.language ?? "en");
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
    if (settingsFrameCache) {
      settingsFrameCache.value = String(
        Number(config?.ui?.frame_cache_mb ?? state.frameCacheMb ?? FRAME_CACHE_DEFAULT_MB)
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
    updateExternalAccessUi();
    captureRestartBaseline();
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
        ...(loadedConfig?.server || {}),
        host: settingsServerExternal?.checked ? "0.0.0.0" : "127.0.0.1",
        port: Math.max(0, Math.min(65535, asInt(settingsServerPort?.value, 0))),
        reload: Boolean(settingsServerReload?.checked),
        ...(settingsAllowedHosts
          ? { allowed_hosts: parseAllowedHosts(settingsAllowedHosts.value) }
          : {}),
        ...(settingsCompression ? { compression: String(settingsCompression.value || "auto") } : {}),
      },
      launcher: {
        ...(loadedConfig?.launcher || {}),
        startup_timeout_sec: Math.max(0.1, asFloat(settingsStartupTimeout?.value, 5.0)),
        ...(settingsStartupHealthTimeout
          ? {
              startup_health_timeout_sec: Math.max(
                0.1,
                asFloat(settingsStartupHealthTimeout.value, 15.0),
              ),
            }
          : {}),
        open_browser: Boolean(settingsOpenBrowser?.checked),
      },
      data: {
        ...(loadedConfig?.data || {}),
        root: (settingsDataRoot?.value || "").trim(),
        allow_abs_paths: Boolean(settingsAllowAbs?.checked),
        scan_cache_sec: Math.max(0, asFloat(settingsScanCache?.value, 2.0)),
        max_scan_depth: Math.max(-1, asInt(settingsMaxScanDepth?.value, -1)),
        max_upload_mb: Math.max(0, asInt(settingsMaxUpload?.value, 0)),
      },
      logging: {
        ...(loadedConfig?.logging || {}),
        level: (settingsLogLevel?.value || "INFO").toUpperCase(),
        dir: (settingsLogDir?.value || "").trim(),
      },
      ui: {
        ...(loadedConfig?.ui || {}),
        tool_hints: Boolean(settingsToolHints?.checked),
        auto_check_updates: Boolean(settingsAutoCheckUpdates?.checked),
        pixel_label_min_cell_px: Math.max(
          8,
          Math.min(64, asInt(settingsPixelLabelMin?.value, state.pixelLabelMinCellPx || pixelLabelDefaultMinCellPx))
        ),
        pixel_label_max_labels: Math.max(
          100,
          Math.min(100000, asInt(settingsPixelLabelMax?.value, state.pixelLabelMaxLabels || pixelLabelDefaultMaxLabels))
        ),
        frame_cache_mb: Math.max(
          0,
          Math.min(4096, asInt(settingsFrameCache?.value, state.frameCacheMb ?? FRAME_CACHE_DEFAULT_MB))
        ),
        pixel_label_format: (() => {
          const format = String(settingsPixelLabelFormat?.value || "auto").toLowerCase();
          return format === "integer" || format === "scientific" ? format : "auto";
        })(),
        pixel_label_show_during_drag: Boolean(settingsPixelLabelDrag?.checked),
        language: String(settingsLanguage?.value || state.language || "en"),
      },
    };
  }

  function applyUiSettings(uiConfig, options = {}) {
    const { source = "config" } = options;
    const cfg = uiConfig && typeof uiConfig === "object" ? uiConfig : {};
    if (typeof cfg.tool_hints !== "undefined") {
      setToolHintsEnabled(Boolean(cfg.tool_hints));
    }
    if (typeof cfg.auto_check_updates !== "undefined") {
      state.autoCheckUpdates = Boolean(cfg.auto_check_updates);
    }
    const minCell = Number(cfg.pixel_label_min_cell_px);
    if (Number.isFinite(minCell)) {
      state.pixelLabelMinCellPx = Math.max(8, Math.min(64, Math.round(minCell)));
    }
    const maxLabels = Number(cfg.pixel_label_max_labels);
    if (Number.isFinite(maxLabels)) {
      state.pixelLabelMaxLabels = Math.max(100, Math.min(100000, Math.round(maxLabels)));
    }
    const frameCacheMb = Number(cfg.frame_cache_mb);
    if (Number.isFinite(frameCacheMb)) {
      state.frameCacheMb = Math.max(0, Math.min(4096, Math.round(frameCacheMb)));
    }
    const format = String(cfg.pixel_label_format || "").toLowerCase();
    if (format === "auto" || format === "integer" || format === "scientific") {
      state.pixelLabelFormat = format;
    }
    if (typeof cfg.pixel_label_show_during_drag !== "undefined") {
      state.pixelLabelShowDuringDrag = Boolean(cfg.pixel_label_show_during_drag);
    }
    if (typeof cfg.language !== "undefined") {
      const applied = applyLanguagePreference?.(String(cfg.language || "en"), { source });
      if (typeof applied === "string" && applied) {
        state.language = applied;
      }
    }
  }

  if (settingsTabs) {
    settingsTabs.addEventListener("click", (event) => {
      const button = event.target.closest("[data-settings-tab]");
      if (button) showSettingsTab(button.dataset.settingsTab);
    });
  }

  if (settingsModal) {
    // One delegated listener rather than ten: any edit re-evaluates the note.
    settingsModal.addEventListener("input", updateRestartNote);
    settingsModal.addEventListener("change", updateRestartNote);
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
    showSettingsTab("viewer");
    openModal(settingsModal, { focusTarget: settingsClose });
    setSettingsModalBusy(true);
    setSettingsMessage(t("settings.message.loading"), false, true);
    try {
      const res = await fetch(`${apiBase}/settings`);
      if (!res.ok) {
        throw new Error(`Settings request failed: ${res.status}`);
      }
      const payload = await res.json();
      if (requestId !== settingsRequestId) return;
      const config = payload?.config || {};
      fillSettingsForm(config, payload?.path || "");
      applyUiSettings(config?.ui, { source: "config" });
      if (settingsToolHints) {
        settingsToolHints.checked = Boolean(config?.ui?.tool_hints ?? state.toolHintsEnabled);
      }
      updateExternalAccessUi();
      setSettingsMessage(t("settings.message.edit_hint"));
    } catch (err) {
      if (requestId !== settingsRequestId) return;
      console.error(err);
      setSettingsMessage(t("settings.message.load_failed"), true);
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
    setSettingsMessage(t("settings.message.saving"), false, true);
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
      applyUiSettings(data?.config?.ui || config?.ui, { source: "user" });
      schedulePixelOverlay();
      updateExternalAccessUi();
      setStatus(t("status.settings.saved"), { tone: "success" });
      if (closeAfter) {
        closeSettingsModal();
      }
    } catch (err) {
      console.error(err);
      const detail = String(err?.message || "").trim();
      setSettingsMessage(detail || t("settings.message.save_failed"), true);
    } finally {
      setSettingsModalBusy(false);
    }
  }

  settingsServerExternal?.addEventListener("change", handleExternalAccessToggle);
  onLanguageChange(() => {
    updateExternalAccessUi();
  });
  updateExternalAccessUi();

  return {
    applyUiSettings,
    closeSettingsModal,
    openSettingsModal,
    saveSettingsFromModal,
    showSettingsTab,
  };
}
