/**
 * Autoload status/meta labels and SIMPLON monitor mode toggling.
 */

import { normalizeSimplonUrlInput } from "./simplon_url_utils.js";
import { formatTimeStamp } from "./format_utils.js";
import { fetchResponse } from "./http.js";

export function createAutoloadStatusController({
  apiBase,
  state,
  elements,
  callbacks,
  core,
}) {
  const {
    autoloadStatus,
    autoloadLatest,
    simplonUrl,
    simplonVersion,
  } = elements;

  const {
    setStatus,
    updateToolbar,
  } = callbacks;

  function updateAutoloadMeta() {
    if (autoloadStatus) {
      autoloadStatus.textContent = state.autoload.lastPoll
        ? formatTimeStamp(state.autoload.lastPoll)
        : "-";
    }
    if (autoloadLatest) {
      autoloadLatest.textContent = state.autoload.lastUpdate
        ? formatTimeStamp(state.autoload.lastUpdate)
        : "-";
    }
    updateToolbar();
  }

  function setAutoloadStatus(text, markUpdate = false) {
    if (text) {
      setStatus(String(text));
    }
    if (markUpdate) {
      state.autoload.lastUpdate = Date.now();
    }
    updateAutoloadMeta();
    core.backendStatus.updateLiveBadge();
  }

  function setAutoloadLatest() {
    updateAutoloadMeta();
  }

  async function setSimplonMode(enabled) {
    if (!simplonUrl || !simplonVersion) return;
    const url = normalizeSimplonUrlInput(simplonUrl);
    if (!url) return;
    const version = simplonVersion.value.trim() || "1.8.0";
    const mode = enabled ? "enabled" : "disabled";
    try {
      await fetchResponse(
        `${apiBase}/simplon/mode?url=${encodeURIComponent(url)}&version=${encodeURIComponent(version)}&mode=${mode}`,
        { method: "POST" },
      );
    } catch (err) {
      console.error(err);
    }
  }

  return {
    formatTimeStamp,
    setAutoloadStatus,
    setAutoloadLatest,
    updateAutoloadMeta,
    setSimplonMode,
  };
}
