/**
 * Autoload status/meta labels and SIMPLON monitor mode toggling.
 */

export function createAutoloadStatusController({
  apiBase,
  state,
  elements,
  callbacks,
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
    updateLiveBadge,
  } = callbacks;

  function formatTimeStamp(ts) {
    if (!ts) return "";
    return new Date(ts).toLocaleTimeString();
  }

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
    updateLiveBadge();
  }

  function setAutoloadLatest() {
    updateAutoloadMeta();
  }

  async function setSimplonMode(enabled) {
    if (!simplonUrl || !simplonVersion) return;
    const url = simplonUrl.value.trim();
    if (!url) return;
    const version = simplonVersion.value.trim() || "1.8.0";
    const mode = enabled ? "enabled" : "disabled";
    try {
      await fetch(
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
