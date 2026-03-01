/**
 * JUNGFRAUJOCH preview bridge lifecycle.
 */

export function createJfjochBridgeController({
  apiBase,
  state,
  callbacks,
}) {
  const {
    setAutoloadStatus,
    updateJfjochMetaUI,
  } = callbacks;

  async function startJfjochPreviewBridge() {
    const endpoint = (state.autoload.jfjochEndpoint || "").trim();
    if (!endpoint) {
      setAutoloadStatus("JFJ: set preview endpoint");
      return false;
    }
    const sourceId = (state.autoload.jfjochSourceId || "jungfraujoch").trim() || "jungfraujoch";
    const payload = {
      endpoint,
      source_id: sourceId,
      topic: state.autoload.jfjochTopic || "",
      channel: state.autoload.jfjochChannel || "",
    };
    const res = await fetch(`${apiBase}/jfjoch/preview/start`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      setAutoloadStatus("JFJ: bridge start failed");
      return false;
    }
    try {
      const status = await res.json();
      state.autoload.jfjochStatus = { ...(state.autoload.jfjochStatus || {}), ...(status || {}) };
    } catch {
      // ignore status payload decode errors
    }
    return true;
  }

  async function stopJfjochPreviewBridge() {
    try {
      const res = await fetch(`${apiBase}/jfjoch/preview/stop`, { method: "POST" });
      if (!res.ok) return false;
      const payload = await res.json().catch(() => ({}));
      state.autoload.jfjochStatus = { ...(state.autoload.jfjochStatus || {}), ...(payload || {}) };
      return true;
    } catch (err) {
      console.warn(err);
      return false;
    }
  }

  async function fetchJfjochPreviewStatus() {
    try {
      const res = await fetch(`${apiBase}/jfjoch/preview/status`, { cache: "no-store" });
      if (!res.ok) return null;
      const payload = await res.json();
      if (!payload || typeof payload !== "object") return null;
      state.autoload.jfjochStatus = payload;
      updateJfjochMetaUI(state.autoload.jfjochMeta || {}, payload);
      return payload;
    } catch (err) {
      console.warn(err);
      return null;
    }
  }

  return {
    startJfjochPreviewBridge,
    stopJfjochPreviewBridge,
    fetchJfjochPreviewStatus,
  };
}
