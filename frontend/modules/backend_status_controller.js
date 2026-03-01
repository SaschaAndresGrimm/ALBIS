/**
 * Backend health and live badge status.
 */

export function createBackendStatusController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    liveBadge,
    backendBadge,
    aboutVersion,
  } = elements;

  const {
    updateFooterVersions,
    updateSplashCallToAction,
    setSplashStatus,
  } = callbacks;

  let backendTimer = null;

  function updateLiveBadge() {
    if (!liveBadge) return;
    const liveMode =
      state.autoload.mode === "simplon" ||
      state.autoload.mode === "remote" ||
      state.autoload.mode === "jungfraujoch";
    if (!state.autoload.running || !liveMode) {
      liveBadge.classList.remove("is-active", "is-wait");
      liveBadge.textContent = "LIVE";
      liveBadge.setAttribute("aria-hidden", "true");
      liveBadge.removeAttribute("aria-label");
      liveBadge.removeAttribute("title");
      return;
    }
    liveBadge.classList.add("is-active");
    const age = Date.now() - (state.autoload.lastUpdate || 0);
    const wait = !state.autoload.lastUpdate || age > state.autoload.interval * 2;
    liveBadge.classList.toggle("is-wait", wait);
    liveBadge.textContent = wait ? "WAIT" : "LIVE";
    liveBadge.setAttribute("aria-label", wait ? "Stream waiting for updates" : "Stream live");
    liveBadge.title = wait ? "Waiting for stream updates" : "Live stream active";
    liveBadge.setAttribute("aria-hidden", "false");
  }

  function updateBackendBadge() {
    if (!backendBadge) return;
    backendBadge.classList.toggle("is-off", !state.backendAlive);
    backendBadge.classList.toggle("is-active", true);
    backendBadge.textContent = state.backendAlive ? "SERVER" : "OFFLINE";
    backendBadge.setAttribute("aria-label", state.backendAlive ? "Backend server online" : "Backend server offline");
    backendBadge.title = state.backendAlive ? "Backend server online" : "Backend server offline";
    backendBadge.setAttribute("aria-hidden", "false");
    updateFooterVersions();
    updateSplashCallToAction();
  }

  function updateAboutVersion() {
    if (!aboutVersion) return;
    aboutVersion.textContent = `Version ${state.backendVersion || "-"}`;
    updateFooterVersions();
  }

  async function checkBackendHealth() {
    let alive = false;
    let version = state.backendVersion || "";
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), 1500);
    try {
      const res = await fetch(`${apiBase}/health`, { signal: controller.signal, cache: "no-store" });
      if (res.ok) {
        alive = true;
        try {
          const data = await res.json();
          if (data?.version) {
            version = String(data.version);
          }
        } catch {
          // ignore parse errors
        }
      }
    } catch {
      alive = false;
    } finally {
      window.clearTimeout(timer);
    }
    if (state.backendAlive !== alive) {
      state.backendAlive = alive;
    }
    if (state.backendVersion !== version) {
      state.backendVersion = version;
      updateAboutVersion();
    }
    updateBackendBadge();
    return alive;
  }

  function startBackendHeartbeat() {
    if (backendTimer) {
      window.clearInterval(backendTimer);
    }
    void checkBackendHealth();
    backendTimer = window.setInterval(() => {
      void checkBackendHealth();
    }, 4000);
  }

  function sleep(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  async function waitForBackendReady(timeoutMs = 20000) {
    const deadline = Date.now() + timeoutMs;
    let attempts = 0;
    while (Date.now() < deadline) {
      attempts += 1;
      setSplashStatus(`Starting backend... (${attempts})`);
      const alive = await checkBackendHealth();
      if (alive) {
        setSplashStatus(`Backend ready (v${state.backendVersion || "-"})`);
        return true;
      }
      await sleep(250);
    }
    setSplashStatus("Backend startup is taking longer than expected...");
    return false;
  }

  return {
    updateLiveBadge,
    updateAboutVersion,
    startBackendHeartbeat,
    waitForBackendReady,
  };
}
