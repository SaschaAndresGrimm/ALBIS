/**
 * Backend health and live badge status.
 */

import { t } from "./i18n.js";

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
      liveBadge.textContent = t("backend.live.live");
      liveBadge.setAttribute("aria-hidden", "true");
      liveBadge.removeAttribute("aria-label");
      liveBadge.removeAttribute("title");
      return;
    }
    liveBadge.classList.add("is-active");
    const age = Date.now() - (state.autoload.lastUpdate || 0);
    const wait = !state.autoload.lastUpdate || age > state.autoload.interval * 2;
    liveBadge.classList.toggle("is-wait", wait);
    liveBadge.textContent = wait ? t("backend.live.wait") : t("backend.live.live");
    liveBadge.setAttribute("aria-label", wait ? t("backend.live.aria.wait") : t("backend.live.aria.live"));
    liveBadge.title = wait ? t("backend.live.title.wait") : t("backend.live.title.live");
    liveBadge.setAttribute("aria-hidden", "false");
  }

  function updateBackendBadge() {
    if (!backendBadge) return;
    backendBadge.classList.toggle("is-off", !state.backendAlive);
    backendBadge.classList.toggle("is-active", true);
    backendBadge.textContent = state.backendAlive ? t("backend.server.online") : t("backend.server.offline");
    backendBadge.setAttribute("aria-label", state.backendAlive ? t("backend.server.aria.online") : t("backend.server.aria.offline"));
    backendBadge.title = state.backendAlive ? t("backend.server.title.online") : t("backend.server.title.offline");
    backendBadge.setAttribute("aria-hidden", "false");
    updateFooterVersions();
    updateSplashCallToAction();
  }

  function updateAboutVersion() {
    if (!aboutVersion) return;
    aboutVersion.textContent = t("about.version", { version: state.backendVersion || "-" });
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
      setSplashStatus("backend.splash.starting", { attempts });
      const alive = await checkBackendHealth();
      if (alive) {
        setSplashStatus("backend.splash.ready", { version: state.backendVersion || "-" });
        return true;
      }
      await sleep(250);
    }
    setSplashStatus("backend.splash.slow_start");
    return false;
  }

  return {
    updateLiveBadge,
    updateAboutVersion,
    startBackendHeartbeat,
    waitForBackendReady,
  };
}
