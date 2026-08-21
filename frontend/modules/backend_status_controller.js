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
      liveBadge.classList.remove("is-active", "is-wait", "is-paused");
      liveBadge.textContent = t("backend.live.live");
      liveBadge.setAttribute("aria-hidden", "true");
      liveBadge.removeAttribute("aria-label");
      liveBadge.removeAttribute("title");
      return;
    }
    const paused = state.autoload.livePaused === true;
    liveBadge.classList.add("is-active");
    const age = Date.now() - (state.autoload.lastUpdate || 0);
    const wait = !paused && (!state.autoload.lastUpdate || age > state.autoload.interval * 2);
    liveBadge.classList.toggle("is-wait", wait);
    liveBadge.classList.toggle("is-paused", paused);
    liveBadge.textContent = paused
      ? t("backend.live.paused")
      : wait
        ? t("backend.live.wait")
        : t("backend.live.live");
    liveBadge.setAttribute(
      "aria-label",
      paused
        ? t("backend.live.aria.paused")
        : wait
          ? t("backend.live.aria.wait")
          : t("backend.live.aria.live"),
    );
    liveBadge.title = paused
      ? t("backend.live.title.paused")
      : wait
        ? t("backend.live.title.wait")
        : t("backend.live.title.live");
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

  /**
   * A stamp that changes whenever the server is a different build.
   *
   * Version alone is too coarse: a rebuild of the same release is a different
   * build with the same number, and that is exactly the case a support question
   * turns on. Commit alone is too fragile, since an unstamped server reports
   * none. Together they change whenever anything about the build changed.
   */
  function buildStamp(version, commit) {
    return `${version || ""}@${commit || ""}`;
  }

  /**
   * Notice that this page is older than the server it is talking to.
   *
   * The cache policy already rules out being served a stale frontend -- entry
   * documents are `no-store` and modules revalidate. What it cannot rule out is
   * a tab that was loaded before the server was upgraded and never reloaded
   * since, which happens to any viewer left open on a workstation. The code in
   * memory then predates the server, and only a reload fixes it.
   *
   * Sticky once set: the tab does not become current again by itself.
   */
  function trackServerBuild(alive, version, commit) {
    if (!alive) return;
    const stamp = buildStamp(version, commit);
    if (state.buildStampAtLoad === null) {
      state.buildStampAtLoad = stamp;
      return;
    }
    if (!state.serverBuildChanged && stamp !== state.buildStampAtLoad) {
      state.serverBuildChanged = true;
    }
  }

  async function checkBackendHealth() {
    let alive = false;
    let version = state.backendVersion || "";
    let commit = state.backendCommit || "";
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
          // Absent on a server older than this field, which is a valid answer:
          // an unstamped build reports no commit rather than a wrong one.
          commit = data?.commit ? String(data.commit) : "";
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
    trackServerBuild(alive, version, commit);
    if (state.backendCommit !== commit) {
      state.backendCommit = commit;
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
    // Exposed so the build-change detection can be tested a poll at a time,
    // rather than through the heartbeat's timers.
    checkBackendHealth,
    startBackendHeartbeat,
    waitForBackendReady,
  };
}
