/**
 * The handoff queue: another program (a beamline's data pipeline) asks ALBIS
 * to open a file by queueing a job at /api/handoff/v1/jobs; every open ALBIS
 * window polls for the newest one and opens it.
 *
 * The last job seen is kept in localStorage, so a reload, or a second window
 * started later, does not reopen a job already handled.
 */

import { fetchResponse } from "./http.js";
import { t } from "./i18n.js";

const LAST_ID_KEY = "albis.handoff.lastId";

export function createHandoffController({ apiBase, callbacks, intervalMs = 1000, core }) {
  const { isHdfFile, selectDatasetIfPresent, setStatus } = callbacks;
  let lastId = Number(localStorage.getItem(LAST_ID_KEY) || "0");
  let busy = false;
  let timer = null;

  async function applyJob(job) {
    const targetPath = String(job?.open_path || "").trim();
    if (!targetPath) {
      setStatus(t("status.handoff.no_target", { id: job?.id || "?" }));
      return;
    }

    await core.fileOpen.openPathInViewer(targetPath, { refreshFileList: true });

    const targetDataset = String(job?.dataset || "").trim();
    if (targetDataset && isHdfFile(targetPath)) {
      try {
        await selectDatasetIfPresent(targetDataset);
      } catch (err) {
        console.warn("Failed applying handoff dataset", err);
      }
    }

    const runId = String(job?.run_id || "").trim();
    if (runId) {
      setStatus(t("status.handoff.opened_with_run", { runId, targetPath }));
    } else {
      setStatus(t("status.handoff.opened", { targetPath }));
    }
  }

  async function poll() {
    if (busy) return;
    busy = true;
    try {
      const res = await fetchResponse(`${apiBase}/handoff/v1/jobs/latest?after_id=${encodeURIComponent(String(lastId))}`, {
        cache: "no-store",
      });
      if (res.status === 204) {
        return;
      }
      const job = await res.json();
      const id = Number(job?.id || 0);
      if (!Number.isFinite(id) || id <= lastId) {
        return;
      }
      lastId = id;
      localStorage.setItem(LAST_ID_KEY, String(lastId));
      await applyJob(job);
    } catch (err) {
      // Keep handoff polling non-blocking and quiet during normal UI use.
      console.warn("Handoff polling error", err);
    } finally {
      busy = false;
    }
  }

  function start() {
    if (timer) {
      window.clearInterval(timer);
    }
    timer = window.setInterval(() => {
      poll();
    }, intervalMs);
    poll();
  }

  return { start, poll };
}
