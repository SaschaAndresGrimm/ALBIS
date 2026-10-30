/**
 * Data -> Pixel Mask (beta): the pixel mask ALBIS applies, where it came from,
 * and saving it or sending it to a detector.
 *
 * The mask is always the open data's own: a file's, the live images', or the
 * negative pixels of a TIFF or CBF. A mask file saved here is opened like any
 * data, and is then its own mask. The bits are kept as they are (bit 0 a gap,
 * bits 1-4 defective pixels, and so on), so a mask saved or sent from here
 * keeps its reasons.
 *
 * Edit on image (pixel_mask_edit_controller.js) and Find bad pixels work on a
 * "pinned" copy of the data's mask, or of an empty one if there is none:
 * mask_cursor_controller.js puts it back in place whenever a file or a live
 * frame brings its own, until Discard edits. The copy as it was when editing
 * started is kept beside it, so the section can say what was changed by hand
 * and Discard edits can bring it back. Unsaved edits are asked about before
 * anything replaces them.
 *
 * The Detector tab's Pixel Mask section is here too, for the detector itself:
 * Open in ALBIS saves its mask as a file in the data folder's masks/ and opens
 * it; Clear unmasks all but its module gaps. Sending (from Data -> Pixel Mask)
 * and clearing come after a confirmation that says what will change; the
 * backend keeps the detector's previous mask as a backup, which Restore sends
 * back.
 *
 * Shown only while `ui.pixel_mask` is on (Settings -> Tools); the detector
 * section also needs a connected detector.
 */

import { getLanguage, t } from "./i18n.js";
import { showConfirmDialog } from "./dialogs.js";
import { countEdits } from "./pixel_mask_edit.js";
import { addFound, findBadPixels } from "./pixel_mask_find.js";
import { fetchJSONWithInit, fetchResponse } from "./http.js";

const GAP_BIT = 0x1;
const DEFECTIVE_BITS = 0x1e;
const BACKUPS_STORAGE_KEY = "albis.pixelMask.backups.v1";

/** How many pixels a mask masks, and for which reasons. */
export function summarizeMask(data) {
  const out = { masked: 0, gap: 0, defective: 0, other: 0 };
  if (!data) return out;
  for (let i = 0; i < data.length; i += 1) {
    const value = data[i];
    if (!value) continue;
    out.masked += 1;
    if (value & GAP_BIT) out.gap += 1;
    else if (value & DEFECTIVE_BITS) out.defective += 1;
    else out.other += 1;
  }
  return out;
}

/** What replacing `before` with `after` changes, pixel by pixel. */
export function diffMasks(before, after) {
  const out = { added: 0, removed: 0, changed: 0 };
  const n = Math.min(before?.length || 0, after?.length || 0);
  for (let i = 0; i < n; i += 1) {
    const a = before[i];
    const b = after[i];
    if (a === b) continue;
    if (!a) out.added += 1;
    else if (!b) out.removed += 1;
    else out.changed += 1;
  }
  return out;
}

/** A name part kept to characters every file system takes, without repeats. */
export function safeNamePart(text) {
  return String(text || "")
    .trim()
    .replace(/[^A-Za-z0-9._-]+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^[._]+|[._]+$/g, "");
}

/**
 * The file name a mask is saved under, without extension:
 * `<source>_pixel_mask[_edited]_<date>-<time>`. The time keeps successive
 * saves of a changing mask apart; a source that is itself a saved mask loses
 * its old suffix, so names do not grow with each round.
 */
export function maskFileName(source, { edited = false, now = new Date() } = {}) {
  const base =
    safeNamePart(source).replace(/_pixel_mask(_edited)?(_\d{8}-\d{4,6})?$/i, "").slice(0, 60) || "detector";
  const pad = (n) => String(n).padStart(2, "0");
  const stamp = `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}-${pad(now.getHours())}${pad(now.getMinutes())}`;
  return `${base}_pixel_mask${edited ? "_edited" : ""}_${stamp}`;
}

/** The host of a detector address typed with or without a scheme. */
export function hostOf(address) {
  const text = String(address || "").trim();
  if (!text) return "";
  try {
    return new URL(/^[a-z][a-z0-9+.-]*:\/\//i.test(text) ? text : `http://${text}`).hostname;
  } catch {
    return "";
  }
}

export function maskFileStem(path) {
  const base = String(path || "").replace(/\\/g, "/").split("/").pop() || "";
  return base.replace(/(_master)?\.(h5|hdf5|tiff?|cbf(\.gz)?|edf)$/i, "") || "pixel_mask";
}

function formatCount(value) {
  try {
    return Number(value || 0).toLocaleString(getLanguage());
  } catch {
    return String(value || 0);
  }
}

function formatTime(timestamp) {
  return new Date(timestamp).toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit" });
}

function shapeText(shape) {
  return Array.isArray(shape) && shape.length === 2 ? `${shape[1]} × ${shape[0]}` : "—";
}

function filenameFrom(disposition, fallback) {
  const match = /filename="?([^";]+)"?/i.exec(String(disposition || ""));
  return match ? match[1] : fallback;
}

export function createPixelMaskController({
  apiBase,
  state,
  elements = {},
  callbacks = {},
  storage = typeof window !== "undefined" ? window.localStorage : null,
}) {
  const {
    section,
    summaryChip,
    stateEl,
    sourceEl,
    saveBtn,
    saveMenu,
    saveH5Btn,
    saveTiffBtn,
    editBtn,
    discardBtn,
    editsEl,
    sendBtn,
    findBtn,
    findPanel,
    findHot,
    findHotSigma,
    findCold,
    findColdPercent,
    findResultEl,
    findAddBtn,
    findCloseBtn,
    detSection,
    detSummary,
    detInfo,
    detOpenBtn,
    detBackup,
    detBackupText,
    detRestoreBtn,
    detClearBtn,
  } = elements;

  const {
    refreshMask,
    getDetector = () => null,
    // The detector the SIMPLON monitor shows, when detector control is on but
    // the Detector tab is not connected; null otherwise.
    getMonitorDetector = () => null,
    // The detector at this address holds a new mask: a live view of it
    // should fetch it again.
    onDetectorMaskWritten = () => {},
    logDetector,
    setStatus,
    setSummaryChip,
    setSectionBadgeState,
    // Open a file (relative to the data root) like File -> Open does.
    openPath = async () => {},
    // The editor on the image, late-bound: it is created after this.
    openEditor = () => false,
    closeEditor = () => {},
    resetEditor = () => {},
    isEditing = () => false,
    // Circle pixels on the image, or null to stop (Find bad pixels' preview).
    setMarkers = () => {},
  } = callbacks;

  let enabled = false;
  let pinned = null;
  let busy = false;
  // What was last read from or sent to each detector, by address.
  const detectorSeen = new Map();
  // The summary of the mask last counted: an image change re-renders, and a
  // mask of 18 M pixels is not counted again for every frame.
  let counted = { data: null, summary: summarizeMask(null) };

  function summaryOf(data) {
    if (counted.data !== data) counted = { data, summary: summarizeMask(data) };
    return counted.summary;
  }

  // The same for the hand edits: each edit step replaces the array, so a
  // render between steps does not count again.
  let editCount = { data: null, counts: { masked: 0, unmasked: 0 } };

  function editsOf() {
    if (!pinned?.base) return { masked: 0, unmasked: 0 };
    if (editCount.data !== pinned.data) editCount = { data: pinned.data, counts: countEdits(pinned.base, pinned.data) };
    return editCount.counts;
  }

  function hasEdits() {
    const counts = editsOf();
    return counts.masked + counts.unmasked > 0;
  }

  /** Unsaved edits: ask before they are replaced. True to go ahead. */
  async function confirmDropEdits() {
    if (!pinned?.dirty || !hasEdits()) return true;
    const counts = editsOf();
    return showConfirmDialog({
      title: t("pixel_mask.discard_title"),
      message: t("pixel_mask.discard_text", {
        masked: formatCount(counts.masked),
        unmasked: formatCount(counts.unmasked),
      }),
      confirmLabel: t("pixel_mask.discard_confirm"),
      danger: true,
    });
  }

  // ---- the mask in use --------------------------------------------------------

  /**
   * The live source shown, by name -- the detector's host for the SIMPLON
   * monitor -- or "" for a file. A live frame's `state.file` is a display
   * label ("SIMPLON monitor (host) S21 Img0 <date>"), not a name to build on.
   */
  function liveSourceName() {
    const autoload = state.autoload || {};
    if (!autoload.running) return "";
    if (autoload.mode === "simplon") return hostOf(autoload.simplonUrl) || "simplon";
    if (autoload.mode === "jungfraujoch") return hostOf(autoload.jfjochEndpoint) || "jungfraujoch";
    if (autoload.mode === "remote") return autoload.remoteSourceId || "remote";
    return "";
  }

  /** What a mask that came with the data is named after. */
  function dataSourceStem() {
    return liveSourceName() || maskFileStem(state.file);
  }

  /** Where the data's own mask is, for the source line: the live source or the file. */
  function dataMaskLabel() {
    const live = liveSourceName();
    if (live) return t("pixel_mask.source.live", { name: live });
    return String(state.file || "").replace(/\\/g, "/").split("/").pop();
  }

  /** The mask ALBIS would save or send: the pinned one, else the data's. */
  function currentMask() {
    if (pinned) return { data: pinned.data, shape: pinned.shape, name: maskFileName(pinned.stem, { edited: hasEdits() }) };
    if (state.maskRaw && Array.isArray(state.maskShape) && state.maskShape.length === 2) {
      return { data: state.maskRaw, shape: state.maskShape, name: maskFileName(dataSourceStem()) };
    }
    return null;
  }

  function pin(data, shape, { origin, label, stem, detectorHost = "" }) {
    if (isEditing()) closeEditor({ keep: true });
    pinned = {
      data,
      shape: shape.slice(),
      origin,
      label: String(label || ""),
      stem: String(stem || "pixel_mask"),
      // The detector this is a copy of the mask of, if any: a write to that
      // detector leaves it out of date.
      detectorHost: String(detectorHost || ""),
      // Set when editing starts: the mask as it was then.
      base: null,
      dirty: false,
    };
    void refreshMask?.();
    render();
  }

  function unpin() {
    if (!pinned) return;
    if (isEditing()) closeEditor({ keep: true });
    pinned = null;
    void refreshMask?.();
    render();
  }

  // ---- editing on the image -------------------------------------------------

  function frameShapeMatches(shape) {
    return Array.isArray(shape) && shape[0] === state.height && shape[1] === state.width;
  }

  // An image is shown. Not `state.hasFrame`: that is set after the mask is
  // synced to a new frame, which is when this section renders.
  function hasImage() {
    return Boolean(state.dataRaw && state.width && state.height);
  }

  /** Why editing cannot start now, or "" if it can. */
  function editBlocker() {
    if (!hasImage()) return t("pixel_mask.edit.no_image");
    if (pinned && !frameShapeMatches(pinned.shape)) return t("pixel_mask.edit.mismatch");
    if (!pinned && state.maskRaw && !frameShapeMatches(state.maskShape)) return t("pixel_mask.edit.mismatch");
    return "";
  }

  /**
   * Make the mask in use editable: pin a copy of the data's mask (or a new,
   * empty one) and keep it as it is now, to count and discard edits against.
   */
  function ensureEditable() {
    const blocker = editBlocker();
    if (blocker) {
      setStatus?.(blocker, { tone: "warning" });
      return false;
    }
    if (!pinned) {
      // The data's own mask, as the image shows it; or a new, empty one.
      const stem = dataSourceStem();
      if (state.maskRaw && frameShapeMatches(state.maskShape)) {
        const autoload = state.autoload || {};
        pin(state.maskRaw.slice(), [state.height, state.width], {
          origin: "data",
          label: dataMaskLabel(),
          stem,
          // The live view's mask is the detector's.
          detectorHost: autoload.running && autoload.mode === "simplon" ? hostOf(autoload.simplonUrl) : "",
        });
      } else {
        pin(new Uint32Array(state.width * state.height), [state.height, state.width], { origin: "new", label: "", stem });
      }
    }
    if (!pinned.base) pinned.base = pinned.data.slice();
    return true;
  }

  function beginEdit() {
    if (isEditing()) {
      closeEditor();
      return;
    }
    if (!ensureEditable()) return;
    if (!openEditor()) {
      setStatus?.(t("pixel_mask.edit.mismatch"), { tone: "warning" });
    }
    render();
  }

  /** What the editor works on: the pinned mask and where editing started. */
  function getEditTarget() {
    if (!enabled || !pinned?.base || !frameShapeMatches(pinned.shape)) return null;
    return { data: pinned.data, base: pinned.base, width: pinned.shape[1], height: pinned.shape[0] };
  }

  /**
   * An edit step ended. The mask is replaced by a copy: the renderer, the
   * colour key and inpainting recognise a changed mask by a new array.
   */
  function onEditCommit() {
    if (!pinned) return;
    pinned.data = pinned.data.slice();
    pinned.dirty = true;
    void refreshMask?.();
    render();
  }

  /** The editor closed. A copy of the data's mask that was never changed goes. */
  function onEditClose({ keep = false } = {}) {
    if (!keep && pinned && !hasEdits()) {
      pinned = null;
      void refreshMask?.();
    }
    render();
  }

  // ---- Find bad pixels ------------------------------------------------------

  let findOpen = false;
  let findRun = 0;
  let findTimer = null;
  let findSearching = false;
  // { hot, dead, cold, data, width }: what the last search found, in which frame.
  let findResult = null;

  function findOptions() {
    const number = (input, fallback) => {
      const value = Number(input?.value);
      return Number.isFinite(value) && value > 0 ? value : fallback;
    };
    return {
      hot: findHot ? findHot.checked : true,
      hotSigma: number(findHotSigma, 8),
      cold: findCold ? findCold.checked : true,
      coldPercent: Math.min(100, number(findColdPercent, 20)),
    };
  }

  const isCounts = (data) =>
    data instanceof Uint8Array || data instanceof Uint16Array || data instanceof Uint32Array ||
    data instanceof Int8Array || data instanceof Int16Array || data instanceof Int32Array;

  /** Search the image shown; a newer search makes an older one's answer moot. */
  async function runFind() {
    findTimer = null;
    const run = ++findRun;
    if (!findOpen) return;
    if (editBlocker()) {
      findResult = null;
      setMarkers(null);
      render();
      return;
    }
    const data = state.dataRaw;
    const width = state.width;
    const mask = currentMask();
    findSearching = true;
    render();
    let last = Date.now();
    const found = await findBadPixels({
      data,
      width,
      height: state.height,
      mask: mask && frameShapeMatches(mask.shape) ? mask.data : null,
      counts: isCounts(data),
      options: findOptions(),
      // Let the page draw now and then on a large image.
      onProgress: () => {
        if (Date.now() - last < 40) return null;
        last = Date.now();
        return new Promise((resolve) => window.setTimeout(resolve, 0));
      },
    });
    if (run !== findRun || !findOpen) return;
    findSearching = false;
    findResult = { ...found, data, width };
    setMarkers({
      width,
      groups: [
        { kind: "hot", indices: found.hot },
        { kind: "cold", indices: found.dead.concat(found.cold) },
      ],
    });
    render();
  }

  function scheduleFind(delay = 250) {
    if (findTimer) window.clearTimeout(findTimer);
    findTimer = window.setTimeout(() => void runFind(), delay);
  }

  function openFind() {
    if (findOpen) {
      closeFind();
      return;
    }
    findOpen = true;
    findResult = null;
    scheduleFind(0);
    render();
  }

  function closeFind() {
    if (findTimer) window.clearTimeout(findTimer);
    findTimer = null;
    findRun += 1;
    findOpen = false;
    findSearching = false;
    findResult = null;
    setMarkers(null);
    render();
  }

  function foundCount(result) {
    return result ? result.hot.length + result.dead.length + result.cold.length : 0;
  }

  /** Add what was found to the mask in use: one edit, Discard edits undoes it. */
  function addFoundToMask() {
    if (!findResult || findResult.data !== state.dataRaw || !foundCount(findResult)) return;
    if (!ensureEditable()) return;
    const added = addFound(pinned.data, findResult);
    onEditCommit();
    resetEditor();
    closeFind();
    setStatus?.(t("pixel_mask.find.added", { count: formatCount(added) }), { tone: "success" });
  }

  function renderFind() {
    if (findBtn) {
      findBtn.disabled = busy || (!findOpen && Boolean(editBlocker()));
      findBtn.setAttribute("aria-pressed", findOpen ? "true" : "false");
      findBtn.classList.toggle("is-active", findOpen);
    }
    if (findPanel) findPanel.hidden = !findOpen;
    if (!findOpen) return;
    // A new frame, or a mask now covering some of them: look again.
    if (!findSearching && !findTimer && !editBlocker() && (!findResult || findResult.data !== state.dataRaw)) {
      scheduleFind();
    }
    if (findResultEl) {
      const blocker = editBlocker();
      if (blocker) findResultEl.textContent = blocker;
      else if (findSearching || !findResult) findResultEl.textContent = t("pixel_mask.find.searching");
      else if (!foundCount(findResult)) findResultEl.textContent = t("pixel_mask.find.none");
      else {
        // Only the kinds found: "Found 6 hot pixels." or "Found 6 hot, 2 dead pixels."
        const list = ["hot", "dead", "cold"]
          .filter((kind) => findResult[kind].length)
          .map((kind) => t(`pixel_mask.find.part.${kind}`, { count: formatCount(findResult[kind].length) }))
          .join(", ");
        findResultEl.textContent = t("pixel_mask.find.result", { list });
      }
    }
    if (findAddBtn) findAddBtn.disabled = busy || findSearching || !foundCount(findResult);
    if (findHotSigma && findHot) findHotSigma.disabled = !findHot.checked;
    if (findColdPercent && findCold) findColdPercent.disabled = !findCold.checked;
  }

  function describeEdits() {
    const counts = editsOf();
    return t("pixel_mask.edit.count", { masked: formatCount(counts.masked), unmasked: formatCount(counts.unmasked) });
  }

  /** Back to the data's own mask. */
  async function discardEdits() {
    if (!pinned?.base || !(await confirmDropEdits())) return;
    if (isEditing()) closeEditor({ keep: true });
    pinned = null;
    void refreshMask?.();
    render();
    setStatus?.(t("pixel_mask.status.discarded"));
  }

  /** " · masked by you" for a pixel changed by hand, for the cursor readout. */
  function cursorNote(index) {
    if (!enabled || !pinned?.base || !frameShapeMatches(pinned.shape)) return "";
    const before = pinned.base[index];
    const now = pinned.data[index];
    if (before === now) return "";
    return t(!(now & ~1) && before & ~1 ? "pixel_mask.cursor.unmasked" : "pixel_mask.cursor.masked");
  }

  // ---- talking to the backend -----------------------------------------------

  // The server's reason for a failed request: its sentence, or the summary of a
  // structured one, else http.js's translated message.
  function failureReason(err) {
    const summary = err?.detailData?.summary;
    return typeof summary === "string" && summary ? summary : err?.detail || err?.message || "";
  }

  async function readMask(url) {
    let res;
    try {
      res = await fetchResponse(url);
    } catch (err) {
      if (!(err?.status >= 400)) throw err;
      const error = new Error(failureReason(err));
      error.status = err.status;
      throw error;
    }
    const shape = String(res.headers.get("X-Shape") || "")
      .split(",")
      .map((part) => Number.parseInt(part, 10));
    const data = new Uint32Array(await res.arrayBuffer());
    if (shape.length !== 2 || shape[0] * shape[1] !== data.length) throw new Error("Invalid mask shape");
    return { data, shape, where: res.headers.get("X-Mask-Path") || "" };
  }

  function maskForm(meta, mask) {
    const form = new FormData();
    form.append("meta", JSON.stringify({ ...meta, shape: mask.shape }));
    const { data } = mask;
    form.append("pixels", new Blob([new Uint8Array(data.buffer, data.byteOffset, data.byteLength)]), "mask.bin");
    return form;
  }

  async function readFileMask(path) {
    return readMask(`${apiBase}/pixel-mask/file?file=${encodeURIComponent(path)}`);
  }

  async function readDetectorMask(connection) {
    const query = `url=${encodeURIComponent(connection.url)}&version=${encodeURIComponent(connection.version)}`;
    return readMask(`${apiBase}/pixel-mask/detector?${query}`);
  }

  async function withBusy(task) {
    if (busy) return;
    busy = true;
    render();
    try {
      await task();
    } finally {
      busy = false;
      render();
    }
  }

  // ---- Data -> Pixel Mask ---------------------------------------------------

  function save(format) {
    const mask = currentMask();
    if (!mask) return Promise.resolve();
    return withBusy(async () => {
      try {
        const res = await fetchResponse(`${apiBase}/pixel-mask/encode`, {
          method: "POST",
          body: maskForm({ format, name: mask.name }, mask),
        });
        const blob = await res.blob();
        const name = filenameFrom(res.headers.get("Content-Disposition"), `${mask.name}.${format}`);
        const link = document.createElement("a");
        link.href = URL.createObjectURL(blob);
        link.download = name;
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.setTimeout(() => URL.revokeObjectURL(link.href), 0);
        if (pinned && mask.data === pinned.data) pinned.dirty = false;
        setStatus?.(t("pixel_mask.status.saved", { name }), { tone: "success" });
      } catch (err) {
        console.error(err);
        setStatus?.(t("pixel_mask.status.save_failed"), { tone: "error" });
      }
    });
  }

  // ---- Detector -> Pixel Mask -------------------------------------------------

  function detectorName(connection) {
    return [connection.model, connection.serial].filter(Boolean).join(" ") || hostOf(connection.url) || connection.url;
  }

  function readBackups() {
    try {
      const parsed = JSON.parse(storage?.getItem(BACKUPS_STORAGE_KEY) || "{}");
      return parsed && typeof parsed === "object" ? parsed : {};
    } catch {
      return {};
    }
  }

  // Backups are kept by host: the Detector tab and the monitor may write the
  // same detector's address differently.
  const backupKey = (url) => hostOf(url) || url;

  function backupFor(url) {
    const entry = readBackups()[backupKey(url)];
    return entry && typeof entry.path === "string" && entry.path ? entry : null;
  }

  function rememberBackup(url, path) {
    const backups = readBackups();
    backups[backupKey(url)] = { path, at: Date.now() };
    try {
      storage?.setItem(BACKUPS_STORAGE_KEY, JSON.stringify(backups));
    } catch {
      // Private mode or a full quota: the backup file is still in masks/backups/.
    }
  }

  /**
   * Open in ALBIS: the backend saves the detector's mask as an HDF5 file in
   * the data folder's masks/, which then opens like any data -- the mask is
   * the file's own, to look at, edit, save or send back.
   */
  function openFromDetector() {
    const connection = getDetector();
    if (!connection) return Promise.resolve();
    return withBusy(async () => {
      if (!(await confirmDropEdits())) return;
      let body;
      try {
        const form = new FormData();
        const serial = connection.serial || hostOf(connection.url);
        form.append("meta", JSON.stringify({ url: connection.url, version: connection.version, serial }));
        body = await fetchJSONWithInit(`${apiBase}/pixel-mask/detector/copy`, { method: "POST", body: form });
        if (!body?.path) throw new Error(t("http.error.generic"));
      } catch (err) {
        console.error(err);
        setStatus?.(t("pixel_mask.detector.status.read_failed", { reason: failureReason(err) }), { tone: "error" });
        return;
      }
      detectorSeen.set(connection.url, { masked: body.masked, at: Date.now() });
      // The file's own mask from now on, not edits made on other data.
      if (pinned) {
        if (isEditing()) closeEditor({ keep: true });
        pinned = null;
        void refreshMask?.();
      }
      await openPath(body.path);
      setStatus?.(t("pixel_mask.detector.status.opened", { count: formatCount(body.masked), name: body.path }), {
        tone: "success",
      });
    });
  }

  /**
   * The detector a mask is sent to: the Detector tab's, else the one the
   * SIMPLON monitor shows. Without the Detector tab nothing says whether that
   * one is busy; the backend checks before writing.
   */
  function sendTarget() {
    return getDetector() || getMonitorDetector();
  }

  /**
   * Send `mask` to the connected detector, after saying what changes. The
   * detector's mask is read again first: it is what the change is counted
   * against, and what the backend saves as the backup.
   */
  async function sendMask(givenMask, { restoring = null, clearing = false } = {}) {
    const connection = sendTarget();
    if (!connection || (!givenMask && !clearing) || connection.locked) return;
    // A copy of this detector's mask in use goes out of date with a Clear or
    // Restore and is dropped then; edits in it are asked about first.
    const staleCopy = Boolean(
      (clearing || restoring) && pinned?.detectorHost && pinned.detectorHost === hostOf(connection.url),
    );
    if (staleCopy && !(await confirmDropEdits())) return;
    let before;
    try {
      before = await readDetectorMask(connection);
    } catch (err) {
      setStatus?.(t("pixel_mask.detector.status.read_failed", { reason: err.message || "" }), { tone: "error" });
      return;
    }
    // Clearing: the detector's own mask with nothing left but its module gaps,
    // where it has no pixels to measure with.
    const mask = clearing
      ? { data: before.data.map((value) => value & GAP_BIT), shape: before.shape }
      : givenMask;
    if (before.shape[0] !== mask.shape[0] || before.shape[1] !== mask.shape[1]) {
      setStatus?.(
        t("pixel_mask.detector.status.shape", { detector: shapeText(before.shape), mask: shapeText(mask.shape) }),
        { tone: "error" },
      );
      return;
    }
    const diff = diffMasks(before.data, mask.data);
    if (!diff.added && !diff.removed && !diff.changed) {
      setStatus?.(t(clearing ? "pixel_mask.detector.status.already_clear" : "pixel_mask.detector.status.same"), {
        tone: "success",
      });
      return;
    }
    const confirmed = await showConfirmDialog(
      clearing
        ? {
            title: t("pixel_mask.detector.clear_title"),
            message: t("pixel_mask.detector.clear_text", {
              detector: detectorName(connection),
              removed: formatCount(diff.removed + diff.changed),
            }),
            confirmLabel: t("pixel_mask.detector.clear_confirm"),
            danger: true,
          }
        : restoring
        ? {
            title: t("pixel_mask.detector.restore_title"),
            message: t("pixel_mask.detector.restore_text", {
              detector: detectorName(connection),
              time: formatTime(restoring.at),
            }),
            confirmLabel: t("pixel_mask.detector.restore"),
            danger: true,
          }
        : {
            title: t("pixel_mask.detector.confirm_title"),
            message: t("pixel_mask.detector.confirm_text", {
              detector: detectorName(connection),
              added: formatCount(diff.added),
              removed: formatCount(diff.removed),
              changed: formatCount(diff.changed),
            }),
            confirmLabel: t("pixel_mask.detector.confirm_send"),
            danger: true,
          },
    );
    if (!confirmed) return;
    // The detector may have started something while the question was open.
    const now = sendTarget();
    if (!now || now.url !== connection.url || now.locked) {
      setStatus?.(t("pixel_mask.detector.status.send_failed", { reason: t("detector.locked") }), { tone: "error" });
      return;
    }
    try {
      const res = await fetch(`${apiBase}/pixel-mask/detector`, {
        method: "POST",
        body: maskForm(
          { url: connection.url, version: connection.version, serial: connection.serial || hostOf(connection.url) },
          mask,
        ),
      });
      let body = null;
      try {
        body = await res.json();
      } catch {
        body = null;
      }
      if (!res.ok) {
        const detail = body?.detail;
        throw new Error(typeof detail === "string" ? detail : detail?.summary || `HTTP ${res.status}`);
      }
      detectorSeen.set(connection.url, { masked: body.masked, at: Date.now() });
      if (staleCopy && pinned?.detectorHost === hostOf(connection.url)) unpin();
      onDetectorMaskWritten(connection.url);
      // A restore keeps the backup it restored from: Restore again must not
      // bring back the mask it just replaced.
      if (!restoring && body.backup) rememberBackup(connection.url, body.backup);
      if (!restoring && pinned && mask.data === pinned.data && body.verified) pinned.dirty = false;
      const count = formatCount(body.masked);
      if (body.verified) {
        let entry = t("pixel_mask.detector.log.sent", { count, previous: formatCount(body.previous_masked) });
        if (restoring) entry = t("pixel_mask.detector.log.restored", { time: formatTime(restoring.at) });
        if (clearing) entry = t("pixel_mask.detector.log.cleared", { previous: formatCount(body.previous_masked) });
        logDetector?.(entry);
        setStatus?.(t(clearing ? "pixel_mask.detector.status.cleared" : "pixel_mask.detector.status.sent", { count }), {
          tone: "success",
        });
      } else {
        logDetector?.(t("pixel_mask.detector.log.unverified"), "error");
        setStatus?.(t("pixel_mask.detector.status.unverified"), { tone: "warning" });
      }
    } catch (err) {
      console.error(err);
      logDetector?.(t("pixel_mask.detector.status.send_failed", { reason: err.message || "" }), "error");
      setStatus?.(t("pixel_mask.detector.status.send_failed", { reason: err.message || "" }), { tone: "error" });
    }
  }

  function sendCurrent() {
    return withBusy(() => sendMask(currentMask()));
  }

  /** Leave the detector's mask with only its module gaps, after asking. */
  function clearDetectorMask() {
    return withBusy(() => sendMask(null, { clearing: true }));
  }

  function restoreBackup() {
    const connection = sendTarget();
    const backup = connection ? backupFor(connection.url) : null;
    if (!backup) return Promise.resolve();
    return withBusy(async () => {
      let mask;
      try {
        mask = await readFileMask(backup.path);
      } catch (err) {
        console.error(err);
        setStatus?.(t("pixel_mask.status.load_failed"), { tone: "error" });
        return;
      }
      await sendMask(mask, { restoring: backup });
    });
  }

  // ---- rendering ------------------------------------------------------------

  function renderDataSection() {
    if (section) section.hidden = !enabled;
    if (!enabled) return;
    const mask = currentMask();
    const summary = summaryOf(mask?.data || null);
    const applies = Boolean(state.maskAvailable);
    if (!mask) {
      setSummaryChip?.(summaryChip, "");
      setSectionBadgeState?.(stateEl, "empty", t("pixel_mask.state.none"));
    } else {
      const edited = hasEdits();
      const chipKey = edited ? "pixel_mask.summary.edited" : pinned?.origin === "new" ? "pixel_mask.summary.new" : "pixel_mask.summary.data";
      setSummaryChip?.(summaryChip, t(chipKey), edited ? "warning" : pinned ? "active" : "default");
      if (!applies && hasImage()) {
        setSectionBadgeState?.(
          stateEl,
          "warning",
          t("pixel_mask.state.mismatch", { mask: shapeText(mask.shape), image: shapeText([state.height, state.width]) }),
        );
      } else {
        setSectionBadgeState?.(stateEl, "active", t("pixel_mask.state.masked", { count: formatCount(summary.masked) }));
      }
    }
    if (sourceEl) {
      // One line: where it came from, then only the classes there are --
      // "From run_7_master.h5 · Gaps 1,024 · Defective 20".
      let source = "";
      if (pinned) source = t(`pixel_mask.source.${pinned.origin}`, { name: pinned.label });
      else if (mask) source = t("pixel_mask.source.data", { name: dataMaskLabel() });
      const parts = mask
        ? ["gap", "defective", "other"]
            .filter((kind) => summary[kind] > 0)
            .map((kind) => t(`pixel_mask.count.${kind}`, { count: formatCount(summary[kind]) }))
        : [];
      const text = [source, ...parts].filter(Boolean).join(" · ");
      sourceEl.textContent = text;
      // Where in the file, for a mask from HDF5.
      sourceEl.title = !pinned && state.maskPath ? `${text}\n${state.maskPath}` : text;
      sourceEl.hidden = !text;
    }
    if (saveBtn) saveBtn.disabled = busy || !mask;
    if (saveH5Btn) saveH5Btn.disabled = busy || !mask;
    if (saveTiffBtn) saveTiffBtn.disabled = busy || !mask;
    if ((busy || !mask) && saveMenu && !saveMenu.hidden) setSaveMenu(false);
    const edited = hasEdits();
    if (editsEl) {
      const counts = editsOf();
      editsEl.textContent = edited
        ? t("pixel_mask.edits", { masked: formatCount(counts.masked), unmasked: formatCount(counts.unmasked) })
        : "";
      editsEl.hidden = !edited;
    }
    if (editBtn) {
      const editing = isEditing();
      const blocker = editing ? "" : editBlocker();
      editBtn.disabled = busy || Boolean(blocker);
      editBtn.classList.toggle("is-active", editing);
      editBtn.setAttribute("aria-pressed", editing ? "true" : "false");
      editBtn.textContent = t(editing ? "pixel_mask.action.editing" : "pixel_mask.action.edit");
      if (blocker) editBtn.dataset.helpReason = blocker;
      else delete editBtn.dataset.helpReason;
    }
    if (discardBtn) {
      discardBtn.hidden = !edited;
      discardBtn.disabled = busy;
    }
    const target = sendTarget();
    if (sendBtn) {
      sendBtn.hidden = !target;
      sendBtn.disabled = busy || !mask || Boolean(target?.locked);
      if (target) sendBtn.title = t("pixel_mask.send_to", { name: detectorName(target) });
    }
  }

  function renderDetectorSection() {
    const connection = enabled ? getDetector() : null;
    if (detSection) detSection.hidden = !connection;
    if (!connection) return;
    const seen = detectorSeen.get(connection.url);
    if (detInfo) {
      detInfo.textContent = seen
        ? t("pixel_mask.detector.read", { count: formatCount(seen.masked), time: formatTime(seen.at) })
        : t("pixel_mask.detector.not_read");
    }
    if (detSummary) {
      detSummary.textContent = connection.locked ? t("detector.locked") : "";
      detSummary.hidden = !connection.locked;
    }
    if (detOpenBtn) detOpenBtn.disabled = busy;
    const backup = backupFor(connection.url);
    if (detBackup) detBackup.hidden = !backup;
    if (backup && detBackupText) detBackupText.textContent = t("pixel_mask.detector.backup", { time: formatTime(backup.at) });
    if (detRestoreBtn) detRestoreBtn.disabled = busy || connection.locked;
    if (detClearBtn) detClearBtn.disabled = busy || connection.locked;
  }

  function render() {
    // An image of another size came: the edits no longer fit what is shown.
    if (isEditing() && !getEditTarget()) closeEditor({ keep: true });
    renderDataSection();
    renderFind();
    renderDetectorSection();
  }

  /** Show or hide the feature. Switched off, a pinned mask goes too. */
  function setEnabled(next) {
    enabled = Boolean(next);
    if (!enabled && pinned) {
      unpin();
      return;
    }
    if (!enabled && isEditing()) closeEditor({ keep: true });
    if (!enabled && findOpen) closeFind();
    render();
  }

  // Save as ▾: a small menu of the two formats, HDF5 first -- the one ALBIS
  // opens again as data; TIFF is for other programs.
  function setSaveMenu(open) {
    if (!saveMenu) return;
    saveMenu.hidden = !open;
    saveBtn?.setAttribute("aria-expanded", open ? "true" : "false");
    if (open) saveH5Btn?.focus?.();
  }
  saveBtn?.addEventListener("click", () => setSaveMenu(Boolean(saveMenu?.hidden)));
  saveMenu?.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      setSaveMenu(false);
      saveBtn?.focus?.();
    }
  });
  if (typeof document !== "undefined") {
    document.addEventListener("pointerdown", (event) => {
      if (saveMenu && !saveMenu.hidden && !saveMenu.parentElement?.contains(event.target)) setSaveMenu(false);
    });
  }
  saveH5Btn?.addEventListener("click", () => {
    setSaveMenu(false);
    void save("h5");
  });
  saveTiffBtn?.addEventListener("click", () => {
    setSaveMenu(false);
    void save("tiff");
  });
  editBtn?.addEventListener("click", () => beginEdit());
  sendBtn?.addEventListener("click", () => void sendCurrent());
  findBtn?.addEventListener("click", () => openFind());
  findCloseBtn?.addEventListener("click", () => closeFind());
  findAddBtn?.addEventListener("click", () => addFoundToMask());
  [findHot, findCold].forEach((box) => box?.addEventListener("change", () => scheduleFind(0)));
  [findHotSigma, findColdPercent].forEach((input) => input?.addEventListener("input", () => scheduleFind()));
  discardBtn?.addEventListener("click", () => void discardEdits());
  // Edits only live in this page until saved or sent.
  if (typeof window !== "undefined") {
    window.addEventListener("beforeunload", (event) => {
      if (!enabled || !pinned?.dirty || !hasEdits()) return;
      event.preventDefault();
      event.returnValue = "";
    });
  }
  detOpenBtn?.addEventListener("click", () => void openFromDetector());
  detRestoreBtn?.addEventListener("click", () => void restoreBackup());
  detClearBtn?.addEventListener("click", () => void clearDetectorMask());

  render();

  return {
    setEnabled,
    render,
    getPinnedMask: () => (enabled ? pinned : null),
    currentMask,
    openFromDetector,
    sendCurrent,
    restoreBackup,
    save,
    unpin,
    beginEdit,
    discardEdits,
    getEditTarget,
    onEditCommit,
    onEditClose,
    describeEdits,
    cursorNote,
    hasEdits,
    clearDetectorMask,
    openFind,
    closeFind,
    addFoundToMask,
  };
}
