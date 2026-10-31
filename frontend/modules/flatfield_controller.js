/**
 * The Flatfield section of the Analysis tab: its switch, where the flat comes
 * from (the image on screen, or a file), the scale, what it is dividing by,
 * and the footer's "Flatfielded" chip.
 *
 * "Use current image" keeps the frame on screen as a TIFF in the data folder's
 * `flatfields/`, so the flat stays reusable and every export divides by the
 * same file. Its gaps, defective and saturated pixels are written as 0: a flat
 * has no usable value there. The prepared flat itself is held here, outside
 * `state`, and handed to inpainting.js's frame pipeline through `getFlat`.
 */

import { t } from "./i18n.js";
import { flatFits, normalizeFlatScale, prepareFlat } from "./flatfield.js";
import { DATA_DEFECTIVE_VALUE, DATA_GAP_VALUE, usesInDataPixelFlags } from "./roi_stats_engine.js";
import { chooseNativePath } from "./native_picker.js";
import { fetchJSONWithInit, fetchResponse } from "./http.js";

function fileName(path) {
  return String(path || "").split(/[\\/]/).pop();
}

export function createFlatfieldController({ apiBase, state, elements, callbacks, core }) {
  const { toggle, useCurrentBtn, chooseBtn, scaleSelect, stateEl, summaryEl, footerChip } = elements;
  const {
    redraw,
    refreshDisplayContrast,
    scheduleHistogram,
    setSectionBadgeState,
    setSummaryChip,
    setStatus,
    getActiveSaturationMax,
    isSaturatedValue,
    parseDtype,
    parseShape,
    typedArrayFrom,
  } = callbacks;

  let flat = null;
  let busy = false;

  /** The prepared flat, or null. */
  function getFlat() {
    return flat;
  }

  function fits() {
    return flatFits(flat, state.width, state.height);
  }

  function updateState() {
    const applied = Boolean(state.flatEnabled && state.dataRaw && fits());
    if (footerChip) footerChip.hidden = !applied;
    if (!state.flatEnabled) {
      setSectionBadgeState(stateEl, "empty", t("flatfield.state.enable_hint"));
      setSummaryChip(summaryEl, t("summary.off"));
      return;
    }
    if (!flat) {
      setSectionBadgeState(stateEl, "warning", t("flatfield.state.choose"));
      setSummaryChip(summaryEl, t("flatfield.summary.no_flat"), "warning");
      return;
    }
    const name = fileName(state.flatPath);
    if (state.dataRaw && !fits()) {
      setSectionBadgeState(
        stateEl,
        "warning",
        t("flatfield.state.mismatch", {
          name,
          flat: `${flat.width}×${flat.height}`,
          image: `${state.width}×${state.height}`,
        }),
      );
      setSummaryChip(summaryEl, t("flatfield.summary.mismatch"), "warning");
      return;
    }
    setSectionBadgeState(
      stateEl,
      "active",
      t("flatfield.state.dividing", { name, mean: Math.round(flat.mean).toLocaleString() }),
    );
    setSummaryChip(summaryEl, name, "active");
  }

  // Everything that shows or measures the image follows the new frame.
  function refresh() {
    refreshDisplayContrast(state.autoScale);
    redraw();
    scheduleHistogram();
    core.roiStats.updateGlobalStats();
    core.roiInteraction.scheduleRoiUpdate();
    updateState();
  }

  function useFlat(prepared, path) {
    flat = prepared;
    state.flatPath = path;
    state.flatShape = prepared ? [prepared.height, prepared.width] : null;
    state.flatEnabled = true;
    if (toggle) toggle.checked = true;
    refresh();
  }

  // The frame on screen, with what a flat cannot use set to 0: its gaps and
  // defective pixels (by the mask, or a CBF's -1 and -2) and saturated ones.
  function flatCopy() {
    const data = state.dataRaw.slice();
    const n = state.width * state.height;
    const mask =
      state.maskAvailable && state.maskRaw && state.maskShape &&
      state.maskShape[0] === state.height && state.maskShape[1] === state.width
        ? state.maskRaw
        : null;
    const inDataFlags = !mask && usesInDataPixelFlags(data);
    const satMax = getActiveSaturationMax();
    for (let i = 0; i < n; i += 1) {
      const v = data[i];
      if (
        (mask && mask[i] & 0x1f) ||
        (inDataFlags && (v === DATA_GAP_VALUE || v === DATA_DEFECTIVE_VALUE)) ||
        (Number.isFinite(satMax) && isSaturatedValue(v, satMax))
      ) {
        data[i] = 0;
      }
    }
    return data;
  }

  async function useCurrentImage() {
    if (busy) return;
    if (!state.dataRaw || !state.width || !state.height) {
      setStatus(t("flatfield.status.no_image"), { tone: "warning" });
      return;
    }
    busy = true;
    try {
      const data = flatCopy();
      const form = new FormData();
      form.append("meta", JSON.stringify({ dtype: state.dtype, shape: [state.height, state.width] }));
      form.append("pixels", new Blob([new Uint8Array(data.buffer, data.byteOffset, data.byteLength)]), "flat.bin");
      const { path } = await fetchJSONWithInit(`${apiBase}/flatfield/save`, { method: "POST", body: form });
      const prepared = prepareFlat(data, state.width, state.height);
      if (!prepared) {
        setStatus(t("flatfield.status.unusable"), { tone: "warning" });
        return;
      }
      useFlat(prepared, String(path || ""));
      setStatus(t("flatfield.status.saved", { name: fileName(path) }), { tone: "success" });
    } catch (err) {
      console.error(err);
      setStatus(t("flatfield.status.save_failed"), { tone: "error" });
    } finally {
      busy = false;
    }
  }

  /** Read a TIFF or CBF from the server and divide by it from now on. */
  async function loadFlat(path) {
    const res = await fetchResponse(`${apiBase}/image?file=${encodeURIComponent(path)}`);
    const dtype = parseDtype(res.headers.get("X-Dtype"));
    const [height, width] = parseShape(res.headers.get("X-Shape"));
    const data = typedArrayFrom(await res.arrayBuffer(), dtype);
    const prepared = prepareFlat(data, width, height);
    if (!prepared) {
      setStatus(t("flatfield.status.unusable"), { tone: "warning" });
      return false;
    }
    useFlat(prepared, String(path));
    return true;
  }

  async function chooseFile() {
    if (busy) return;
    busy = true;
    try {
      const path = await chooseNativePath(apiBase, { exts: ".tif,.tiff,.cbf" });
      if (!path) return;
      await loadFlat(path);
    } catch (err) {
      console.error(err);
      setStatus(t("flatfield.status.load_failed"), { tone: "error" });
    } finally {
      busy = false;
    }
  }

  /** Use a file already known by path as the flat: Combine Frames' "Use as flat". */
  async function useFile(path) {
    if (busy || !path) return false;
    busy = true;
    try {
      const used = await loadFlat(path);
      if (used) setStatus(t("flatfield.status.using", { name: fileName(path) }), { tone: "success" });
      return used;
    } catch (err) {
      console.error(err);
      setStatus(t("flatfield.status.load_failed"), { tone: "error" });
      return false;
    } finally {
      busy = false;
    }
  }

  toggle?.addEventListener("change", () => {
    state.flatEnabled = Boolean(toggle.checked);
    refresh();
  });
  scaleSelect?.addEventListener("change", () => {
    state.flatScale = normalizeFlatScale(scaleSelect.value);
    refresh();
  });
  useCurrentBtn?.addEventListener("click", () => void useCurrentImage());
  chooseBtn?.addEventListener("click", () => void chooseFile());
  if (toggle) toggle.checked = Boolean(state.flatEnabled);
  if (scaleSelect) scaleSelect.value = normalizeFlatScale(state.flatScale);
  updateState();

  return { getFlat, updateState, useCurrentImage, chooseFile, useFile };
}
