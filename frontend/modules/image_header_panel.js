/**
 * The Image Header section: the text header of a TIFF, CBF or EDF file.
 *
 * Split out of inspector_panel_controller.js, which shows either this or the
 * HDF5 inspector depending on the file and still decides which.
 */

import { t } from "./i18n.js";

export function createImageHeaderPanel({ apiBase, state, elements, callbacks }) {
  const { imageHeaderStateEl, imageHeaderLoading, imageHeaderText, imageHeaderEmpty } = elements;
  const { fetchJSON, isHeaderCapableFile, setSectionBadgeState, renderSkeletonBlock } = callbacks;

  function setImageHeaderSectionState(tone, message) {
    setSectionBadgeState(imageHeaderStateEl, tone, message);
  }

  function setImageHeaderLoading(loading) {
    if (!imageHeaderLoading) return;
    if (loading) {
      renderSkeletonBlock(imageHeaderLoading, 7);
      imageHeaderLoading.classList.remove("is-hidden");
      imageHeaderLoading.setAttribute("aria-hidden", "false");
      return;
    }
    imageHeaderLoading.classList.add("is-hidden");
    imageHeaderLoading.setAttribute("aria-hidden", "true");
    imageHeaderLoading.innerHTML = "";
  }

  function setImageHeader(text) {
    if (!imageHeaderText || !imageHeaderEmpty) return;
    const headerText = typeof text === "string" ? text.trim() : "";
    const hasText = headerText.length > 0;
    setImageHeaderLoading(false);
    imageHeaderText.textContent = hasText ? text : "";
    imageHeaderText.classList.toggle("is-hidden", !hasText);
    imageHeaderEmpty.classList.toggle("is-hidden", hasText);
    if (hasText) {
      setImageHeaderSectionState("active", t("inspector.header.loaded"));
    } else {
      setImageHeaderSectionState("empty", t("inspector.no_header"));
    }
  }

  function clearImageHeader() {
    state.imageHeaderFile = "";
    state.imageHeaderText = "";
    setImageHeaderLoading(false);
    setImageHeader("");
  }

  async function loadImageHeader(file) {
    if (!file || !isHeaderCapableFile(file)) {
      clearImageHeader();
      return;
    }
    if (state.imageHeaderFile === file && state.imageHeaderText) {
      setImageHeader(state.imageHeaderText);
      return;
    }
    setImageHeaderSectionState("loading", t("inspector.header.loading"));
    setImageHeaderLoading(true);
    if (imageHeaderText) {
      imageHeaderText.textContent = "";
      imageHeaderText.classList.add("is-hidden");
    }
    if (imageHeaderEmpty) {
      imageHeaderEmpty.classList.add("is-hidden");
    }
    try {
      const data = await fetchJSON(`${apiBase}/image/header?file=${encodeURIComponent(file)}`, {
        timeoutMs: 30000,
      });
      const text = typeof data.header === "string" ? data.header : "";
      state.imageHeaderFile = file;
      state.imageHeaderText = text;
      setImageHeader(text);
    } catch (err) {
      console.warn(err);
      state.imageHeaderFile = file;
      state.imageHeaderText = "";
      setImageHeaderLoading(false);
      if (imageHeaderText) {
        imageHeaderText.textContent = "";
        imageHeaderText.classList.add("is-hidden");
      }
      if (imageHeaderEmpty) {
        imageHeaderEmpty.classList.remove("is-hidden");
        imageHeaderEmpty.textContent = t("inspector.no_header");
      }
      setImageHeaderSectionState("warning", t("inspector.header.unavailable"));
    }
  }

  return { clearImageHeader, loadImageHeader };
}
