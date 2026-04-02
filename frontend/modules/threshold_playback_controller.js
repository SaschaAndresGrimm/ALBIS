/**
 * Threshold selector and playback-control UI state.
 */

import { t } from "./i18n.js";

export function createThresholdPlaybackController({
  state,
  constants,
  elements,
  callbacks,
}) {
  const { frameStepOptions } = constants;

  const {
    thresholdSelect,
    thresholdField,
    toolbarThresholdWrap,
    toolbarThresholdSelect,
    toolbarMoreThreshold,
    toolbarMoreThresholdField,
    fpsSelect,
    toolbarMoreFps,
    frameStep,
    toolbarMoreStep,
    playBtn,
    prevBtn,
    nextBtn,
  } = elements;

  const {
    formatEnergy,
    option,
    syncToolbarMoreControls,
    updateViewerFooter,
    updateFpsLabel,
    stopPlayback,
    startPlayback,
    loadMask,
    requestFrame,
  } = callbacks;

  function getThresholdDisplayOrder(count = state.thresholdCount, energies = state.thresholdEnergies) {
    const safeCount = Math.max(1, Number(count) || 1);
    const order = Array.from({ length: safeCount }, (_, i) => i);
    const energyList = Array.isArray(energies) ? energies : [];
    const hasFiniteEnergy = order.some((idx) => Number.isFinite(Number(energyList[idx])));
    if (!hasFiniteEnergy) return order;
    order.sort((a, b) => {
      const energyA = Number(energyList[a]);
      const energyB = Number(energyList[b]);
      const aFinite = Number.isFinite(energyA);
      const bFinite = Number.isFinite(energyB);
      if (aFinite && bFinite) {
        if (energyA === energyB) return a - b;
        return energyB - energyA;
      }
      if (aFinite !== bFinite) return aFinite ? -1 : 1;
      return a - b;
    });
    return order;
  }

  function getDefaultThresholdIndex() {
    const order = getThresholdDisplayOrder();
    return order.length ? order[order.length - 1] : 0;
  }

  function getThresholdIndexAtOffset(offset) {
    const order = getThresholdDisplayOrder();
    if (!order.length) return 0;
    const current = order.includes(state.thresholdIndex) ? state.thresholdIndex : getDefaultThresholdIndex();
    const currentPos = Math.max(0, order.indexOf(current));
    const nextPos = Math.max(0, Math.min(order.length - 1, currentPos + Math.round(offset)));
    return order[nextPos];
  }

  function updateThresholdOptions() {
    if (!thresholdSelect || !thresholdField) return;
    const count = Math.max(1, state.thresholdCount || 1);
    const show = count > 1 && state.autoload.mode === "file";
    thresholdField.classList.toggle("is-hidden", !show);
    if (toolbarThresholdWrap) {
      toolbarThresholdWrap.classList.toggle("is-hidden", !show);
    }
    thresholdSelect.innerHTML = "";
    if (toolbarThresholdSelect) {
      toolbarThresholdSelect.innerHTML = "";
    }
    if (toolbarMoreThreshold) {
      toolbarMoreThreshold.innerHTML = "";
    }
    const energies = Array.isArray(state.thresholdEnergies) ? state.thresholdEnergies : [];
    const order = getThresholdDisplayOrder(count, energies);
    order.forEach((thresholdIndex) => {
      const energy = Number(energies[thresholdIndex]);
      const energyText = Number.isFinite(energy) ? ` ${formatEnergy(energy)} eV` : "";
      const label = `Thr${thresholdIndex + 1}${energyText}`;
      thresholdSelect.appendChild(option(label, String(thresholdIndex)));
      if (toolbarThresholdSelect) {
        toolbarThresholdSelect.appendChild(option(label, String(thresholdIndex)));
      }
      if (toolbarMoreThreshold) {
        toolbarMoreThreshold.appendChild(option(label, String(thresholdIndex)));
      }
    });
    const idx = order.includes(state.thresholdIndex) ? state.thresholdIndex : getDefaultThresholdIndex();
    state.thresholdIndex = idx;
    thresholdSelect.value = String(idx);
    if (toolbarThresholdSelect) {
      toolbarThresholdSelect.value = String(idx);
    }
    if (toolbarMoreThreshold) {
      toolbarMoreThreshold.value = String(idx);
      toolbarMoreThreshold.disabled = count <= 1;
    }
    if (toolbarMoreThresholdField) {
      toolbarMoreThresholdField.classList.toggle("is-hidden", !show);
    }
    thresholdSelect.disabled = count <= 1;
    if (toolbarThresholdSelect) {
      toolbarThresholdSelect.disabled = count <= 1;
    }
    syncToolbarMoreControls();
    updateViewerFooter();
  }

  async function setThresholdIndex(nextIndex) {
    const count = Math.max(1, state.thresholdCount || 1);
    const parsed = Number(nextIndex);
    if (!Number.isFinite(parsed)) return;
    const clamped = Math.max(0, Math.min(count - 1, Math.round(parsed)));
    if (clamped === state.thresholdIndex) return;
    state.thresholdIndex = clamped;
    if (thresholdSelect) thresholdSelect.value = String(clamped);
    if (toolbarThresholdSelect) toolbarThresholdSelect.value = String(clamped);
    if (toolbarMoreThreshold) toolbarMoreThreshold.value = String(clamped);
    state.maskFile = "";
    await loadMask(true);
    requestFrame(state.frameIndex);
  }

  function setFps(value) {
    const clamped = Math.max(1, Math.min(10, Math.round(value)));
    state.fps = clamped;
    if (fpsSelect) fpsSelect.value = String(clamped);
    if (toolbarMoreFps) toolbarMoreFps.value = String(clamped);
    updateFpsLabel();
    if (state.playing) {
      stopPlayback();
      startPlayback();
    }
  }

  function setFrameStep(value) {
    const parsed = Math.round(Number(value || 1));
    const next = frameStepOptions.includes(parsed) ? parsed : frameStepOptions[0];
    state.step = next;
    if (frameStep) {
      frameStep.value = String(next);
    }
    if (toolbarMoreStep) {
      toolbarMoreStep.value = String(next);
    }
  }

  function updatePlayButtons() {
    const liveMode =
      state.autoload.mode === "simplon" ||
      state.autoload.mode === "remote" ||
      state.autoload.mode === "jungfraujoch";
    const liveHistoryLength = Array.isArray(state.autoload.historyEntries) ? state.autoload.historyEntries.length : 0;
    const liveHistoryActive = liveMode && liveHistoryLength > 0;
    if (liveHistoryActive) {
      const livePaused = state.autoload.livePaused === true;
      const currentIndex = Math.max(0, Math.min(liveHistoryLength - 1, Number(state.frameIndex) || 0));
      if (playBtn) {
        playBtn.classList.toggle("is-active", !livePaused);
        playBtn.disabled = false;
        if (!livePaused) {
          playBtn.textContent = t("backend.live.stop");
          playBtn.setAttribute("aria-label", t("toolbar.play.stop_live_aria"));
          playBtn.title = t("toolbar.play.stop_live_title");
          playBtn.dataset.help = t("hint.frame.stop_live");
        } else {
          playBtn.textContent = t("backend.live.live");
          playBtn.setAttribute("aria-label", t("toolbar.play.go_live_aria"));
          playBtn.title = t("toolbar.play.go_live_title");
          playBtn.dataset.help = t("hint.frame.go_live");
        }
      }
      if (prevBtn) prevBtn.disabled = liveHistoryLength <= 1 || currentIndex <= 0;
      if (nextBtn) nextBtn.disabled = liveHistoryLength <= 1 || currentIndex >= liveHistoryLength - 1;
      return;
    }
    const hasSeries = Array.isArray(state.seriesFiles) && state.seriesFiles.length > 0;
    const disabled = !state.file || (!state.dataset && !hasSeries) || state.frameCount <= 1;
    if (playBtn) {
      playBtn.classList.toggle("is-active", state.playing);
      playBtn.disabled = disabled;
      playBtn.textContent = state.playing ? "⏸" : "⏯";
      playBtn.setAttribute("aria-label", t("toolbar.play.toggle"));
      playBtn.title = "";
      playBtn.dataset.help = t("hint.frame.play_pause");
    }
    if (prevBtn) prevBtn.disabled = disabled;
    if (nextBtn) nextBtn.disabled = disabled;
  }

  return {
    getThresholdDisplayOrder,
    getDefaultThresholdIndex,
    getThresholdIndexAtOffset,
    updateThresholdOptions,
    setThresholdIndex,
    setFps,
    setFrameStep,
    updatePlayButtons,
  };
}
