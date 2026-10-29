/**
 * The Resolution Rings section's list of d-spacings: as many rings as the
 * user wants, up to MAX_RINGS, each a field with a remove button, plus Add
 * ring and the presets.
 *
 * `analysisState.rings` is the one source of truth -- the valid values, in the
 * list's order -- and everything that draws or drags rings reads it by index.
 * The fields only show it. A field being typed in may hold a draft (empty, or
 * not yet a number) that is left out of the rings until it is one.
 *
 * The list is sorted by d, and drafts dropped, when focus leaves it rather
 * than on every keystroke, so nothing moves under the cursor. It is kept in
 * localStorage, like the sections' open state, so the rings outlast a reload.
 */

import { t } from "./i18n.js";
import { attachNumberStepper } from "./number_stepper.js";

export const MAX_RINGS = 16;
export const DEFAULT_RINGS = [1.0, 3.67, 11.01];
const STORAGE_KEY = "albis.rings";

/** Hexagonal ice (Ih) d-spacings in Å: the rings ice puts on a diffraction image. */
export const ICE_RINGS = [3.897, 3.669, 3.441, 2.671, 2.249, 2.072, 1.948, 1.918, 1.883, 1.721];

export function parseRing(raw) {
  const text = String(raw ?? "").trim();
  if (!text) return null;
  const value = Number(text);
  return Number.isFinite(value) && value > 0 ? value : null;
}

/** Valid, distinct (to 0.001 Å) and ascending, at most MAX_RINGS. */
export function normalizeRings(values) {
  const out = [];
  for (const raw of Array.isArray(values) ? values : []) {
    const value = parseRing(raw);
    if (value === null || out.some((d) => Math.abs(d - value) < 0.001)) continue;
    out.push(value);
  }
  return out.sort((a, b) => a - b).slice(0, MAX_RINGS);
}

function readStored() {
  try {
    const stored = JSON.parse(window.localStorage?.getItem(STORAGE_KEY) || "null");
    return Array.isArray(stored) ? normalizeRings(stored) : null;
  } catch {
    return null;
  }
}

function store(rings) {
  try {
    window.localStorage?.setItem(STORAGE_KEY, JSON.stringify(rings));
  } catch {
    // A reload then starts from the defaults again; nothing else depends on it.
  }
}

export function createRingListController({ analysisState, elements, callbacks = {} }) {
  const { list, addBtn, iceBtn, clearBtn } = elements;
  const { onChange } = callbacks;

  // What the fields hold, drafts included.
  let entries = (readStored() ?? normalizeRings(analysisState.rings ?? DEFAULT_RINGS)).map(String);

  function publish(notify = true) {
    analysisState.rings = entries.map(parseRing).filter((value) => value !== null);
    analysisState.ringCount = analysisState.rings.length;
    if (notify) onChange?.();
  }

  function updateButtons() {
    if (addBtn) addBtn.disabled = entries.length >= MAX_RINGS;
    if (clearBtn) clearBtn.disabled = entries.length === 0;
  }

  function render() {
    if (!list) return;
    list.replaceChildren(
      ...entries.map((value, index) => {
        const row = document.createElement("span");
        row.className = "ring-entry";
        const input = document.createElement("input");
        input.type = "number";
        input.min = "0";
        input.step = "0.1";
        input.value = value;
        input.dataset.ringIndex = String(index);
        input.setAttribute("aria-label", t("rings.ring_label", { index: index + 1 }));
        input.classList.toggle("is-invalid", value.trim() !== "" && parseRing(value) === null);
        const remove = document.createElement("button");
        remove.type = "button";
        remove.className = "ring-remove";
        remove.dataset.ringRemove = String(index);
        remove.textContent = "×";
        remove.title = t("rings.remove");
        remove.setAttribute("aria-label", t("rings.remove"));
        row.append(input, remove);
        attachNumberStepper(input);
        return row;
      }),
    );
    updateButtons();
  }

  /** Sort, drop drafts, remember -- once the user is done with the list. */
  function commit() {
    const rings = normalizeRings(entries);
    entries = rings.map(String);
    store(rings);
    render();
    publish();
  }

  function setRings(values) {
    entries = normalizeRings(values).map(String);
    commit();
  }

  function addRing() {
    if (entries.length >= MAX_RINGS) return;
    entries.push("");
    render();
    updateButtons();
    list?.querySelector(`input[data-ring-index="${entries.length - 1}"]`)?.focus();
  }

  /** A ring dragged on the image: index into `analysisState.rings`. */
  function setRingValue(index, value) {
    let seen = -1;
    const entryIndex = entries.findIndex((entry) => parseRing(entry) !== null && ++seen === index);
    if (entryIndex < 0 || !(value > 0)) return;
    entries[entryIndex] = String(Number(value.toFixed(3)));
    const input = list?.querySelector(`input[data-ring-index="${entryIndex}"]`);
    if (input) input.value = entries[entryIndex];
    publish();
  }

  list?.addEventListener("input", (event) => {
    const input = event.target.closest?.("input[data-ring-index]");
    if (!input) return;
    entries[Number(input.dataset.ringIndex)] = input.value;
    input.classList.toggle("is-invalid", input.value.trim() !== "" && parseRing(input.value) === null);
    publish();
  });
  list?.addEventListener("click", (event) => {
    const remove = event.target.closest?.("[data-ring-remove]");
    if (!remove) return;
    entries.splice(Number(remove.dataset.ringRemove), 1);
    commit();
  });
  list?.addEventListener("focusout", (event) => {
    if (list.contains(event.relatedTarget)) return;
    commit();
  });
  list?.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && event.target.matches?.("input[data-ring-index]")) {
      event.preventDefault();
      commit();
    }
  });
  addBtn?.addEventListener("click", addRing);
  iceBtn?.addEventListener("click", () => setRings([...entries, ...ICE_RINGS]));
  clearBtn?.addEventListener("click", () => setRings([]));

  // Values only: what draws the rings may not exist yet when this is created.
  render();
  publish(false);

  return { setRings, setRingValue, commit, refresh: render };
}
