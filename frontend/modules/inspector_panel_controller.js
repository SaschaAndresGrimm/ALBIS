/**
 * Inspector tree/search and image-header panel orchestration.
 */

import { copyText } from "./clipboard_copy.js";
import { t } from "./i18n.js";
import { formatInspectorShape, inspectorRowMeta, splitInspectorPath, splitMatches } from "./inspector_format_utils.js";
import { treeKeyAction } from "./inspector_tree_nav_utils.js";
import { createImageHeaderPanel } from "./image_header_panel.js";
import { createInspectorPreviewRenderer } from "./inspector_preview_renderer.js";

export function createInspectorPanelController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    inspectorTree,
    inspectorSearchInput,
    inspectorResults,
    inspectorStateEl,
    inspectorPath,
    inspectorFacts,
    inspectorCopyPath,
    inspectorAttrs,
    inspectorPreview,
    inspectorDetails,
    inspectorSection,
    imageHeaderSection,
  } = elements;

  const {
    fetchJSON,
    isHdf5File,
    isHeaderCapableFile,
    setSectionBadgeState,
    renderSkeletonBlock,
    formatInspectorValue,
  } = callbacks;

  const { clearImageHeader, loadImageHeader } = createImageHeaderPanel({ apiBase, state, elements, callbacks });
  const { renderInspectorPreview, showDatasetInViewer, refreshViewerAction } = createInspectorPreviewRenderer({
    apiBase,
    state,
    elements,
    callbacks,
  });

  let inspectorSelectedRow = null;
  let inspectorSearchRequestId = 0;
  let inspectorNodeRequestId = 0;
  let inspectorDetailsPath = "";
  let copyFeedbackTimer = null;

  function normalizeInspectorType(type) {
    return String(type || "").trim().toLowerCase();
  }

  // The status line names the open file once the inspector is idle. It used
  // to announce "Loaded group" or "Loaded dataset" after every click, which
  // repeated what the user had just done, and a live region read it aloud.
  function setInspectorIdle() {
    const name = String(state.file || "").split(/[\\/]/).pop();
    setSectionBadgeState(inspectorStateEl, "active", name || t("inspector.tree.loaded"));
  }

  // One part of the summary line: the value on screen, "Shape: 40000" for a
  // screen reader and as the tooltip, since the line has no labels.
  function summaryPart(key, value, className) {
    const part = document.createElement("span");
    part.className = className;
    const spoken = t("inspector.fact", { label: t(key), value });
    part.title = spoken;
    const hidden = document.createElement("span");
    hidden.className = "sr-only";
    hidden.textContent = spoken;
    const shown = document.createElement("span");
    shown.setAttribute("aria-hidden", "true");
    shown.textContent = value;
    part.appendChild(hidden);
    part.appendChild(shown);
    return part;
  }

  /**
   * Type, shape, dtype and units on one line, and a link's target under it.
   * They were a labelled row each, three or four rows of panel height for
   * values that say what they are (`float64` is plainly a dtype); the labels
   * live on in the tooltips.
   */
  function renderInspectorFacts({ type = "", target = "", shape = "", dtype = "", units = "" } = {}) {
    if (!inspectorFacts) return;
    inspectorFacts.innerHTML = "";
    const parts = [
      ["inspector.label.type", type],
      ["inspector.label.shape", shape],
      ["inspector.label.dtype", dtype],
      ["inspector.label.units", units],
    ].filter(([, value]) => value);
    if (parts.length) {
      const line = document.createElement("p");
      line.className = "inspector-summary";
      parts.forEach(([key, value], index) => {
        if (index) {
          const sep = document.createElement("span");
          sep.className = "inspector-summary-sep";
          sep.setAttribute("aria-hidden", "true");
          sep.textContent = "·";
          line.appendChild(sep);
        }
        line.appendChild(summaryPart(key, value, "inspector-summary-part"));
      });
      inspectorFacts.appendChild(line);
    }
    if (target) {
      inspectorFacts.appendChild(summaryPart("inspector.target", target, "inspector-summary-target"));
    }
  }

  function resetInspectorDetails() {
    inspectorDetailsPath = "";
    if (inspectorPath) inspectorPath.innerHTML = "";
    renderInspectorFacts();
    if (inspectorAttrs) inspectorAttrs.innerHTML = "";
    if (inspectorPreview) inspectorPreview.innerHTML = "";
    inspectorDetails?.classList.add("is-empty");
  }

  /**
   * The path as a breadcrumb: each ancestor a button that selects it, so the
   * way back up from a search result or a deep dataset is one click.
   */
  function renderInspectorCrumbs(path) {
    inspectorDetailsPath = path || "";
    inspectorDetails?.classList.remove("is-empty");
    if (!inspectorPath) return;
    inspectorPath.innerHTML = "";
    const parts = String(path || "").split("/").filter(Boolean);
    parts.forEach((part, index) => {
      const sep = document.createElement("span");
      sep.className = "inspector-crumb-sep";
      sep.textContent = "/";
      sep.setAttribute("aria-hidden", "true");
      inspectorPath.appendChild(sep);
      const crumbPath = `/${parts.slice(0, index + 1).join("/")}`;
      if (index === parts.length - 1) {
        const current = document.createElement("strong");
        current.className = "inspector-crumb is-current";
        current.setAttribute("aria-current", "location");
        current.textContent = part;
        inspectorPath.appendChild(current);
        return;
      }
      const crumb = document.createElement("button");
      crumb.type = "button";
      crumb.className = "inspector-crumb";
      crumb.dataset.crumbPath = crumbPath;
      crumb.textContent = part;
      inspectorPath.appendChild(crumb);
    });
  }

  async function copyInspectorPath() {
    if (!inspectorDetailsPath || !inspectorCopyPath) return false;
    const copied = await copyText(inspectorDetailsPath);
    const label = copied ? t("inspector.path_copied") : t("inspector.copy_failed");
    inspectorCopyPath.classList.toggle("is-copied", copied);
    inspectorCopyPath.title = label;
    inspectorCopyPath.setAttribute("aria-label", label);
    setSectionBadgeState(inspectorStateEl, copied ? "active" : "warning", label);
    if (copyFeedbackTimer) window.clearTimeout(copyFeedbackTimer);
    copyFeedbackTimer = window.setTimeout(() => {
      copyFeedbackTimer = null;
      inspectorCopyPath.classList.remove("is-copied");
      inspectorCopyPath.title = t("inspector.copy_path");
      inspectorCopyPath.setAttribute("aria-label", t("inspector.copy_path"));
      setInspectorIdle();
    }, 1500);
    return copied;
  }

  // A breadcrumb's group is always in the tree, since its child was reached
  // through it; a search result's may not be, and then only the details move.
  /**
   * Select `path` in the tree, opening the groups above it on the way. A
   * search result used to load only its details, leaving the tree's
   * highlight on whatever was selected before, so the two disagreed. When the
   * path cannot be reached in the tree (a capped listing, a path through a
   * link), the details still load and the tree selection is cleared.
   */
  async function revealInspectorPath(path, { linkTarget = "" } = {}) {
    const parts = String(path || "").split("/").filter(Boolean);
    let item = null;
    for (let depth = 1; depth <= parts.length; depth += 1) {
      const prefix = `/${parts.slice(0, depth).join("/")}`;
      item = treeItems().find((el) => el.dataset.path === prefix) || null;
      if (!item) break;
      if (depth < parts.length && item.hasAttribute("aria-expanded") && !item.classList.contains("is-open")) {
        await setInspectorGroupOpen(item, true);
      }
    }
    if (!item || item.dataset.path !== path) {
      selectInspectorRow(null);
      await showInspectorNode(path, { linkTarget });
      return;
    }
    focusInspectorItem(item);
    item.scrollIntoView?.({ block: "nearest" });
    await activateInspectorNode(item);
  }

  function localizeInspectorType(type) {
    const normalized = normalizeInspectorType(type);
    if (normalized === "group") return t("inspector.type.group");
    if (normalized === "dataset") return t("inspector.type.dataset");
    if (normalized === "link") return t("inspector.type.link");
    if (!normalized) return t("inspector.node.default_type");
    return String(type);
  }

  function rowMeta(node) {
    return inspectorRowMeta(node, {
      typeLabel: localizeInspectorType(node.type),
      scalarLabel: t("inspector.shape.scalar"),
    });
  }

  // A small drawn mark for the node's kind, so the right-hand column can say
  // something more useful than "group".
  function typeIcon(type) {
    const icon = document.createElement("span");
    icon.className = "inspector-icon";
    icon.dataset.kind = normalizeInspectorType(type) || "node";
    icon.setAttribute("aria-hidden", "true");
    return icon;
  }

  function rowLabel(name, node, metaText) {
    return [name, localizeInspectorType(node.type), metaText].filter(Boolean).join(", ");
  }

  function setInspectorMessage(message, tone = "empty") {
    if (!inspectorTree) return;
    inspectorSelectedRow = null;
    inspectorTree.innerHTML = `<div class="inspector-empty">${message}</div>`;
    resetInspectorDetails();
    setSectionBadgeState(inspectorStateEl, tone, message);
    if (inspectorResults) {
      inspectorResults.innerHTML = "";
      setSearchMode(false);
    }
  }

  function updateInspectorHeaderVisibility(file) {
    const target = file || "";
    const showInspector = Boolean(target && isHdf5File(target));
    const showHeader = Boolean(target && isHeaderCapableFile(target));
    if (inspectorSection) inspectorSection.classList.toggle("is-hidden", !showInspector);
    if (imageHeaderSection) imageHeaderSection.classList.toggle("is-hidden", !showHeader);
    if (showInspector) {
      clearImageHeader();
      if (!inspectorTree || !inspectorTree.children.length) {
        setInspectorMessage(t("inspector.select_hdf5"), "empty");
      }
    } else {
      if (inspectorSection) setInspectorMessage(t("inspector.hdf5_only"), "empty");
      if (showHeader) {
        loadImageHeader(target);
      } else {
        clearImageHeader();
      }
    }
  }

  /**
   * While a query has results on show, they take the tree's place: results,
   * tree and preview used to be three small scrolling boxes stacked in the
   * panel. Picking a result hands the space back to the tree, with the entry
   * selected; focusing the search field again brings the results back.
   */
  function setSearchMode(on) {
    const show = Boolean(on) && Boolean(inspectorResults?.childElementCount);
    inspectorSection?.classList.toggle("is-searching", show);
    inspectorResults?.classList.toggle("is-hidden", !show);
  }

  function clearInspectorSearch() {
    if (inspectorSearchInput) inspectorSearchInput.value = "";
    if (inspectorResults) inspectorResults.innerHTML = "";
    setSearchMode(false);
  }

  function highlighted(text, query, className) {
    const el = document.createElement("span");
    el.className = className;
    splitMatches(text, query).forEach((piece) => {
      if (!piece.match) {
        el.appendChild(document.createTextNode(piece.text));
        return;
      }
      const mark = document.createElement("mark");
      mark.textContent = piece.text;
      el.appendChild(mark);
    });
    return el;
  }

  function renderInspectorResults(results, query) {
    if (!inspectorResults) return;
    inspectorResults.innerHTML = "";
    if (!query) {
      setSearchMode(false);
      setInspectorIdle();
      return;
    }
    if (!Array.isArray(results) || results.length === 0) {
      inspectorResults.innerHTML = `<div class=\"inspector-empty\">${t("inspector.search.no_matches")}</div>`;
      setSectionBadgeState(inspectorStateEl, "empty", t("inspector.search.no_matches_for", { query }));
      setSearchMode(true);
      return;
    }
    setSectionBadgeState(
      inspectorStateEl,
      "active",
      t("inspector.search.found_count", { count: results.length, query }),
    );
    results.forEach((item) => {
      const row = document.createElement("div");
      row.className = "inspector-result";
      row.dataset.path = item.path || "";
      row.dataset.type = item.type || "";
      row.tabIndex = 0;
      row.setAttribute("role", "button");
      const itemType = normalizeInspectorType(item.type);
      if (itemType === "link" && item.target) {
        row.dataset.target = item.target;
      }
      // Name first, where the eye starts, and the group it lives in after it:
      // a full path put every result's distinguishing part at the far end.
      const { name: nodeName, parent } = splitInspectorPath(item.path || item.name || "");
      const label = document.createElement("span");
      label.className = "inspector-result-label";
      label.appendChild(highlighted(nodeName, query, "inspector-result-name"));
      label.appendChild(highlighted(parent, query, "inspector-result-parent"));
      const meta = document.createElement("span");
      meta.className = "inspector-result-meta";
      const { text: metaText, title: metaTitle } = rowMeta(item);
      meta.textContent = metaText;
      row.title = [item.path, metaTitle].filter(Boolean).join("\n");
      row.setAttribute("aria-label", rowLabel(item.path || nodeName, item, metaText));
      row.appendChild(typeIcon(item.type));
      row.appendChild(label);
      row.appendChild(meta);
      inspectorResults.appendChild(row);
    });
    setSearchMode(true);
  }

  async function pickSearchResult(row) {
    if (!row?.dataset.path) return;
    const linkTarget = normalizeInspectorType(row.dataset.type) === "link" ? row.dataset.target || "-" : "";
    setSearchMode(false);
    await revealInspectorPath(row.dataset.path, { linkTarget });
  }

  function resultRows() {
    return inspectorResults ? [...inspectorResults.querySelectorAll(".inspector-result")] : [];
  }

  // Escape leaves the search wherever focus is in it; with nothing typed it
  // is left to the panel (and to whatever closes on Escape).
  function leaveSearch(event) {
    if (!inspectorSearchInput?.value) return false;
    event.preventDefault();
    event.stopPropagation();
    clearInspectorSearch();
    runInspectorSearch("");
    inspectorSearchInput.focus();
    return true;
  }

  function handleSearchInputKey(event) {
    if (event.key === "Escape") {
      leaveSearch(event);
      return;
    }
    if (event.key === "ArrowDown") {
      const first = resultRows()[0];
      if (!first) return;
      event.preventDefault();
      setSearchMode(true);
      first.focus();
    }
  }

  function handleResultsKey(event) {
    const rows = resultRows();
    const index = rows.indexOf(event.target?.closest?.(".inspector-result"));
    if (index < 0) return;
    let next = null;
    if (event.key === "Escape") {
      leaveSearch(event);
      return;
    }
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      void pickSearchResult(rows[index]);
      return;
    }
    if (event.key === "ArrowDown") next = rows[index + 1];
    else if (event.key === "ArrowUp") next = index === 0 ? inspectorSearchInput : rows[index - 1];
    else if (event.key === "Home") next = rows[0];
    else if (event.key === "End") next = rows[rows.length - 1];
    else return;
    event.preventDefault();
    next?.focus();
  }

  function showSearchResultsAgain() {
    if (inspectorSearchInput?.value.trim()) setSearchMode(true);
  }

  async function runInspectorSearch(query) {
    const requestId = ++inspectorSearchRequestId;
    if (!isHdf5File(state.file)) {
      setSectionBadgeState(inspectorStateEl, "empty", t("inspector.hdf5_only"));
      if (inspectorResults) {
        inspectorResults.innerHTML = "";
        setSearchMode(false);
      }
      return;
    }
    if (!query) {
      renderInspectorResults([], "");
      return;
    }
    setSectionBadgeState(inspectorStateEl, "loading", t("inspector.search.loading_for", { query }));
    try {
      const data = await fetchJSON(
        `${apiBase}/hdf5/search?file=${encodeURIComponent(state.file)}&query=${encodeURIComponent(query)}`,
      );
      if (requestId !== inspectorSearchRequestId) return;
      renderInspectorResults(data.matches || [], query);
    } catch (err) {
      if (requestId !== inspectorSearchRequestId) return;
      console.error(err);
      setSectionBadgeState(inspectorStateEl, "warning", t("inspector.search.failed"));
      if (inspectorResults) {
        inspectorResults.innerHTML = `<div class=\"inspector-empty\">${t("inspector.search.failed_short")}</div>`;
        setSearchMode(true);
      }
    }
  }

  function renderInspectorLink(path, target) {
    renderInspectorCrumbs(path);
    renderInspectorFacts({ type: t("inspector.type.link"), target });
    if (inspectorAttrs) inspectorAttrs.innerHTML = "";
    if (inspectorPreview) inspectorPreview.innerHTML = "";
    setInspectorIdle();
  }

  function buildInspectorRow(node, level = 1) {
    const li = document.createElement("li");
    li.className = "inspector-node";
    li.dataset.path = node.path || "";
    li.dataset.type = node.type || "";
    const nodeType = normalizeInspectorType(node.type);
    if (nodeType === "truncated") {
      // A note, not a node: no path to select, nothing to expand, and not an
      // item the keyboard stops on.
      li.setAttribute("role", "none");
      li.classList.add("is-note");
      li.dataset.path = "";
      const note = document.createElement("div");
      note.className = "inspector-row inspector-row-note";
      note.textContent = node.name || "";
      li.appendChild(note);
      return li;
    }
    if (nodeType === "link" && node.target) {
      li.dataset.target = node.target;
    }
    // The item, not its toggle, takes focus and carries the state, as the
    // ARIA tree pattern has it; the toggle stays a mouse target only.
    li.setAttribute("role", "treeitem");
    li.setAttribute("aria-level", String(level));
    li.setAttribute("aria-selected", "false");
    li.tabIndex = -1;
    const row = document.createElement("div");
    row.className = "inspector-row";

    const toggle = document.createElement("button");
    toggle.type = "button";
    toggle.className = "inspector-toggle";
    toggle.tabIndex = -1;
    toggle.setAttribute("aria-hidden", "true");
    if (nodeType === "group" && node.hasChildren) {
      toggle.textContent = "▸";
      li.setAttribute("aria-expanded", "false");
    } else {
      // A placeholder that keeps its width, not `.is-hidden`: that is
      // `display: none`, which pulled every leaf's name left of its siblings'.
      toggle.textContent = "";
      toggle.classList.add("is-placeholder");
    }

    const name = document.createElement("span");
    name.className = "inspector-name";
    name.textContent = node.name || node.path || "/";

    const meta = document.createElement("span");
    meta.className = "inspector-meta";
    const { text: metaText, title: metaTitle } = rowMeta(node);
    meta.textContent = metaText;
    if (metaTitle) row.title = metaTitle;

    row.appendChild(toggle);
    row.appendChild(typeIcon(node.type));
    row.appendChild(name);
    row.appendChild(meta);
    li.appendChild(row);
    li.setAttribute("aria-label", rowLabel(name.textContent, node, metaText));

    if (nodeType === "group") {
      const children = document.createElement("ul");
      children.className = "inspector-children";
      children.setAttribute("role", "group");
      li.appendChild(children);
    }

    return li;
  }

  function renderInspectorTree(nodes, container) {
    if (!container) return;
    container.innerHTML = "";
    const isChildren = container.classList.contains("inspector-children");
    const target = isChildren ? container : document.createElement("ul");
    if (!isChildren) {
      target.className = "inspector-node";
      target.setAttribute("role", "tree");
      target.setAttribute("aria-label", t("inspector.title"));
    }
    const parentItem = isChildren ? container.closest('[role="treeitem"]') : null;
    const level = parentItem ? Number(parentItem.getAttribute("aria-level") || 0) + 1 : 1;
    nodes.forEach((node) => {
      target.appendChild(buildInspectorRow(node, level));
    });
    if (target !== container) {
      container.appendChild(target);
    }
    ensureInspectorTabStop();
  }

  function treeItems() {
    return inspectorTree ? [...inspectorTree.querySelectorAll('[role="treeitem"]')] : [];
  }

  // Items inside a closed group are in the DOM but not on screen.
  function visibleTreeItems() {
    return treeItems().filter(
      (item) => !item.parentElement?.closest('[role="treeitem"]:not(.is-open)'),
    );
  }

  // One item in the tree is reachable with Tab (roving tabindex); arrows move
  // between the rest.
  function setInspectorTabStop(item) {
    treeItems().forEach((other) => {
      other.tabIndex = other === item ? 0 : -1;
    });
  }

  function ensureInspectorTabStop() {
    const visible = visibleTreeItems();
    if (!visible.some((item) => item.tabIndex === 0) && visible.length) {
      setInspectorTabStop(visible.find((item) => item.getAttribute("aria-selected") === "true") || visible[0]);
    }
  }

  function noteInspectorFocus(target) {
    const item = target?.closest?.('[role="treeitem"]');
    if (item && inspectorTree?.contains(item)) setInspectorTabStop(item);
  }

  function focusInspectorItem(item) {
    if (!item) return;
    setInspectorTabStop(item);
    item.focus();
  }

  async function setInspectorGroupOpen(item, open) {
    if (!item || !item.hasAttribute("aria-expanded")) return;
    item.classList.toggle("is-open", open);
    item.setAttribute("aria-expanded", String(open));
    const toggle = item.querySelector(":scope > .inspector-row > .inspector-toggle");
    if (toggle) toggle.textContent = open ? "▾" : "▸";
    if (!open) {
      // Focus inside a group that closes would be left on a hidden item.
      if (item.querySelector(":scope .inspector-children [tabindex='0']")) setInspectorTabStop(item);
      return;
    }
    if (item.dataset.loaded === "true") return;
    const container = item.querySelector(":scope > .inspector-children");
    try {
      setSectionBadgeState(inspectorStateEl, "loading", t("inspector.tree.loading_children"));
      renderSkeletonBlock(container, 4);
      const children = await fetchInspectorTree(item.dataset.path || "");
      renderInspectorTree(children, container);
      item.dataset.loaded = "true";
      setInspectorIdle();
    } catch (err) {
      console.error(err);
      setSectionBadgeState(inspectorStateEl, "warning", t("inspector.tree.failed_children"));
    }
  }

  /**
   * A click or Enter on an item. The toggle only opens and closes. The row
   * selects and shows the details, and opens a closed group; a second click on
   * the selected, open group closes it. Clicking a group used to toggle it
   * every time, so reading an open group's attributes meant collapsing it.
   */
  async function activateInspectorNode(item, { toggleOnly = false } = {}) {
    if (!item || item.getAttribute("role") !== "treeitem") return;
    const nodeType = normalizeInspectorType(item.dataset.type);
    const isExpandable = item.hasAttribute("aria-expanded");
    const isOpen = item.classList.contains("is-open");
    if (toggleOnly) {
      await setInspectorGroupOpen(item, !isOpen);
      return;
    }
    const wasSelected = item.getAttribute("aria-selected") === "true";
    selectInspectorRow(item.querySelector(":scope > .inspector-row"));
    setInspectorTabStop(item);
    if (isExpandable) {
      void setInspectorGroupOpen(item, !(wasSelected && isOpen));
    }
    if (!item.dataset.path) return;
    if (nodeType === "link") {
      const data = await showInspectorNode(item.dataset.path, { linkTarget: item.dataset.target || "-" });
      if (data && normalizeInspectorType(data.type) === "group" && !isExpandable) {
        makeExpandable(item);
        void setInspectorGroupOpen(item, true);
      }
      return;
    }
    await showInspectorNode(item.dataset.path);
  }

  function handleInspectorTreeKey(event) {
    const item = event.target?.closest?.('[role="treeitem"]');
    if (!item) return;
    const items = visibleTreeItems();
    const index = items.indexOf(item);
    const plan = treeKeyAction(
      items.map((el) => ({
        level: Number(el.getAttribute("aria-level") || 1),
        expandable: el.hasAttribute("aria-expanded"),
        expanded: el.getAttribute("aria-expanded") === "true",
      })),
      index,
      event.key,
    );
    if (!plan) return;
    event.preventDefault();
    const target = items[plan.index];
    if (plan.action === "focus") focusInspectorItem(target);
    else if (plan.action === "expand") void setInspectorGroupOpen(target, true);
    else if (plan.action === "collapse") void setInspectorGroupOpen(target, false);
    else if (plan.action === "activate") void activateInspectorNode(target);
  }

  async function fetchInspectorTree(path = "/") {
    const res = await fetchJSON(
      `${apiBase}/hdf5/tree?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(path)}`,
    );
    const children = res.children || [];
    if (!res.truncated) return children;
    // The listing is capped server-side: a master written one file per frame can
    // link millions of nodes into one group. Carried as a trailing note row so
    // every caller that renders children shows it without knowing about it, and
    // so the count the user sees is the group's real size, not what arrived.
    return children.concat({
      name: t("inspector.tree.truncated", {
        shown: children.length.toLocaleString(),
        total: Number(res.childCount || 0).toLocaleString(),
      }),
      path,
      type: "truncated",
    });
  }

  async function loadInspectorRoot() {
    if (!inspectorTree) return;
    clearInspectorSearch();
    if (!isHdf5File(state.file)) {
      setInspectorMessage(t("inspector.hdf5_only"), "empty");
      return;
    }
    setSectionBadgeState(inspectorStateEl, "loading", t("inspector.tree.loading"));
    renderSkeletonBlock(inspectorTree, 7);
    resetInspectorDetails();
    try {
      const children = await fetchInspectorTree("/");
      renderInspectorTree(children, inspectorTree);
      inspectorSelectedRow = null;
      resetInspectorDetails();
      if (children.length) {
        setInspectorIdle();
      } else {
        setSectionBadgeState(inspectorStateEl, "empty", t("inspector.tree.no_nodes"));
      }
    } catch (err) {
      console.error(err);
      setInspectorMessage(t("inspector.tree.failed"), "warning");
    }
  }

  function selectInspectorRow(row) {
    if (inspectorSelectedRow) {
      inspectorSelectedRow.classList.remove("is-selected");
      inspectorSelectedRow.parentElement?.setAttribute("aria-selected", "false");
    }
    inspectorSelectedRow = row;
    if (inspectorSelectedRow) {
      inspectorSelectedRow.classList.add("is-selected");
      inspectorSelectedRow.parentElement?.setAttribute("aria-selected", "true");
    }
  }

  function appendAttrRow(name, value) {
    if (!inspectorAttrs) return;
    const row = document.createElement("div");
    row.className = "inspector-attr-row";
    const nameEl = document.createElement("span");
    nameEl.textContent = name;
    const valueEl = document.createElement("span");
    valueEl.textContent = value;
    row.appendChild(nameEl);
    row.appendChild(valueEl);
    inspectorAttrs.appendChild(row);
  }

  /**
   * Fill the details for `path` and return the node, or null if it could not
   * be read. With `linkTarget`, `path` is a link: the backend follows it (with
   * the same confinement as the frame path), and the details show what it
   * points to, with the target on top. A link that does not resolve keeps its
   * target on show; it used to be all a link ever showed.
   *
   * A failure leaves the tree alone. It used to replace the whole tree with
   * an error message, so one dangling link lost the user their place.
   */
  async function showInspectorNode(path, { linkTarget = "" } = {}) {
    if (!inspectorDetails) return null;
    const requestId = ++inspectorNodeRequestId;
    setSectionBadgeState(inspectorStateEl, "loading", t("inspector.node.loading"));
    try {
      const data = await fetchJSON(
        `${apiBase}/hdf5/node?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(path)}`,
      );
      if (requestId !== inspectorNodeRequestId) return null;
      const typeLabel = localizeInspectorType(data.type);
      renderInspectorCrumbs(data.path || path);
      const unitsAttr = Array.isArray(data.attrs) ? data.attrs.find((attr) => attr.name === "units") : null;
      renderInspectorFacts({
        type: linkTarget ? t("inspector.type.link_to", { type: typeLabel }) : typeLabel,
        target: linkTarget,
        shape: formatInspectorShape(data.shape, t("inspector.shape.scalar")),
        dtype: data.dtype || "",
        units: typeof unitsAttr?.value === "string" ? unitsAttr.value.trim() : "",
      });
      if (inspectorAttrs) {
        inspectorAttrs.innerHTML = "";
        if (Array.isArray(data.attrs) && data.attrs.length) {
          const heading = document.createElement("div");
          heading.className = "inspector-subhead";
          heading.textContent = t("inspector.attributes");
          inspectorAttrs.appendChild(heading);
          data.attrs.forEach((attr) => appendAttrRow(attr.name, formatInspectorValue(attr.value)));
        }
      }
      const nodeType = normalizeInspectorType(data.type);
      if (nodeType === "dataset") {
        try {
          const valueData = await fetchJSON(
            `${apiBase}/hdf5/value?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(path)}`,
          );
          if (requestId !== inspectorNodeRequestId) return null;
          renderInspectorPreview(valueData);
        } catch (err) {
          console.error(err);
          if (inspectorPreview) inspectorPreview.innerHTML = "";
        }
      } else if (inspectorPreview) {
        inspectorPreview.innerHTML = "";
      }
      setInspectorIdle();
      return data;
    } catch (err) {
      if (requestId !== inspectorNodeRequestId) return null;
      console.error(err);
      if (linkTarget) {
        renderInspectorLink(path, linkTarget);
        setSectionBadgeState(inspectorStateEl, "warning", t("inspector.link.unresolved"));
      } else {
        resetInspectorDetails();
        setSectionBadgeState(inspectorStateEl, "warning", t("inspector.node.failed"));
      }
      return null;
    }
  }

  // A link found to point at a group becomes expandable like one; the tree
  // route follows the link the same way the node route does.
  function makeExpandable(item) {
    if (item.hasAttribute("aria-expanded")) return;
    item.setAttribute("aria-expanded", "false");
    const toggle = item.querySelector(":scope > .inspector-row > .inspector-toggle");
    if (toggle) {
      toggle.classList.remove("is-placeholder");
      toggle.textContent = "▸";
    }
    if (!item.querySelector(":scope > .inspector-children")) {
      const children = document.createElement("ul");
      children.className = "inspector-children";
      children.setAttribute("role", "group");
      item.appendChild(children);
    }
  }

  return {
    clearImageHeader,
    loadImageHeader,
    updateInspectorHeaderVisibility,
    clearInspectorSearch,
    runInspectorSearch,
    renderInspectorLink,
    renderInspectorTree,
    fetchInspectorTree,
    loadInspectorRoot,
    selectInspectorRow,
    pickSearchResult,
    handleSearchInputKey,
    handleResultsKey,
    showSearchResultsAgain,
    showInspectorNode,
    activateInspectorNode,
    setInspectorGroupOpen,
    copyInspectorPath,
    revealInspectorPath,
    showDatasetInViewer,
    refreshViewerAction,
    handleInspectorTreeKey,
    noteInspectorFocus,
  };
}
