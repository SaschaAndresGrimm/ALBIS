/**
 * Inspector tree/search and image-header panel orchestration.
 */

import { t } from "./i18n.js";
import { formatInspectorShape, inspectorRowMeta } from "./inspector_format_utils.js";
import { treeKeyAction } from "./inspector_tree_nav_utils.js";

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
    inspectorType,
    inspectorShape,
    inspectorDtype,
    inspectorAttrs,
    inspectorPreview,
    inspectorDetails,
    inspectorSection,
    imageHeaderSection,
    imageHeaderStateEl,
    imageHeaderLoading,
    imageHeaderText,
    imageHeaderEmpty,
  } = elements;

  const {
    fetchJSON,
    isHdf5File,
    isHeaderCapableFile,
    setSectionBadgeState,
    renderSkeletonBlock,
    formatInspectorValue,
    resetInspectorDetails,
  } = callbacks;

  let inspectorSelectedRow = null;
  let inspectorSearchRequestId = 0;
  let inspectorNodeRequestId = 0;

  function normalizeInspectorType(type) {
    return String(type || "").trim().toLowerCase();
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

  function setInspectorMessage(message, tone = "empty") {
    if (!inspectorTree) return;
    inspectorSelectedRow = null;
    inspectorTree.innerHTML = `<div class="inspector-empty">${message}</div>`;
    resetInspectorDetails();
    setSectionBadgeState(inspectorStateEl, tone, message);
    if (inspectorResults) {
      inspectorResults.innerHTML = "";
      inspectorResults.classList.add("is-hidden");
    }
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

  function clearInspectorSearch() {
    if (inspectorSearchInput) inspectorSearchInput.value = "";
    if (inspectorResults) {
      inspectorResults.innerHTML = "";
      inspectorResults.classList.add("is-hidden");
    }
  }

  function renderInspectorResults(results, query) {
    if (!inspectorResults) return;
    if (!query) {
      inspectorResults.innerHTML = "";
      inspectorResults.classList.add("is-hidden");
      setSectionBadgeState(inspectorStateEl, "active", t("inspector.browser_ready"));
      return;
    }
    inspectorResults.classList.remove("is-hidden");
    if (!Array.isArray(results) || results.length === 0) {
      inspectorResults.innerHTML = `<div class=\"inspector-empty\">${t("inspector.search.no_matches")}</div>`;
      setSectionBadgeState(inspectorStateEl, "empty", t("inspector.search.no_matches_for", { query }));
      return;
    }
    setSectionBadgeState(
      inspectorStateEl,
      "active",
      t("inspector.search.found_count", { count: results.length, query }),
    );
    inspectorResults.innerHTML = "";
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
      const name = document.createElement("span");
      name.className = "inspector-result-name";
      name.textContent = item.path || item.name || "";
      const meta = document.createElement("span");
      meta.className = "inspector-result-meta";
      const { text: metaText, title: metaTitle } = rowMeta(item);
      meta.textContent = metaText;
      if (metaTitle) row.title = metaTitle;
      const resultName = String(name.textContent || "").trim();
      row.setAttribute("aria-label", metaText ? `${resultName}, ${metaText}` : resultName);
      row.appendChild(name);
      row.appendChild(meta);
      inspectorResults.appendChild(row);
    });
  }

  async function runInspectorSearch(query) {
    const requestId = ++inspectorSearchRequestId;
    if (!isHdf5File(state.file)) {
      setSectionBadgeState(inspectorStateEl, "empty", t("inspector.hdf5_only"));
      if (inspectorResults) {
        inspectorResults.innerHTML = "";
        inspectorResults.classList.add("is-hidden");
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
        inspectorResults.classList.remove("is-hidden");
        inspectorResults.innerHTML = `<div class=\"inspector-empty\">${t("inspector.search.failed_short")}</div>`;
      }
    }
  }

  function renderInspectorLink(path, target) {
    if (inspectorPath) inspectorPath.textContent = path || "-";
    if (inspectorType) inspectorType.textContent = t("inspector.type.link");
    if (inspectorShape) inspectorShape.textContent = "-";
    if (inspectorDtype) inspectorDtype.textContent = "-";
    if (inspectorAttrs) {
      inspectorAttrs.innerHTML = "";
      const attrRow = document.createElement("div");
      attrRow.className = "inspector-attr-row";
      const name = document.createElement("span");
      name.textContent = t("inspector.target");
      const value = document.createElement("span");
      value.textContent = target || "-";
      attrRow.appendChild(name);
      attrRow.appendChild(value);
      inspectorAttrs.appendChild(attrRow);
    }
    if (inspectorPreview) inspectorPreview.innerHTML = "";
    setSectionBadgeState(inspectorStateEl, "active", t("inspector.link_loaded"));
  }

  function formatInspectorCell(value) {
    if (value === null || value === undefined) return "-";
    if (typeof value === "number") {
      return Number.isFinite(value) ? String(value) : "-";
    }
    return String(value);
  }

  function buildInspectorTable1D(values) {
    const table = document.createElement("table");
    table.className = "inspector-table";
    const head = document.createElement("thead");
    const headRow = document.createElement("tr");
    [t("inspector.table.index"), t("inspector.table.value")].forEach((label) => {
      const th = document.createElement("th");
      th.textContent = label;
      headRow.appendChild(th);
    });
    head.appendChild(headRow);
    table.appendChild(head);
    const body = document.createElement("tbody");
    values.forEach((value, idx) => {
      const row = document.createElement("tr");
      const indexCell = document.createElement("td");
      indexCell.textContent = String(idx);
      const valueCell = document.createElement("td");
      valueCell.textContent = formatInspectorCell(value);
      row.appendChild(indexCell);
      row.appendChild(valueCell);
      body.appendChild(row);
    });
    table.appendChild(body);
    return table;
  }

  function buildInspectorTable2D(values) {
    const table = document.createElement("table");
    table.className = "inspector-table";
    const head = document.createElement("thead");
    const headRow = document.createElement("tr");
    const corner = document.createElement("th");
    corner.textContent = "";
    headRow.appendChild(corner);
    const cols = values.length ? values[0].length : 0;
    for (let c = 0; c < cols; c += 1) {
      const th = document.createElement("th");
      th.textContent = String(c);
      headRow.appendChild(th);
    }
    head.appendChild(headRow);
    table.appendChild(head);
    const body = document.createElement("tbody");
    values.forEach((rowValues, r) => {
      const row = document.createElement("tr");
      const indexCell = document.createElement("td");
      indexCell.textContent = String(r);
      row.appendChild(indexCell);
      rowValues.forEach((value) => {
        const cell = document.createElement("td");
        cell.textContent = formatInspectorCell(value);
        row.appendChild(cell);
      });
      body.appendChild(row);
    });
    table.appendChild(body);
    return table;
  }

  function renderInspectorPreview(data) {
    if (!inspectorPreview) return;
    inspectorPreview.innerHTML = "";
    if (!data || data.preview === null || data.preview === undefined) {
      return;
    }
    const actions = document.createElement("div");
    actions.className = "inspector-preview-actions";
    const link = document.createElement("a");
    link.href = `${apiBase}/hdf5/value?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(
      data.path || "",
    )}&max_cells=65536`;
    link.target = "_blank";
    link.rel = "noopener";
    link.textContent = t("inspector.open_new_tab");
    actions.appendChild(link);
    if (Array.isArray(data.shape) && data.shape.length > 0) {
      const csvLink = document.createElement("a");
      csvLink.href = `${apiBase}/hdf5/csv?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(
        data.path || "",
      )}&max_cells=65536`;
      csvLink.textContent = t("inspector.download_csv");
      actions.appendChild(csvLink);
    }
    inspectorPreview.appendChild(actions);

    const preview = data.preview;
    if (Array.isArray(preview)) {
      const table = Array.isArray(preview[0]) ? buildInspectorTable2D(preview) : buildInspectorTable1D(preview);
      inspectorPreview.appendChild(table);
    } else {
      const text = document.createElement("div");
      text.textContent = formatInspectorValue(preview);
      inspectorPreview.appendChild(text);
    }

    if (data.preview_shape) {
      const note = document.createElement("div");
      note.className = "inspector-preview-note";
      const shapeText = data.preview_shape.join("×");
      note.textContent = t("inspector.preview", {
        shapeText,
        suffix: data.truncated ? t("inspector.preview.truncated_suffix") : "",
      });
      if (data.slice && Array.isArray(data.slice.lead) && data.slice.lead.length) {
        note.textContent += ` • ${t("inspector.preview.slice")} [${data.slice.lead.join(", ")}]`;
      }
      inspectorPreview.appendChild(note);
    }
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
    row.appendChild(name);
    row.appendChild(meta);
    li.appendChild(row);
    li.setAttribute("aria-label", metaText ? `${name.textContent}, ${metaText}` : name.textContent);

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
      setSectionBadgeState(inspectorStateEl, "active", t("inspector.tree.loaded"));
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
        setSectionBadgeState(inspectorStateEl, "active", t("inspector.tree.loaded"));
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
      if (inspectorPath) inspectorPath.textContent = data.path || path;
      if (inspectorType) {
        inspectorType.textContent = linkTarget ? t("inspector.type.link_to", { type: typeLabel }) : typeLabel;
      }
      if (inspectorShape) inspectorShape.textContent =
          formatInspectorShape(data.shape, t("inspector.shape.scalar")) || "-";
      if (inspectorDtype) inspectorDtype.textContent = data.dtype || "-";
      if (inspectorAttrs) {
        inspectorAttrs.innerHTML = "";
        if (linkTarget) appendAttrRow(t("inspector.target"), linkTarget);
        if (Array.isArray(data.attrs)) {
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
      setSectionBadgeState(inspectorStateEl, "active", t("inspector.node.loaded", { type: typeLabel }));
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
    showInspectorNode,
    activateInspectorNode,
    setInspectorGroupOpen,
    handleInspectorTreeKey,
    noteInspectorFocus,
  };
}
