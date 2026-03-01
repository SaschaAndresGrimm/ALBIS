/**
 * Inspector tree/search and image-header panel orchestration.
 */

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

  function setInspectorMessage(message) {
    if (!inspectorTree) return;
    inspectorSelectedRow = null;
    inspectorTree.innerHTML = `<div class="inspector-empty">${message}</div>`;
    resetInspectorDetails();
    setSectionBadgeState(inspectorStateEl, /fail|error/i.test(message) ? "warning" : "empty", message);
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
      setImageHeaderSectionState("active", "Header loaded.");
    } else {
      setImageHeaderSectionState("empty", "No header available.");
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
    setImageHeaderSectionState("loading", "Loading image header…");
    setImageHeaderLoading(true);
    if (imageHeaderText) {
      imageHeaderText.textContent = "";
      imageHeaderText.classList.add("is-hidden");
    }
    if (imageHeaderEmpty) {
      imageHeaderEmpty.classList.add("is-hidden");
    }
    try {
      const data = await fetchJSON(`${apiBase}/image/header?file=${encodeURIComponent(file)}`);
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
        imageHeaderEmpty.textContent = "No header available.";
      }
      setImageHeaderSectionState("warning", "Header unavailable for this image.");
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
        setInspectorMessage("Select an HDF5 file to browse metadata.");
      }
    } else {
      if (inspectorSection) setInspectorMessage("File inspector is available for HDF5 files only.");
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
      setSectionBadgeState(inspectorStateEl, "active", "Metadata browser ready.");
      return;
    }
    inspectorResults.classList.remove("is-hidden");
    if (!Array.isArray(results) || results.length === 0) {
      inspectorResults.innerHTML = `<div class="inspector-empty">No matches.</div>`;
      setSectionBadgeState(inspectorStateEl, "empty", `No matches for "${query}".`);
      return;
    }
    setSectionBadgeState(
      inspectorStateEl,
      "active",
      `Found ${results.length} match${results.length === 1 ? "" : "es"} for "${query}".`,
    );
    inspectorResults.innerHTML = "";
    results.forEach((item) => {
      const row = document.createElement("div");
      row.className = "inspector-result";
      row.dataset.path = item.path || "";
      row.dataset.type = item.type || "";
      row.tabIndex = 0;
      row.setAttribute("role", "button");
      if (item.type === "link" && item.target) {
        row.dataset.target = item.target;
      }
      const name = document.createElement("span");
      name.className = "inspector-result-name";
      name.textContent = item.path || item.name || "";
      const meta = document.createElement("span");
      meta.className = "inspector-result-meta";
      let metaText = "";
      if (item.type === "dataset" && item.shape && item.dtype) {
        metaText = `${item.shape.join("×")} ${item.dtype}`;
      } else if (item.type === "link" && item.target) {
        metaText = item.target;
      } else {
        metaText = item.type || "";
      }
      meta.textContent = metaText;
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
      setSectionBadgeState(inspectorStateEl, "empty", "File inspector is available for HDF5 files only.");
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
    setSectionBadgeState(inspectorStateEl, "loading", `Searching metadata for "${query}"…`);
    try {
      const data = await fetchJSON(
        `${apiBase}/hdf5/search?file=${encodeURIComponent(state.file)}&query=${encodeURIComponent(query)}`,
      );
      if (requestId !== inspectorSearchRequestId) return;
      renderInspectorResults(data.matches || [], query);
    } catch (err) {
      if (requestId !== inspectorSearchRequestId) return;
      console.error(err);
      setSectionBadgeState(inspectorStateEl, "warning", "Search failed. Please try again.");
      if (inspectorResults) {
        inspectorResults.classList.remove("is-hidden");
        inspectorResults.innerHTML = `<div class="inspector-empty">Search failed.</div>`;
      }
    }
  }

  function renderInspectorLink(path, target) {
    if (inspectorPath) inspectorPath.textContent = path || "-";
    if (inspectorType) inspectorType.textContent = "link";
    if (inspectorShape) inspectorShape.textContent = "-";
    if (inspectorDtype) inspectorDtype.textContent = "-";
    if (inspectorAttrs) {
      inspectorAttrs.innerHTML = "";
      const attrRow = document.createElement("div");
      attrRow.className = "inspector-attr-row";
      const name = document.createElement("span");
      name.textContent = "Target";
      const value = document.createElement("span");
      value.textContent = target || "-";
      attrRow.appendChild(name);
      attrRow.appendChild(value);
      inspectorAttrs.appendChild(attrRow);
    }
    if (inspectorPreview) inspectorPreview.innerHTML = "";
    setSectionBadgeState(inspectorStateEl, "active", "Link target loaded.");
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
    ["Index", "Value"].forEach((label) => {
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
    link.textContent = "Open in new tab";
    actions.appendChild(link);
    if (Array.isArray(data.shape) && data.shape.length > 0) {
      const csvLink = document.createElement("a");
      csvLink.href = `${apiBase}/hdf5/csv?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(
        data.path || "",
      )}&max_cells=65536`;
      csvLink.textContent = "Download CSV";
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
      note.textContent = `Preview ${shapeText}${data.truncated ? " (truncated)" : ""}`;
      if (data.slice && Array.isArray(data.slice.lead) && data.slice.lead.length) {
        note.textContent += ` • Slice [${data.slice.lead.join(", ")}]`;
      }
      inspectorPreview.appendChild(note);
    }
  }

  function buildInspectorRow(node) {
    const li = document.createElement("li");
    li.className = "inspector-node";
    li.dataset.path = node.path || "";
    li.dataset.type = node.type || "";
    if (node.type === "link" && node.target) {
      li.dataset.target = node.target;
    }
    const row = document.createElement("div");
    row.className = "inspector-row";

    const toggle = document.createElement("button");
    toggle.className = "inspector-toggle";
    if (node.type === "group" && node.hasChildren) {
      toggle.textContent = "▸";
    } else {
      toggle.textContent = "";
      toggle.classList.add("is-hidden");
    }

    const name = document.createElement("span");
    name.textContent = node.name || node.path || "/";

    const meta = document.createElement("span");
    meta.className = "inspector-meta";
    if (node.type === "dataset" && node.shape && node.dtype) {
      meta.textContent = `${node.shape.join("×")} ${node.dtype}`;
    } else if (node.type === "link" && node.target) {
      meta.textContent = node.target;
    } else if (node.type === "group") {
      meta.textContent = "Group";
    } else {
      meta.textContent = node.type || "";
    }

    row.appendChild(toggle);
    row.appendChild(name);
    row.appendChild(meta);
    li.appendChild(row);

    if (node.type === "group") {
      const children = document.createElement("ul");
      children.className = "inspector-children";
      li.appendChild(children);
    }

    return li;
  }

  function renderInspectorTree(nodes, container) {
    if (!container) return;
    container.innerHTML = "";
    const target =
      container.classList.contains("inspector-children") ? container : document.createElement("ul");
    if (!container.classList.contains("inspector-children")) {
      target.className = "inspector-node";
    }
    nodes.forEach((node) => {
      target.appendChild(buildInspectorRow(node));
    });
    if (target !== container) {
      container.appendChild(target);
    }
  }

  async function fetchInspectorTree(path = "/") {
    const res = await fetchJSON(
      `${apiBase}/hdf5/tree?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(path)}`,
    );
    return res.children || [];
  }

  async function loadInspectorRoot() {
    if (!inspectorTree) return;
    clearInspectorSearch();
    if (!isHdf5File(state.file)) {
      setInspectorMessage("File inspector is available for HDF5 files only.");
      return;
    }
    setSectionBadgeState(inspectorStateEl, "loading", "Loading metadata tree…");
    renderSkeletonBlock(inspectorTree, 7);
    resetInspectorDetails();
    try {
      const children = await fetchInspectorTree("/");
      renderInspectorTree(children, inspectorTree);
      inspectorSelectedRow = null;
      resetInspectorDetails();
      if (children.length) {
        setSectionBadgeState(inspectorStateEl, "active", "Metadata tree loaded.");
      } else {
        setSectionBadgeState(inspectorStateEl, "empty", "No metadata nodes found.");
      }
    } catch (err) {
      console.error(err);
      setInspectorMessage("Failed to load HDF5 tree.");
    }
  }

  function selectInspectorRow(row) {
    if (inspectorSelectedRow) {
      inspectorSelectedRow.classList.remove("is-selected");
    }
    inspectorSelectedRow = row;
    if (inspectorSelectedRow) {
      inspectorSelectedRow.classList.add("is-selected");
    }
  }

  async function showInspectorNode(path) {
    if (!inspectorDetails) return;
    setSectionBadgeState(inspectorStateEl, "loading", "Loading node details…");
    try {
      const data = await fetchJSON(
        `${apiBase}/hdf5/node?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(path)}`,
      );
      if (inspectorPath) inspectorPath.textContent = data.path || path;
      if (inspectorType) inspectorType.textContent = data.type || "-";
      if (inspectorShape) inspectorShape.textContent = data.shape ? data.shape.join("×") : "-";
      if (inspectorDtype) inspectorDtype.textContent = data.dtype || "-";
      if (inspectorAttrs) {
        inspectorAttrs.innerHTML = "";
        if (Array.isArray(data.attrs) && data.attrs.length) {
          data.attrs.forEach((attr) => {
            const row = document.createElement("div");
            row.className = "inspector-attr-row";
            const name = document.createElement("span");
            name.textContent = attr.name;
            const val = document.createElement("span");
            val.textContent = formatInspectorValue(attr.value);
            row.appendChild(name);
            row.appendChild(val);
            inspectorAttrs.appendChild(row);
          });
        }
      }
      if (data.type === "dataset") {
        try {
          const valueData = await fetchJSON(
            `${apiBase}/hdf5/value?file=${encodeURIComponent(state.file)}&path=${encodeURIComponent(path)}`,
          );
          renderInspectorPreview(valueData);
        } catch (err) {
          console.error(err);
          if (inspectorPreview) inspectorPreview.innerHTML = "";
        }
      } else if (inspectorPreview) {
        inspectorPreview.innerHTML = "";
      }
      setSectionBadgeState(inspectorStateEl, "active", `Loaded ${data.type || "node"} details.`);
    } catch (err) {
      console.error(err);
      setInspectorMessage("Failed to load node details.");
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
  };
}
