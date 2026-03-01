/**
 * Inspector tree + search interaction bindings.
 */

export function bindInspectorInteractions({
  inspectorTree,
  inspectorSearchInput,
  inspectorSearchClear,
  inspectorResults,
  inspectorStateEl,
  callbacks,
}) {
  const {
    selectInspectorRow,
    renderInspectorLink,
    setSectionBadgeState,
    renderSkeletonBlock,
    fetchInspectorTree,
    renderInspectorTree,
    showInspectorNode,
    clearInspectorSearch,
    runInspectorSearch,
  } = callbacks;

  inspectorTree?.addEventListener("click", async (event) => {
    const row = event.target.closest(".inspector-row");
    if (!row) return;
    const node = row.parentElement;
    if (!node) return;
    const nodeType = node.dataset.type || "";
    const nodePath = node.dataset.path || "";
    selectInspectorRow(row);
    if (nodeType === "link") {
      renderInspectorLink(nodePath || "-", node.dataset.target || "-");
      return;
    }
    if (nodeType === "group") {
      const toggle = node.querySelector(".inspector-toggle");
      const willOpen = !node.classList.contains("is-open");
      node.classList.toggle("is-open", willOpen);
      if (toggle) {
        toggle.textContent = willOpen ? "▾" : "▸";
      }
      if (willOpen && node.dataset.loaded !== "true") {
        try {
          setSectionBadgeState(inspectorStateEl, "loading", "Loading child nodes…");
          const container = node.querySelector(".inspector-children");
          renderSkeletonBlock(container, 4);
          const children = await fetchInspectorTree(nodePath);
          if (container) {
            renderInspectorTree(children, container);
          }
          node.dataset.loaded = "true";
          setSectionBadgeState(inspectorStateEl, "active", "Metadata tree loaded.");
        } catch (err) {
          console.error(err);
          setSectionBadgeState(inspectorStateEl, "warning", "Failed to load child nodes.");
        }
      }
    }
    if (nodePath) {
      await showInspectorNode(nodePath);
    }
  });

  let inspectorSearchTimer = null;
  inspectorSearchInput?.addEventListener("input", () => {
    if (inspectorSearchTimer) {
      window.clearTimeout(inspectorSearchTimer);
    }
    const query = inspectorSearchInput.value.trim();
    inspectorSearchTimer = window.setTimeout(() => {
      runInspectorSearch(query);
    }, 250);
  });

  inspectorSearchClear?.addEventListener("click", () => {
    clearInspectorSearch();
    runInspectorSearch("");
  });

  inspectorResults?.addEventListener("click", async (event) => {
    const row = event.target.closest(".inspector-result");
    if (!row) return;
    const nodePath = row.dataset.path || "";
    const nodeType = row.dataset.type || "";
    if (!nodePath) return;
    if (nodeType === "link") {
      renderInspectorLink(nodePath, row.dataset.target || "-");
      return;
    }
    await showInspectorNode(nodePath);
  });

  inspectorResults?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const row = event.target.closest(".inspector-result");
    if (!row) return;
    event.preventDefault();
    row.click();
  });
}
