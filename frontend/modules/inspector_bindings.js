/**
 * Inspector tree + search interaction bindings.
 */

export function bindInspectorInteractions({
  inspectorTree,
  inspectorSearchInput,
  inspectorSearchClear,
  inspectorResults,
  inspectorDetails,
  inspectorCopyPath,
  callbacks,
  core,
}) {
  const { clearInspectorSearch } = callbacks;

  inspectorTree?.addEventListener("click", (event) => {
    const row = event.target.closest(".inspector-row");
    if (!row) return;
    const toggleOnly = Boolean(event.target.closest(".inspector-toggle"));
    void core.inspectorPanel.activateInspectorNode(row.parentElement, { toggleOnly });
  });

  inspectorTree?.addEventListener("keydown", (event) => {
    core.inspectorPanel.handleInspectorTreeKey(event);
  });

  inspectorTree?.addEventListener("focusin", (event) => {
    core.inspectorPanel.noteInspectorFocus(event.target);
  });

  inspectorDetails?.addEventListener("click", (event) => {
    const crumb = event.target.closest("[data-crumb-path]");
    if (!crumb) return;
    void core.inspectorPanel.revealInspectorPath(crumb.dataset.crumbPath);
  });

  inspectorCopyPath?.addEventListener("click", () => {
    void core.inspectorPanel.copyInspectorPath();
  });

  let inspectorSearchTimer = null;
  inspectorSearchInput?.addEventListener("input", () => {
    if (inspectorSearchTimer) {
      window.clearTimeout(inspectorSearchTimer);
    }
    const query = inspectorSearchInput.value.trim();
    inspectorSearchTimer = window.setTimeout(() => {
      core.inspectorPanel.runInspectorSearch(query);
    }, 250);
  });

  inspectorSearchClear?.addEventListener("click", () => {
    clearInspectorSearch();
    core.inspectorPanel.runInspectorSearch("");
  });

  inspectorResults?.addEventListener("click", async (event) => {
    const row = event.target.closest(".inspector-result");
    if (!row) return;
    const nodePath = row.dataset.path || "";
    const nodeType = String(row.dataset.type || "").toLowerCase();
    if (!nodePath) return;
    const linkTarget = nodeType === "link" ? row.dataset.target || "-" : "";
    await core.inspectorPanel.revealInspectorPath(nodePath, { linkTarget });
  });

  inspectorResults?.addEventListener("keydown", (event) => {
    if (event.key !== "Enter" && event.key !== " ") return;
    const row = event.target.closest(".inspector-result");
    if (!row) return;
    event.preventDefault();
    row.click();
  });
}
