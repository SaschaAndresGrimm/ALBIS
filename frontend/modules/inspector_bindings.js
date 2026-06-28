/**
 * Inspector tree + search interaction bindings.
 */

import { t } from "./i18n.js";

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
    const nodeType = String(node.dataset.type || "").toLowerCase();
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
        toggle.setAttribute("aria-expanded", String(willOpen));
      }
      if (willOpen && node.dataset.loaded !== "true") {
        try {
          setSectionBadgeState(inspectorStateEl, "loading", t("inspector.tree.loading_children"));
          const container = node.querySelector(".inspector-children");
          renderSkeletonBlock(container, 4);
          const children = await fetchInspectorTree(nodePath);
          if (container) {
            renderInspectorTree(children, container);
          }
          node.dataset.loaded = "true";
          setSectionBadgeState(inspectorStateEl, "active", t("inspector.tree.loaded"));
        } catch (err) {
          console.error(err);
          setSectionBadgeState(inspectorStateEl, "warning", t("inspector.tree.failed_children"));
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
    const nodeType = String(row.dataset.type || "").toLowerCase();
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
