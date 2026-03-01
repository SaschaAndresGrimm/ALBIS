/**
 * File ingestion bindings (file picker + drag/drop target).
 */

export function bindFileIngress({
  fileInput,
  canvasShell,
  onFilesSelected,
}) {
  if (fileInput) {
    fileInput.addEventListener("change", async () => {
      const selected = Array.from(fileInput.files || []);
      if (!selected.length) return;
      await onFilesSelected(selected);
    });
  }

  const clearDropTarget = () => {
    canvasShell?.classList.remove("is-file-drop-target");
  };

  document.addEventListener("dragover", (event) => {
    const transfer = event.dataTransfer;
    if (!transfer || !Array.from(transfer.types || []).includes("Files")) return;
    event.preventDefault();
    transfer.dropEffect = "copy";
    canvasShell?.classList.add("is-file-drop-target");
  });

  document.addEventListener("dragleave", (event) => {
    if (event.relatedTarget) return;
    clearDropTarget();
  });

  document.addEventListener("dragend", clearDropTarget);
  window.addEventListener("blur", clearDropTarget);

  document.addEventListener("drop", async (event) => {
    const transfer = event.dataTransfer;
    if (!transfer || !Array.from(transfer.types || []).includes("Files")) return;
    event.preventDefault();
    clearDropTarget();
    const files = Array.from(transfer.files || []);
    if (!files.length) return;
    await onFilesSelected(files);
  });
}
