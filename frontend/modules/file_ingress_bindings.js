/**
 * File ingestion bindings (file picker + drag/drop target).
 */

export function bindFileIngress({
  fileInput,
  canvasShell,
  onFilesSelected,
  allowDocumentDrop = true,
  onDocumentDropDisabled,
}) {
  const cleanup = [];

  if (fileInput) {
    const handleFileInputChange = async () => {
      const selected = Array.from(fileInput.files || []);
      if (!selected.length) return;
      await onFilesSelected(selected);
    };
    fileInput.addEventListener("change", handleFileInputChange);
    cleanup.push(() => fileInput.removeEventListener("change", handleFileInputChange));
  }

  const clearDropTarget = () => {
    canvasShell?.classList.remove("is-file-drop-target");
    canvasShell?.classList.remove("is-file-drop-disabled");
  };

  const hasFileTransfer = (transfer) => Boolean(
    transfer && Array.from(transfer.types || []).includes("Files"),
  );

  const addDocumentListener = (type, handler) => {
    document.addEventListener(type, handler);
    cleanup.push(() => document.removeEventListener(type, handler));
  };

  const addWindowListener = (type, handler) => {
    window.addEventListener(type, handler);
    cleanup.push(() => window.removeEventListener(type, handler));
  };

  addDocumentListener("dragover", (event) => {
    const transfer = event.dataTransfer;
    if (!hasFileTransfer(transfer)) return;
    event.preventDefault();
    if (!allowDocumentDrop) {
      transfer.dropEffect = "none";
      clearDropTarget();
      canvasShell?.classList.add("is-file-drop-disabled");
      return;
    }
    transfer.dropEffect = "copy";
    canvasShell?.classList.remove("is-file-drop-disabled");
    canvasShell?.classList.add("is-file-drop-target");
  });

  addDocumentListener("dragleave", (event) => {
    if (event.relatedTarget) return;
    clearDropTarget();
  });

  addDocumentListener("dragend", clearDropTarget);
  addWindowListener("blur", clearDropTarget);

  addDocumentListener("drop", async (event) => {
    const transfer = event.dataTransfer;
    if (!hasFileTransfer(transfer)) return;
    event.preventDefault();
    clearDropTarget();
    if (!allowDocumentDrop) {
      transfer.dropEffect = "none";
      if (typeof onDocumentDropDisabled === "function") {
        onDocumentDropDisabled();
      }
      return;
    }
    const files = Array.from(transfer.files || []);
    if (!files.length) return;
    await onFilesSelected(files);
  });

  return () => {
    cleanup.forEach((removeListener) => removeListener());
  };
}
