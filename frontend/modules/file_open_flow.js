/**
 * File open/select flow.
 */

export function createFileOpenController({
  apiBase,
  state,
  getBackendIsLocal,
  elements,
  callbacks,
}) {
  const {
    autoloadDir,
    fileSelect,
    fileInput,
    filesystemMode,
  } = elements;

  const {
    dirnameFromPath,
    syncSeriesSumOutputPath,
    loadFiles,
    option,
    fileLabel,
    isHdfFile,
    loadDatasets,
    loadImageSeries,
    closeMenu,
    ensureFileMode,
    setStatus,
    openFileDialog,
  } = callbacks;

  async function openPathInViewer(path, { refreshFileList = true } = {}) {
    if (!path) return;
    const folder = dirnameFromPath(path);
    if (autoloadDir) {
      autoloadDir.value = folder;
    }
    state.autoload.dir = folder;
    state.file = path;
    syncSeriesSumOutputPath();
    if (refreshFileList) {
      await loadFiles();
    }
    if (fileSelect) {
      const existing = Array.from(fileSelect.options).some((opt) => opt.value === path);
      if (!existing) {
        fileSelect.appendChild(option(fileLabel(path), path));
      }
      fileSelect.value = path;
    }
    if (isHdfFile(path)) {
      await loadDatasets();
    } else {
      await loadImageSeries(path);
    }
  }

  async function openFileModal() {
    closeMenu();
    await ensureFileMode();

    if (getBackendIsLocal()) {
      try {
        const res = await fetch(`${apiBase}/choose-file`);
        if (res.status === 204) return;
        if (res.ok) {
          const data = await res.json();
          const path = data?.path;
          if (!path) return;
          await openPathInViewer(path);
          return;
        }
      } catch (err) {
        console.error(err);
      }
      try {
        const selectedFile = await openFileDialog();
        if (!selectedFile) return;
        await openPathInViewer(selectedFile);
        return;
      } catch (err) {
        console.error(err);
        setStatus("File picker unavailable");
        return;
      }
    } else if (filesystemMode?.value === "local") {
      fileInput.accept = ".h5,.hdf5,.tif,.tiff,.cbf,.cbf.gz,.edf";
      fileInput.multiple = true;
      fileInput.click();
      return;
    } else {
      try {
        const selectedFile = await openFileDialog();
        if (!selectedFile) return;
        await openPathInViewer(selectedFile);
        return;
      } catch (err) {
        console.error(err);
      }
    }

    if (fileInput) {
      fileInput.accept = ".h5,.hdf5,.tif,.tiff,.cbf,.cbf.gz,.edf";
      fileInput.multiple = true;
    }
    fileInput?.click();
  }

  return {
    openPathInViewer,
    openFileModal,
  };
}
