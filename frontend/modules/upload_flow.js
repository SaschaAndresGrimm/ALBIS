/**
 * File upload and open flow.
 */

function isSupportedUploadFile(fileName) {
  const name = String(fileName || "").toLowerCase();
  return (
    name.endsWith(".h5") ||
    name.endsWith(".hdf5") ||
    name.endsWith(".tif") ||
    name.endsWith(".tiff") ||
    name.endsWith(".cbf") ||
    name.endsWith(".cbf.gz") ||
    name.endsWith(".edf")
  );
}

function splitSeriesNameClient(name) {
  if (!name) return null;
  const lower = String(name).toLowerCase();
  let stem = name;
  if (lower.endsWith(".cbf.gz")) {
    stem = name.slice(0, -7);
  } else {
    const dot = name.lastIndexOf(".");
    stem = dot > 0 ? name.slice(0, dot) : name;
  }
  const match = stem.match(/^(.*?)(\d+)([^\d]*)$/);
  if (!match) return null;
  return {
    prefix: match[1],
    index: Number.parseInt(match[2], 10),
    suffix: match[3],
  };
}

function sortFilesForSeriesUpload(files) {
  const list = Array.from(files || []);
  return list.sort((a, b) => {
    const aName = (a?.name || "").toLowerCase();
    const bName = (b?.name || "").toLowerCase();
    const aMaster = aName.includes("master");
    const bMaster = bName.includes("master");
    if (aMaster !== bMaster) return aMaster ? -1 : 1;

    const aSeries = splitSeriesNameClient(a?.name || "");
    const bSeries = splitSeriesNameClient(b?.name || "");
    if (aSeries && bSeries) {
      if (aSeries.prefix !== bSeries.prefix) {
        return aSeries.prefix.localeCompare(bSeries.prefix);
      }
      if (aSeries.suffix !== bSeries.suffix) {
        return aSeries.suffix.localeCompare(bSeries.suffix);
      }
      if (aSeries.index !== bSeries.index) return aSeries.index - bSeries.index;
    } else if (aSeries || bSeries) {
      return aSeries ? -1 : 1;
    }
    return (a?.name || "").localeCompare(b?.name || "");
  });
}

function uploadSingleFile(file, uploadUrl, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", uploadUrl, true);
    xhr.responseType = "json";
    xhr.upload.addEventListener("progress", (event) => {
      if (typeof onProgress === "function") onProgress(event);
    });
    xhr.addEventListener("load", () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(xhr.response);
      } else {
        reject(new Error(xhr.response?.detail || "Upload failed"));
      }
    });
    xhr.addEventListener("error", () => reject(new Error("Upload failed")));
    const form = new FormData();
    form.append("file", file);
    xhr.send(form);
  });
}

export function createUploadFlowController({
  apiBase,
  state,
  elements,
  callbacks,
}) {
  const {
    autoloadDir,
    fileInput,
    uploadBar,
    uploadBarFill,
    uploadBarText,
  } = elements;

  const {
    ensureFileMode,
    setLoading,
    setStatus,
    loadFiles,
    openPathInViewer,
    fileLabel,
    fetchJSON,
  } = callbacks;

  function showUploadProgress() {
    if (!uploadBar) return;
    uploadBar.classList.add("is-active");
    uploadBar.classList.remove("is-processing");
    uploadBar.setAttribute("aria-hidden", "false");
    if (uploadBarFill) uploadBarFill.style.width = "0%";
    if (uploadBarText) uploadBarText.textContent = "Uploading 0%";
  }

  function updateUploadProgress(percent) {
    if (!uploadBar) return;
    const value = Math.max(0, Math.min(100, percent));
    if (uploadBarFill) uploadBarFill.style.width = `${value}%`;
    if (uploadBarText) uploadBarText.textContent = `Uploading ${value}%`;
  }

  function hideUploadProgress() {
    if (!uploadBar) return;
    uploadBar.classList.remove("is-active");
    uploadBar.classList.remove("is-processing");
    uploadBar.setAttribute("aria-hidden", "true");
  }

  async function findExistingFile(filename, folder = "") {
    if (!filename) return null;
    try {
      const url = folder ? `${apiBase}/files?folder=${encodeURIComponent(folder)}` : `${apiBase}/files`;
      const data = await fetchJSON(url);
      const matches = data.files.filter((file) => file === filename || file.endsWith(`/${filename}`));
      if (matches.length === 0) return null;
      const exact = matches.find((file) => file === filename);
      if (exact) return exact;
      matches.sort((a, b) => a.length - b.length);
      return matches[0];
    } catch (err) {
      console.error(err);
      return null;
    }
  }

  async function uploadAndOpenSelectedFiles(selectedFiles) {
    const selected = Array.isArray(selectedFiles) ? selectedFiles.filter(Boolean) : [];
    if (!selected.length) return;
    const files = sortFilesForSeriesUpload(selected.filter((item) => isSupportedUploadFile(item?.name)));
    if (!files.length) {
      setStatus("No supported image files in selection");
      return;
    }

    await ensureFileMode();
    const uploadFolder = (autoloadDir?.value || state.autoload.dir || "").trim();
    const total = files.length;
    setLoading(true);
    setStatus(total > 1 ? `Preparing ${total} files…` : "Checking for existing file…");

    try {
      showUploadProgress();
      const uploadUrl = uploadFolder
        ? `${apiBase}/upload?folder=${encodeURIComponent(uploadFolder)}`
        : `${apiBase}/upload`;
      const uploadedTargets = [];
      for (let i = 0; i < files.length; i += 1) {
        const file = files[i];
        const existing = await findExistingFile(file.name, uploadFolder);
        if (existing) {
          uploadedTargets.push(existing);
          updateUploadProgress(Math.round(((i + 1) / total) * 100));
          continue;
        }

        setStatus(total > 1 ? `Uploading ${i + 1}/${total}: ${file.name}` : "Uploading file…");
        const payload = await uploadSingleFile(file, uploadUrl, (event) => {
          if (!event.lengthComputable) {
            updateUploadProgress(Math.round((i / total) * 100));
            return;
          }
          const part = event.total > 0 ? event.loaded / event.total : 0;
          const overall = ((i + part) / total) * 100;
          updateUploadProgress(Math.round(overall));
        });

        if (payload?.path || payload?.filename) {
          uploadedTargets.push(payload.path || payload.filename);
        }
      }

      updateUploadProgress(100);
      await loadFiles();
      const openTarget = uploadedTargets[0];
      if (openTarget) {
        setStatus(`Opening ${fileLabel(openTarget)}…`);
        await openPathInViewer(openTarget, { refreshFileList: false });
      }
    } catch (err) {
      console.error(err);
      setStatus("Failed to upload selected files");
      setLoading(false);
    } finally {
      hideUploadProgress();
      if (fileInput) {
        fileInput.value = "";
      }
    }
  }

  return {
    uploadAndOpenSelectedFiles,
    hideUploadProgress,
  };
}
