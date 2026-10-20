/**
 * File upload and open flow.
 */

import { t } from "./i18n.js";

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
    if (uploadBarText) uploadBarText.textContent = t("upload.progress", { value: 0 });
  }

  function updateUploadProgress(percent) {
    if (!uploadBar) return;
    const value = Math.max(0, Math.min(100, percent));
    if (uploadBarFill) uploadBarFill.style.width = `${value}%`;
    if (uploadBarText) uploadBarText.textContent = t("upload.progress", { value });
  }

  function hideUploadProgress() {
    if (!uploadBar) return;
    uploadBar.classList.remove("is-active");
    uploadBar.classList.remove("is-processing");
    uploadBar.setAttribute("aria-hidden", "true");
  }

  // The file the upload would write, when it is already there with the same
  // size. A same-named copy that stopped short -- uploaded while the file was
  // still being written -- is uploaded over instead of being reopened.
  async function findExistingFile(file, folder = "") {
    if (!file?.name) return null;
    try {
      const params = new URLSearchParams({ name: file.name, size: String(file.size) });
      if (folder) params.set("folder", folder);
      const data = await fetchJSON(`${apiBase}/upload/existing?${params}`);
      return data?.path || null;
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
      setStatus(t("status.upload.none_supported"));
      return;
    }

    await ensureFileMode();
    const uploadFolder = (autoloadDir?.value || state.autoload.dir || "").trim();
    const total = files.length;
    setLoading(true);
    setStatus(total > 1
      ? t("status.upload.preparing_many", { total })
      : t("status.upload.checking_existing"));

    try {
      showUploadProgress();
      const uploadUrl = (file) => {
        const params = new URLSearchParams({ size: String(file.size) });
        if (uploadFolder) params.set("folder", uploadFolder);
        return `${apiBase}/upload?${params}`;
      };
      const uploadedTargets = [];
      for (let i = 0; i < files.length; i += 1) {
        const file = files[i];
        const existing = await findExistingFile(file, uploadFolder);
        if (existing) {
          uploadedTargets.push(existing);
          updateUploadProgress(Math.round(((i + 1) / total) * 100));
          continue;
        }

        setStatus(total > 1
          ? t("status.upload.uploading_many", { index: i + 1, total, name: file.name })
          : t("status.upload.uploading_single"));
        const payload = await uploadSingleFile(file, uploadUrl(file), (event) => {
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
        setStatus(t("status.upload.opening", { target: fileLabel(openTarget) }));
        await openPathInViewer(openTarget, { refreshFileList: false });
      }
    } catch (err) {
      console.error(err);
      const detail = err?.message && err.message !== "Upload failed" ? `: ${err.message}` : "";
      setStatus(`${t("status.upload.failed")}${detail}`, { tone: "error" });
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
