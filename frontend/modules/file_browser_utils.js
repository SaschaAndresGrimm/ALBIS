/**
 * Pure helpers for the file browser modal: the browse constants, extension
 * and search normalisation, natural sorting, path joining and splitting,
 * client-side series grouping, breadcrumb segments and size formatting.
 * No DOM, storage or network; file_browser.js and its renderer use them.
 */

import { CBF_EXTS, EDF_EXTS, HDF_EXTS, SERIES_IMAGE_EXTS, TIFF_EXTS } from "./file_type_utils.js";

export const DEFAULT_SORT = "name_asc";
export const DEFAULT_SERIES_MODE = "first_only";
export const DEFAULT_VIEW_MODE = "details";
export const FORMAT_ALL = "__all__";
export const EXPT_EXTS = [".expt"];
export const DEFAULT_BROWSE_EXTS = [...HDF_EXTS, ...TIFF_EXTS, ...CBF_EXTS, ...EDF_EXTS];
export const KNOWN_BROWSE_EXTS = new Set([...DEFAULT_BROWSE_EXTS, ".expt"]);
export const LOCAL_SERIES_EXTS = new Set(SERIES_IMAGE_EXTS);
export const HDF_SERIES_EXTS = new Set(HDF_EXTS);
export const BROWSE_SERIES_CAPABLE_EXTS = [...SERIES_IMAGE_EXTS, ...HDF_EXTS];
export const BROWSE_SERIES_CAPABLE_SET = new Set(BROWSE_SERIES_CAPABLE_EXTS);
export const HDF_MASTER_RE = /^(.+?)_master\.(?:h5|hdf5)$/i;
export const HDF_DATA_RE = /^(.+?)_data_(\d+)\.(?:h5|hdf5)$/i;
export const VALID_SORTS = new Set([
  "name_asc",
  "name_desc",
  "mtime_desc",
  "mtime_asc",
  "type_asc",
  "type_desc",
  "size_asc",
  "size_desc",
]);
// Maps a Details column to its [ascending, descending] sort mode.
export const COLUMN_SORTS = {
  name: ["name_asc", "name_desc"],
  type: ["type_asc", "type_desc"],
  modified: ["mtime_asc", "mtime_desc"],
  size: ["size_asc", "size_desc"],
};
export const SORT_COLUMN = {
  name_asc: "name",
  name_desc: "name",
  type_asc: "type",
  type_desc: "type",
  mtime_asc: "modified",
  mtime_desc: "modified",
  size_asc: "size",
  size_desc: "size",
};
export const VALID_SERIES_MODES = new Set(["all", "first_only"]);
export const VALID_VIEW_MODES = new Set(["list", "details"]);
export const STORAGE_KEYS = {
  filesystemMode: "albis.filesystemMode",
  browsePaneWidth: "albis.browsePaneWidth",
  browseSort: "albis.browseSort",
  browseSeriesMode: "albis.browseSeriesMode",
  browseFormat: "albis.browseFormat",
  browseViewMode: "albis.browseViewMode",
  browseLastPath: "albis.browseLastPath",
};
export const BROWSE_BREAKPOINT = 760;
export const BROWSE_MIN_FOLDER_WIDTH = 220;
export const BROWSE_MIN_FILE_WIDTH = 280;
export const BROWSE_SPLITTER_WIDTH = 8;
export const DEFAULT_BROWSE_PANE_WIDTH = 320;
export const EMPTY_VALUE = "—";
export const FORMAT_GROUPS = [
  { value: "hdf5", labelKey: "browse.filter.option.hdf5", exts: HDF_EXTS },
  { value: "tiff", labelKey: "browse.filter.option.tiff", exts: TIFF_EXTS },
  { value: "cbf", labelKey: "browse.filter.option.cbf", exts: CBF_EXTS },
  { value: "edf", labelKey: "browse.filter.option.edf", exts: EDF_EXTS },
  { value: "geometry", labelKey: "browse.filter.option.geometry", exts: EXPT_EXTS },
];
export const NATURAL_SPLIT_RE = /(\d+)/;

export function normalizeExtToken(raw) {
  let token = String(raw || "").trim().toLowerCase();
  if (!token) return "";
  if (!token.startsWith(".")) {
    token = `.${token}`;
  }
  return token;
}

export function normalizeRequestedExts(raw) {
  const tokens = new Set(raw ? [] : DEFAULT_BROWSE_EXTS);
  if (!raw) {
    return tokens;
  }
  String(raw)
    .split(",")
    .map((item) => normalizeExtToken(item))
    .filter(Boolean)
    .forEach((token) => {
      tokens.add(token);
      if (token === ".cbf") {
        tokens.add(".cbf.gz");
      }
    });
  return tokens;
}

export function serializeExts(exts) {
  return Array.from(exts)
    .filter(Boolean)
    .sort((a, b) => String(a).localeCompare(String(b)))
    .join(",");
}

export function naturalSortKey(value) {
  return String(value || "")
    .toLowerCase()
    .split(NATURAL_SPLIT_RE)
    .filter((part) => part !== "")
    .map((part) => (/^\d+$/.test(part) ? Number(part) : part));
}

export function intersectsExts(exts, candidates) {
  return candidates.some((candidate) => exts.has(candidate));
}

export function compareNatural(a, b) {
  const left = naturalSortKey(a);
  const right = naturalSortKey(b);
  const length = Math.max(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const leftPart = left[index];
    const rightPart = right[index];
    if (typeof leftPart === "undefined") return -1;
    if (typeof rightPart === "undefined") return 1;
    if (leftPart === rightPart) continue;
    if (typeof leftPart === "number" && typeof rightPart === "number") {
      return leftPart - rightPart;
    }
    return String(leftPart).localeCompare(String(rightPart));
  }
  return 0;
}

export function joinBrowsePath(base, name) {
  const root = String(base || "").trim();
  const child = String(name || "").trim();
  if (!root) return child;
  const separator = root.includes("\\") && !root.includes("/") ? "\\" : "/";
  return `${root.replace(/[\\/]$/, "")}${separator}${child}`;
}

export function normalizeSearchQuery(raw) {
  return String(raw || "").trim().toLowerCase();
}

export function fileTypeLabelKey(ext) {
  if (HDF_EXTS.includes(ext)) return "browse.filter.option.hdf5";
  if (TIFF_EXTS.includes(ext)) return "browse.filter.option.tiff";
  if (CBF_EXTS.includes(ext)) return "browse.filter.option.cbf";
  if (EDF_EXTS.includes(ext)) return "browse.filter.option.edf";
  if (EXPT_EXTS.includes(ext)) return "browse.filter.option.geometry";
  return "";
}

export function matchesBrowseQuery(name, query) {
  return !query || String(name || "").toLowerCase().includes(query);
}

export function inferFileExt(name) {
  const lower = String(name || "").toLowerCase();
  if (lower.endsWith(".cbf.gz")) return ".cbf.gz";
  const dot = lower.lastIndexOf(".");
  return dot >= 0 ? lower.slice(dot) : "";
}

export function splitSeriesName(name) {
  const ext = inferFileExt(name);
  const stem = ext === ".cbf.gz"
    ? String(name || "").slice(0, -ext.length)
    : String(name || "").replace(new RegExp(`${ext.replace(".", "\\.")}$`, "i"), "");
  const match = stem.match(/^(.*?)(\d+)([^\d]*)$/);
  if (!match) return null;
  return { prefix: match[1], digits: match[2], suffix: match[3] };
}

export function splitHdfSeries(name) {
  const raw = String(name || "");
  const master = raw.match(HDF_MASTER_RE);
  if (master) return { kind: "master", prefix: master[1].toLowerCase(), index: 0 };
  const data = raw.match(HDF_DATA_RE);
  if (data) return { kind: "data", prefix: data[1].toLowerCase(), index: Number(data[2]) };
  return null;
}

export function deriveParentPath(currentPath) {
  const raw = String(currentPath || "").trim();
  if (!raw) return "";
  if (/^[A-Za-z]:\\/.test(raw)) {
    const parts = raw.split("\\").filter(Boolean);
    return parts.length > 1 ? parts.slice(0, -1).join("\\") : "";
  }
  const isAbsolute = raw.startsWith("/");
  const parts = raw.split("/").filter(Boolean);
  if (!parts.length) return "";
  if (isAbsolute && parts.length === 1) {
    return "/";
  }
  const parent = parts.slice(0, -1).join("/");
  return isAbsolute ? (parent ? `/${parent}` : "/") : parent;
}

export function sortFileItems(items, sortMode) {
  const working = [...items];
  if (sortMode === "name_desc") {
    return working.sort((left, right) => compareNatural(right.name, left.name));
  }
  if (sortMode === "mtime_desc") {
    return working.sort((left, right) => {
      const delta = Number(right.mtime || 0) - Number(left.mtime || 0);
      return delta || compareNatural(left.name, right.name);
    });
  }
  if (sortMode === "mtime_asc") {
    return working.sort((left, right) => {
      const delta = Number(left.mtime || 0) - Number(right.mtime || 0);
      return delta || compareNatural(left.name, right.name);
    });
  }
  if (sortMode === "type_asc") {
    return working.sort((left, right) => {
      const extDelta = compareNatural(left.ext, right.ext);
      return extDelta || compareNatural(left.name, right.name);
    });
  }
  if (sortMode === "type_desc") {
    return working.sort((left, right) => {
      const extDelta = compareNatural(right.ext, left.ext);
      return extDelta || compareNatural(left.name, right.name);
    });
  }
  if (sortMode === "size_asc") {
    return working.sort((left, right) => {
      const delta = Number(left.sizeBytes || 0) - Number(right.sizeBytes || 0);
      return delta || compareNatural(left.name, right.name);
    });
  }
  if (sortMode === "size_desc") {
    return working.sort((left, right) => {
      const delta = Number(right.sizeBytes || 0) - Number(left.sizeBytes || 0);
      return delta || compareNatural(left.name, right.name);
    });
  }
  return working.sort((left, right) => compareNatural(left.name, right.name));
}

export function aggregateSeriesLocally(items) {
  const singles = [];
  const groups = new Map();
  const hdfGroups = new Map();
  for (const item of items) {
    const ext = inferFileExt(item.ext || item.name);
    if (HDF_SERIES_EXTS.has(ext)) {
      const parsed = splitHdfSeries(item.name);
      if (!parsed) {
        singles.push(item);
        continue;
      }
      let group = hdfGroups.get(parsed.prefix);
      if (!group) {
        group = { master: null, data: [] };
        hdfGroups.set(parsed.prefix, group);
      }
      if (parsed.kind === "master") {
        group.master = item;
      } else {
        group.data.push({ index: parsed.index, item });
      }
      continue;
    }
    if (!LOCAL_SERIES_EXTS.has(ext)) {
      singles.push(item);
      continue;
    }
    const parts = splitSeriesName(item.name);
    if (!parts) {
      singles.push(item);
      continue;
    }
    const index = Number(parts.digits);
    if (!Number.isFinite(index)) {
      singles.push(item);
      continue;
    }
    const key = `${parts.prefix}\u0000${parts.suffix}\u0000${ext}`;
    const current = groups.get(key);
    if (!current) {
      groups.set(key, {
        ...item,
        ext,
        isSeriesLead: false,
        seriesCount: 1,
        _leadIndex: index,
      });
      continue;
    }
    current.seriesCount += 1;
    current.mtime = Math.max(Number(current.mtime || 0), Number(item.mtime || 0));
    if (index < current._leadIndex) {
      current.name = item.name;
      current.path = item.path;
      current.ext = ext;
      current.sizeBytes = item.sizeBytes;
      current._leadIndex = index;
    }
  }
  for (const group of hdfGroups.values()) {
    const data = [...group.data].sort((left, right) => left.index - right.index);
    const lead = group.master || (data.length ? data[0].item : null);
    if (!lead) continue;
    const members = [group.master, ...data.map((entry) => entry.item)].filter(Boolean);
    const maxMtime = members.reduce((acc, member) => Math.max(acc, Number(member.mtime || 0)), 0);
    singles.push({
      ...lead,
      ext: inferFileExt(lead.ext || lead.name),
      mtime: maxMtime,
      seriesCount: Math.max(1, data.length),
    });
  }
  const merged = [...singles, ...groups.values()];
  return merged.map((item) => ({
    ...item,
    isSeriesLead: Number(item.seriesCount || 1) > 1,
    seriesCount: Math.max(1, Number(item.seriesCount || 1)),
  }));
}

export function buildBreadcrumbSegments(currentPath) {
  const raw = String(currentPath || "").trim();
  if (!raw) return [];

  if (/^[A-Za-z]:\\/.test(raw)) {
    const parts = raw.split("\\").filter(Boolean);
    let accumulated = parts[0] || "";
    return parts.map((part, index) => {
      if (index === 0) {
        return { label: part, path: accumulated };
      }
      accumulated = `${accumulated}\\${part}`;
      return { label: part, path: accumulated };
    });
  }

  const unixAbsolute = raw.startsWith("/");
  const parts = raw.split("/").filter(Boolean);
  let accumulated = unixAbsolute ? "" : "";
  return parts.map((part) => {
    if (unixAbsolute) {
      accumulated = accumulated ? `${accumulated}/${part}` : `/${part}`;
    } else {
      accumulated = accumulated ? `${accumulated}/${part}` : part;
    }
    return { label: part, path: accumulated };
  });
}

export function formatFileSize(sizeBytes) {
  const size = Number(sizeBytes);
  if (!(size > 0)) {
    return EMPTY_VALUE;
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = size;
  let unitIndex = 0;
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024;
    unitIndex += 1;
  }
  const precision = value >= 10 || unitIndex === 0 || Number.isInteger(value) ? 0 : 1;
  return `${value.toFixed(precision)} ${units[unitIndex]}`;
}

export function normalizeFileItems(data, currentPath) {
  if (Array.isArray(data?.fileItems) && data.fileItems.length > 0) {
    return data.fileItems.map((item) => ({
      name: String(item?.name || ""),
      path: String(item?.path || joinBrowsePath(currentPath, item?.name || "")),
      ext: String(item?.ext || inferFileExt(item?.name || "")),
      mtime: Number(item?.mtime || 0),
      sizeBytes: Number(item?.sizeBytes || 0),
      isSeriesLead: Boolean(item?.isSeriesLead),
      seriesCount: Math.max(1, Number(item?.seriesCount || 1)),
    }));
  }
  if (Array.isArray(data?.files)) {
    return data.files.map((name) => ({
      name: String(name || ""),
      path: joinBrowsePath(currentPath, name || ""),
      ext: inferFileExt(name),
      mtime: 0,
      sizeBytes: 0,
      isSeriesLead: false,
      seriesCount: 1,
    }));
  }
  return [];
}
