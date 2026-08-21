"""HDF5 dataset discovery, metadata parsing, and frame extraction helpers."""

from __future__ import annotations

import contextlib
import logging
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

from .hdf5_units import (
    coerce_scalar as _coerce_scalar,
)
from .hdf5_units import (
    get_units as _get_units,
)
from .hdf5_units import (
    norm_unit as _norm_unit,
)
from .hdf5_units import (
    to_ev as _to_ev,
)
from .hdf5_units import (
    to_mm as _to_mm,
)
from .hdf5_units import (
    to_um as _to_um,
)
from .hdf5_units import (
    wavelength_to_ev as _wavelength_to_ev,
)

_log = logging.getLogger("albis.hdf5_stack")


def open_hdf5_read_only(h5py: Any, path: Path) -> Any:
    """Open an HDF5 file for reading, including one a writer still holds open.

    A detector filewriter keeps its output open in SWMR mode for the length of a
    series, and HDF5 refuses a plain read-only open of such a file: it reports
    `file is already open for write`, the same bare `OSError` it uses for a
    corrupt file. Read as "unreadable" that is simply wrong -- the file is
    readable, it just has to be asked for in the mode the writer promised. A
    reader that opens with `swmr=True` sees the frames flushed so far, and
    because every request opens its own handle, the next one sees the frames
    written since.

    Plain first, SWMR second. The plain open is the overwhelmingly common case
    (a finished file) and stays exactly as fast as it was; the retry costs one
    failed open, and only for a file that would otherwise have been refused
    outright. When both fail the *first* error is raised, because for a genuinely
    truncated file it is the one that names the real problem.
    """
    handle, _writer_present = open_hdf5_read_only_reporting_writer(h5py, path)
    return handle


def open_hdf5_read_only_reporting_writer(h5py: Any, path: Path) -> tuple[Any, bool]:
    """Open for reading, and say whether a writer is still holding the file.

    The second half matters to the interface. A reader that only ever asks once
    sees the frame count the series happened to have at open time, and a series
    being written grows -- so "is someone still writing this" is the difference
    between a slider that follows an acquisition and one that is quietly stale.
    Needing SWMR to open the file at all is exactly that signal, and it costs
    nothing extra to report it.
    """
    try:
        return h5py.File(path, "r"), False
    except FileNotFoundError:
        raise
    except OSError as plain_exc:
        try:
            handle = h5py.File(path, "r", swmr=True)
        except (OSError, ValueError):
            # ValueError is what older HDF5 raises when it cannot do SWMR at
            # all. Either way the plain error is the better diagnosis.
            raise plain_exc from None
        _log.info("Opened %s in SWMR mode: a writer still holds it open", path)
        return handle, True


def open_hdf5_for_read(h5py: Any, path: Path) -> Any:
    """Open an HDF5 file for reading, reporting an undecodable file as 422.

    h5py signals a truncated or corrupt file with a bare `OSError`, which would
    otherwise escape a route as a 500. That is the wrong story to tell: the
    request was fine, the bytes on disk are not -- and at a beamline the usual
    reason is a file the filewriter has not finished writing yet, which the
    user can simply retry. 422 with the cause named matches how the MYTHEN
    readers already report an unreadable acquisition.
    """
    handle, _writer_present = open_hdf5_for_read_reporting_writer(h5py, path)
    return handle


def open_hdf5_for_read_reporting_writer(h5py: Any, path: Path) -> tuple[Any, bool]:
    """`open_hdf5_for_read`, plus whether a writer still holds the file."""
    try:
        return open_hdf5_read_only_reporting_writer(h5py, path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail="File not found") from exc
    except OSError as exc:
        _log.warning("Cannot open HDF5 file %s: %s", path, exc)
        raise HTTPException(
            status_code=422,
            detail=f"Cannot read {path.name}: not a readable HDF5 file "
            "(it may be incomplete or corrupt)",
        ) from exc


def read_hdf5_array(path: Path, read: Callable[[], Any]) -> Any:
    """Run an HDF5 read, reporting a failure to decode the data as 422.

    Opening a file successfully does not mean its contents can be read. Detector
    data is stored with a compression filter, and HDF5 only needs the matching
    plugin library when the bytes are actually pulled in -- so a missing filter,
    or a chunk the filewriter has not finished flushing, surfaces here rather
    than at open time and would otherwise escape the route as a 500.
    """
    try:
        return read()
    except HTTPException:
        raise
    except OSError as exc:
        _log.warning("Cannot read HDF5 data from %s: %s", path, exc)
        raise HTTPException(
            status_code=422,
            detail=(
                f"Cannot read data from {path.name}: {exc}. The file may be "
                "incomplete, or its compression filter may be unavailable."
            ),
        ) from exc


MASK_PATHS = (
    "/entry/instrument/detector/detectorSpecific/pixel_mask",
    "/entry/instrument/detector/pixel_mask",
    "/entry/instrument/detector/detectorSpecific/bad_pixel_mask",
    "/entry/instrument/detector/bad_pixel_mask",
    "/entry/instrument/detector/pixel_mask_applied",
    "/entry/instrument/detector/detectorSpecific/pixel_mask_applied",
)

_LINKED_DATA_NAME_RE = re.compile(r"^data(?:[_-]?(\d+))?$")
_THRESHOLD_GROUP_NAME_RE = re.compile(r"^threshold_(\d+)_channel$")


@dataclass
class HDF5StackService:
    data_dir: Path
    get_allow_abs_paths: Callable[[], bool]
    is_within: Callable[[Path, Path], bool]
    get_h5py: Callable[[], Any]

    def resolve_external_path(self, base_file: Path, filename: str | None) -> Path | None:
        if not filename:
            return None
        target = Path(str(filename))
        if not target.is_absolute():
            target = base_file.parent / target
        target = target.expanduser().resolve()
        if not self.get_allow_abs_paths():
            allowed_root = self.data_dir.resolve() if self.data_dir else base_file.parent.resolve()
            if allowed_root and not self.is_within(target, allowed_root):
                return None
        if not target.exists() or target.suffix.lower() not in {".h5", ".hdf5"}:
            return None
        return target

    def dataset_info(self, name: str, obj: Any) -> dict[str, Any] | None:
        h5py = self.get_h5py()
        if not isinstance(obj, h5py.Dataset):
            return None
        shape = tuple(int(x) for x in obj.shape)
        dtype = str(obj.dtype)
        ndim = obj.ndim
        size = int(np.prod(shape)) if shape else 0
        return {
            "path": f"/{name}" if not name.startswith("/") else name,
            "shape": shape,
            "dtype": dtype,
            "ndim": ndim,
            "size": size,
            "chunks": obj.chunks,
            "maxshape": obj.maxshape,
        }

    @staticmethod
    def is_image_dataset(info: dict[str, Any]) -> bool:
        if info["ndim"] not in (2, 3, 4):
            return False
        dtype = info["dtype"]
        return any(token in dtype for token in ("int", "uint", "float"))

    def serialize_h5_value(self, value: Any) -> Any:
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8")
            except Exception:
                return value.decode("utf-8", "replace")
        if isinstance(value, np.generic):
            try:
                return value.item()
            except Exception:
                return str(value)
        if isinstance(value, np.ndarray):
            if value.size == 1:
                return self.serialize_h5_value(value.reshape(-1)[0])
            if value.dtype.kind == "S":
                try:
                    return [v.decode("utf-8", "replace") for v in value.reshape(-1)[:16]]
                except Exception:
                    return value.reshape(-1)[:16].tolist()
            if value.size <= 16:
                return value.tolist()
            return {
                "shape": value.shape,
                "dtype": str(value.dtype),
                "preview": value.reshape(-1)[:16].tolist(),
                "truncated": True,
            }
        if isinstance(value, list | tuple):
            return [self.serialize_h5_value(v) for v in value]
        return value

    def collect_h5_attrs(self, obj: Any) -> list[dict[str, Any]]:
        attrs: list[dict[str, Any]] = []
        try:
            for key in obj.attrs.keys():
                try:
                    attrs.append(
                        {"name": str(key), "value": self.serialize_h5_value(obj.attrs[key])}
                    )
                except Exception:
                    attrs.append({"name": str(key), "value": "<unreadable>"})
        except Exception:
            return []
        return attrs

    def array_preview_to_list(self, arr: np.ndarray) -> Any:
        if arr.ndim == 0:
            return self.serialize_h5_value(arr.item())
        if arr.ndim == 1:
            return [self.serialize_h5_value(v) for v in arr.tolist()]
        if arr.ndim == 2:
            return [[self.serialize_h5_value(v) for v in row] for row in arr.tolist()]
        return self.serialize_h5_value(arr)

    def dataset_value_preview(
        self,
        dset: Any,
        max_cells: int = 2048,
        max_rows: int = 128,
        max_cols: int = 128,
    ) -> tuple[Any | None, tuple[int, ...] | None, bool, dict[str, Any] | None]:
        shape = tuple(int(x) for x in dset.shape) if dset.shape else ()
        total = int(np.prod(shape)) if shape else 1
        if total <= 0:
            return None, None, False, None
        try:
            if dset.ndim == 0:
                value = np.asarray(dset[()])
                return self.array_preview_to_list(value), shape, False, None
            if dset.ndim == 1:
                count = max(1, min(shape[0], max_cells))
                data = np.asarray(dset[:count])
                truncated = count < shape[0]
                return self.array_preview_to_list(data), (count,), truncated, None
            rows = max(1, min(shape[-2], max_rows))
            cols = max(1, min(shape[-1], max_cols))
            if rows * cols > max_cells:
                scale = math.sqrt(max_cells / max(rows * cols, 1))
                rows = max(1, int(rows * scale))
                cols = max(1, int(cols * scale))
            lead = (0,) * max(0, dset.ndim - 2)
            data = np.asarray(dset[lead + (slice(0, rows), slice(0, cols))])
            preview_shape = tuple(int(x) for x in data.shape)
            truncated = rows < shape[-2] or cols < shape[-1] or dset.ndim > 2
            slice_info = {"lead": list(lead), "rows": rows, "cols": cols} if dset.ndim > 2 else None
            return self.array_preview_to_list(data), preview_shape, truncated, slice_info
        except Exception:
            return None, None, False, None

    @staticmethod
    def dataset_preview_array(
        dset: Any,
        max_cells: int = 65536,
        max_rows: int = 1024,
        max_cols: int = 1024,
    ) -> tuple[np.ndarray | None, bool, dict[str, Any] | None]:
        shape = tuple(int(x) for x in dset.shape) if dset.shape else ()
        total = int(np.prod(shape)) if shape else 1
        if total <= 0:
            return None, False, None
        try:
            if dset.ndim == 0:
                return np.asarray(dset[()]), False, None
            if dset.ndim == 1:
                count = max(1, min(shape[0], max_cells))
                data = np.asarray(dset[:count])
                return data, count < shape[0], None
            rows = max(1, min(shape[-2], max_rows))
            cols = max(1, min(shape[-1], max_cols))
            if rows * cols > max_cells:
                scale = math.sqrt(max_cells / max(rows * cols, 1))
                rows = max(1, int(rows * scale))
                cols = max(1, int(cols * scale))
            lead = (0,) * max(0, dset.ndim - 2)
            data = np.asarray(dset[lead + (slice(0, rows), slice(0, cols))])
            truncated = rows < shape[-2] or cols < shape[-1] or dset.ndim > 2
            slice_info = {"lead": list(lead), "rows": rows, "cols": cols} if dset.ndim > 2 else None
            return data, truncated, slice_info
        except Exception:
            return None, False, None

    def find_threshold_pixel_masks(self, h5: Any) -> list[Any]:
        h5py = self.get_h5py()
        detector = h5.get("/entry/instrument/detector")
        if not isinstance(detector, h5py.Group):
            return []
        matches: list[tuple[int, Any]] = []
        for name in detector.keys():
            match = _THRESHOLD_GROUP_NAME_RE.match(str(name))
            if not match:
                continue
            try:
                group = detector[name]
            except Exception:
                continue
            if not isinstance(group, h5py.Group):
                continue
            try:
                dset = group.get("pixel_mask")
            except Exception:
                dset = None
            if isinstance(dset, h5py.Dataset) and dset.ndim == 2:
                matches.append((int(match.group(1)), dset))
        matches.sort(key=lambda item: item[0])
        return [dset for _idx, dset in matches]

    def find_pixel_mask(self, h5: Any, threshold: int | None = None) -> Any | None:
        h5py = self.get_h5py()
        if threshold is not None:
            key = f"/entry/instrument/detector/threshold_{threshold + 1}_channel/pixel_mask"
            if key in h5:
                obj = h5[key]
                if isinstance(obj, h5py.Dataset) and obj.ndim == 2:
                    return obj
        for path in MASK_PATHS:
            if path in h5:
                obj = h5[path]
                if isinstance(obj, h5py.Dataset) and obj.ndim == 2:
                    return obj
        threshold_masks = self.find_threshold_pixel_masks(h5)
        if threshold is None and len(threshold_masks) == 1:
            return threshold_masks[0]
        return None

    @staticmethod
    def coerce_scalar(value: Any) -> float | None:
        return _coerce_scalar(value)

    @staticmethod
    def get_units(obj: Any) -> str | None:
        return _get_units(obj)

    def read_scalar(self, h5: Any, paths: list[str]) -> tuple[float | None, str | None]:
        h5py = self.get_h5py()
        for path in paths:
            if path in h5:
                obj = h5[path]
                if not isinstance(obj, h5py.Dataset):
                    continue
                try:
                    value = self.coerce_scalar(obj[()])
                except Exception:
                    value = None
                units = self.get_units(obj)
                if value is not None:
                    return value, units
        return None, None

    @staticmethod
    def norm_unit(unit: str | None) -> str:
        return _norm_unit(unit)

    def to_mm(self, value: float, unit: str | None) -> float:
        return _to_mm(value, unit)

    def to_um(self, value: float, unit: str | None) -> float:
        return _to_um(value, unit)

    def to_ev(self, value: float, unit: str | None) -> float:
        return _to_ev(value, unit)

    def wavelength_to_ev(self, value: float, unit: str | None) -> float | None:
        return _wavelength_to_ev(value, unit)

    @staticmethod
    def read_threshold_energies(h5: Any, count: int) -> list[float | None]:
        energies: list[float | None] = []
        for idx in range(count):
            energy = None
            key = f"/entry/instrument/detector/threshold_{idx + 1}_channel/threshold_energy"
            if key in h5:
                try:
                    data = h5[key][()]
                    arr = np.asarray(data)
                    if arr.size:
                        energy = float(arr.reshape(-1)[0])
                except Exception:
                    energy = None
            energies.append(energy)
        return energies

    def walk_datasets(
        self,
        obj: Any,
        base_path: str,
        file_path: Path,
        results: list[dict[str, Any]],
        ancestors: set[tuple[Path, Any]],
        file_cache: dict[Path, Any],
    ) -> None:
        h5py = self.get_h5py()
        if isinstance(obj, h5py.Dataset):
            info = self.dataset_info(base_path, obj)
            if info:
                info["image"] = self.is_image_dataset(info)
                results.append(info)
            return
        if not isinstance(obj, h5py.Group):
            return
        obj_ref = (file_path, obj.id)
        if obj_ref in ancestors:
            return
        next_ancestors = set(ancestors)
        next_ancestors.add(obj_ref)

        for name in obj.keys():
            try:
                link = obj.get(name, getlink=True)
            except Exception as _exc:
                _log.warning("Skipping node %s in %s: cannot read link: %s", name, file_path, _exc)
                continue
            child_path = f"{base_path}/{name}" if base_path != "/" else f"/{name}"
            if isinstance(link, h5py.ExternalLink):
                target_path = self.resolve_external_path(file_path, link.filename)
                if not target_path:
                    continue
                target_file = file_cache.get(target_path)
                if target_file is None:
                    try:
                        target_file = open_hdf5_read_only(h5py, target_path)
                    except OSError as _exc:
                        _log.warning("Skipping external link %s: cannot open %s: %s", child_path, link.filename, _exc)
                        continue
                    file_cache[target_path] = target_file
                try:
                    target_obj = target_file[link.path]
                except Exception as _exc:
                    _log.warning("Skipping external link %s: path %s not found: %s", child_path, link.path, _exc)
                    continue
                self.walk_datasets(
                    target_obj,
                    child_path,
                    target_path,
                    results,
                    next_ancestors,
                    file_cache,
                )
                continue
            if isinstance(link, h5py.SoftLink):
                try:
                    target_obj = obj[link.path]
                except Exception as _exc:
                    _log.warning("Skipping soft link %s -> %s: %s", child_path, link.path, _exc)
                    continue
                self.walk_datasets(
                    target_obj,
                    child_path,
                    file_path,
                    results,
                    next_ancestors,
                    file_cache,
                )
                continue
            try:
                target_obj = obj[name]
            except Exception as _exc:
                _log.warning("Skipping node %s in %s: %s", child_path, file_path, _exc)
                continue
            self.walk_datasets(
                target_obj,
                child_path,
                file_path,
                results,
                next_ancestors,
                file_cache,
            )

    @staticmethod
    def is_linked_data_member(name: str) -> bool:
        return _LINKED_DATA_NAME_RE.match(name) is not None

    @staticmethod
    def linked_member_sort_key(path_or_name: str) -> tuple[int, int, str]:
        name = path_or_name.rsplit("/", 1)[-1]
        match = _LINKED_DATA_NAME_RE.match(name)
        if not match:
            return (1, 0, name)
        suffix = match.group(1)
        return (0, int(suffix) if suffix else 0, name)

    def aggregate_linked_stack_datasets(
        self, results: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for info in results:
            if not info.get("image"):
                continue
            path = str(info.get("path", ""))
            if "/" not in path.strip("/"):
                continue
            parent, name = path.rsplit("/", 1)
            if not self.is_linked_data_member(name):
                continue
            grouped.setdefault(parent, []).append(info)

        remove_paths: set[str] = set()
        synthetic: list[dict[str, Any]] = []
        for parent, members in grouped.items():
            if len(members) < 2:
                continue
            ordered = sorted(
                members, key=lambda item: self.linked_member_sort_key(str(item.get("path", "")))
            )
            first = ordered[0]
            ndim = int(first.get("ndim") or 0)
            if ndim not in (3, 4):
                continue
            dtype = str(first.get("dtype", ""))
            shape = tuple(int(x) for x in (first.get("shape") or ()))
            if len(shape) != ndim:
                continue
            tail = shape[1:]
            total_frames = 0
            valid: list[dict[str, Any]] = []
            for item in ordered:
                item_shape = tuple(int(x) for x in (item.get("shape") or ()))
                if (
                    int(item.get("ndim") or 0) != ndim
                    or str(item.get("dtype", "")) != dtype
                    or len(item_shape) != ndim
                    or tuple(item_shape[1:]) != tail
                    or int(item_shape[0]) <= 0
                ):
                    continue
                total_frames += int(item_shape[0])
                valid.append(item)
            if len(valid) < 2 or total_frames <= 0:
                continue
            agg_shape = (int(total_frames),) + tail
            synthetic.append(
                {
                    "path": parent,
                    "shape": agg_shape,
                    "dtype": dtype,
                    "ndim": ndim,
                    "size": int(np.prod(agg_shape)),
                    "chunks": None,
                    "maxshape": None,
                    "image": True,
                    "linked_stack": True,
                    "members": [str(item.get("path")) for item in valid],
                }
            )
            for item in valid:
                remove_paths.add(str(item.get("path", "")))

        if not synthetic:
            return results
        filtered = [item for item in results if str(item.get("path", "")) not in remove_paths]
        filtered.extend(synthetic)
        return filtered

    def _resolve_path_node(self, h5: Any, base_file: Path, path: str) -> tuple[Any, Path, list[Any]]:
        h5py = self.get_h5py()
        parts = [p for p in path.strip("/").split("/") if p]
        if not parts:
            return h5["/"], base_file, []
        current: Any = h5["/"]
        current_file = base_file
        opened: list[Any] = []

        try:
            for idx, part in enumerate(parts):
                if not isinstance(current, h5py.Group):
                    raise KeyError("Path not found")
                link = current.get(part, getlink=True)
                if link is None:
                    raise KeyError("Path not found")
                if isinstance(link, h5py.ExternalLink):
                    target_path = self.resolve_external_path(current_file, link.filename)
                    if not target_path:
                        raise KeyError("Path not found")
                    try:
                        target_file = open_hdf5_read_only(h5py, target_path)
                    except OSError as exc:
                        raise KeyError("Path not found") from exc
                    opened.append(target_file)
                    current_file = target_path
                    try:
                        current = target_file[link.path]
                    except Exception as exc:
                        raise KeyError("Path not found") from exc
                elif isinstance(link, h5py.SoftLink):
                    try:
                        current = current[link.path]
                    except Exception as exc:
                        raise KeyError("Path not found") from exc
                else:
                    try:
                        current = current[part]
                    except Exception as exc:
                        raise KeyError("Path not found") from exc
                if idx < len(parts) - 1 and isinstance(current, h5py.Dataset):
                    raise KeyError("Path not found")
        except Exception:
            for handle in opened:
                with contextlib.suppress(Exception):
                    handle.close()

            raise
        return current, current_file, opened

    def resolve_node(self, h5: Any, base_file: Path, path: str) -> tuple[Any, Path, list[Any]]:
        return self._resolve_path_node(h5, base_file, path)

    def resolve_group_linked_stack(
        self,
        group: Any,
        group_path: str,
        group_file: Path,
        opened: list[Any],
    ) -> dict[str, Any] | None:
        h5py = self.get_h5py()
        segments: list[dict[str, Any]] = []
        ndim: int | None = None
        dtype: str | None = None
        tail: tuple[int, ...] | None = None

        for name in sorted(group.keys(), key=self.linked_member_sort_key):
            if not self.is_linked_data_member(name):
                continue
            try:
                link = group.get(name, getlink=True)
            except Exception as _exc:
                _log.warning("Skipping linked member %s in %s: cannot read link: %s", name, group_file, _exc)
                continue
            if isinstance(link, h5py.ExternalLink):
                target_path = self.resolve_external_path(group_file, link.filename)
                if not target_path:
                    continue
                try:
                    target_file = open_hdf5_read_only(h5py, target_path)
                except OSError as _exc:
                    _log.warning("Skipping linked member %s: cannot open %s: %s", name, link.filename, _exc)
                    continue
                opened.append(target_file)
                try:
                    child = target_file[link.path]
                except Exception as _exc:
                    _log.warning("Skipping linked member %s: path %s not found: %s", name, link.path, _exc)
                    continue
            elif isinstance(link, h5py.SoftLink):
                try:
                    child = group[link.path]
                except Exception as _exc:
                    _log.warning("Skipping soft-linked member %s -> %s: %s", name, link.path, _exc)
                    continue
            else:
                try:
                    child = group[name]
                except Exception as _exc:
                    _log.warning("Skipping member %s in %s: %s", name, group_file, _exc)
                    continue
            if not isinstance(child, h5py.Dataset):
                continue
            child_shape = tuple(int(x) for x in child.shape)
            child_ndim = int(child.ndim)
            if child_ndim not in (3, 4) or len(child_shape) != child_ndim or child_shape[0] <= 0:
                continue
            child_dtype = str(child.dtype)
            child_tail = child_shape[1:]
            if ndim is None:
                ndim = child_ndim
                dtype = child_dtype
                tail = child_tail
            elif child_ndim != ndim or child_dtype != dtype or child_tail != tail:
                continue
            child_path = f"{group_path.rstrip('/')}/{name}" if group_path != "/" else f"/{name}"
            segments.append(
                {
                    "path": child_path,
                    "dataset": child,
                    "frames": int(child_shape[0]),
                    "shape": child_shape,
                }
            )

        if not segments or ndim is None or tail is None or dtype is None:
            return None
        total_frames = sum(int(seg["frames"]) for seg in segments)
        if total_frames <= 0:
            return None
        shape = (int(total_frames),) + tail
        return {
            "kind": "linked_stack",
            "path": group_path,
            "shape": shape,
            "dtype": dtype,
            "ndim": ndim,
            "segments": segments,
        }

    def resolve_dataset(self, h5: Any, base_file: Path, dataset: str) -> tuple[Any, list[Any]]:
        h5py = self.get_h5py()
        node, _current_file, opened = self._resolve_path_node(h5, base_file, dataset)
        if not isinstance(node, h5py.Dataset):
            for handle in opened:
                with contextlib.suppress(Exception):
                    handle.close()
            raise KeyError("Dataset not found")
        return node, opened

    def resolve_dataset_view(
        self, h5: Any, base_file: Path, dataset: str
    ) -> tuple[dict[str, Any], list[Any]]:
        h5py = self.get_h5py()
        node, current_file, opened = self.resolve_node(h5, base_file, dataset)
        if isinstance(node, h5py.Dataset):
            return (
                {
                    "kind": "dataset",
                    "path": dataset,
                    "shape": tuple(int(x) for x in node.shape),
                    "dtype": str(node.dtype),
                    "ndim": int(node.ndim),
                    "dataset": node,
                },
                opened,
            )
        if isinstance(node, h5py.Group):
            stack = self.resolve_group_linked_stack(node, dataset, current_file, opened)
            if stack is not None:
                return stack, opened
        for handle in opened:
            with contextlib.suppress(Exception):
                handle.close()

        raise KeyError("Dataset not found")

    @staticmethod
    def extract_frame(view: dict[str, Any], index: int, threshold: int) -> np.ndarray:
        if view["kind"] == "dataset":
            dset = view["dataset"]
            if dset.ndim == 4:
                if index >= dset.shape[0]:
                    raise HTTPException(status_code=416, detail="Frame index out of range")
                if threshold >= dset.shape[1]:
                    raise HTTPException(status_code=416, detail="Threshold index out of range")
                return np.asarray(dset[index, threshold, :, :])
            if dset.ndim == 3:
                if index >= dset.shape[0]:
                    raise HTTPException(status_code=416, detail="Frame index out of range")
                return np.asarray(dset[index, :, :])
            if dset.ndim == 2:
                return np.asarray(dset[:, :])
            raise HTTPException(status_code=400, detail="Dataset is not 2D, 3D, or 4D")

        if view["kind"] == "linked_stack":
            shape = tuple(int(x) for x in view["shape"])
            if not shape:
                raise HTTPException(status_code=400, detail="Dataset has invalid shape")
            total_frames = int(shape[0])
            if index >= total_frames:
                raise HTTPException(status_code=416, detail="Frame index out of range")
            ndim = int(view["ndim"])
            if ndim == 4 and threshold >= int(shape[1]):
                raise HTTPException(status_code=416, detail="Threshold index out of range")
            local = int(index)
            selected: dict[str, Any] | None = None
            for segment in view["segments"]:
                frames = int(segment["frames"])
                if local < frames:
                    selected = segment
                    break
                local -= frames
            if selected is None:
                raise HTTPException(status_code=416, detail="Frame index out of range")
            dset = selected["dataset"]
            if ndim == 4:
                return np.asarray(dset[local, threshold, :, :])
            if ndim == 3:
                return np.asarray(dset[local, :, :])
            raise HTTPException(status_code=400, detail="Dataset is not 3D or 4D")

        raise HTTPException(status_code=400, detail="Unsupported dataset view")
