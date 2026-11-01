"""HDF5 dataset discovery, metadata parsing, and frame extraction helpers."""

from __future__ import annotations

import contextlib
import itertools
import logging
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException

from .hdf5_open import _text, open_hdf5_read_only
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
from .hdf5_walk_report import GROUP_CHILD_LIMIT, WalkReport

_log = logging.getLogger("albis.hdf5_stack")

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

    def _companion_file_allowed(self, base_file: Path, filename: str) -> bool:
        """Whether libhdf5 may open a companion file while reading a dataset.

        Location only, unlike `resolve_external_path`: external raw-data
        storage points at arbitrary bytes rather than an HDF5 file, so neither
        an `.h5` suffix nor prior existence can be required here. A file that
        is inside the root but missing is libhdf5's error to raise.
        """
        if self.get_allow_abs_paths():
            return True
        try:
            target = Path(filename)
            if not target.is_absolute():
                target = base_file.parent / target
            target = target.expanduser().resolve()
        except (OSError, ValueError):
            return False
        allowed_root = self.data_dir.resolve() if self.data_dir else base_file.parent.resolve()
        return bool(allowed_root) and bool(self.is_within(target, allowed_root))

    def _companion_files(self, dset: Any) -> list[str]:
        """Every other file libhdf5 would open to read this dataset.

        Two storage mechanisms name files below h5py, so neither is visible to
        `resolve_external_path`: external raw-data storage (`H5Pset_external`)
        and virtual dataset sources. A self-reference -- `.` or the dataset's
        own file -- is not a companion and is left out.
        """
        targets: list[str] = []

        with contextlib.suppress(Exception):
            plist = dset.id.get_create_plist()
            for index in range(plist.get_external_count()):
                name = _text(plist.get_external(index)[0])
                if name:
                    targets.append(name)
        with contextlib.suppress(Exception):
            if dset.is_virtual:
                for source in dset.virtual_sources():
                    name = _text(source.file_name)
                    if name and name != ".":
                        targets.append(name)
        return targets

    def assert_dataset_storage_confined(self, dset: Any, base_file: Path) -> None:
        """Refuse a dataset whose bytes live outside the permitted data root.

        `data.allow_abs_paths: false` is documented -- in the Dockerfile and in
        the Power User Guide -- as the reason a deployment cannot read the
        filesystem around it, and `resolve_external_path` enforces that for an
        h5py-level `ExternalLink`. It cannot see the two mechanisms libhdf5
        resolves for itself, so a crafted `.h5` placed inside the root could
        name any file the server process can open and have its bytes returned
        as pixels or as a CSV download.

        Confined rather than refused outright: a virtual dataset whose sources
        sit beside it inside the root is ordinary detector output, and a
        filewriter that splits a series across companion files is the normal
        case this viewer exists to open.
        """
        for name in self._companion_files(dset):
            if not self._companion_file_allowed(base_file, name):
                _log.warning(
                    "Refusing %s: dataset stores data in %s, outside the data root",
                    base_file,
                    name,
                )
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "This dataset keeps its data in a file outside the permitted "
                        "data directory, so it cannot be read. Move the referenced file "
                        "inside the data root, or enable data.allow_abs_paths."
                    ),
                )

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

    def group_child_names(
        self, group: Any, limit: int = GROUP_CHILD_LIMIT
    ) -> tuple[list[str], int]:
        """At most `limit` child names, plus the group's true number of children.

        Iterating a group by name makes libhdf5 order the whole link table
        before it yields the first name: for the 5,000,000-link group that
        prompted this, the *first* name took 122 s and every later one was free.
        The count itself is free -- it sits in the group header -- so a group
        past the cap is sampled in the file's own heap order, which needs no
        ordering pass at all and returned instantly on that same group.

        Heap order is arbitrary, so the sample is an arbitrary one. That is the
        honest trade for a view that is truncated anyway, and callers report the
        truncation rather than presenting the sample as the whole group.
        """
        h5py = self.get_h5py()
        try:
            total = int(len(group))
        except Exception:
            total = 0
        if total <= limit:
            return list(group.keys()), total

        names: list[str] = []

        def collect(raw: Any) -> Any:
            names.append(_text(raw))
            # Any truthy return aborts H5Literate; None keeps it going.
            return True if len(names) >= limit else None

        try:
            group.id.links.iterate(collect, idx_type=h5py.h5.INDEX_NAME, order=h5py.h5.ITER_NATIVE)
        except Exception:
            # Older h5py without the low-level link proxy: pay the ordering
            # pass rather than lose the listing entirely.
            names = list(itertools.islice(group.keys(), limit))
        return names[:limit], total

    def dangling_link_target(self, group: Any, name: str) -> str | None:
        """The file an unresolvable external link names, or None if not external.

        Reached only when `Group.get(..., getlink=True)` hands back its default.
        It does that whenever `name not in group`, and on some libhdf5 builds
        `__contains__` resolves the link, so a *dangling* external link reads
        back as `None`. This asks the link table directly, which never follows
        the link and so answers the same way on every build.
        """
        h5py = self.get_h5py()
        try:
            key = name.encode("utf-8")
            if group.id.links.get_info(key).type != h5py.h5l.TYPE_EXTERNAL:
                return None
            filename, _target = group.id.links.get_val(key)
        except Exception:
            return None
        return _text(filename)

    def walk_datasets(
        self,
        obj: Any,
        base_path: str,
        file_path: Path,
        results: list[dict[str, Any]],
        ancestors: set[tuple[Path, Any]],
        file_cache: dict[Path, Any],
        report: WalkReport | None = None,
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

        walk_report = report if report is not None else WalkReport()
        names, total = self.group_child_names(obj)
        if total > len(names):
            walk_report.note_truncated_group(base_path, total)

        # Counted per group so a dead stack group can be told apart from a file
        # that simply has no frames: see `WalkReport.dead_link_groups`.
        group_missing = 0
        group_produced = 0

        for name in names:
            try:
                link = obj.get(name, getlink=True)
            except Exception as _exc:
                walk_report.note_skip(f"cannot read link ({_exc})")
                continue
            child_path = f"{base_path}/{name}" if base_path != "/" else f"/{name}"
            if link is None:
                # `Group.get` returns its default when `name not in obj`, and on
                # some libhdf5 builds `__contains__` resolves the link -- so a
                # dangling external link arrives here as None. It used to fall
                # through to the hard-link branch below and raise there, one
                # warning per link. A hard link cannot dangle, so a name that
                # enumerated but will not resolve is a broken soft or external
                # link either way.
                group_missing += 1
                walk_report.note_missing_external(self.dangling_link_target(obj, name))
                continue
            if isinstance(link, h5py.ExternalLink):
                target_path = self.resolve_external_path(file_path, link.filename)
                if not target_path:
                    group_missing += 1
                    walk_report.note_missing_external(link.filename)
                    continue
                target_file = file_cache.get(target_path)
                if target_file is None:
                    try:
                        target_file = open_hdf5_read_only(h5py, target_path)
                    except OSError as _exc:
                        group_missing += 1
                        walk_report.note_unreadable_external(link.filename, _exc)
                        continue
                    file_cache[target_path] = target_file
                try:
                    target_obj = target_file[link.path]
                except Exception as _exc:
                    group_missing += 1
                    walk_report.note_skip(f"external link path {link.path} not found ({_exc})")
                    continue
                before = len(results)
                self.walk_datasets(
                    target_obj,
                    child_path,
                    target_path,
                    results,
                    next_ancestors,
                    file_cache,
                    walk_report,
                )
                if len(results) > before:
                    group_produced += 1
                continue
            if isinstance(link, h5py.SoftLink):
                try:
                    target_obj = obj[link.path]
                except Exception as _exc:
                    group_missing += 1
                    walk_report.note_skip(f"soft link to {link.path} is broken ({_exc})")
                    continue
                before = len(results)
                self.walk_datasets(
                    target_obj,
                    child_path,
                    file_path,
                    results,
                    next_ancestors,
                    file_cache,
                    walk_report,
                )
                if len(results) > before:
                    group_produced += 1
                continue
            try:
                target_obj = obj[name]
            except Exception as _exc:
                walk_report.note_skip(f"cannot open node ({_exc})")
                continue
            before = len(results)
            self.walk_datasets(
                target_obj,
                child_path,
                file_path,
                results,
                next_ancestors,
                file_cache,
                walk_report,
            )
            if len(results) > before:
                group_produced += 1

        if group_missing and not group_produced:
            # `total`, not `group_missing`: past the cap we only looked at the
            # first 10,000 of the group's links, and reporting that as the whole
            # count would understate the file by a factor of 500.
            walk_report.note_dead_link_group(base_path or "/", group_missing, total)
        if report is None:
            # Top-level call made its own report, so nothing else will log it.
            walk_report.log_summary(file_path)

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

    def _resolve_path_node(
        self, h5: Any, base_file: Path, path: str
    ) -> tuple[Any, Path, list[Any]]:
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
            if isinstance(current, h5py.Dataset):
                self.assert_dataset_storage_confined(current, current_file)
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

        # Capped and aggregated for the same reasons as `walk_datasets`: this is
        # the path a partly-downloaded master reaches once the dataset is chosen,
        # and it used to sort every one of five million member names and then log
        # a line per member that would not open.
        report = WalkReport()
        member_names, member_total = self.group_child_names(group)
        if member_total > len(member_names):
            report.note_truncated_group(group_path, member_total)

        for name in sorted(member_names, key=self.linked_member_sort_key):
            if not self.is_linked_data_member(name):
                continue
            try:
                link = group.get(name, getlink=True)
            except Exception as _exc:
                report.note_skip(f"cannot read link ({_exc})")
                continue
            if link is None:
                # See the same branch in `walk_datasets`: a dangling external
                # link reads back as None on some libhdf5 builds.
                report.note_missing_external(self.dangling_link_target(group, name))
                continue
            if isinstance(link, h5py.ExternalLink):
                target_path = self.resolve_external_path(group_file, link.filename)
                if not target_path:
                    report.note_missing_external(link.filename)
                    continue
                try:
                    target_file = open_hdf5_read_only(h5py, target_path)
                except OSError as _exc:
                    report.note_unreadable_external(link.filename, _exc)
                    continue
                opened.append(target_file)
                try:
                    child = target_file[link.path]
                except Exception as _exc:
                    report.note_skip(f"external link path {link.path} not found ({_exc})")
                    continue
            elif isinstance(link, h5py.SoftLink):
                try:
                    child = group[link.path]
                except Exception as _exc:
                    report.note_skip(f"soft link to {link.path} is broken ({_exc})")
                    continue
            else:
                try:
                    child = group[name]
                except Exception as _exc:
                    report.note_skip(f"cannot open member ({_exc})")
                    continue
            if not isinstance(child, h5py.Dataset):
                continue
            try:
                self.assert_dataset_storage_confined(child, group_file)
            except HTTPException as _exc:
                report.note_skip(f"stores data outside the data root ({_exc.detail})")
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

        report.log_summary(group_file)
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
