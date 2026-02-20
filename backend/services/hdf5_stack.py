from __future__ import annotations

"""HDF5 dataset discovery, metadata parsing, and frame extraction helpers."""

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import numpy as np
from fastapi import HTTPException

MASK_PATHS = (
    "/entry/instrument/detector/detectorSpecific/pixel_mask",
    "/entry/instrument/detector/pixel_mask",
    "/entry/instrument/detector/detectorSpecific/bad_pixel_mask",
    "/entry/instrument/detector/bad_pixel_mask",
    "/entry/instrument/detector/pixel_mask_applied",
    "/entry/instrument/detector/detectorSpecific/pixel_mask_applied",
)

_LINKED_DATA_NAME_RE = re.compile(r"^data(?:[_-]?(\d+))?$")


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
        if isinstance(value, (list, tuple)):
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
        return None

    @staticmethod
    def coerce_scalar(value: Any) -> float | None:
        try:
            if isinstance(value, np.ndarray):
                if value.size == 0:
                    return None
                value = value.reshape(-1)[0]
            if isinstance(value, np.generic):
                value = value.item()
            return float(value)
        except Exception:
            return None

    @staticmethod
    def get_units(obj: Any) -> str | None:
        try:
            units = obj.attrs.get("units") or obj.attrs.get("unit")
            if isinstance(units, bytes):
                return units.decode("utf-8", "replace")
            if isinstance(units, np.ndarray) and units.size == 1:
                units = units.reshape(-1)[0]
            if isinstance(units, np.generic):
                units = units.item()
            if isinstance(units, bytes):
                return units.decode("utf-8", "replace")
            return str(units) if units is not None else None
        except Exception:
            return None

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
        return (unit or "").strip().lower().replace("µ", "u")

    def to_mm(self, value: float, unit: str | None) -> float:
        u = self.norm_unit(unit)
        if u in {"m", "meter", "metre", "meters", "metres"}:
            return value * 1000
        if u in {"cm", "centimeter", "centimetre", "centimeters", "centimetres"}:
            return value * 10
        if u in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
            return value
        if u in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
            return value / 1000
        if u in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
            return value / 1e6
        if value < 0.5:
            return value * 1000
        return value

    def to_um(self, value: float, unit: str | None) -> float:
        u = self.norm_unit(unit)
        if u in {"m", "meter", "metre", "meters", "metres"}:
            return value * 1e6
        if u in {"cm", "centimeter", "centimetre", "centimeters", "centimetres"}:
            return value * 1e4
        if u in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
            return value * 1000
        if u in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
            return value
        if u in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
            return value / 1000
        if value < 1e-2:
            return value * 1e6
        if value < 1:
            return value * 1000
        return value

    def to_ev(self, value: float, unit: str | None) -> float:
        u = self.norm_unit(unit)
        if u in {"kev", "kiloelectronvolt", "kiloelectronvolts"}:
            return value * 1000
        if u in {"ev", "electronvolt", "electronvolts"}:
            return value
        if value < 1000:
            return value * 1000
        return value

    def wavelength_to_ev(self, value: float, unit: str | None) -> float | None:
        u = self.norm_unit(unit)
        wavelength_m = None
        if u in {"m", "meter", "metre", "meters", "metres"}:
            wavelength_m = value
        elif u in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
            wavelength_m = value * 1e-9
        elif u in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
            wavelength_m = value * 1e-6
        elif u in {"a", "ang", "angstrom", "angstroms"}:
            wavelength_m = value * 1e-10
        elif value < 1e-6:
            wavelength_m = value
        if wavelength_m is None or wavelength_m <= 0:
            return None
        return 12398.4193 / (wavelength_m * 1e10)

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
            except Exception:
                continue
            child_path = f"{base_path}/{name}" if base_path != "/" else f"/{name}"
            if isinstance(link, h5py.ExternalLink):
                target_path = self.resolve_external_path(file_path, link.filename)
                if not target_path:
                    continue
                target_file = file_cache.get(target_path)
                if target_file is None:
                    try:
                        target_file = h5py.File(target_path, "r")
                    except OSError:
                        continue
                    file_cache[target_path] = target_file
                try:
                    target_obj = target_file[link.path]
                except Exception:
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
                except Exception:
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
            except Exception:
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

    def resolve_node(self, h5: Any, base_file: Path, path: str) -> tuple[Any, Path, list[Any]]:
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
                        target_file = h5py.File(target_path, "r")
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
                try:
                    handle.close()
                except Exception:
                    pass
            raise
        return current, current_file, opened

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
            except Exception:
                continue
            if isinstance(link, h5py.ExternalLink):
                target_path = self.resolve_external_path(group_file, link.filename)
                if not target_path:
                    continue
                try:
                    target_file = h5py.File(target_path, "r")
                except OSError:
                    continue
                opened.append(target_file)
                try:
                    child = target_file[link.path]
                except Exception:
                    continue
            elif isinstance(link, h5py.SoftLink):
                try:
                    child = group[link.path]
                except Exception:
                    continue
            else:
                try:
                    child = group[name]
                except Exception:
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
        parts = [p for p in dataset.strip("/").split("/") if p]
        if not parts:
            raise KeyError("Dataset not found")
        current: Any = h5["/"]
        current_file = base_file
        opened: list[Any] = []

        for idx, part in enumerate(parts):
            if not isinstance(current, h5py.Group):
                raise KeyError("Dataset not found")
            link = current.get(part, getlink=True)
            if link is None:
                raise KeyError("Dataset not found")
            if isinstance(link, h5py.ExternalLink):
                target_path = self.resolve_external_path(current_file, link.filename)
                if not target_path:
                    raise KeyError("Dataset not found")
                try:
                    target_file = h5py.File(target_path, "r")
                except OSError as exc:
                    raise KeyError("Dataset not found") from exc
                opened.append(target_file)
                current_file = target_path
                try:
                    current = target_file[link.path]
                except Exception as exc:
                    raise KeyError("Dataset not found") from exc
            elif isinstance(link, h5py.SoftLink):
                try:
                    current = current[link.path]
                except Exception as exc:
                    raise KeyError("Dataset not found") from exc
            else:
                try:
                    current = current[part]
                except Exception as exc:
                    raise KeyError("Dataset not found") from exc

            if idx < len(parts) - 1 and isinstance(current, h5py.Dataset):
                raise KeyError("Dataset not found")

        if not isinstance(current, h5py.Dataset):
            raise KeyError("Dataset not found")
        return current, opened

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
            try:
                handle.close()
            except Exception:
                pass
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
