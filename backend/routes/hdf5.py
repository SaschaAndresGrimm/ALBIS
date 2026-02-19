from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response


@dataclass(frozen=True)
class HDF5RouteDeps:
    ensure_hdf5_stack: Callable[[], None]
    get_h5py: Callable[[], Any]
    resolve_file: Callable[[str], Path]
    walk_datasets: Callable[..., None]
    aggregate_linked_stack_datasets: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    collect_h5_attrs: Callable[[Any], dict[str, Any]]
    serialize_h5_value: Callable[[Any], Any]
    dataset_value_preview: Callable[[Any, int], tuple[Any, Any, bool, dict[str, Any] | None]]
    dataset_preview_array: Callable[[Any, int], tuple[Any, bool, dict[str, Any] | None]]


def register_hdf5_routes(app: FastAPI, deps: HDF5RouteDeps) -> None:
    @app.get("/api/datasets")
    def datasets(file: str = Query(..., min_length=1)) -> dict[str, Any]:
        """Discover image-capable datasets, including synthetic linked stacks."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        path = deps.resolve_file(file)
        results: list[dict[str, Any]] = []
        with h5py.File(path, "r") as h5:
            file_cache: dict[Path, Any] = {path: h5}
            try:
                deps.walk_datasets(h5["/"], "/", path, results, set(), file_cache)
            finally:
                for cache_path, handle in file_cache.items():
                    if cache_path == path:
                        continue
                    try:
                        handle.close()
                    except Exception:
                        pass
        return {"datasets": deps.aggregate_linked_stack_datasets(results)}

    @app.get("/api/hdf5/tree")
    def hdf5_tree(file: str = Query(..., min_length=1), path: str = Query("/")) -> dict[str, Any]:
        """Return one tree level for the file inspector."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
        with h5py.File(file_path, "r") as h5:
            if path not in h5:
                raise HTTPException(status_code=404, detail="Path not found")
            obj = h5[path]
            if not isinstance(obj, h5py.Group):
                return {"path": path, "children": []}
            children: list[dict[str, Any]] = []
            for name in obj.keys():
                child_path = f"{path}/{name}" if path != "/" else f"/{name}"
                try:
                    link = obj.get(name, getlink=True)
                except Exception:
                    link = None
                if isinstance(link, h5py.ExternalLink):
                    children.append(
                        {
                            "name": name,
                            "path": child_path,
                            "type": "link",
                            "link": "external",
                            "target": f"{link.filename}:{link.path}",
                        }
                    )
                    continue
                if isinstance(link, h5py.SoftLink):
                    children.append(
                        {
                            "name": name,
                            "path": child_path,
                            "type": "link",
                            "link": "soft",
                            "target": str(link.path),
                        }
                    )
                    continue
                try:
                    child = obj[name]
                except Exception:
                    continue
                if isinstance(child, h5py.Group):
                    children.append(
                        {
                            "name": name,
                            "path": child_path,
                            "type": "group",
                            "hasChildren": len(child.keys()) > 0,
                        }
                    )
                elif isinstance(child, h5py.Dataset):
                    children.append(
                        {
                            "name": name,
                            "path": child_path,
                            "type": "dataset",
                            "shape": tuple(int(x) for x in child.shape),
                            "dtype": str(child.dtype),
                        }
                    )
            children.sort(key=lambda item: (item.get("type") != "group", item.get("name", "")))
            return {"path": path, "children": children}

    @app.get("/api/hdf5/node")
    def hdf5_node(
        file: str = Query(..., min_length=1), path: str = Query(..., min_length=1)
    ) -> dict[str, Any]:
        """Return node metadata and attributes for the inspector details pane."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
        with h5py.File(file_path, "r") as h5:
            if path not in h5:
                raise HTTPException(status_code=404, detail="Path not found")
            obj = h5[path]
            if isinstance(obj, h5py.Group):
                return {"path": path, "type": "group", "attrs": deps.collect_h5_attrs(obj)}
            if isinstance(obj, h5py.Dataset):
                preview = None
                try:
                    if obj.size <= 64 or obj.ndim == 0:
                        preview = deps.serialize_h5_value(obj[()])
                except Exception:
                    preview = None
                return {
                    "path": path,
                    "type": "dataset",
                    "shape": tuple(int(x) for x in obj.shape),
                    "dtype": str(obj.dtype),
                    "attrs": deps.collect_h5_attrs(obj),
                    "preview": preview,
                }
            raise HTTPException(status_code=400, detail="Unsupported node type")

    @app.get("/api/hdf5/value")
    def hdf5_value(
        file: str = Query(..., min_length=1),
        path: str = Query(..., min_length=1),
        max_cells: int = Query(2048, ge=16, le=65536),
    ) -> dict[str, Any]:
        """Return value preview payload for scalar/array inspector rendering."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
        with h5py.File(file_path, "r") as h5:
            if path not in h5:
                raise HTTPException(status_code=404, detail="Path not found")
            obj = h5[path]
            if not isinstance(obj, h5py.Dataset):
                raise HTTPException(status_code=400, detail="Not a dataset")
            preview, preview_shape, truncated, slice_info = deps.dataset_value_preview(
                obj, max_cells=max_cells
            )
            return {
                "path": path,
                "type": "dataset",
                "shape": tuple(int(x) for x in obj.shape),
                "dtype": str(obj.dtype),
                "preview": preview,
                "preview_shape": preview_shape,
                "truncated": truncated,
                "slice": slice_info,
            }

    @app.get("/api/hdf5/search")
    def hdf5_search(
        file: str = Query(..., min_length=1),
        query: str = Query(..., min_length=1),
        limit: int = Query(200, ge=1, le=1000),
    ) -> dict[str, Any]:
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        needle = query.strip().lower()
        if not needle:
            return {"matches": []}
        file_path = deps.resolve_file(file)
        matches: list[dict[str, Any]] = []
        with h5py.File(file_path, "r") as h5:
            stack: list[tuple[str, Any]] = [("/", h5["/"])]
            while stack and len(matches) < limit:
                base_path, group = stack.pop()
                try:
                    names = sorted(group.keys())
                except Exception:
                    continue
                for name in names:
                    child_path = f"{base_path}/{name}" if base_path != "/" else f"/{name}"
                    is_match = needle in name.lower() or needle in child_path.lower()
                    try:
                        link = group.get(name, getlink=True)
                    except Exception:
                        link = None
                    if isinstance(link, h5py.ExternalLink):
                        if is_match:
                            matches.append(
                                {
                                    "name": name,
                                    "path": child_path,
                                    "type": "link",
                                    "link": "external",
                                    "target": f"{link.filename}:{link.path}",
                                }
                            )
                        continue
                    if isinstance(link, h5py.SoftLink):
                        if is_match:
                            matches.append(
                                {
                                    "name": name,
                                    "path": child_path,
                                    "type": "link",
                                    "link": "soft",
                                    "target": str(link.path),
                                }
                            )
                        continue
                    try:
                        child = group[name]
                    except Exception:
                        continue
                    if isinstance(child, h5py.Group):
                        if is_match:
                            matches.append(
                                {
                                    "name": name,
                                    "path": child_path,
                                    "type": "group",
                                    "hasChildren": len(child.keys()) > 0,
                                }
                            )
                        stack.append((child_path, child))
                    elif isinstance(child, h5py.Dataset) and is_match:
                        matches.append(
                            {
                                "name": name,
                                "path": child_path,
                                "type": "dataset",
                                "shape": tuple(int(x) for x in child.shape),
                                "dtype": str(child.dtype),
                            }
                        )
                    if len(matches) >= limit:
                        break
        return {"matches": matches}

    @app.get("/api/hdf5/csv")
    def hdf5_csv(
        file: str = Query(..., min_length=1),
        path: str = Query(..., min_length=1),
        max_cells: int = Query(65536, ge=64, le=262144),
    ) -> Response:
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
        with h5py.File(file_path, "r") as h5:
            if path not in h5:
                raise HTTPException(status_code=404, detail="Path not found")
            obj = h5[path]
            if not isinstance(obj, h5py.Dataset):
                raise HTTPException(status_code=400, detail="Not a dataset")
            data, truncated, slice_info = deps.dataset_preview_array(obj, max_cells=max_cells)
            if data is None:
                raise HTTPException(status_code=500, detail="Unable to read dataset")
            output = io.StringIO()
            if slice_info:
                output.write(
                    f"# slice={slice_info.get('lead')} rows={slice_info.get('rows')} cols={slice_info.get('cols')}\n"
                )
            writer = csv.writer(output)
            if data.ndim == 0:
                writer.writerow([deps.serialize_h5_value(data.item())])
            elif data.ndim == 1:
                writer.writerow(["index", "value"])
                for idx, value in enumerate(data.tolist()):
                    writer.writerow([idx, deps.serialize_h5_value(value)])
            else:
                for row in data.tolist():
                    writer.writerow([deps.serialize_h5_value(v) for v in row])
            if truncated:
                output.write("# truncated\n")
            filename = path.strip("/").replace("/", "_") or "dataset"
            headers = {"Content-Disposition": f'attachment; filename="{filename}.csv"'}
            return Response(content=output.getvalue(), media_type="text/csv", headers=headers)
