from __future__ import annotations

import contextlib
import csv
import io
import logging
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response

from ..api_models import (
    HDF5DatasetsResponse,
    HDF5NodeResponse,
    HDF5SearchResponse,
    HDF5TreeChild,
    HDF5TreeResponse,
    HDF5ValueResponse,
)
from ..image_formats import producer_string
from ..services.hdf5_stack import WalkReport, open_hdf5_for_read
from ..services.log_safety import sanitize_log_value

_log = logging.getLogger("albis.hdf5_routes")

HDF5_CSV_RESPONSE_DOCS: dict[int, dict[str, Any]] = {
    200: {
        "description": "CSV preview export for the requested HDF5 dataset.",
        "content": {"text/csv": {"schema": {"type": "string"}}},
        "headers": {
            "Content-Disposition": {
                "description": "Attachment filename for the exported CSV payload.",
                "schema": {"type": "string"},
            }
        },
    }
}


@dataclass(frozen=True)
class HDF5RouteDeps:
    ensure_hdf5_stack: Callable[[], None]
    get_h5py: Callable[[], Any]
    resolve_file: Callable[[str], Path]
    walk_datasets: Callable[..., None]
    # Child names of one group, capped, with the group's true child count.
    group_child_names: Callable[..., tuple[list[str], int]]
    aggregate_linked_stack_datasets: Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
    collect_h5_attrs: Callable[[Any], dict[str, Any]]
    serialize_h5_value: Callable[[Any], Any]
    dataset_value_preview: Callable[[Any, int], tuple[Any, Any, bool, dict[str, Any] | None]]
    dataset_preview_array: Callable[[Any, int], tuple[Any, bool, dict[str, Any] | None]]
    # Resolves a path the way the frame path does, applying the external-storage
    # confinement that plain `h5[path]` indexing skips entirely.
    resolve_node: Callable[[Any, Path, str], tuple[Any, Path, list[Any]]]


@contextlib.contextmanager
def _resolved_node(deps: HDF5RouteDeps, h5: Any, file_path: Path, path: str) -> Any:
    """Yield the node at `path`, closing whatever files were opened to reach it.

    Indexing `h5[path]` directly hands back a dataset without the checks the
    frame path applies, so a crafted file whose data lives outside the data
    root had its bytes returned by the inspector routes with
    `data.allow_abs_paths` off.
    """
    try:
        node, _node_file, opened = deps.resolve_node(h5, file_path, path)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=404, detail="Path not found") from exc
    try:
        yield node
    finally:
        for handle in opened:
            with contextlib.suppress(Exception):
                handle.close()


def _dead_master_detail(group: str, link_count: int, report: WalkReport) -> dict[str, Any]:
    """Explain a master whose linked data never resolved, in its own terms.

    Two different problems reach here and they need different advice. If the
    files are not on disk, the user has an incomplete copy and needs to fetch
    the rest. If they are on disk and would not open, the path was already
    resolved -- so the data is where they put it and something else stopped the
    read (no descriptors left, no permission, a mount that dropped out), and
    telling them to copy files they already have would waste their time.

    Structured rather than a sentence, following the SIMPLON failure detail:
    `code` lets the interface render this in the user's own language, and
    `message` keeps a readable English line for the log and for anything
    reading the API directly. The counts stay here in full even though the
    localized text is deliberately short -- the wording a user reads on the
    splash has no room for them, and the log line does.
    """
    if report.unreadable_external_count and not report.missing_external_count:
        example = report.unreadable_external[0] if report.unreadable_external else ""
        reason = report.unreadable_reasons[0] if report.unreadable_reasons else "unknown error"
        return {
            "code": "master_data_unreadable",
            "message": (
                f"{group} links to {link_count:,} data file(s) and none could be read. "
                f"They are present but will not open"
                + (f", starting with '{example}'" if example else "")
                + f": {reason}"
            ),
            "group": group,
            "count": link_count,
            "example": example,
            "reason": reason,
            "unreadable": report.unreadable_external_count,
        }
    example = report.missing_external[0] if report.missing_external else ""
    message = (
        f"{group} links to {link_count:,} data file(s) and none could be read. "
        f"They are not next to this master"
        + (f", starting with '{example}'" if example else "")
        + ". Copy the linked data files into the same folder and open it again."
    )
    if report.unreadable_external_count:
        message += (
            f" A further {report.unreadable_external_count:,} were present but would not open."
        )
    return {
        "code": "master_data_missing",
        "message": message,
        "group": group,
        "count": link_count,
        "example": example,
        "unreadable": report.unreadable_external_count,
    }


def register_hdf5_routes(app: FastAPI, deps: HDF5RouteDeps) -> None:
    @app.get("/api/datasets", response_model=HDF5DatasetsResponse)
    def datasets(file: str = Query(..., min_length=1)) -> HDF5DatasetsResponse:
        """Discover image-capable datasets, including synthetic linked stacks."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        path = deps.resolve_file(file)
        results: list[dict[str, Any]] = []
        report = WalkReport()
        with open_hdf5_for_read(h5py, path) as h5:
            file_cache: dict[Path, Any] = {path: h5}
            try:
                deps.walk_datasets(h5["/"], "/", path, results, set(), file_cache, report)
            finally:
                for cache_path, handle in file_cache.items():
                    if cache_path == path:
                        continue
                    with contextlib.suppress(Exception):
                        handle.close()
        report.log_summary(path)
        datasets = deps.aggregate_linked_stack_datasets(results)

        if report.dead_link_groups:
            # A master file on its own: the group that should hold the frames
            # holds only links, and not one of them resolved. Saying so beats
            # opening the master and displaying its flatfield, which is what an
            # incomplete download used to look like.
            group, link_count = report.dead_link_groups[0]
            raise HTTPException(
                status_code=422, detail=_dead_master_detail(group, link_count, report)
            )

        return HDF5DatasetsResponse(datasets=datasets)

    @app.get("/api/hdf5/tree", response_model=HDF5TreeResponse)
    def hdf5_tree(file: str = Query(..., min_length=1), path: str = Query("/")) -> HDF5TreeResponse:
        """Return one tree level for the file inspector."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
        with open_hdf5_for_read(h5py, file_path) as h5, contextlib.ExitStack() as stack:
            if path not in h5:
                raise HTTPException(status_code=404, detail="Path not found")
            obj = stack.enter_context(_resolved_node(deps, h5, file_path, path))
            if not isinstance(obj, h5py.Group):
                return HDF5TreeResponse(path=path, children=[])
            children: list[dict[str, Any]] = []
            # Capped: a group with one external link per frame can hold millions,
            # and modelling them all cost 7.2 GiB before this limit existed.
            names, child_count = deps.group_child_names(obj)
            for name in names:
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
            truncated = child_count > len(names)
            if truncated:
                # The group path comes from the request and names a group in
                # the file, and HDF5 allows a newline in a group name -- so it
                # is escaped rather than written into the line as it stands.
                _log.warning(
                    "Listing only %d of %d children of %s in %s",
                    len(names),
                    child_count,
                    sanitize_log_value(path),
                    sanitize_log_value(file_path),
                )
            return HDF5TreeResponse(
                path=path,
                children=[HDF5TreeChild(**item) for item in children],
                childCount=child_count,
                truncated=truncated,
            )

    @app.get("/api/hdf5/node", response_model=HDF5NodeResponse)
    def hdf5_node(
        file: str = Query(..., min_length=1), path: str = Query(..., min_length=1)
    ) -> HDF5NodeResponse:
        """Return node metadata and attributes for the inspector details pane."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
        with open_hdf5_for_read(h5py, file_path) as h5, contextlib.ExitStack() as stack:
            if path not in h5:
                raise HTTPException(status_code=404, detail="Path not found")
            obj = stack.enter_context(_resolved_node(deps, h5, file_path, path))
            if isinstance(obj, h5py.Group):
                return HDF5NodeResponse(path=path, type="group", attrs=deps.collect_h5_attrs(obj))
            if isinstance(obj, h5py.Dataset):
                preview = None
                try:
                    if obj.size <= 64 or obj.ndim == 0:
                        preview = deps.serialize_h5_value(obj[()])
                except Exception:
                    preview = None
                return HDF5NodeResponse(
                    path=path,
                    type="dataset",
                    shape=[int(x) for x in obj.shape],
                    dtype=str(obj.dtype),
                    attrs=deps.collect_h5_attrs(obj),
                    preview=preview,
                )
            raise HTTPException(status_code=400, detail="Unsupported node type")

    @app.get("/api/hdf5/value", response_model=HDF5ValueResponse)
    def hdf5_value(
        file: str = Query(..., min_length=1),
        path: str = Query(..., min_length=1),
        max_cells: int = Query(2048, ge=16, le=65536),
    ) -> HDF5ValueResponse:
        """Return value preview payload for scalar/array inspector rendering."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
        with open_hdf5_for_read(h5py, file_path) as h5, contextlib.ExitStack() as stack:
            if path not in h5:
                raise HTTPException(status_code=404, detail="Path not found")
            obj = stack.enter_context(_resolved_node(deps, h5, file_path, path))
            if not isinstance(obj, h5py.Dataset):
                raise HTTPException(status_code=400, detail="Not a dataset")
            preview, preview_shape, truncated, slice_info = deps.dataset_value_preview(
                obj, max_cells=max_cells
            )
            return HDF5ValueResponse(
                path=path,
                type="dataset",
                shape=[int(x) for x in obj.shape],
                dtype=str(obj.dtype),
                preview=preview,
                preview_shape=[int(x) for x in preview_shape] if preview_shape else None,
                truncated=truncated,
                slice=slice_info,
            )

    @app.get("/api/hdf5/search", response_model=HDF5SearchResponse)
    def hdf5_search(
        file: str = Query(..., min_length=1),
        query: str = Query(..., min_length=1),
        limit: int = Query(200, ge=1, le=1000),
    ) -> HDF5SearchResponse:
        """Depth-first search over group/dataset names and full HDF5 paths."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        needle = query.strip().lower()
        if not needle:
            return HDF5SearchResponse(matches=[])
        file_path = deps.resolve_file(file)
        matches: list[dict[str, Any]] = []
        with open_hdf5_for_read(h5py, file_path) as h5:
            stack: list[tuple[str, Any]] = [("/", h5["/"])]
            while stack and len(matches) < limit:
                base_path, group = stack.pop()
                try:
                    # Capped for the same reason as the tree route: `sorted` over
                    # a million-link group materialises every name first.
                    names = sorted(deps.group_child_names(group)[0])
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
        return HDF5SearchResponse(matches=[HDF5TreeChild(**item) for item in matches])

    @app.get("/api/hdf5/csv", responses=HDF5_CSV_RESPONSE_DOCS)
    def hdf5_csv(
        file: str = Query(..., min_length=1),
        path: str = Query(..., min_length=1),
        max_cells: int = Query(65536, ge=64, le=262144),
    ) -> Response:
        """Export a bounded dataset preview as CSV for quick download/inspection."""
        deps.ensure_hdf5_stack()
        h5py = deps.get_h5py()
        file_path = deps.resolve_file(file)
        with open_hdf5_for_read(h5py, file_path) as h5, contextlib.ExitStack() as stack:
            if path not in h5:
                raise HTTPException(status_code=404, detail="Path not found")
            obj = stack.enter_context(_resolved_node(deps, h5, file_path, path))
            if not isinstance(obj, h5py.Dataset):
                raise HTTPException(status_code=400, detail="Not a dataset")
            data, truncated, slice_info = deps.dataset_preview_array(obj, max_cells=max_cells)
            if data is None:
                raise HTTPException(status_code=500, detail="Unable to read dataset")
            output = io.StringIO()
            # Every comment this file has, before the table starts. `# truncated`
            # used to be appended after the last row, where a reader that skips a
            # comment prefix never sees it and one that does not gets a ragged
            # final line -- the marker matters most to whoever is about to treat
            # a preview as the whole dataset, so it goes where it is read.
            output.write(f"# Produced by {producer_string()}\n")
            # Same one-line guarantee the log entries get, and for the same
            # reason: the dataset path is whatever the file calls its groups,
            # HDF5 permits a newline in a name, and a comment that breaks in two
            # puts the second half in the table as a row.
            output.write(
                f"# Source: {sanitize_log_value(file_path.name)} {sanitize_log_value(path)}\n"
            )
            if slice_info:
                output.write(
                    f"# slice={slice_info.get('lead')} rows={slice_info.get('rows')} cols={slice_info.get('cols')}\n"
                )
            if truncated:
                output.write("# truncated\n")
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
            filename = path.strip("/").replace("/", "_") or "dataset"
            headers = {"Content-Disposition": f'attachment; filename="{filename}.csv"'}
            return Response(content=output.getvalue(), media_type="text/csv", headers=headers)
