"""Output files for series operations: where they go and what they carry.

Part of `SeriesSummingService` (`series_summing.py`): choosing and confining
the output path, checking free disk space, naming the file after the
operation, copying the source's HDF5 metadata, and embedding the geometry
used for the analysis. Also reads a single non-HDF5 image for the job.
Methods expect the host class to provide `self._deps` (a `SeriesSummingDeps`).
"""

from __future__ import annotations

import contextlib
import json
import shutil
from pathlib import Path
from typing import Any

import numpy as np
from fastapi import HTTPException


class SeriesOutputFilesMixin:
    """Output-path, HDF5-metadata and geometry helpers for series jobs."""

    def _resolve_output_base(self, output_path: str | None) -> Path:
        raw = (output_path or "").strip()
        if raw:
            target = Path(raw).expanduser()
            if not target.is_absolute():
                target = (self._deps.data_dir / target).resolve()
            else:
                target = target.resolve()
        else:
            target = (self._deps.data_dir / "output" / "series_sum").resolve()

        if target.exists() and target.is_dir():
            target = (target / "series_sum").resolve()

        allowed_root = self._deps.data_dir.resolve()
        if not self._deps.get_allow_abs_paths() and not self._deps.is_within(target, allowed_root):
            raise HTTPException(status_code=400, detail="Output path is outside data directory")
        target.parent.mkdir(parents=True, exist_ok=True)
        return target

    @staticmethod
    def _next_available_path(path: Path) -> Path:
        if not path.exists():
            return path
        stem = path.stem
        suffix = path.suffix
        for idx in range(1, 10000):
            candidate = path.with_name(f"{stem}_{idx:03d}{suffix}")
            if not candidate.exists():
                return candidate
        raise HTTPException(status_code=500, detail="Unable to allocate output file name")

    @staticmethod
    def _check_disk_space(out_dir: Path, estimated_bytes: int, margin: float = 1.5) -> None:
        """Raise RuntimeError if free disk space is insufficient for the output."""
        try:
            free = shutil.disk_usage(out_dir).free
        except OSError:
            return  # can't determine — proceed and let the write fail naturally
        required = int(estimated_bytes * margin)
        if free < required:
            free_mb = free // (1024 * 1024)
            req_mb = required // (1024 * 1024)
            raise RuntimeError(
                f"Insufficient disk space: {free_mb} MB free, ~{req_mb} MB required for output"
            )

    @staticmethod
    def _operation_base_name(base_name: str, operation: str) -> str:
        name = str(base_name or "").strip() or "series_sum"
        op = str(operation or "sum").lower()
        if op not in {"sum", "mean", "median"}:
            op = "sum"

        series_suffixes = ("_series_sum", "_series_mean", "_series_median")
        for suffix in series_suffixes:
            if name.lower().endswith(suffix):
                return f"{name[: -len(suffix)]}_series_{op}"

        direct_suffixes = ("_sum", "_mean", "_median")
        for suffix in direct_suffixes:
            if name.lower().endswith(suffix):
                return f"{name[: -len(suffix)]}_{op}"

        if name.lower().endswith(f"_{op}"):
            return name
        return f"{name}_{op}"

    # A detector group's own datasets are scalars and short strings; a frame
    # stack or a pixel mask is orders of magnitude bigger. The cap is what
    # separates "metadata worth carrying" from "the data itself", without
    # needing a list of every name a writer might use.
    _METADATA_DATASET_MAX_ELEMENTS = 4096
    # Never carried: the mask is large, and it describes the source array --
    # a summed output has its own masking applied already.
    _METADATA_DATASET_SKIP = frozenset({"pixel_mask", "data"})

    def _copy_small_datasets(self, src_group: Any, dst_group: Any) -> None:
        """Copy a group's own scalar and short datasets, leaving bulk data alone.

        `_copy_h5_metadata` used to copy only ATTRIBUTES here, so everything a
        NeXus writer states as a dataset was dropped: the detector description,
        sensor thickness, count_time, frame_time, saturation_value, and the
        beam's incident wavelength. Geometry survived only because
        `_write_analysis_geometry_metadata` writes it explicitly afterwards --
        and, running afterwards, it still overrides whatever is copied here,
        which is the right way round: a distance the user corrected in ALBIS
        should win over the stale one in the file.
        """
        h5py = self._deps.get_h5py()
        for name in src_group:
            if name in self._METADATA_DATASET_SKIP or name in dst_group:
                continue
            try:
                item = src_group[name]
            except Exception:
                continue
            if not isinstance(item, h5py.Dataset):
                continue
            if item.size > self._METADATA_DATASET_MAX_ELEMENTS:
                continue
            with contextlib.suppress(Exception):
                created = dst_group.create_dataset(name, data=item[()])
                for key, val in item.attrs.items():
                    created.attrs[key] = val

    def _copy_h5_metadata(self, src_h5: Any, dst_h5: Any, threshold_count: int) -> None:
        h5py = self._deps.get_h5py()
        for key, val in src_h5.attrs.items():
            if key not in dst_h5.attrs:
                with contextlib.suppress(Exception):
                    dst_h5.attrs[key] = val

        if "/entry/instrument/detector" in src_h5:
            src_detector = src_h5["/entry/instrument/detector"]
            dst_detector = dst_h5.require_group("/entry/instrument/detector")
            for key, val in src_detector.attrs.items():
                if key not in dst_detector.attrs:
                    with contextlib.suppress(Exception):
                        dst_detector.attrs[key] = val
            self._copy_small_datasets(src_detector, dst_detector)

            for thr in range(threshold_count):
                channel_path = f"threshold_{thr + 1}_channel"
                if channel_path in src_detector:
                    src_channel = src_detector[channel_path]
                    dst_channel = dst_detector.require_group(channel_path)
                    for key, val in src_channel.attrs.items():
                        if key not in dst_channel.attrs:
                            with contextlib.suppress(Exception):
                                dst_channel.attrs[key] = val

                    for item_name in src_channel:
                        if item_name in ("pixel_mask",):
                            continue
                        src_item = src_channel[item_name]
                        if isinstance(src_item, h5py.Dataset) and item_name not in dst_channel:
                            with contextlib.suppress(Exception):
                                dst_channel.create_dataset(item_name, data=src_item[()])

        if "/entry/instrument/beam" in src_h5:
            with contextlib.suppress(Exception):
                self._copy_small_datasets(
                    src_h5["/entry/instrument/beam"],
                    dst_h5.require_group("/entry/instrument/beam"),
                )

        for group_path in ("/entry/instrument", "/entry/sample", "/entry/data"):
            if group_path in src_h5 and group_path not in dst_h5:
                try:
                    src_group = src_h5[group_path]
                    dst_group = dst_h5.require_group(group_path)
                    for key, val in src_group.attrs.items():
                        if key not in dst_group.attrs:
                            with contextlib.suppress(Exception):
                                dst_group.attrs[key] = val

                except Exception:
                    pass

    @staticmethod
    def _finite_float(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if np.isfinite(number) else None

    @classmethod
    def _normalize_geometry_payload(cls, payload: Any) -> dict[str, Any] | None:
        if not isinstance(payload, dict):
            return None
        if str(payload.get("mode") or "") != "geometry":
            return None
        panels = payload.get("panels")
        if not isinstance(panels, list) or not panels:
            return None
        normalized_panels: list[dict[str, Any]] = []
        for panel in panels:
            if not isinstance(panel, dict):
                return None
            name = str(panel.get("name") or "").strip()
            origin = panel.get("origin_mm")
            fast = panel.get("fast_axis")
            slow = panel.get("slow_axis")
            pixel_size = panel.get("pixel_size_mm")
            image_size = panel.get("image_size_px")
            raw_offset = panel.get("raw_offset_px")
            if not (
                name
                and isinstance(origin, list)
                and isinstance(fast, list)
                and isinstance(slow, list)
                and isinstance(pixel_size, list)
                and isinstance(image_size, list)
                and isinstance(raw_offset, list)
                and len(origin) == 3
                and len(fast) == 3
                and len(slow) == 3
                and len(pixel_size) == 2
                and len(image_size) == 2
                and len(raw_offset) == 2
            ):
                return None
            normalized_panel = {
                "name": name,
                "origin_mm": [float(item) for item in origin],
                "fast_axis": [float(item) for item in fast],
                "slow_axis": [float(item) for item in slow],
                "pixel_size_mm": [float(item) for item in pixel_size],
                "image_size_px": [int(item) for item in image_size],
                "raw_offset_px": [float(item) for item in raw_offset],
            }
            if not (
                all(np.isfinite(item) for item in normalized_panel["origin_mm"])
                and all(np.isfinite(item) for item in normalized_panel["fast_axis"])
                and all(np.isfinite(item) for item in normalized_panel["slow_axis"])
                and all(
                    np.isfinite(item) and item > 0 for item in normalized_panel["pixel_size_mm"]
                )
                and all(item > 0 for item in normalized_panel["image_size_px"])
                and all(np.isfinite(item) for item in normalized_panel["raw_offset_px"])
            ):
                return None
            normalized_panels.append(normalized_panel)
        return {
            "mode": "geometry",
            "detector": str(payload.get("detector") or ""),
            "source": str(payload.get("source") or ""),
            "panels": normalized_panels,
        }

    @classmethod
    def _geometry_pixel_size_um(cls, geometry: dict[str, Any] | None) -> float | None:
        if not geometry:
            return None
        panels = geometry.get("panels")
        if not isinstance(panels, list) or not panels:
            return None
        panel = panels[0]
        if not isinstance(panel, dict):
            return None
        pixel_size = panel.get("pixel_size_mm")
        if not isinstance(pixel_size, list) or len(pixel_size) != 2:
            return None
        x = cls._finite_float(pixel_size[0])
        y = cls._finite_float(pixel_size[1])
        if x is None or y is None or x <= 0 or y <= 0:
            return None
        return ((x + y) / 2.0) * 1000.0

    def _write_scalar_dataset(self, group: Any, name: str, value: float | None, units: str) -> None:
        number = self._finite_float(value)
        if number is None:
            return
        if name in group:
            with contextlib.suppress(Exception):
                del group[name]
        dset = group.create_dataset(name, data=np.asarray(number, dtype=np.float64))
        dset.attrs["units"] = units

    def _write_embedded_geometry(self, out_h5: Any, geometry: dict[str, Any] | None) -> bool:
        payload = self._normalize_geometry_payload(geometry)
        if not payload:
            return False
        h5py = self._deps.get_h5py()
        geom_group = out_h5.require_group("/entry/albis/geometry")
        if "json" in geom_group:
            with contextlib.suppress(Exception):
                del geom_group["json"]
        geom_group.attrs["schema_version"] = 1
        geom_group.create_dataset(
            "json",
            data=json.dumps(payload),
            dtype=h5py.string_dtype(encoding="utf-8"),
        )
        return True

    def _write_analysis_geometry_metadata(
        self,
        out_h5: Any,
        *,
        distance_mm: float | None,
        pixel_size_um: float | None,
        energy_ev: float | None,
        center_x_px: float | None,
        center_y_px: float | None,
    ) -> None:
        detector_group = out_h5.require_group("/entry/instrument/detector")
        beam_group = out_h5.require_group("/entry/instrument/beam")
        self._write_scalar_dataset(detector_group, "detector_distance", distance_mm, "mm")
        self._write_scalar_dataset(detector_group, "x_pixel_size", pixel_size_um, "um")
        self._write_scalar_dataset(detector_group, "y_pixel_size", pixel_size_um, "um")
        self._write_scalar_dataset(detector_group, "beam_center_x", center_x_px, "pixel")
        self._write_scalar_dataset(detector_group, "beam_center_y", center_y_px, "pixel")
        self._write_scalar_dataset(beam_group, "incident_energy", energy_ev, "eV")

    def _read_non_h5_image(self, path: Path) -> np.ndarray:
        ext_name = self._deps.image_ext_name(path.name)
        if ext_name in {".tif", ".tiff"}:
            return self._deps.read_tiff(path, index=0)
        if ext_name == ".cbf":
            return self._deps.read_cbf(path)
        if ext_name == ".cbf.gz":
            return self._deps.read_cbf_gz(path)
        if ext_name == ".edf":
            return self._deps.read_edf(path)
        raise HTTPException(status_code=400, detail="Unsupported image format")
