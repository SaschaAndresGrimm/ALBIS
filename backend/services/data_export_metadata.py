"""Data export: the header facts read from a source, for the files written from it."""

from __future__ import annotations

import contextlib
import math
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np

if TYPE_CHECKING:
    from .data_export import DataExportDeps


class DataExportMetadataMixin:
    """Source metadata for DataExportService: HDF5 and single-image headers, units."""

    _deps: DataExportDeps

    # Fields that describe ONE exposure and stop being true of a frame made
    # from several. Kept out of a combined frame's header rather than carried
    # across, because each would state a number that is no longer the case.
    COMBINED_FRAME_DROPS = frozenset(
        {
            "exposure_time_s",
            "exposure_period_s",
            "count_cutoff",
            "start_angle_deg",
            "angle_increment_deg",
            "image_datetime",
            "image_number",
            "source_frame",
            "source_frame_count",
        }
    )

    def source_instrument_metadata(
        self, path: Path, dataset: str = "", *, frame_index: int = 0
    ) -> dict[str, Any]:
        """The instrument facts a source states, for another service to reuse.

        Series Operations writes TIFFs too and had no way to read any of this,
        so a summed TIFF carried nothing but its provenance. Rather than give
        that service a second copy of these readers, it borrows this one.
        """
        h5py = None
        ext = self._deps.image_ext_name(path.name)
        if ext in {".h5", ".hdf5"} and dataset:
            with contextlib.suppress(Exception):
                h5py = self._deps.get_h5py()
            if h5py is not None:
                with contextlib.suppress(Exception), h5py.File(path, "r") as h5:
                    return self._hdf5_base_export_metadata(h5, path, dataset)
            return {}
        with contextlib.suppress(Exception):
            return self._image_source_export_metadata(path, frame_index)
        return {}

    def combined_frame_metadata(
        self, path: Path, dataset: str, *, description: str
    ) -> dict[str, Any]:
        """Instrument facts for a frame combined from several, plus what it is.

        `description` is stated verbatim in the header ("sum of 10 frames
        1-10"), and is what replaces the per-exposure fields dropped here.
        """
        meta = {
            key: value
            for key, value in self.source_instrument_metadata(path, dataset).items()
            if key not in self.COMBINED_FRAME_DROPS
        }
        meta["source_name"] = path.name
        if dataset:
            meta["source_dataset"] = dataset
        meta["combined_description"] = description
        return meta

    @staticmethod
    def _finite_float(value: Any) -> float | None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number if math.isfinite(number) else None

    @classmethod
    def _finite_int(cls, value: Any) -> int | None:
        number = cls._finite_float(value)
        if number is None:
            return None
        return int(round(number))

    @staticmethod
    def _decode_h5_text(value: Any) -> str:
        if isinstance(value, np.ndarray):
            if value.size == 0:
                return ""
            value = value.reshape(-1)[0]
        if isinstance(value, np.generic):
            value = value.item()
        if isinstance(value, bytes | np.bytes_):
            return bytes(value).decode("utf-8", errors="replace").strip("\x00").strip()
        return str(value or "").strip()

    @staticmethod
    def _h5_units(obj: Any) -> str | None:
        try:
            units = obj.attrs.get("units") or obj.attrs.get("unit")
        except Exception:
            return None
        if isinstance(units, np.ndarray):
            if units.size == 0:
                return None
            units = units.reshape(-1)[0]
        if isinstance(units, np.generic):
            units = units.item()
        if isinstance(units, bytes | np.bytes_):
            return bytes(units).decode("utf-8", errors="replace")
        return str(units) if units is not None else None

    @staticmethod
    def _to_seconds(value: float, units: str | None) -> float:
        unit = (units or "").strip().lower().replace("µ", "u")
        if unit in {"ms", "millisecond", "milliseconds"}:
            return value / 1000.0
        if unit in {"us", "microsecond", "microseconds"}:
            return value / 1_000_000.0
        if unit in {"ns", "nanosecond", "nanoseconds"}:
            return value / 1_000_000_000.0
        return value

    @staticmethod
    def _to_degrees(value: float, units: str | None) -> float:
        unit = (units or "").strip().lower()
        if unit in {"rad", "radian", "radians"}:
            return math.degrees(value)
        return value

    @staticmethod
    def _to_angstrom(value: float, units: str | None) -> float:
        unit = (units or "").strip().lower().replace("å", "a").replace("µ", "u")
        if unit in {"m", "meter", "metre", "meters", "metres"}:
            return value * 1e10
        if unit in {"mm", "millimeter", "millimetre", "millimeters", "millimetres"}:
            return value * 1e7
        if unit in {"um", "micrometer", "micrometre", "micrometers", "micrometres"}:
            return value * 1e4
        if unit in {"nm", "nanometer", "nanometre", "nanometers", "nanometres"}:
            return value * 10.0
        if unit in {"a", "ang", "angstrom", "angstroms"}:
            return value
        if value < 1e-6:
            return value * 1e10
        if value < 0.2:
            return value * 10.0
        return value

    def _read_h5_text(self, h5: Any, paths: list[str]) -> str | None:
        h5py = self._deps.get_h5py()
        for path in paths:
            if path not in h5:
                continue
            obj = h5[path]
            if not isinstance(obj, h5py.Dataset):
                continue
            try:
                text = self._decode_h5_text(obj[()])
            except Exception:
                continue
            if text:
                return text
        return None

    def _read_h5_number(self, h5: Any, paths: list[str]) -> tuple[float | None, str | None]:
        value, units = self._deps.read_scalar(h5, paths)
        number = self._finite_float(value)
        if number is None:
            return None, units
        return number, units

    def _read_h5_indexed_number(
        self, h5: Any, path: str, index: int
    ) -> tuple[float | None, str | None]:
        h5py = self._deps.get_h5py()
        if path not in h5:
            return None, None
        obj = h5[path]
        if not isinstance(obj, h5py.Dataset):
            return None, None
        try:
            arr = np.asarray(obj[()])
        except Exception:
            return None, self._h5_units(obj)
        units = self._h5_units(obj)
        if arr.size == 0:
            return None, units
        if arr.ndim == 0:
            value = arr.item()
        else:
            flat = arr.reshape(-1)
            value = flat[min(max(0, int(index)), flat.size - 1)]
        return self._finite_float(value), units

    def _read_h5_length_m(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        try:
            return self._deps.to_um(value, units) / 1e6
        except Exception:
            return None

    def _read_h5_sensor_thickness_m(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        thickness_m = self._read_h5_length_m(h5, paths)
        unit = (units or "").strip().lower()
        if thickness_m is not None and thickness_m > 0.1 and unit in {"m", "meter", "metre"}:
            # Some Dectris/Jungfrau master files report micrometer values with a meter unit.
            return value / 1e6
        return thickness_m

    def _read_h5_distance_m(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        try:
            return self._deps.to_mm(value, units) / 1000.0
        except Exception:
            return None

    def _read_h5_energy_ev(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        try:
            return self._deps.to_ev(value, units)
        except Exception:
            return None

    def _read_h5_seconds(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        return self._to_seconds(value, units)

    def _read_h5_angle_deg(self, h5: Any, paths: list[str]) -> float | None:
        value, units = self._read_h5_number(h5, paths)
        if value is None:
            return None
        return self._to_degrees(value, units)

    def _read_h5_threshold_energy_ev(self, h5: Any, threshold_index: int) -> float | None:
        return self._read_h5_energy_ev(
            h5,
            [
                f"/entry/instrument/detector/threshold_{threshold_index + 1}_channel/threshold_energy",
                f"/entry/instrument/detector/detectorSpecific/threshold_{threshold_index + 1}_energy",
                "/entry/instrument/detector/threshold_energy",
            ],
        )

    def _hdf5_base_export_metadata(
        self, h5: Any, source_path: Path, dataset: str
    ) -> dict[str, Any]:
        meta: dict[str, Any] = {
            "source_path": str(source_path),
            "source_dataset": dataset,
            # Provenance for the written file's header. The name, not the full
            # path: enough to trace the frame back, without writing someone's
            # directory layout into a file they may send to a collaborator.
            "source_name": source_path.name,
        }
        text_fields = {
            "detector_description": [
                "/entry/instrument/detector/description",
                "/entry/instrument/detector/name",
            ],
            "detector_serial_number": [
                "/entry/instrument/detector/detector_number",
                "/entry/instrument/detector/serial_number",
                "/entry/instrument/detector/detectorSpecific/detector_number",
            ],
            "series_unique_id": [
                "/entry/instrument/detector/detectorSpecific/series_unique_id",
                "/entry/instrument/detector/detectorSpecific/series_id",
                "/entry/experiment_identifier",
            ],
            "image_datetime": [
                "/entry/start_time",
                "/entry/instrument/detector/detectorSpecific/image_datetime",
            ],
        }
        for key, paths in text_fields.items():
            value = self._read_h5_text(h5, paths)
            if value:
                meta[key] = value

        series_number, _ = self._read_h5_number(
            h5,
            [
                "/entry/instrument/detector/detectorSpecific/series_number",
                "/entry/instrument/detector/detectorSpecific/series",
            ],
        )
        if series_number is not None:
            meta["series_number"] = int(round(series_number))

        image_start, _ = self._read_h5_number(
            h5,
            [
                "/entry/instrument/detector/detectorSpecific/image_nr_start",
                "/entry/instrument/detector/detectorSpecific/image_nr_low",
            ],
        )
        if image_start is not None:
            meta["image_number_start"] = int(round(image_start))

        x_pixel = self._read_h5_length_m(
            h5,
            [
                "/entry/instrument/detector/x_pixel_size",
                "/entry/instrument/detector/detectorSpecific/x_pixel_size",
            ],
        )
        y_pixel = self._read_h5_length_m(
            h5,
            [
                "/entry/instrument/detector/y_pixel_size",
                "/entry/instrument/detector/detectorSpecific/y_pixel_size",
            ],
        )
        if x_pixel is not None:
            meta["pixel_size_x_m"] = x_pixel
        if y_pixel is not None:
            meta["pixel_size_y_m"] = y_pixel
        if x_pixel is not None and y_pixel is None:
            meta["pixel_size_y_m"] = x_pixel
        if y_pixel is not None and x_pixel is None:
            meta["pixel_size_x_m"] = y_pixel

        sensor_thickness = self._read_h5_sensor_thickness_m(
            h5, ["/entry/instrument/detector/sensor_thickness"]
        )
        if sensor_thickness is not None:
            meta["sensor_thickness_m"] = sensor_thickness

        exposure_time = self._read_h5_seconds(
            h5,
            [
                "/entry/instrument/detector/count_time",
                "/entry/instrument/detector/exposure_time",
            ],
        )
        if exposure_time is not None:
            meta["exposure_time_s"] = exposure_time
        exposure_period = self._read_h5_seconds(
            h5,
            [
                "/entry/instrument/detector/frame_time",
                "/entry/instrument/detector/exposure_period",
            ],
        )
        if exposure_period is not None:
            meta["exposure_period_s"] = exposure_period

        for path, increment in (
            ("/entry/instrument/detector/detectorSpecific/saturation_value", 0),
            ("/entry/instrument/detector/saturation_value", 0),
            (
                "/entry/instrument/detector/detectorSpecific/countrate_correction_count_cutoff",
                1,
            ),
            (
                "/entry/instrument/detector/detectorSpecific/detectorModule_000/countrate_correction_count_cutoff",
                1,
            ),
        ):
            value, _units = self._read_h5_number(h5, [path])
            if value is not None:
                if increment == 0:
                    meta["saturation_value"] = int(round(value))
                meta["count_cutoff"] = int(round(value)) + increment
                break

        wavelength_value, wavelength_units = self._read_h5_number(
            h5,
            [
                "/entry/sample/beam/incident_wavelength",
                "/entry/instrument/beam/wavelength",
                "/entry/instrument/monochromator/wavelength",
                "/entry/instrument/beam/incident_wavelength",
            ],
        )
        if wavelength_value is not None:
            wavelength_a = self._to_angstrom(wavelength_value, wavelength_units)
            meta["wavelength_a"] = wavelength_a
            energy_from_wavelength = self._deps.wavelength_to_ev(wavelength_value, wavelength_units)
            if energy_from_wavelength is not None:
                meta["incident_energy_ev"] = energy_from_wavelength

        incident_energy = self._read_h5_energy_ev(
            h5,
            [
                "/entry/instrument/beam/incident_energy",
                "/entry/instrument/beam/energy",
                "/entry/instrument/source/energy",
                "/entry/sample/beam/incident_energy",
                "/entry/instrument/detector/detectorSpecific/photon_energy",
            ],
        )
        if incident_energy is not None:
            meta["incident_energy_ev"] = incident_energy

        distance = self._read_h5_distance_m(
            h5,
            [
                "/entry/instrument/detector/distance",
                "/entry/instrument/detector/detector_distance",
            ],
        )
        if distance is not None:
            meta["detector_distance_m"] = distance

        beam_x, _ = self._read_h5_number(h5, ["/entry/instrument/detector/beam_center_x"])
        beam_y, _ = self._read_h5_number(h5, ["/entry/instrument/detector/beam_center_y"])
        if beam_x is not None and beam_y is not None:
            meta["beam_center_x_px"] = beam_x
            meta["beam_center_y_px"] = beam_y

        angle_increment = self._read_h5_angle_deg(
            h5,
            [
                "/entry/sample/goniometer/omega_range_average",
                "/entry/sample/goniometer/omega_increment",
            ],
        )
        if angle_increment is not None:
            meta["angle_increment_deg"] = angle_increment

        return meta

    def _hdf5_frame_export_metadata(
        self,
        h5: Any,
        base: dict[str, Any],
        *,
        frame_index: int,
        threshold_index: int,
        frame_count: int | None = None,
        threshold_count: int | None = None,
    ) -> dict[str, Any]:
        meta = dict(base)
        meta["source_frame"] = frame_index + 1
        if frame_count:
            meta["source_frame_count"] = int(frame_count)
        if threshold_count and threshold_count > 1:
            meta["source_threshold"] = threshold_index + 1
            meta["source_threshold_count"] = int(threshold_count)
        image_start = self._finite_int(meta.get("image_number_start"))
        meta["image_number"] = (
            (image_start + frame_index) if image_start is not None else frame_index + 1
        )
        meta["threshold_ids"] = [threshold_index + 1]
        threshold_energy = self._read_h5_threshold_energy_ev(h5, threshold_index)
        if threshold_energy is not None:
            meta["threshold_energies_ev"] = [threshold_energy]

        start_angle, start_units = self._read_h5_indexed_number(
            h5, "/entry/sample/goniometer/omega", frame_index
        )
        if start_angle is not None:
            meta["start_angle_deg"] = self._to_degrees(start_angle, start_units)
        elif self._finite_float(meta.get("angle_increment_deg")) is not None:
            meta["start_angle_deg"] = float(meta["angle_increment_deg"]) * frame_index
        else:
            omega_start = self._read_h5_angle_deg(
                h5,
                [
                    "/entry/sample/goniometer/omega_start",
                    "/entry/sample/goniometer/start_angle",
                ],
            )
            if omega_start is not None:
                meta["start_angle_deg"] = omega_start

        return meta

    def _image_source_export_metadata(
        self,
        path: Path,
        frame_index: int,
        frame_count: int | None = None,
        header: tuple[dict[str, Any], str] | None = None,
    ) -> dict[str, Any]:
        """The header facts of a single-image source; `header`, the parsed
        header and its text, when the file was already read for its pixels."""
        meta: dict[str, Any] = {
            "source_path": str(path),
            "image_number": frame_index + 1,
            "source_name": path.name,
            "source_frame": frame_index + 1,
        }
        if frame_count:
            meta["source_frame_count"] = int(frame_count)
        parsed: dict[str, Any] = header[0] if header else {}
        if header is None:
            with contextlib.suppress(Exception):
                parsed = self._deps.pilatus_meta_from_image(path)
        if parsed.get("series_unique_id"):
            meta["series_unique_id"] = parsed.get("series_unique_id")
        if parsed.get("series_number") is not None:
            meta["series_number"] = parsed.get("series_number")
        if parsed.get("image_number") is not None:
            meta["image_number"] = parsed.get("image_number")
        if parsed.get("image_datetime"):
            meta["image_datetime"] = parsed.get("image_datetime")
        if parsed.get("threshold_ids"):
            meta["threshold_ids"] = parsed.get("threshold_ids")
        if parsed.get("threshold_energies_ev"):
            meta["threshold_energies_ev"] = parsed.get("threshold_energies_ev")
        elif parsed.get("threshold_energy_ev") is not None:
            meta["threshold_energies_ev"] = [parsed.get("threshold_energy_ev")]
        if parsed.get("exposure_time_s") is not None:
            meta["exposure_time_s"] = parsed.get("exposure_time_s")
        # Instrument and acquisition facts that stay true of a re-exported
        # frame. Fields describing the pixel array's own corrections
        # (N_excluded_pixels, Excluded_pixels, Flat_field, Trim_file) are
        # deliberately not carried: an export substitutes -1 for masked gaps
        # and -2 for bad or saturated pixels, so they would describe an array
        # that no longer exists.
        for key in (
            "detector_description",
            "detector_serial_number",
            "detector_location",
            "sensor_thickness_m",
            "exposure_period_s",
            "tau_s",
            "count_cutoff",
            "gain_setting",
            "start_angle_deg",
            "angle_increment_deg",
        ):
            if parsed.get(key) is not None:
                meta[key] = parsed.get(key)
        if parsed.get("energy_ev") is not None:
            meta["incident_energy_ev"] = parsed.get("energy_ev")
        if parsed.get("wavelength_a") is not None:
            meta["wavelength_a"] = parsed.get("wavelength_a")
        if parsed.get("lost_pixel_count") is not None:
            meta["lost_pixel_count"] = parsed.get("lost_pixel_count")
        if parsed.get("distance_mm") is not None:
            meta["detector_distance_m"] = float(parsed["distance_mm"]) / 1000.0
        if parsed.get("pixel_size_um") is not None:
            pixel_m = float(parsed["pixel_size_um"]) / 1e6
            meta["pixel_size_x_m"] = pixel_m
            meta["pixel_size_y_m"] = pixel_m
        beam_center = parsed.get("beam_center_px")
        if isinstance(beam_center, list | tuple) and len(beam_center) >= 2:
            meta["beam_center_x_px"] = beam_center[0]
            meta["beam_center_y_px"] = beam_center[1]
        header_text = header[1] if header else ""
        if header is None:
            with contextlib.suppress(Exception):
                header_text = self._deps.pilatus_header_text(path)
        if header_text:
            meta["source_header_text"] = header_text
        return meta
