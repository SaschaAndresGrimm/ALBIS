"""Metadata an export keeps, for the two containers that were losing it.

Closing the same gap twice more, after the CBF header:

* A TIFF's private DECTRIS tag has no slot for the detector model, serial,
  location, pixel size, sensor thickness, tau, count cutoff, gain or the
  rotation angles. Inventing private tag ids for them would mean guessing
  inside DECTRIS's own numbering, so they go in the standard ImageDescription
  instead -- the same text the CBF header carries.

* `_copy_h5_metadata` copied a detector group's ATTRIBUTES and not its
  datasets, so everything a NeXus writer states as a dataset was dropped from a
  summed output: description, sensor_thickness, count_time, frame_time,
  saturation_value, and the beam's incident wavelength.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from backend import image_formats as fmt

h5py = pytest.importorskip("h5py")
tifffile = pytest.importorskip("tifffile")

RICH_METADATA = {
    "source_name": "series_master.h5",
    "source_frame": 3,
    "source_frame_count": 180,
    "detector_description": "PILATUS3 2M",
    "detector_serial_number": "24-0119",
    "detector_location": "Swiss Light Source",
    "image_datetime": "2026-09-11T12:00:00.500",
    "pixel_size_x_m": 172e-6,
    "pixel_size_y_m": 172e-6,
    "sensor_thickness_m": 320e-6,
    "exposure_time_s": 0.1,
    "exposure_period_s": 0.105,
    "tau_s": 383.8e-9,
    "count_cutoff": 1302749,
    "threshold_energy_ev": 6331.0,
    "gain_setting": "high gain (vrf = -0.150)",
    "wavelength_a": 1.0,
    "detector_distance_m": 0.25,
    "beam_center_x_px": 1055.0,
    "beam_center_y_px": 1010.5,
    "start_angle_deg": 12.5,
    "angle_increment_deg": 0.1,
    # Private-tag-only fields, which the text header has no slot for.
    "series_unique_id": "01ABCDEF",
    "series_number": 7,
    "image_number": 3,
    "threshold_ids": [1],
    "threshold_energies_ev": [6331.0],
    "lost_pixel_count": 21,
}


class TestTiffDescription:
    @pytest.fixture
    def written(self, tmp_path: Path) -> Path:
        path = tmp_path / "frame.tiff"
        fmt._write_tiff(path, np.arange(64, dtype=np.int32).reshape(8, 8), RICH_METADATA)
        return path

    def test_the_description_states_what_the_private_tag_cannot(self, written: Path) -> None:
        with tifffile.TiffFile(written) as tiff:
            description = tiff.pages[0].description or ""
        for expected in (
            "# Detector: PILATUS3 2M, S/N 24-0119, Swiss Light Source",
            "# 2026-09-11T12:00:00.500",
            "# Pixel_size 0.000172 m x 0.000172 m",
            "# Silicon sensor, thickness 0.00032 m",
            "# Exposure_period 0.105 s",
            "# Tau = 3.838e-07 s",
            "# Count_cutoff 1302749 counts",
            "# Threshold_setting: 6331 eV",
            "# Gain_setting: high gain (vrf = -0.150)",
            "# Start_angle 12.5 deg.",
            "# Angle_increment 0.1 deg.",
        ):
            assert expected in description, f"missing from ImageDescription: {expected}"

    def test_it_still_says_the_file_is_derived(self, written: Path) -> None:
        with tifffile.TiffFile(written) as tiff:
            page = tiff.pages[0]
            description = page.description or ""
            software = page.tags.get("Software")
        assert "derived data, not raw detector output" in description
        assert "# Source: series_master.h5 frame 3/180" in description
        assert software is not None and "ALBIS" in str(software.value)

    def test_reading_it_back_recovers_both_halves(self, written: Path) -> None:
        """The reader used to return whichever source answered first."""
        meta = fmt._pilatus_meta_from_tiff(written)
        # From the text header, which the private tag has no slot for.
        assert meta["detector_description"] == "PILATUS3 2M"
        assert meta["detector_serial_number"] == "24-0119"
        assert meta["detector_location"] == "Swiss Light Source"
        assert meta["pixel_size_um"] == pytest.approx(172.0)
        assert meta["sensor_thickness_m"] == pytest.approx(320e-6)
        assert meta["tau_s"] == pytest.approx(383.8e-9)
        assert meta["count_cutoff"] == 1302749
        assert meta["gain_setting"] == "high gain (vrf = -0.150)"
        assert meta["angle_increment_deg"] == pytest.approx(0.1)
        # From the private tag, which the text header has no slot for.
        assert meta["series_unique_id"] == "01ABCDEF"
        assert meta["series_number"] == 7
        assert meta["lost_pixel_count"] == 21

    def test_a_tiff_with_no_metadata_still_says_who_made_it(self, tmp_path: Path) -> None:
        path = tmp_path / "bare.tiff"
        fmt._write_tiff(path, np.zeros((4, 4), dtype=np.int32), None)
        with tifffile.TiffFile(path) as tiff:
            description = tiff.pages[0].description or ""
        assert "derived data, not raw detector output" in description


class TestHdf5DetectorCarryover:
    @pytest.fixture
    def source(self, tmp_path: Path) -> Path:
        path = tmp_path / "src.h5"
        with h5py.File(path, "w") as h5:
            det = h5.require_group("/entry/instrument/detector")
            det["description"] = "EIGER2 CdTe 4M"
            det["sensor_thickness"] = 750e-6
            det["count_time"] = 0.0099
            det["frame_time"] = 0.01
            det["saturation_value"] = 65535
            det["x_pixel_size"] = 75e-6
            det.create_dataset("pixel_mask", data=np.zeros((512, 512), dtype=np.uint32))
            det.create_dataset("big_correction", data=np.zeros(99999, dtype=np.float32))
            det["sensor_thickness"].attrs["units"] = "m"
            beam = h5.require_group("/entry/instrument/beam")
            beam["incident_wavelength"] = 1.0332
            beam["incident_wavelength"].attrs["units"] = "angstrom"
        return path

    def _copy(self, source: Path, dest: Path) -> None:
        from backend.services.series_summing import SeriesSummingService

        svc = SeriesSummingService.__new__(SeriesSummingService)
        svc._deps = SimpleNamespace(get_h5py=lambda: h5py)
        with h5py.File(source, "r") as src, h5py.File(dest, "w") as dst:
            svc._copy_h5_metadata(src, dst, 1)

    @pytest.mark.parametrize(
        ("path", "expected"),
        [
            ("/entry/instrument/detector/description", "EIGER2 CdTe 4M"),
            ("/entry/instrument/detector/sensor_thickness", 750e-6),
            ("/entry/instrument/detector/count_time", 0.0099),
            ("/entry/instrument/detector/frame_time", 0.01),
            ("/entry/instrument/detector/saturation_value", 65535),
            ("/entry/instrument/beam/incident_wavelength", 1.0332),
        ],
    )
    def test_the_detector_and_beam_facts_survive(
        self, source: Path, tmp_path: Path, path: str, expected: object
    ) -> None:
        dest = tmp_path / "out.h5"
        self._copy(source, dest)
        with h5py.File(dest, "r") as h5:
            assert path in h5, f"{path} was dropped"
            value = h5[path][()]
            if isinstance(expected, str):
                decoded = value.decode() if isinstance(value, bytes) else str(value)
                assert decoded == expected
            else:
                assert float(value) == pytest.approx(expected)

    def test_units_travel_with_the_value(self, source: Path, tmp_path: Path) -> None:
        # A thickness without its unit is a number nobody can use.
        dest = tmp_path / "out.h5"
        self._copy(source, dest)
        with h5py.File(dest, "r") as h5:
            assert h5["/entry/instrument/detector/sensor_thickness"].attrs["units"] == "m"
            assert h5["/entry/instrument/beam/incident_wavelength"].attrs["units"] == "angstrom"

    def test_the_pixel_mask_and_bulk_arrays_are_left_behind(
        self, source: Path, tmp_path: Path
    ) -> None:
        """Metadata, not data: the mask describes the source array anyway."""
        dest = tmp_path / "out.h5"
        self._copy(source, dest)
        with h5py.File(dest, "r") as h5:
            assert "/entry/instrument/detector/pixel_mask" not in h5
            assert "/entry/instrument/detector/big_correction" not in h5

    def test_a_value_already_written_is_not_overwritten(self, source: Path, tmp_path: Path) -> None:
        """The geometry ALBIS writes must win over the source's stale copy."""
        dest = tmp_path / "out.h5"
        with h5py.File(dest, "w") as dst:
            dst.require_group("/entry/instrument/detector")["x_pixel_size"] = 999.0
        from backend.services.series_summing import SeriesSummingService

        svc = SeriesSummingService.__new__(SeriesSummingService)
        svc._deps = SimpleNamespace(get_h5py=lambda: h5py)
        with h5py.File(source, "r") as src, h5py.File(dest, "a") as dst:
            svc._copy_h5_metadata(src, dst, 1)
        with h5py.File(dest, "r") as h5:
            assert float(h5["/entry/instrument/detector/x_pixel_size"][()]) == 999.0
