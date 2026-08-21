"""Cover the gate in front of series summing.

`POST /api/analysis/series-sum/start` accepts a request and hands it to a
background job that reads a whole stack and writes a file. Everything the job
would otherwise discover the hard way is checked here first, and none of those
twenty-odd rules was tested: a regression would not fail loudly, it would start
a long job with nonsense parameters and leave a wrong output file behind.

The routes are registered against fake dependencies rather than the real app, so
these tests exercise the validation and nothing else -- no stack is read and no
file is written.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routes.analysis import AnalysisRouteDeps, register_analysis_routes


class _FakeJobs:
    """Just enough of the series-summing service to answer the routes."""

    def __init__(self) -> None:
        self.started: list[dict[str, Any]] = []
        self.jobs: dict[str, dict[str, Any]] = {}
        self.cancelled: list[str] = []
        self.cancel_result = True

    def start(self, **kwargs: Any) -> str:
        job_id = f"job-{len(self.started) + 1}"
        self.started.append(kwargs)
        self.jobs[job_id] = {"job_id": job_id, "status": "queued", "progress": 0.0}
        return job_id

    def get(self, job_id: str) -> dict[str, Any] | None:
        return self.jobs.get(job_id)

    def cancel(self, job_id: str) -> bool:
        self.cancelled.append(job_id)
        if self.cancel_result:
            self.jobs[job_id] = {**self.jobs.get(job_id, {}), "status": "cancelled"}
        return self.cancel_result


@pytest.fixture
def jobs() -> _FakeJobs:
    return _FakeJobs()


@pytest.fixture
def client(jobs: _FakeJobs) -> TestClient:
    app = FastAPI()
    register_analysis_routes(
        app,
        AnalysisRouteDeps(
            ensure_hdf5_stack=lambda: None,
            get_h5py=lambda: None,
            resolve_file=Path,
            resolve_optional_path=Path,
            resolve_dataset_view=lambda *a, **k: ({}, []),
            read_scalar=lambda *a, **k: (None, None),
            image_ext_name=lambda name: Path(name).suffix.lower(),
            pilatus_meta_from_image=lambda path: {},
            to_mm=lambda value, unit: value,
            to_um=lambda value, unit: value,
            to_ev=lambda value, unit: value,
            wavelength_to_ev=lambda value, unit: value,
            norm_unit=lambda unit: (unit or "").lower(),
            read_threshold_energies=lambda *a, **k: [],
            start_series_sum_job=jobs.start,
            get_series_sum_job=jobs.get,
            cancel_series_sum_job=jobs.cancel,
        ),
    )
    return TestClient(app)


def _request(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "file": "/data/series_master.h5",
        "dataset": "/entry/data/data",
        "mode": "all",
        "operation": "sum",
        "format": "hdf5",
    }
    payload.update(overrides)
    return payload


def _start(client: TestClient, **overrides: Any):
    return client.post("/api/analysis/series-sum/start", json=_request(**overrides))


# --------------------------------------------------------------------------
# What must be present
# --------------------------------------------------------------------------


def test_a_valid_request_is_queued(client: TestClient, jobs: _FakeJobs) -> None:
    response = _start(client)

    assert response.status_code == 200
    assert response.json() == {"job_id": "job-1", "status": "queued"}
    assert len(jobs.started) == 1


def test_a_missing_file_is_rejected(client: TestClient, jobs: _FakeJobs) -> None:
    response = _start(client, file="   ")

    assert response.status_code == 400
    assert "file" in response.json()["detail"].lower()
    assert jobs.started == []


def test_hdf5_without_a_dataset_is_rejected(client: TestClient) -> None:
    """A stack has many datasets; summing "the file" is not a request."""
    response = _start(client, dataset="")

    assert response.status_code == 400
    assert "dataset" in response.json()["detail"].lower()


def test_an_image_series_needs_no_dataset(client: TestClient) -> None:
    response = _start(client, file="/data/frame_0001.cbf", dataset="")

    assert response.status_code == 200


# --------------------------------------------------------------------------
# Vocabularies
# --------------------------------------------------------------------------


@pytest.mark.parametrize("mode", ["all", "chunks", "nth", "range"])
def test_every_documented_mode_is_accepted(client: TestClient, mode: str) -> None:
    assert _start(client, mode=mode).status_code == 200


def test_step_is_accepted_as_an_alias_for_chunks(client: TestClient, jobs: _FakeJobs) -> None:
    """Older clients say `step`; the service only knows `chunks`."""
    assert _start(client, mode="step").status_code == 200
    assert jobs.started[-1]["mode"] == "chunks"


@pytest.mark.parametrize("mode", ["every", "sum", ""])
def test_an_unknown_mode_is_rejected(client: TestClient, mode: str) -> None:
    response = _start(client, mode=mode)

    assert response.status_code == 400
    assert "mode" in response.json()["detail"].lower()


@pytest.mark.parametrize("mode", ["ALL", " all ", "Range"])
def test_case_and_whitespace_in_a_mode_are_forgiven(client: TestClient, mode: str) -> None:
    """Deliberate: the vocabulary is matched on the normalized value."""
    assert _start(client, mode=mode).status_code == 200


@pytest.mark.parametrize("operation", ["sum", "mean", "median"])
def test_every_documented_operation_is_accepted(client: TestClient, operation: str) -> None:
    assert _start(client, operation=operation).status_code == 200


@pytest.mark.parametrize("operation", ["max", "average", ""])
def test_an_unknown_operation_is_rejected(client: TestClient, operation: str) -> None:
    assert _start(client, operation=operation).status_code == 400


@pytest.mark.parametrize("output_format", ["hdf5", "h5", "tiff", "tif"])
def test_every_documented_output_format_is_accepted(client: TestClient, output_format: str) -> None:
    assert _start(client, format=output_format).status_code == 200


@pytest.mark.parametrize("output_format", ["cbf", "png", "jpeg"])
def test_an_unsupported_output_format_is_rejected(client: TestClient, output_format: str) -> None:
    assert _start(client, format=output_format).status_code == 400


def test_an_empty_output_format_means_the_default(client: TestClient, jobs: _FakeJobs) -> None:
    assert _start(client, format="").status_code == 200
    assert jobs.started[-1]["output_format"] == "hdf5"


# --------------------------------------------------------------------------
# Numbers
# --------------------------------------------------------------------------


@pytest.mark.parametrize("step", [0, -1])
def test_a_step_below_one_is_rejected(client: TestClient, step: int) -> None:
    """A zero step is an infinite loop dressed as a parameter.

    `step` used to be read as `payload.step or 10`, and the model already
    defaults it to 10 -- so the fallback never handled a missing value, it only
    rewrote an explicit `0` into `10` and made this rule unreachable for the one
    value a user is most likely to send by mistake.
    """
    response = _start(client, mode="chunks", step=step)

    assert response.status_code == 400
    assert "step" in response.json()["detail"].lower()


def test_an_omitted_step_uses_the_documented_default(client: TestClient, jobs: _FakeJobs) -> None:
    payload = _request(mode="chunks")
    payload.pop("step", None)

    assert client.post("/api/analysis/series-sum/start", json=payload).status_code == 200
    assert jobs.started[-1]["step"] == 10


def test_frame_numbers_are_one_based_so_zero_is_rejected(client: TestClient) -> None:
    assert _start(client, mode="range", range_start=0).status_code == 400
    assert _start(client, mode="range", range_end=0).status_code == 400


def test_a_reversed_range_is_rejected(client: TestClient) -> None:
    response = _start(client, mode="range", range_start=9, range_end=4)

    assert response.status_code == 400
    assert "range" in response.json()["detail"].lower()


def test_a_single_frame_range_is_accepted(client: TestClient) -> None:
    assert _start(client, mode="range", range_start=4, range_end=4).status_code == 200


# --------------------------------------------------------------------------
# Normalization
# --------------------------------------------------------------------------


def test_normalizing_by_a_frame_needs_the_frame(client: TestClient) -> None:
    response = _start(client, normalize_method="frame")

    assert response.status_code == 400
    assert "frame" in response.json()["detail"].lower()


def test_a_normalize_frame_alone_still_means_normalize_by_frame(
    client: TestClient, jobs: _FakeJobs
) -> None:
    """Backward compatibility: older clients send the frame and no method."""
    assert _start(client, normalize_frame=3).status_code == 200
    assert jobs.started[-1]["normalize_method"] == "frame"
    assert jobs.started[-1]["normalize_frame"] == 3


def test_a_normalize_frame_below_one_is_rejected(client: TestClient) -> None:
    assert _start(client, normalize_method="frame", normalize_frame=0).status_code == 400


def test_normalizing_by_a_scalar_needs_the_scalar(client: TestClient) -> None:
    assert _start(client, normalize_method="scalar").status_code == 400


def test_a_zero_scalar_is_rejected(client: TestClient) -> None:
    """Dividing every pixel by zero is not a normalization."""
    response = _start(client, normalize_method="scalar", normalize_scalar=0)

    assert response.status_code == 400
    assert "non-zero" in response.json()["detail"].lower()


def test_a_negative_scalar_is_allowed(client: TestClient) -> None:
    assert _start(client, normalize_method="scalar", normalize_scalar=-2.5).status_code == 200


def test_normalizing_by_an_image_needs_the_image(client: TestClient) -> None:
    assert _start(client, normalize_method="image", normalize_image="  ").status_code == 400


def test_an_unknown_normalization_method_is_rejected(client: TestClient) -> None:
    assert _start(client, normalize_method="magic").status_code == 400


# --------------------------------------------------------------------------
# Unknown fields
# --------------------------------------------------------------------------


def test_a_field_the_client_invented_is_refused(client: TestClient) -> None:
    """Strict request models: a typo must not be silently ignored."""
    response = client.post("/api/analysis/series-sum/start", json=_request(oepration="sum"))

    assert response.status_code == 422


# --------------------------------------------------------------------------
# Status and cancel
# --------------------------------------------------------------------------


def test_status_reports_a_known_job(client: TestClient) -> None:
    job_id = _start(client).json()["job_id"]

    response = client.get("/api/analysis/series-sum/status", params={"job_id": job_id})

    assert response.status_code == 200
    assert response.json()["status"] == "queued"


def test_status_of_an_unknown_job_is_a_404(client: TestClient) -> None:
    response = client.get("/api/analysis/series-sum/status", params={"job_id": "nope"})

    assert response.status_code == 404


def test_cancelling_a_running_job_is_accepted(client: TestClient, jobs: _FakeJobs) -> None:
    job_id = _start(client).json()["job_id"]

    response = client.post("/api/analysis/series-sum/cancel", json={"job_id": job_id})

    assert response.status_code == 200
    assert response.json()["accepted"] is True
    assert jobs.cancelled == [job_id]


@pytest.mark.parametrize("status", ["done", "error", "cancelled"])
def test_cancelling_a_finished_job_is_declined_not_an_error(
    client: TestClient, jobs: _FakeJobs, status: str
) -> None:
    """There is nothing to stop, and saying so beats pretending it worked."""
    job_id = _start(client).json()["job_id"]
    jobs.jobs[job_id]["status"] = status

    response = client.post("/api/analysis/series-sum/cancel", json={"job_id": job_id})

    assert response.status_code == 200
    assert response.json() == {"job_id": job_id, "status": status, "accepted": False}
    assert jobs.cancelled == []


def test_cancelling_an_unknown_job_is_a_404(client: TestClient) -> None:
    assert (
        client.post("/api/analysis/series-sum/cancel", json={"job_id": "nope"}).status_code == 404
    )


def test_cancelling_without_a_job_id_is_a_400(client: TestClient) -> None:
    assert client.post("/api/analysis/series-sum/cancel", json={"job_id": "  "}).status_code == 400


def test_a_declined_cancel_is_reported_as_declined(client: TestClient, jobs: _FakeJobs) -> None:
    job_id = _start(client).json()["job_id"]
    jobs.cancel_result = False

    response = client.post("/api/analysis/series-sum/cancel", json={"job_id": job_id})

    assert response.status_code == 200
    assert response.json()["accepted"] is False


# --------------------------------------------------------------------------
# What reaches the job
# --------------------------------------------------------------------------


def test_geometry_overrides_are_passed_through_to_the_job(
    client: TestClient, jobs: _FakeJobs
) -> None:
    """The summed output carries geometry, so what was sent has to arrive."""
    response = _start(
        client,
        distance_mm=150.0,
        pixel_size_um=75.0,
        energy_ev=12000.0,
        center_x_px=512.5,
        center_y_px=530.0,
        apply_mask=True,
        output_path="/data/out/summed.h5",
    )

    assert response.status_code == 200
    started = jobs.started[-1]
    assert started["distance_mm"] == 150.0
    assert started["pixel_size_um"] == 75.0
    assert started["energy_ev"] == 12000.0
    assert started["center_x_px"] == 512.5
    assert started["center_y_px"] == 530.0
    assert started["apply_mask"] is True
    assert started["output_path"] == "/data/out/summed.h5"
