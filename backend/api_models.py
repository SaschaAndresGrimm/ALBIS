from __future__ import annotations

"""Pydantic request/response contracts for ALBIS HTTP APIs."""

from typing import Any

from pydantic import BaseModel, Field


class _StrictModel(BaseModel):
    class Config:
        extra = "forbid"


class HealthResponse(_StrictModel):
    status: str
    version: str


class SettingsPayloadResponse(_StrictModel):
    config: dict[str, Any]
    defaults: dict[str, Any]
    path: str
    restart_required: bool


class SettingsSaveRequest(_StrictModel):
    config: dict[str, Any]


class ClientLogRequest(_StrictModel):
    level: str = "info"
    message: str = ""
    context: Any | None = None
    url: str | None = None
    userAgent: str | None = None
    extra: Any | None = None


class StatusResponse(_StrictModel):
    status: str


class PathStatusResponse(StatusResponse):
    path: str


class OpenPathRequest(_StrictModel):
    path: str


class AnalysisParamsResponse(_StrictModel):
    distance_mm: float | None = None
    pixel_size_um: float | None = None
    energy_ev: float | None = None
    center_x_px: float | None = None
    center_y_px: float | None = None
    shape: list[int] | None = None


class SeriesSumStartRequest(_StrictModel):
    file: str
    dataset: str = ""
    mode: str = "all"
    step: int = 10
    operation: str = "sum"
    normalize_frame: int | None = None
    range_start: int | None = None
    range_end: int | None = None
    output_path: str | None = None
    format: str = "hdf5"
    apply_mask: bool = True


class SeriesSumStartResponse(_StrictModel):
    job_id: str
    status: str


class SeriesSumCancelRequest(_StrictModel):
    job_id: str


class SeriesSumCancelResponse(_StrictModel):
    job_id: str
    status: str
    accepted: bool


class ImageHeaderResponse(_StrictModel):
    header: str


class SimplonModeResponse(StatusResponse):
    mode: str


class RemoteFrameIngestResponse(StatusResponse):
    source_id: str
    seq: int


class RemoteMetaResponse(_StrictModel):
    source_id: str
    seq: int
    updated_at: float | None = None
    display_name: str = ""
    series_number: int | None = None
    image_number: int | None = None
    image_datetime: str = ""
    resolution: dict[str, Any] = Field(default_factory=dict)
    peak_sets: list[dict[str, Any]] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)


class RemoteMetaConflictResponse(_StrictModel):
    detail: str
    current_seq: int


class FilesListResponse(_StrictModel):
    files: list[str]


class SeriesInfoResponse(_StrictModel):
    files: list[str]
    index: int
    series: bool


class FoldersListResponse(_StrictModel):
    folders: list[str]


class PathSelectionResponse(_StrictModel):
    path: str


class BrowseResponse(_StrictModel):
    folders: list[str]
    files: list[str]
    currentPath: str
    root: str
    canGoUp: bool
    allowAbsolutePaths: bool


class AutoloadLatestResponse(_StrictModel):
    file: str
    ext: str
    mtime: float
    absolute: bool


class UploadResponse(_StrictModel):
    filename: str
    path: str


class HDF5DatasetEntry(_StrictModel):
    path: str
    shape: list[int] = Field(default_factory=list)
    dtype: str
    ndim: int
    size: int | None = None
    chunks: Any | None = None
    maxshape: Any | None = None
    image: bool | None = None
    linked_stack: bool | None = None
    members: list[str] | None = None


class HDF5DatasetsResponse(_StrictModel):
    datasets: list[HDF5DatasetEntry]


class HDF5TreeChild(_StrictModel):
    name: str
    path: str
    type: str
    hasChildren: bool | None = None
    shape: list[int] | None = None
    dtype: str | None = None
    link: str | None = None
    target: str | None = None


class HDF5TreeResponse(_StrictModel):
    path: str
    children: list[HDF5TreeChild]


class HDF5AttrItem(_StrictModel):
    name: str
    value: Any


class HDF5NodeResponse(_StrictModel):
    path: str
    type: str
    attrs: list[HDF5AttrItem] = Field(default_factory=list)
    shape: list[int] | None = None
    dtype: str | None = None
    preview: Any | None = None


class HDF5ValueResponse(_StrictModel):
    path: str
    type: str
    shape: list[int]
    dtype: str
    preview: Any | None = None
    preview_shape: list[int] | None = None
    truncated: bool
    slice: dict[str, Any] | None = None


class HDF5SearchResponse(_StrictModel):
    matches: list[HDF5TreeChild]


class FrameMetadataResponse(_StrictModel):
    path: str
    shape: list[int]
    dtype: str
    ndim: int
    chunks: Any | None = None
    maxshape: Any | None = None
    linked_stack: bool
    threshold_energies: list[float | None] | None = None
