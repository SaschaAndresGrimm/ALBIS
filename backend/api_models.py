"""Pydantic request/response contracts for ALBIS HTTP APIs."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class _StrictModel(BaseModel):
    class Config:
        extra = "forbid"


class HealthResponse(_StrictModel):
    status: str
    version: str


class UpdateCheckResponse(_StrictModel):
    status: Literal["update_available", "up_to_date", "unavailable"]
    current_version: str
    latest_version: str
    release_url: str
    message: str = ""


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
    opened: bool = True


class LogTailResponse(_StrictModel):
    path: str
    text: str = ""
    requested_lines: int
    returned_lines: int
    truncated: bool = False
    size_bytes: int = 0
    modified_at: float | None = None


class HandoffJobCreateRequest(_StrictModel):
    manifest_path: str


class HandoffJobResponse(_StrictModel):
    id: int
    manifest_path: str
    open_path: str = ""
    dataset: str = ""
    run_id: str = ""


class AnalysisParamsResponse(_StrictModel):
    distance_mm: float | None = None
    # pixel_size_um is the reference (fast / X-axis) pixel size, kept for
    # backwards compatibility. pixel_size_x_um / pixel_size_y_um carry the
    # per-axis sizes so anisotropic ("strixel") detectors render correctly.
    pixel_size_um: float | None = None
    pixel_size_x_um: float | None = None
    pixel_size_y_um: float | None = None
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
    normalize_method: str = "none"
    normalize_frame: int | None = None
    normalize_scalar: float | None = None
    normalize_image: str | None = None
    range_start: int | None = None
    range_end: int | None = None
    output_path: str | None = None
    format: str = "hdf5"
    apply_mask: bool = True
    geometry: ImageGeometryResponse | None = None
    distance_mm: float | None = None
    pixel_size_um: float | None = None
    energy_ev: float | None = None
    center_x_px: float | None = None
    center_y_px: float | None = None


class SeriesSumStartResponse(_StrictModel):
    job_id: str
    status: str


class SeriesSumCancelRequest(_StrictModel):
    job_id: str


class SeriesSumCancelResponse(_StrictModel):
    job_id: str
    status: str
    accepted: bool


class DataExportStartRequest(_StrictModel):
    file: str
    dataset: str = ""
    format: str = "tiff"
    output_dir: str | None = None
    output_prefix: str | None = None
    frame_mode: str = "all"
    frame_start: int | None = None
    frame_end: int | None = None
    threshold_mode: str = "current"
    threshold_index: int | None = None
    overwrite: bool = False


class DataExportStartResponse(_StrictModel):
    job_id: str
    status: str


class DataExportCancelRequest(_StrictModel):
    job_id: str


class DataExportCancelResponse(_StrictModel):
    job_id: str
    status: str
    accepted: bool


class ImageHeaderResponse(_StrictModel):
    header: str


class ImageGeometryPanelResponse(_StrictModel):
    name: str
    origin_mm: list[float] = Field(default_factory=list)
    fast_axis: list[float] = Field(default_factory=list)
    slow_axis: list[float] = Field(default_factory=list)
    pixel_size_mm: list[float] = Field(default_factory=list)
    image_size_px: list[int] = Field(default_factory=list)
    raw_offset_px: list[float] = Field(default_factory=list)


class ImageGeometryResponse(_StrictModel):
    mode: Literal["planar", "geometry"] = "planar"
    detector: str = ""
    source: str = ""
    panels: list[ImageGeometryPanelResponse] = Field(default_factory=list)


class SimplonModeResponse(StatusResponse):
    mode: str


class SimplonProbeResponse(_StrictModel):
    """Result of a SIMPLON connection test — a diagnosis, not a transport error."""

    status: Literal["ok", "error"]
    code: str
    url: str = ""
    api_version: str = ""
    # Set only when api_version differs from what the caller asked for, i.e. the
    # configured version was absent and a known alternative answered.
    requested_version: str = ""
    message: str = ""
    detector: str = ""
    serial: str = ""
    port: int | None = None
    http_status: int | None = None
    timeout_s: float | None = None


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


class JungfraujochPreviewStartRequest(_StrictModel):
    endpoint: str
    source_id: str = "jungfraujoch"
    topic: str = ""
    channel: str = ""


class JungfraujochPreviewControlResponse(StatusResponse):
    running: bool
    source_id: str


class JungfraujochPreviewStatusResponse(_StrictModel):
    running: bool
    endpoint: str = ""
    source_id: str = "jungfraujoch"
    topic: str = ""
    channel: str = ""
    started_at: float | None = None
    last_message_at: float | None = None
    last_frame_at: float | None = None
    last_frame_seq: int = 0
    ingested_frames: int = 0
    series_number: int | None = None
    image_number: int | None = None
    display_name: str = ""
    last_error: str = ""


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


class BrowseFileItem(_StrictModel):
    name: str
    path: str
    ext: str
    mtime: float
    sizeBytes: int = 0
    isSeriesLead: bool = False
    seriesCount: int = 1


class BrowseResponse(_StrictModel):
    folders: list[str]
    files: list[str]
    fileItems: list[BrowseFileItem] = Field(default_factory=list)
    currentPath: str
    parentPath: str = ""
    root: str
    canGoUp: bool
    allowAbsolutePaths: bool
    requestedPathMissing: bool = False


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
