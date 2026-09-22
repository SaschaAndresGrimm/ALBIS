"""Pydantic request/response contracts for ALBIS HTTP APIs."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HealthResponse(_StrictModel):
    status: str
    version: str
    # The build behind `version`, empty when unstamped. A version number cannot
    # distinguish two builds of the same release, which is what a bug report and
    # the interface's own upgrade detection both need.
    commit: str = ""
    # Response encodings this build can actually produce, best first. zstd needs a
    # native extension that a packaged build can silently fail to bundle, in which
    # case remote sessions quietly fall back to gzip — reporting it here makes that
    # visible to the packaged-binary smoke test and to support.
    compression_encodings: list[str] = []


class UpdateCheckResponse(_StrictModel):
    status: Literal["update_available", "up_to_date", "unavailable"]
    current_version: str
    latest_version: str
    release_url: str
    message: str = ""
    # How ALBIS was installed, so the interface can tell the user what to do
    # rather than only that something is available. One of the values in
    # `backend/install_kind.py`; an interface that does not recognise it falls
    # back to the releases page.
    install_kind: str = ""
    # The single release asset for this install, resolved from the release's
    # asset list. Empty when nothing matched, when the install updates by
    # command instead (Docker, a checkout), or when there is no update.
    download_url: str = ""
    download_name: str = ""
    # The copyable shell line for installs a download cannot update.
    update_command: str = ""
    # Whether this build will fetch the asset itself and verify it, or only
    # hand over the link. Follows `ui.allow_update_download`, and is false
    # whenever there is no asset to fetch in the first place.
    download_supported: bool = False
    # Whether this build could apply a verified download and close itself.
    # Says nothing about whether one is ready now -- only that the platform,
    # the setting and the launcher all allow it, so the interface knows
    # whether to offer the step at all.
    apply_supported: bool = False


class UpdateDownloadStartRequest(_StrictModel):
    # The interface echoes back the asset the check offered rather than naming
    # its own: both are validated again here, because a request body is not
    # evidence that the backend ever proposed this file.
    url: str
    name: str


class UpdateApplyStatusResponse(_StrictModel):
    status: Literal["idle", "applying", "applied", "failed"]
    message: str = ""
    # Why applying is not available right now, as a code the interface
    # translates: "disabled", "unsupported_install", "no_verified_download",
    # "unverified_download", "busy", "target_unknown", "shutdown_unavailable".
    # Empty when an apply would be accepted.
    refusal: str = ""


class UpdateApplyStartRequest(_StrictModel):
    # What the interface has in progress, so the backend can refuse over live
    # work. Sent rather than inferred because the session lives in the browser:
    # which file is open and whether a watch is running are not facts the
    # backend holds.
    busy: list[str] = Field(default_factory=list)


class UpdateDownloadStatusResponse(_StrictModel):
    status: Literal["idle", "downloading", "verifying", "ready", "failed", "cancelled"]
    name: str = ""
    # The file's location on this machine once it is ready, for display. The
    # interface never sends a path back; opening the file goes through the
    # backend's own record of what it downloaded.
    path: str = ""
    bytes_downloaded: int = 0
    bytes_total: int = 0
    sha256: str = ""
    checksum: Literal["pending", "verified", "mismatch", "unavailable"] = "pending"
    signature: Literal["pending", "verified", "invalid", "unavailable"] = "pending"
    message: str = ""


class SettingsPayloadResponse(_StrictModel):
    config: dict[str, Any]
    defaults: dict[str, Any]
    # The paths ALBIS is actually using, for the keys whose default is not a
    # value but a rule: leaving `data.root` or `logging.dir` empty means "work
    # it out at start", and what it works out depends on whether this is a
    # packaged build and where the config file sits. `defaults` cannot say --
    # it holds the empty string those keys really default to -- so the
    # interface had nothing to show and showed nothing.
    effective: dict[str, Any] = Field(default_factory=dict)
    path: str
    restart_required: bool
    # `section.key` names the environment is deciding. Saving the file cannot
    # change these, so the interface shows them as not editable instead of
    # accepting an edit the next start would ignore.
    env_overrides: list[str] = Field(default_factory=list)


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


class JungfraujochProbeResponse(_StrictModel):
    """Reachability check for a preview endpoint — TCP only, not a protocol check."""

    status: Literal["ok", "error"]
    code: str
    endpoint: str = ""
    host: str = ""
    port: int | None = None
    message: str = ""
    timeout_s: float | None = None


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
    # True when the scan hit its entry or time budget and stopped early, so the
    # list is "what fitted", not "what is there". See data.max_scan_entries.
    truncated: bool = False


class SeriesInfoResponse(_StrictModel):
    files: list[str]
    index: int
    series: bool


class FoldersListResponse(_StrictModel):
    folders: list[str]
    truncated: bool = False


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
    # True when this directory holds more entries than one listing may visit.
    truncated: bool = False


class AutoloadLatestResponse(_StrictModel):
    file: str
    ext: str
    mtime: float
    absolute: bool
    truncated: bool = False


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
    # `children` is capped, so a group past the cap reports its real size here
    # and the inspector says the listing is partial instead of implying it is all.
    childCount: int | None = None
    truncated: bool | None = None


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
    # True when a writer still holds the file, so `shape` is the count so far
    # rather than the count. A client watching an acquisition asks again; one
    # looking at a finished file does not need to.
    writer_present: bool = False
