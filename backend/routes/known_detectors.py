"""Known detectors: the shared list under the SIMPLON address fields.

One list per ALBIS server, the same for everyone using it; see
``services/known_detectors.py`` for where it is kept and its rules. Every route
answers with the whole list, so a client simply shows what it gets back.
Changes are POST/PATCH/DELETE and so pass the request guard's cross-site check.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException, Query

from ..api_models import (
    KnownDetectorImportRequest,
    KnownDetectorSeenRequest,
    KnownDetectorsResponse,
    KnownDetectorUpdateRequest,
)
from ..services import known_detectors
from ..services.known_detectors import KnownDetectorStore


@dataclass(frozen=True)
class KnownDetectorRouteDeps:
    logger: Any
    store: KnownDetectorStore


def register_known_detector_routes(app: FastAPI, deps: KnownDetectorRouteDeps) -> None:
    def answer(entries: list[dict[str, Any]]) -> KnownDetectorsResponse:
        return KnownDetectorsResponse(detectors=entries)

    def change(apply) -> KnownDetectorsResponse:
        try:
            return answer(deps.store.change(apply))
        except OSError as exc:
            deps.logger.warning("Known detectors: cannot write %s: %s", deps.store.path, exc)
            raise HTTPException(
                status_code=500, detail=f"Cannot save the detector list ({deps.store.path})."
            ) from exc

    def address_of(raw: str) -> str:
        address = known_detectors.normalize_address(raw)
        if not address:
            raise HTTPException(status_code=400, detail="Not a detector address.")
        return address

    @app.get("/api/known-detectors", response_model=KnownDetectorsResponse)
    def known_detectors_list() -> KnownDetectorsResponse:
        return answer(deps.store.list())

    @app.post("/api/known-detectors/seen", response_model=KnownDetectorsResponse)
    def known_detectors_seen(payload: KnownDetectorSeenRequest) -> KnownDetectorsResponse:
        address = address_of(payload.address)
        return change(
            lambda entries: known_detectors.record(entries, address, payload.model, payload.serial)
        )

    @app.patch("/api/known-detectors", response_model=KnownDetectorsResponse)
    def known_detectors_update(payload: KnownDetectorUpdateRequest) -> KnownDetectorsResponse:
        address = address_of(payload.address)
        changes = payload.model_dump(include={"name", "kept"}, exclude_none=True)
        return change(lambda entries: known_detectors.update(entries, address, changes))

    @app.delete("/api/known-detectors", response_model=KnownDetectorsResponse)
    def known_detectors_forget(
        address: str = Query(..., min_length=1, max_length=512),
    ) -> KnownDetectorsResponse:
        at = address_of(address)
        return change(lambda entries: known_detectors.forget(entries, at))

    @app.post("/api/known-detectors/import", response_model=KnownDetectorsResponse)
    def known_detectors_import(payload: KnownDetectorImportRequest) -> KnownDetectorsResponse:
        return change(lambda entries: known_detectors.merge(entries, payload.detectors))
