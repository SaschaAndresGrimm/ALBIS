/**
 * Overview minimap pointer interaction bindings.
 */

export function bindOverviewInteractions({
  state,
  overviewState,
  elements,
  callbacks,
}) {
  const { overviewCanvas } = elements;
  if (!overviewCanvas) return;

  const {
    overviewEventToImage,
    overviewEventToOverview,
    getViewRect,
    getOverviewHandleAt,
    getAnchorForHandle,
    resizeViewFromHandle,
    panToImageCenter,
  } = callbacks;

  function stopOverviewDrag(event) {
    if (!overviewState.dragging) return;
    overviewState.dragging = false;
    overviewState.dragMode = null;
    overviewState.handle = null;
    overviewState.anchor = null;
    overviewState.resizeCenter = false;
    overviewCanvas.classList.remove("is-dragging");
    overviewCanvas.style.cursor = "";
    if (event && overviewCanvas.hasPointerCapture(event.pointerId)) {
      overviewCanvas.releasePointerCapture(event.pointerId);
    }
  }

  overviewCanvas.addEventListener("pointerdown", (event) => {
    if (!state.hasFrame) return;
    const point = overviewEventToImage(event);
    const overviewPoint = overviewEventToOverview(event);
    const view = getViewRect();
    if (!point || !overviewPoint || !view) return;

    const handle = getOverviewHandleAt(overviewPoint);
    if (handle) {
      overviewState.dragMode = "resize";
      overviewState.handle = handle;
      overviewState.resizeCenter = event.altKey;
      overviewState.anchor = getAnchorForHandle(view, handle, overviewState.resizeCenter);
      overviewState.dragOffset = { x: 0, y: 0 };
    } else {
      overviewState.dragMode = "move";
      overviewState.resizeCenter = false;
      const inView =
        point.x >= view.viewX &&
        point.x <= view.viewX + view.viewW &&
        point.y >= view.viewY &&
        point.y <= view.viewY + view.viewH;
      if (inView) {
        const centerX = view.viewX + view.viewW / 2;
        const centerY = view.viewY + view.viewH / 2;
        overviewState.dragOffset = { x: point.x - centerX, y: point.y - centerY };
      } else {
        overviewState.dragOffset = { x: 0, y: 0 };
      }
    }

    overviewState.dragging = true;
    overviewCanvas.style.cursor = "";
    overviewCanvas.classList.add("is-dragging");
    overviewCanvas.setPointerCapture(event.pointerId);
    if (overviewState.dragMode === "resize") {
      resizeViewFromHandle(point, overviewState.handle, overviewState.resizeCenter);
    } else {
      panToImageCenter(point.x - overviewState.dragOffset.x, point.y - overviewState.dragOffset.y);
    }
  });

  overviewCanvas.addEventListener("pointermove", (event) => {
    if (!state.hasFrame) return;
    if (!overviewState.dragging) {
      const overviewPoint = overviewEventToOverview(event);
      if (!overviewPoint) return;
      const handle = getOverviewHandleAt(overviewPoint);
      if (handle) {
        overviewCanvas.style.cursor =
          handle === "nw" || handle === "se" ? "nwse-resize" : "nesw-resize";
      } else {
        overviewCanvas.style.cursor = "";
      }
      return;
    }

    const point = overviewEventToImage(event);
    if (!point) return;
    if (overviewState.dragMode === "resize") {
      resizeViewFromHandle(point, overviewState.handle, overviewState.resizeCenter);
    } else {
      panToImageCenter(point.x - overviewState.dragOffset.x, point.y - overviewState.dragOffset.y);
    }
  });

  overviewCanvas.addEventListener("pointerup", (event) => {
    stopOverviewDrag(event);
  });

  overviewCanvas.addEventListener("pointercancel", (event) => {
    stopOverviewDrag(event);
  });

  overviewCanvas.addEventListener("pointerleave", () => {
    if (!overviewState.dragging) {
      overviewCanvas.style.cursor = "";
    }
  });
}
