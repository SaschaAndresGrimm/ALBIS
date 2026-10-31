"""The simulated JUNGFRAUJOCH publisher speaks what ALBIS reads.

test_scripts/fake_jfjoch_publisher.py feeds `npm run ui:live` and anyone
trying the JUNGFRAUJOCH source without a detector. If its messages drifted
from what the bridge decodes, the live check would fail for the wrong reason,
or pass while the bridge no longer read a real stream's shape.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import cbor2

from backend.services.jungfraujoch_preview import (
    JungfraujochPreviewBridge,
    _BridgeConfig,
    jfjoch_tag_hook,
)

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "test_scripts"))
from fake_jfjoch_publisher import image_message, start_message  # noqa: E402


def test_an_image_from_the_publisher_becomes_a_frame_with_its_spots() -> None:
    stored: list[dict[str, Any]] = []
    bridge = JungfraujochPreviewBridge(
        logger=logging.getLogger("test.fake_jfjoch"),
        remote_store_frame=lambda **kwargs: stored.append(kwargs) or len(stored),
    )
    config = _BridgeConfig(
        endpoint="tcp://127.0.0.1:5400", source_id="jfjoch", topic="", channel=""
    )

    bridge._apply_start_message(cbor2.loads(start_message(3), tag_hook=jfjoch_tag_hook))
    bridge._apply_image_message(
        config, cbor2.loads(image_message(3, 7), tag_hook=jfjoch_tag_hook), 1000.0
    )

    assert len(stored) == 1
    frame = stored[0]["frame"]
    assert frame.shape == (256, 256)
    assert str(frame.dtype) == "uint16"
    meta = stored[0]["meta"]
    assert meta["series_number"] == 3
    assert meta["image_number"] == 7
    assert sum(len(peaks["points"]) for peaks in meta["peak_sets"]) == 2
