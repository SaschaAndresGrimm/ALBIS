"""Cover the JUNGFRAUJOCH preview bridge without a publisher.

The bridge's own tests covered the control endpoints and two decode rejections.
What was untested is everything between a CBOR message arriving and a frame
reaching the viewer: the tag hook that turns typed and multidimensional arrays
into pixels, the channel selection for a multi-channel detector, the geometry
carried in a `start` message, and the spot lists drawn as overlays.

None of that needs ZeroMQ. The messages are built here and handed straight to
the handlers, which is also the only way to test the failure paths -- a real
publisher does not send a malformed frame on request.
"""

from __future__ import annotations

import logging
from typing import Any

import cbor2
import numpy as np
import pytest

from backend.services.jungfraujoch_preview import (
    _CBOR_DECODE_VALUE_ERROR,
    JungfraujochPreviewBridge,
    _as_float,
    _as_int,
    _BridgeConfig,
    _select_channel_image,
    _to_mm,
    _to_um,
    jfjoch_peak_sets_from_spots,
    jfjoch_tag_hook,
)

_TYPED_UINT16_LE = 69
_TYPED_UINT32_LE = 70
_MULTIDIM_ROW_MAJOR = 40
_MULTIDIM_COLUMN_MAJOR = 1040


# --------------------------------------------------------------------------
# Numbers on the wire
# --------------------------------------------------------------------------


@pytest.mark.parametrize("value,expected", [(1, 1.0), ("2.5", 2.5), (np.float32(3.5), 3.5)])
def test_as_float_accepts_what_a_cbor_message_can_carry(value: Any, expected: float) -> None:
    assert _as_float(value) == pytest.approx(expected)


@pytest.mark.parametrize("value", [None, "", "abc", float("nan"), float("inf"), {}, []])
def test_as_float_rejects_what_cannot_be_a_measurement(value: Any) -> None:
    """A NaN beam centre would travel all the way into the ring overlay."""
    assert _as_float(value) is None


def test_as_int_truncates_and_rejects_the_unusable() -> None:
    assert _as_int(7.9) == 7
    assert _as_int("12") == 12
    assert _as_int(None) is None
    assert _as_int(float("nan")) is None


def test_distances_arrive_in_metres_and_are_shown_in_mm() -> None:
    """JUNGFRAUJOCH reports metres; a value already in mm must not be scaled again."""
    assert _to_mm(0.15) == pytest.approx(150.0)
    assert _to_mm(150.0) == pytest.approx(150.0)
    assert _to_mm(None) is None


def test_pixel_sizes_arrive_in_metres_and_are_shown_in_um() -> None:
    assert _to_um(75e-6) == pytest.approx(75.0)
    assert _to_um(75.0) == pytest.approx(75.0)
    assert _to_um(None) is None


# --------------------------------------------------------------------------
# The CBOR tag hook
# --------------------------------------------------------------------------


def test_a_typed_array_tag_decodes_to_pixels() -> None:
    pixels = np.arange(6, dtype="<u2")
    decoded = jfjoch_tag_hook(cbor2.CBORTag(_TYPED_UINT16_LE, pixels.tobytes()), False)

    assert decoded.dtype == np.dtype("<u2")
    assert np.array_equal(decoded, pixels)


def test_a_typed_array_accepts_a_memoryview_payload() -> None:
    pixels = np.arange(4, dtype="<u4")
    decoded = jfjoch_tag_hook(cbor2.CBORTag(_TYPED_UINT32_LE, memoryview(pixels.tobytes())), False)

    assert np.array_equal(decoded, pixels)


def test_a_row_major_multidim_array_keeps_its_shape() -> None:
    payload = [[2, 3], list(range(6))]
    decoded = jfjoch_tag_hook(cbor2.CBORTag(_MULTIDIM_ROW_MAJOR, payload), False)

    assert decoded.shape == (2, 3)
    assert np.array_equal(decoded, np.arange(6).reshape(2, 3))


def test_a_column_major_multidim_array_is_read_column_first() -> None:
    """Getting this wrong transposes the image silently."""
    payload = [[2, 3], list(range(6))]
    decoded = jfjoch_tag_hook(cbor2.CBORTag(_MULTIDIM_COLUMN_MAJOR, payload), False)

    assert decoded.shape == (2, 3)
    assert np.array_equal(decoded, np.arange(6).reshape((2, 3), order="F"))


@pytest.mark.parametrize(
    "payload",
    [
        [[0, 3], [1, 2, 3]],
        [[-1, 3], [1, 2, 3]],
        [[], [1]],
        ["not-a-pair"],
        [[2, 2]],
    ],
)
def test_a_malformed_multidim_array_is_refused(payload: Any) -> None:
    with pytest.raises(_CBOR_DECODE_VALUE_ERROR):
        jfjoch_tag_hook(cbor2.CBORTag(_MULTIDIM_ROW_MAJOR, payload), False)


def test_an_unknown_tag_is_passed_through_untouched() -> None:
    """A tag ALBIS does not know is not an error; it is simply not pixels."""
    tag = cbor2.CBORTag(4242, {"anything": 1})

    assert jfjoch_tag_hook(tag, False) is tag


def test_a_whole_message_round_trips_through_cbor() -> None:
    """The path a real message actually takes: cbor2.loads with the hook."""
    pixels = np.arange(12, dtype="<u2").reshape(3, 4)
    encoded = cbor2.dumps(
        {
            "type": "image",
            "image_id": 7,
            "data": {
                "channel0": cbor2.CBORTag(_MULTIDIM_ROW_MAJOR, [[3, 4], pixels.ravel().tolist()])
            },
        }
    )

    message = cbor2.loads(encoded, tag_hook=jfjoch_tag_hook)

    assert message["image_id"] == 7
    assert np.array_equal(message["data"]["channel0"], pixels)


# --------------------------------------------------------------------------
# Channel selection
# --------------------------------------------------------------------------


def test_the_requested_channel_is_the_one_shown() -> None:
    message = {"data": {"raw": np.zeros((2, 2)), "corrected": np.ones((2, 2))}}

    frame, name = _select_channel_image(message, "corrected")

    assert name == "corrected"
    assert frame is not None and frame.mean() == pytest.approx(1.0)


def test_an_absent_channel_falls_back_to_the_first_one() -> None:
    """Better the wrong channel than no image, and the name says which it is."""
    message = {"data": {"raw": np.zeros((2, 2))}}

    frame, name = _select_channel_image(message, "does-not-exist")

    assert name == "raw"
    assert frame is not None


@pytest.mark.parametrize(
    "message", [{}, {"data": {}}, {"data": "not-a-map"}, {"data": {"a": None}}]
)
def test_a_message_with_no_usable_image_yields_nothing(message: dict[str, Any]) -> None:
    assert _select_channel_image(message, "raw") == (None, None)


# --------------------------------------------------------------------------
# Spot overlays
# --------------------------------------------------------------------------


def test_spots_are_split_into_indexed_and_unindexed_sets() -> None:
    spots = [
        {"x": 1.0, "y": 2.0, "I": 10.0, "indexed": True},
        {"x": 3.0, "y": 4.0, "I": 20.0, "indexed": False},
    ]

    sets = jfjoch_peak_sets_from_spots(spots)

    assert [entry["name"] for entry in sets] == ["Indexed reflections", "Reflections"]
    assert sets[0]["points"] == [[1.0, 2.0, 10.0]]
    assert sets[1]["points"] == [[3.0, 4.0, 20.0]]


def test_spots_without_coordinates_are_dropped() -> None:
    sets = jfjoch_peak_sets_from_spots(
        [{"I": 5.0}, {"x": 1.0}, {"y": 2.0}, {"x": "a", "y": "b"}, "not-a-spot"]
    )

    assert sum(len(entry["points"]) for entry in sets) == 0


def test_a_spot_list_that_is_not_a_list_is_no_spots() -> None:
    assert jfjoch_peak_sets_from_spots(None) == []
    assert jfjoch_peak_sets_from_spots({"x": 1}) == []


def test_an_enormous_spot_list_is_capped() -> None:
    """20000 overlay markers is already more than anyone can see."""
    spots = [{"x": float(i), "y": 0.0, "indexed": True} for i in range(25000)]

    sets = jfjoch_peak_sets_from_spots(spots)

    assert sum(len(entry["points"]) for entry in sets) <= 20000


# --------------------------------------------------------------------------
# From message to stored frame
# --------------------------------------------------------------------------


class _Store:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.raise_on_next = False

    def __call__(self, **kwargs: Any) -> int:
        if self.raise_on_next:
            raise RuntimeError("store is full")
        self.calls.append(kwargs)
        return len(self.calls)


@pytest.fixture
def bridge() -> tuple[JungfraujochPreviewBridge, _Store, _BridgeConfig]:
    store = _Store()
    instance = JungfraujochPreviewBridge(
        logger=logging.getLogger("test.jfjoch"),
        remote_store_frame=store,
    )
    config = _BridgeConfig(
        endpoint="tcp://127.0.0.1:5555",
        source_id="jfjoch",
        topic="",
        channel="channel0",
    )
    return instance, store, config


def _image_message(**overrides: Any) -> dict[str, Any]:
    message: dict[str, Any] = {
        "type": "image",
        "series_id": 4,
        "image_id": 11,
        "series_unique_id": "series-abc",
        "series_date": "2026-08-21T10:00:00Z",
        "data": {"channel0": np.arange(6, dtype="<u2").reshape(2, 3)},
    }
    message.update(overrides)
    return message


def test_an_image_message_is_stored_as_a_frame(bridge) -> None:
    instance, store, config = bridge

    instance._apply_image_message(config, _image_message(), 1000.0)

    assert len(store.calls) == 1
    call = store.calls[0]
    assert call["source_id"] == "jfjoch"
    assert call["frame"].shape == (2, 3)
    assert call["meta"]["series_number"] == 4
    assert call["meta"]["image_number"] == 11
    assert call["meta"]["extra"] == {"kind": "jungfraujoch_preview", "channel": "channel0"}


def test_the_display_name_says_which_series_and_image(bridge) -> None:
    instance, store, config = bridge

    instance._apply_image_message(config, _image_message(), 1000.0)

    name = store.calls[0]["meta"]["display_name"]
    assert "JUNGFRAUJOCH Preview (jfjoch)" in name
    assert "series-abc" in name
    assert "S4" in name
    assert "Img11" in name


def test_geometry_from_the_start_message_reaches_the_frame(bridge) -> None:
    """The start message carries the geometry; the image messages do not."""
    instance, store, config = bridge
    instance._apply_start_message(
        {
            "type": "start",
            "series_id": 4,
            "detector_distance": 0.15,
            "beam_center_x": 512.0,
            "beam_center_y": 530.0,
            "pixel_size_x": 75e-6,
            "incident_energy": 12000.0,
        }
    )

    instance._apply_image_message(config, _image_message(), 1000.0)

    resolution = store.calls[0]["meta"]["resolution"]
    assert resolution["distance_mm"] == pytest.approx(150.0)
    assert resolution["beam_center_px"] == [512.0, 530.0]
    assert resolution["pixel_size_um"] == pytest.approx(75.0)
    assert resolution["energy_ev"] == pytest.approx(12000.0)


def test_a_message_with_no_image_stores_nothing(bridge) -> None:
    instance, store, config = bridge

    instance._apply_image_message(config, _image_message(data={}), 1000.0)

    assert store.calls == []


def test_a_frame_that_cannot_be_stored_is_reported_not_raised(bridge) -> None:
    """The worker loop must survive a bad frame and keep receiving."""
    instance, store, config = bridge
    store.raise_on_next = True

    instance._apply_image_message(config, _image_message(), 1000.0)

    assert instance.status()["last_error"]
    assert store.calls == []


def test_counters_advance_only_on_a_stored_frame(bridge) -> None:
    instance, store, config = bridge

    instance._apply_image_message(config, _image_message(), 1000.0)
    instance._apply_image_message(config, _image_message(image_id=12), 1001.0)
    instance._apply_image_message(config, _image_message(data={}), 1002.0)

    status = instance.status()
    assert status["ingested_frames"] == 2
    assert status["image_number"] == 12
    assert status["last_frame_seq"] == 2


def test_a_successful_frame_clears_an_earlier_error(bridge) -> None:
    instance, store, config = bridge
    instance._set_error("something went wrong earlier")

    instance._apply_image_message(config, _image_message(), 1000.0)

    assert instance.status()["last_error"] == ""


def test_the_tag_hook_works_the_way_cbor2_actually_calls_it() -> None:
    """The bug this file found, pinned so it cannot come back.

    `cbor2.loads` is the only caller that matters, and the C implementation in
    the pinned 6.1.x calls the hook as `(tag, immutable)` while its own
    documentation says `(decoder, tag)`. A hook written to the documented order
    raised on every message, the worker logged "CBOR decode failed" and carried
    on, and the preview never showed a frame. Calling the hook directly -- as
    the earlier tests did -- could not see it.
    """
    pixels = np.arange(12, dtype="<u2").reshape(3, 4)
    encoded = cbor2.dumps(
        {
            "type": "image",
            "data": {
                "channel0": cbor2.CBORTag(_MULTIDIM_ROW_MAJOR, [[3, 4], pixels.ravel().tolist()])
            },
        }
    )

    message = cbor2.loads(encoded, tag_hook=jfjoch_tag_hook)

    assert np.array_equal(message["data"]["channel0"], pixels)


def test_a_typed_array_survives_the_real_decoder_too() -> None:
    pixels = np.arange(6, dtype="<u2")
    encoded = cbor2.dumps({"data": {"channel0": cbor2.CBORTag(_TYPED_UINT16_LE, pixels.tobytes())}})

    message = cbor2.loads(encoded, tag_hook=jfjoch_tag_hook)

    assert np.array_equal(message["data"]["channel0"], pixels)


def test_an_image_message_decoded_from_bytes_reaches_the_store(bridge) -> None:
    """End to end on the real path: CBOR bytes in, stored frame out."""
    instance, store, config = bridge
    pixels = np.arange(12, dtype="<u2").reshape(3, 4)
    encoded = cbor2.dumps(
        {
            "type": "image",
            "series_id": 4,
            "image_id": 11,
            "data": {
                "channel0": cbor2.CBORTag(_MULTIDIM_ROW_MAJOR, [[3, 4], pixels.ravel().tolist()])
            },
        }
    )

    message = cbor2.loads(encoded, tag_hook=jfjoch_tag_hook)
    instance._apply_image_message(config, message, 1000.0)

    assert len(store.calls) == 1
    assert np.array_equal(store.calls[0]["frame"], pixels)
    assert instance.status()["last_error"] == ""
