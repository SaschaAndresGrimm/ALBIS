"""Publish a simulated JUNGFRAUJOCH preview stream, to try ALBIS's JUNGFRAUJOCH source.

    python test_scripts/fake_jfjoch_publisher.py                 # tcp://127.0.0.1:5400
    python test_scripts/fake_jfjoch_publisher.py --port 5401 --rate 4

Enter tcp://127.0.0.1:5400 as the preview endpoint in ALBIS's Data tab
(JUNGFRAUJOCH mode). The stream is what ALBIS reads from a real one: a CBOR
`start` message with the geometry, then `image` messages with a 256 x 256
uint16 frame (a ring pattern that moves) and two spots, image_id counting up.
"""

from __future__ import annotations

import argparse
import time

import cbor2
import numpy as np
import zmq

# RFC 8746 tags, as JUNGFRAUJOCH writes its arrays.
_TYPED_UINT16_LE = 69
_MULTIDIM_ROW_MAJOR = 40
_SIZE = 256


def _frame(image_id: int) -> np.ndarray:
    y, x = np.mgrid[0:_SIZE, 0:_SIZE]
    r = np.hypot(x - _SIZE / 2, y - _SIZE / 2)
    return (300 * (1 + np.cos(r / 6.0 - image_id * 0.5)) * np.exp(-r / 120)).astype("<u2")


def start_message(series_id: int) -> bytes:
    return cbor2.dumps(
        {
            "type": "start",
            "series_id": series_id,
            "series_unique_id": f"fake_series_{series_id}",
            "detector_distance": 0.15,
            "incident_energy": 12400.0,
            "beam_center_x": _SIZE / 2,
            "beam_center_y": _SIZE / 2,
            "pixel_size_x": 75e-6,
            "pixel_size_y": 75e-6,
        }
    )


def image_message(series_id: int, image_id: int) -> bytes:
    pixels = _frame(image_id)
    array = cbor2.CBORTag(
        _MULTIDIM_ROW_MAJOR, [[_SIZE, _SIZE], cbor2.CBORTag(_TYPED_UINT16_LE, pixels.tobytes())]
    )
    return cbor2.dumps(
        {
            "type": "image",
            "series_id": series_id,
            "image_id": image_id,
            "data": {"default": array},
            "spots": [
                {"x": 100.0, "y": 120.0, "I": 500.0, "indexed": True},
                {"x": 160.0, "y": 90.0, "I": 300.0, "indexed": False},
            ],
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=5400)
    parser.add_argument("--rate", type=float, default=2.0, help="images per second")
    parser.add_argument("--series", type=int, default=1, help="series_id to publish")
    args = parser.parse_args()
    context = zmq.Context.instance()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://127.0.0.1:{args.port}")
    print(f"Publishing at tcp://127.0.0.1:{args.port} (Ctrl+C to stop)", flush=True)
    image_id = 0
    try:
        while True:
            # A late subscriber misses the first start message, so it is sent
            # again every 20 images, as a stream that is joined midway needs.
            if image_id % 20 == 0:
                socket.send(start_message(args.series))
            socket.send(image_message(args.series, image_id))
            image_id += 1
            time.sleep(1.0 / args.rate)
    except KeyboardInterrupt:
        pass
    finally:
        socket.close(0)


if __name__ == "__main__":
    main()
