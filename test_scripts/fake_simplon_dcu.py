"""Run a simulated SIMPLON 1.8 detector control unit, to try the Detector tab.

    python test_scripts/fake_simplon_dcu.py            # http://127.0.0.1:8100
    python test_scripts/fake_simplon_dcu.py --port 9000
    python test_scripts/fake_simplon_dcu.py --thresholds 4   # like a PILATUS4
    python test_scripts/fake_simplon_dcu.py --api-version 1.6.0   # like an EIGER1

Enter the printed address in ALBIS's Detector tab (Settings -> Viewer -> Beta:
detector control). The detector starts uninitialized, as a real one does after
power-up; series are simulated, files are tiny placeholders.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.fake_simplon import FakeDCUServer  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--init-delay", type=float, default=4.0, help="seconds initialize takes")
    parser.add_argument("--max-series", type=float, default=8.0, help="longest simulated series, s")
    parser.add_argument(
        "--thresholds", type=int, default=1, choices=range(1, 5), help="energy thresholds, 1-4"
    )
    parser.add_argument(
        "--api-version", default="1.8.0", help="the SIMPLON version served (1.6.0 for an EIGER1)"
    )
    args = parser.parse_args()
    with FakeDCUServer(
        args.port,
        init_delay=args.init_delay,
        max_series_s=args.max_series,
        thresholds=args.thresholds,
        api_version=args.api_version,
    ) as server:
        print(f"Simulated detector at {server.url} (Ctrl+C to stop)", flush=True)
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
