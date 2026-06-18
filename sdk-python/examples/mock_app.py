"""Mock inspire-* app for manual end-to-end testing of atrium's Running view.

Run with:

    python examples/mock_app.py

Or with a non-default broker host:

    INSPIRE_BROKER_HOST=192.168.1.10 python examples/mock_app.py

Mirrors sdk-node/examples/mock-app.ts. Atrium's Running view should display
this row within 2s of launch (spec AC 4) and evict it within 60s of SIGKILL
(AC 5) or within ~2s of graceful Ctrl-C (LWT-cleared).
"""

from __future__ import annotations

import os
import signal
import sys
import threading
import time

from inspire_sdk import Inspire


def main() -> int:
    host = os.environ.get("INSPIRE_BROKER_HOST", "127.0.0.1")
    port = int(os.environ.get("INSPIRE_BROKER_PORT", "1883"))
    print(f"connecting to {host}:{port} ...", flush=True)

    client = Inspire.start(
        slug="mock-app-py",
        version="0.1.0",
        broker={"host": host, "port": port},
    )
    print(f"connected; node_id={client.node_id}", flush=True)
    client.set_status("ready", "mock app py running")

    stop = threading.Event()

    def handle_sigint(_signum, _frame):
        print("received SIGINT, clearing retained presence and exiting", flush=True)
        stop.set()

    def handle_sigterm(_signum, _frame):
        print("received SIGTERM, clearing retained presence and exiting", flush=True)
        stop.set()

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigterm)

    client.on_command(
        "shutdown",
        lambda _msg: stop.set(),
    )

    while not stop.wait(1.0):
        pass

    client.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
