# inspire-sdk (Python)

The Python SDK for inspire-* apps to participate in the inspire-atrium MQTT bus.
Wire format: `docs/INSPIRE_ATRIUM_SPEC_ADDENDUM_2.md` §4.2.

This is the runtime presence/heartbeat surface — at parity with `sdk-node/`. The
Phase 6 bootstrap surface (Stage 2 install agent, preflight, diagnostics) is
**not** in this library and is scoped to the eventual vitara fork.

## Install

This package is sibling to atrium in this monorepo (`sdk-python/`). Install
into a target app's venv via:

```bash
pip install -e ../inspire-atrium/sdk-python
```

## Boot snippet

Add this near app startup. The five lines:

```python
from inspire_sdk import Inspire

client = Inspire.start(slug="inspire-music", version="0.1.0")
client.set_status("ready", "library loaded")
# ... app runs ...
client.stop()  # on graceful shutdown
```

`Inspire.start()` connects to `127.0.0.1:1883` by default. Override via
`broker={"host": ..., "port": ...}` or read from your `.inspire/config.toml`.

## What you get

- Retained `PresenceMsg` published on connect (atrium's Running view picks it
  up within 2s — spec AC 4).
- 10s heartbeat loop on `inspire/heartbeat/<slug>/<nodeId>`.
- LWT clears your retained presence on crash, so atrium auto-cleans.
- `set_status(state, detail)` publishes retained `StatusMsg`.
- `log(level, msg, fields=...)` for opt-in verbose logging.
- `on_command(verb, handler)` for inbound atrium → app commands.
- `stop()` clears retained presence, stops the heartbeat, disconnects.

## Mock app

```bash
python examples/mock_app.py
INSPIRE_BROKER_HOST=192.168.1.10 python examples/mock_app.py
```

## Testing

```bash
pip install -e ".[dev]"
pytest
```

Tests spawn a real Mosquitto on a random port per test (Mosquitto is a Phase 5
prereq per the migration guide; no in-process Python broker needed).
