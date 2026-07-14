# inspire-sdk (Python)

The Python SDK for inspire-* apps to participate in the inspire-atrium MQTT bus.
**Start with [`../docs/BUS.md`](../docs/BUS.md)** — the fleet-bus map (two buses, topology, onboarding, gotchas).
Wire format: atrium repo `docs/INSPIRE_ATRIUM_SPEC_ADDENDUM_2.md` §4.2 (envelope only — its topology section is superseded);
RPC + capability manifests: atrium `docs/INSPIRE_ATRIUM_SPEC_ADDENDUM_3.md`.

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
`broker={"host": ..., "port": ...}`.

> ⚠️ **Divergence from Node (by current implementation, 2026-07-13):** unlike the
> Node SDK, this package reads **neither** `INSPIRE_BROKER_HOST`/`INSPIRE_BROKER_PORT`
> env vars **nor** `.inspire/config.toml` (`_client.py` — broker comes only from the
> `broker=` argument or the localhost default). If your app should honor those, read
> them yourself and pass `broker=`. Tracked as a parity decision, not a bug.

## What you get

- Retained `PresenceMsg` published on connect (atrium's Running view picks it
  up within 2s — spec AC 4).
- 10s heartbeat loop on `inspire/heartbeat/<slug>/<nodeId>`.
- LWT clears your retained presence on crash, so atrium auto-cleans.
- `set_status(state, detail)` publishes retained `StatusMsg`.
- `log(level, msg, fields=...)` for opt-in verbose logging.
- `on_command(verb, handler)` for inbound atrium → app commands.
- **RPC + capability manifest** (previously undocumented here): `on_call(verb, handler, spec=...)`
  registers a callable verb and (re)publishes the retained `CapabilityManifestMsg` on
  `inspire/manifest/<slug>/<nodeId>`; `call(slug, node_id, verb, args, timeout=8.0)`
  invokes a remote verb with `corr_id` correlation. Contract: atrium Addendum 3 §2-3;
  implementation `inspire_sdk/_client.py`.
- `stop()` clears retained presence, stops the heartbeat, disconnects.
  (Note: python clears presence→manifest, node clears manifest→presence — a brief
  live-manifest window exists for stopping python apps; see `../docs/BUS.md` gotchas.)

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
