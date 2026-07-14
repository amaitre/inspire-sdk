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

`Inspire.start()` resolves the broker with the **same precedence chain as the
Node SDK** (`_config.py`, mirroring `src/config.ts`):

1. explicit `broker={"host": ..., "port": ...}` argument
2. `INSPIRE_BROKER_HOST` / `INSPIRE_BROKER_PORT` env vars
3. bare `BROKER_HOST` / `BROKER_PORT` env vars
4. `.inspire/config.toml` `[broker]` host/port (walk-up search from cwd to root)
5. `127.0.0.1:1883`

Host and port resolve independently (e.g. env host + toml port is valid). A
missing or malformed `config.toml` is ignored, never fatal. On Python 3.10 the
`tomli` backport is pulled in automatically (stdlib `tomllib` is 3.11+).

> **Parity note (landed 2026-07-14):** this package previously read neither the
> env vars nor `.inspire/config.toml` — broker came only from the `broker=`
> argument or the localhost default. That divergence from Node is now resolved;
> both SDKs share the chain above.

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
- `stop()` clears the retained manifest first, then presence (same order as
  node — closes the dead-but-advertised window), stops the heartbeat,
  disconnects. (Ordering parity with node landed 2026-07-14.)

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
