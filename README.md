# inspire-sdk

> Monorepo for the inspire-* fleet SDK — **Node** (this root) + **Python** (`sdk-python/`).
> Both languages version together (one git tag); the Node↔Python wire conformance
> test (`test/conformance.spec.ts`) keeps them in lockstep. Extracted from
> inspire-atrium 2026-06-18 (Slice E). Node consumers: `bun add github:amaitre/inspire-sdk#vX.Y.Z`.

---

# inspire-sdk (Node)

The Node SDK for inspire-* apps to participate in the inspire-atrium MQTT bus.
Wire format: `INSPIRE_ATRIUM_SPEC_ADDENDUM_2.md` §4.2.

## Install

This package is sibling to atrium in this monorepo (`sdk-node/`). Slice E will
extract it to its own repo with no source dependency on atrium.

## Boot snippet

Add this near app startup. The five lines:

```ts
import { Inspire } from 'inspire-sdk'

const client = await Inspire.start({ slug: 'inspire-financial', version: '0.4.2' })
await client.setStatus('ready', 'all systems online')
process.on('SIGTERM', () => void client.stop())
process.on('SIGINT', () => void client.stop())
```

### Broker resolution

`Inspire.start()` resolves the broker with this precedence:

1. explicit `broker: { host, port }`
2. env — `INSPIRE_BROKER_HOST` / `INSPIRE_BROKER_PORT` (or bare `BROKER_HOST` / `BROKER_PORT`)
3. **`.inspire/config.toml`** — walked up from cwd, `[broker] host/port` (and `[reporting] heartbeat_interval_s`)
4. default `127.0.0.1:1883`

The SDK now owns `.inspire/config.toml` parsing — consumers no longer hand-roll
it. A missing or malformed config never throws; it falls through to env/defaults.
Pass `loadConfig: false` to skip file resolution entirely.

```ts
import { loadInspireConfig, resolveBroker } from 'inspire-sdk' // exported for advanced use
```

Example `.inspire/config.toml`:

```toml
schema_version = 1
[broker]
host = "192.168.1.156"  # fleet broker (Ming) to federate across boxes
port = 1883
[reporting]
heartbeat_interval_s = 10
```

## What you get

- Retained `PresenceMsg` published on connect (atrium's Running view picks
  it up within 2s — spec AC 4).
- 10s heartbeat loop on `inspire/heartbeat/<slug>/<nodeId>`.
- LWT clears your retained presence on crash, so atrium auto-cleans.
- `setStatus(state, detail)` publishes retained `StatusMsg`.
- `log(level, msg)` for opt-in verbose logging (Slice C).
- `onCommand(verb, handler)` for inbound atrium → app commands (Slice B+E).
- `stop()` clears retained presence, stops the heartbeat, disconnects.

## Mock app

```bash
bun run examples/mock-app.ts
BROKER_HOST=192.168.1.10 bun run examples/mock-app.ts
```

## Testing

```bash
bun test test/
```

Tests use an in-process `aedes` broker on a random port — no Mosquitto needed.

> CI: `docs/ci.yml.example` is the intended `.github/workflows/ci.yml`. It was not
> committed under `.github/` because the push token lacks the `workflow` scope.
> Enable with: `gh auth refresh -s workflow` then move it into place, or add via the GitHub UI.
