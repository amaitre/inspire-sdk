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

`Inspire.start()` connects to `127.0.0.1:1883` by default. Override via
`broker: { host, port }` or your `.inspire/config.toml` parsing.

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
