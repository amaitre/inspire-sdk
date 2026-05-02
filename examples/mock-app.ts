#!/usr/bin/env bun
// mock-app — drives Acceptance Criterion 4 manual verification.
//
// Boots the SDK pointed at $BROKER_HOST:$BROKER_PORT (defaults
// 127.0.0.1:1883), publishes retained presence, holds open the heartbeat
// loop, and gracefully stops on SIGTERM / SIGINT (clears retained
// presence, then exits 0).
//
// Run from sdk-node/:
//   bun run examples/mock-app.ts
//   BROKER_HOST=192.168.1.10 BROKER_PORT=1883 bun run examples/mock-app.ts

import { Inspire } from '../src/index'

async function main(): Promise<void> {
  const host = process.env.BROKER_HOST ?? '127.0.0.1'
  const port = Number(process.env.BROKER_PORT ?? 1883)
  const slug = process.env.MOCK_APP_SLUG ?? 'mock-app'
  const version = process.env.MOCK_APP_VERSION ?? '0.1.0'

  // eslint-disable-next-line no-console
  console.log(`[mock-app] connecting to ${host}:${port} as ${slug} v${version}`)

  const client = await Inspire.start({
    slug,
    version,
    broker: { host, port },
  })

  await client.setStatus('ready', 'mock app online')
  // eslint-disable-next-line no-console
  console.log(`[mock-app] online as ${client.slug}|${client.nodeId} — Ctrl-C to stop`)

  const shutdown = async (sig: string) => {
    // eslint-disable-next-line no-console
    console.log(`[mock-app] received ${sig}, clearing retained presence and exiting`)
    try {
      await client.setStatus('stopping', `received ${sig}`)
    } catch {
      /* broker may already be unreachable */
    }
    await client.stop()
    process.exit(0)
  }
  process.on('SIGTERM', () => void shutdown('SIGTERM'))
  process.on('SIGINT', () => void shutdown('SIGINT'))

  // Keep process alive — the heartbeat interval already does this, but
  // make it explicit so a future SDK refactor that uses setImmediate
  // doesn't break the example.
  setInterval(() => {
    /* idle */
  }, 60_000)
}

void main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('[mock-app] fatal:', err)
  process.exit(1)
})
