// Tests for the consumer/hub bus client (Inspire.observe): observe a real app's
// presence/heartbeat/status/manifest, call its RPC verbs, and publish/clear an
// optional retained self-presence.

import net from 'node:net'

import { afterEach, beforeEach, describe, expect, it } from 'bun:test'

import Aedes from 'aedes'
import mqtt from 'mqtt'

import { Inspire, type BusClient, type InspireClient } from '../src/index'

interface Harness {
  port: number
  close: () => Promise<void>
}
async function createBroker(): Promise<Harness> {
  const broker = new Aedes()
  const server = net.createServer(broker.handle)
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject)
      resolve()
    })
  })
  const port = (server.address() as net.AddressInfo).port
  return {
    port,
    close: async () => {
      await new Promise<void>((r) => server.close(() => r()))
      await broker.close()
    },
  }
}

function waitEvent(c: BusClient, event: string, predicate: (...a: any[]) => boolean, ms = 3000): Promise<any[]> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`no '${event}' within ${ms}ms`)), ms)
    const h = (...args: any[]) => {
      if (predicate(...args)) {
        clearTimeout(timer)
        c.off(event, h)
        resolve(args)
      }
    }
    c.on(event, h)
  })
}

let harness: Harness
let app: InspireClient | undefined
let consumer: BusClient | undefined

beforeEach(async () => {
  harness = await createBroker()
})
afterEach(async () => {
  if (consumer) await consumer.stop().catch(() => {})
  if (app) await app.stop().catch(() => {})
  consumer = undefined
  app = undefined
  await harness.close()
})

describe('Inspire.observe — consumer/hub bus client', () => {
  it('observes a real app: presence, heartbeat, status, manifest', async () => {
    consumer = Inspire.observe({ broker: { host: '127.0.0.1', port: harness.port } })
    const connected = waitEvent(consumer, 'connect', () => true)
    consumer.connect()
    await connected

    const presenceP = waitEvent(consumer, 'presence', (msg, slug) => slug === 'obs-app' && msg !== null)
    const heartbeatP = waitEvent(consumer, 'heartbeat', (_m, slug) => slug === 'obs-app')
    const manifestP = waitEvent(
      consumer,
      'manifest',
      (msg, slug) => slug === 'obs-app' && msg !== null && msg.verbs?.some((v: any) => v.name === 'ping'),
    )

    app = await Inspire.start({
      slug: 'obs-app',
      version: '1.0.0',
      nodeId: 'test-node',
      broker: { host: '127.0.0.1', port: harness.port },
      heartbeatIntervalMs: 150,
    })
    // Attach the status listener BEFORE setStatus — a live status publish is
    // delivered once and not replayed to a listener that attaches afterward.
    const statusP = waitEvent(consumer, 'status', (_m, slug) => slug === 'obs-app')
    app.onCall('ping', () => 'pong', { description: 'health' })
    await app.setStatus('ready', 'all good')

    const [presence] = await presenceP
    expect(presence.app_slug).toBe('obs-app')
    const [heartbeat] = await heartbeatP
    expect(typeof heartbeat.rss_mb).toBe('number')
    const [manifest] = await manifestP
    expect(manifest.verbs.map((v: any) => v.name)).toContain('ping')
    const [status] = await statusP
    expect(status.state).toBe('ready')
  })

  it('calls a target app verb and gets the result', async () => {
    consumer = Inspire.observe({ broker: { host: '127.0.0.1', port: harness.port } })
    const connected = waitEvent(consumer, 'connect', () => true)
    consumer.connect()
    await connected
    app = await Inspire.start({
      slug: 'rpc-app',
      version: '1.0.0',
      nodeId: 'rpc-node',
      broker: { host: '127.0.0.1', port: harness.port },
      heartbeatIntervalMs: 500,
    })
    app.onCall('add', (args) => (args.a as number) + (args.b as number))
    const result = await consumer.call({ slug: 'rpc-app', nodeId: 'rpc-node' }, 'add', { a: 2, b: 3 })
    expect(result).toBe(5)
  })

  it('emits presence=null when an app clears its presence (graceful stop / LWT)', async () => {
    consumer = Inspire.observe({ broker: { host: '127.0.0.1', port: harness.port } })
    const connected = waitEvent(consumer, 'connect', () => true)
    consumer.connect()
    await connected
    const seenP = waitEvent(consumer, 'presence', (msg, slug) => slug === 'leaving-app' && msg !== null)
    app = await Inspire.start({
      slug: 'leaving-app',
      version: '1.0.0',
      nodeId: 'gone',
      broker: { host: '127.0.0.1', port: harness.port },
      heartbeatIntervalMs: 500,
    })
    await seenP
    const clearedP = waitEvent(consumer, 'presence', (msg, slug) => slug === 'leaving-app' && msg === null)
    await app.stop()
    app = undefined
    const [cleared] = await clearedP
    expect(cleared).toBeNull()
  })

  it('publishes a retained self-presence and clears it on stop (hub mode)', async () => {
    const TOPIC = 'inspire/atrium/presence'
    consumer = Inspire.observe({
      broker: { host: '127.0.0.1', port: harness.port },
      callerId: 'atrium-test',
      selfPresence: { topic: TOPIC, message: () => ({ v: 1, node_id: 'atrium-test', version: '9.9.9' }) },
    })
    const connected = waitEvent(consumer, 'connect', () => true)
    consumer.connect()
    await connected

    // A late snoop must see the RETAINED self-presence.
    const snoop = mqtt.connect(`mqtt://127.0.0.1:${harness.port}`)
    const got = await new Promise<Record<string, unknown>>((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('no retained self-presence')), 3000)
      snoop.on('connect', () => snoop.subscribe(TOPIC))
      snoop.on('message', (_t, payload) => {
        if (payload.length === 0) return
        clearTimeout(t)
        resolve(JSON.parse(payload.toString()))
      })
    })
    expect(got.node_id).toBe('atrium-test')

    // On stop, the retained self-presence is cleared (empty payload).
    const clearedP = new Promise<boolean>((resolve) => {
      const t = setTimeout(() => resolve(false), 3000)
      snoop.on('message', (_t, payload) => {
        if (payload.length === 0) {
          clearTimeout(t)
          resolve(true)
        }
      })
    })
    await consumer.stop()
    consumer = undefined
    expect(await clearedP).toBe(true)
    await new Promise<void>((r) => snoop.end(true, {}, () => r()))
  })
})
