// Integration tests for the inspire-sdk RPC + capability-manifest layer
// (spec §6). Same in-process aedes harness as inspire.spec.ts: a real broker
// on a random port, two real SDK clients, real wire round-trips. RPC is
// exercised end-to-end — a caller's call() reaches a server's onCall() handler
// and the result comes back over the bus.

import net from 'node:net'

import { afterEach, beforeEach, describe, expect, it } from 'bun:test'

import Aedes from 'aedes'
import mqtt from 'mqtt'

import { Inspire, type InspireClient } from '../src/index'

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
  const addr = server.address()
  if (!addr || typeof addr === 'string') {
    server.close()
    broker.close()
    throw new Error('aedes harness: no port')
  }
  return {
    port: addr.port,
    close: async () => {
      await new Promise<void>((r) => server.close(() => r()))
      await new Promise<void>((r) => broker.close(() => r()))
    },
  }
}

function startApp(port: number, slug: string, nodeId: string): Promise<InspireClient> {
  return Inspire.start({
    slug,
    version: '0.1.0',
    broker: { host: '127.0.0.1', port },
    nodeId,
    heartbeatIntervalMs: 60_000, // suppress heartbeats during RPC tests
    reconnectPeriod: 0,
  })
}

describe('inspire-sdk RPC + manifest', () => {
  let harness: Harness
  const clients: InspireClient[] = []

  beforeEach(async () => {
    harness = await createBroker()
  })

  afterEach(async () => {
    while (clients.length) {
      const c = clients.pop()
      if (c) await c.stop()
    }
    await harness.close()
  })

  it('publishes a retained manifest built from registered verbs', async () => {
    const sub = mqtt.connect({ host: '127.0.0.1', port: harness.port, reconnectPeriod: 0 })
    await new Promise<void>((r) => sub.on('connect', () => r()))

    const app = await startApp(harness.port, 'mock-app', 'host-1')
    clients.push(app)
    app.onCall('ping', () => 'pong', { description: 'health check' })
    // give the republish a beat
    await app.setStatus('ready', 'up')

    const got = new Promise<Record<string, unknown>>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('no manifest')), 2000)
      sub.on('message', (topic, payload) => {
        if (topic !== 'inspire/manifest/mock-app/host-1') return
        if (payload.length === 0) return
        const parsed = JSON.parse(payload.toString('utf8'))
        if (parsed.verbs?.length >= 1) {
          clearTimeout(timer)
          resolve(parsed)
        }
      })
    })
    sub.subscribe('inspire/manifest/mock-app/host-1', { qos: 1 })
    const manifest = await got
    sub.end(true)

    expect(manifest.app_slug).toBe('mock-app')
    expect(manifest.node_id).toBe('host-1')
    const verbs = manifest.verbs as Array<{ name: string; description?: string }>
    expect(verbs.some((v) => v.name === 'ping' && v.description === 'health check')).toBe(true)
  })

  it('round-trips an RPC call between two app instances', async () => {
    const server = await startApp(harness.port, 'calc', 'host-server')
    clients.push(server)
    server.onCall('add', (args) => {
      const a = Number(args.a ?? 0)
      const b = Number(args.b ?? 0)
      return { sum: a + b }
    })

    const caller = await startApp(harness.port, 'client', 'host-caller')
    clients.push(caller)

    const result = (await caller.call(
      { slug: 'calc', nodeId: 'host-server' },
      'add',
      { a: 2, b: 40 },
    )) as { sum: number }

    expect(result.sum).toBe(42)
  })

  it('resolves async handler return values', async () => {
    const server = await startApp(harness.port, 'svc', 'n1')
    clients.push(server)
    server.onCall('slow', async (args) => {
      await new Promise((r) => setTimeout(r, 20))
      return { echo: args.x }
    })
    const caller = await startApp(harness.port, 'cli', 'n2')
    clients.push(caller)

    const res = (await caller.call({ slug: 'svc', nodeId: 'n1' }, 'slow', { x: 'hi' })) as {
      echo: string
    }
    expect(res.echo).toBe('hi')
  })

  it('propagates a handler error as a rejected call', async () => {
    const server = await startApp(harness.port, 'svc', 'n1')
    clients.push(server)
    server.onCall('boom', () => {
      throw new Error('kaboom')
    })
    const caller = await startApp(harness.port, 'cli', 'n2')
    clients.push(caller)

    await expect(caller.call({ slug: 'svc', nodeId: 'n1' }, 'boom')).rejects.toThrow('kaboom')
  })

  it('rejects a call to an unknown verb', async () => {
    const server = await startApp(harness.port, 'svc', 'n1')
    clients.push(server)
    const caller = await startApp(harness.port, 'cli', 'n2')
    clients.push(caller)

    await expect(
      caller.call({ slug: 'svc', nodeId: 'n1' }, 'does-not-exist'),
    ).rejects.toThrow(/unknown verb/)
  })

  it('times out a call to a non-existent target', async () => {
    const caller = await startApp(harness.port, 'cli', 'n2')
    clients.push(caller)
    await expect(
      caller.call({ slug: 'ghost', nodeId: 'nowhere' }, 'x', {}, { timeoutMs: 300 }),
    ).rejects.toThrow(/timed out/)
  })

  it('exposes registered verbs via the verbs getter', async () => {
    const app = await startApp(harness.port, 'a', 'n')
    clients.push(app)
    app.onCall('one', () => 1)
    app.onCall('two', () => 2, { description: 'the second' })
    expect(app.verbs.map((v) => v.name).sort()).toEqual(['one', 'two'])
    expect(app.verbs.find((v) => v.name === 'two')?.description).toBe('the second')
  })

  it('clears the retained manifest on stop', async () => {
    const sub = mqtt.connect({ host: '127.0.0.1', port: harness.port, reconnectPeriod: 0 })
    await new Promise<void>((r) => sub.on('connect', () => r()))

    const app = await startApp(harness.port, 'mock-app', 'host-1')
    app.onCall('ping', () => 'pong')

    const events: number[] = []
    const sawClear = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`only saw ${events.length} manifest msgs`)), 2500)
      sub.on('message', (topic, payload) => {
        if (topic !== 'inspire/manifest/mock-app/host-1') return
        events.push(payload.length)
        if (payload.length === 0) {
          clearTimeout(timer)
          resolve()
        }
      })
    })
    sub.subscribe('inspire/manifest/mock-app/host-1', { qos: 1 })
    await app.stop()
    await sawClear
    sub.end(true)
    expect(events.some((len) => len === 0)).toBe(true)
  })
})
