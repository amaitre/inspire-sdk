// Integration tests for inspire-sdk: spin up an in-process aedes broker,
// start the SDK, snoop the bus with a raw mqtt subscriber, and assert the
// wire format / heartbeat cadence / graceful-stop behaviour.
//
// Tests use a SHORT heartbeat interval (200ms) via the test-only override
// so we don't sit here for 10 seconds per case. The 10s production default
// is a separate guarantee verified by reading the source.

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
  const port = addr.port
  return {
    port,
    close: async () => {
      await new Promise<void>((r) => server.close(() => r()))
      await new Promise<void>((r) => broker.close(() => r()))
    },
  }
}

describe('Inspire SDK', () => {
  let harness: Harness
  let client: InspireClient | null = null

  beforeEach(async () => {
    harness = await createBroker()
  })

  afterEach(async () => {
    if (client) {
      await client.stop()
      client = null
    }
    await harness.close()
  })

  it('publishes retained PresenceMsg after start', async () => {
    client = await Inspire.start({
      slug: 'mock-app',
      version: '0.1.0',
      broker: { host: '127.0.0.1', port: harness.port },
      nodeId: 'host-1',
      heartbeatIntervalMs: 60_000, // suppress heartbeats for this test
      reconnectPeriod: 0,
    })

    const sub = mqtt.connect({ host: '127.0.0.1', port: harness.port, reconnectPeriod: 0 })
    const got = new Promise<{ topic: string; payload: string }>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('timeout waiting for retained presence')), 2000)
      sub.on('connect', () => {
        sub.subscribe('inspire/presence/mock-app/host-1', { qos: 1 })
      })
      sub.on('message', (topic, payload) => {
        clearTimeout(timer)
        resolve({ topic, payload: payload.toString('utf8') })
      })
    })
    const { topic, payload } = await got
    sub.end(true)
    expect(topic).toBe('inspire/presence/mock-app/host-1')
    const parsed = JSON.parse(payload)
    expect(parsed.v).toBe(1)
    expect(parsed.app_slug).toBe('mock-app')
    expect(parsed.node_id).toBe('host-1')
    expect(parsed.version).toBe('0.1.0')
    expect(parsed.service_mode).toBe(false)
    expect(typeof parsed.pid).toBe('number')
  })

  it('starts heartbeat that fires at the configured interval', async () => {
    client = await Inspire.start({
      slug: 'mock-app',
      version: '0.1.0',
      broker: { host: '127.0.0.1', port: harness.port },
      nodeId: 'host-1',
      heartbeatIntervalMs: 150,
      reconnectPeriod: 0,
    })

    const sub = mqtt.connect({ host: '127.0.0.1', port: harness.port, reconnectPeriod: 0 })
    let heartbeats = 0
    const seen = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(`only saw ${heartbeats} heartbeats in 1s`)), 1500)
      sub.on('connect', () => {
        sub.subscribe('inspire/heartbeat/mock-app/host-1', { qos: 0 })
      })
      sub.on('message', (_topic, payload) => {
        heartbeats++
        try {
          const parsed = JSON.parse(payload.toString('utf8'))
          expect(parsed.v).toBe(1)
          expect(typeof parsed.uptime_s).toBe('number')
          expect(typeof parsed.rss_mb).toBe('number')
        } catch (e) {
          clearTimeout(timer)
          reject(e)
          return
        }
        if (heartbeats >= 2) {
          clearTimeout(timer)
          resolve()
        }
      })
    })
    await seen
    sub.end(true)
    expect(heartbeats).toBeGreaterThanOrEqual(2)
  })

  it('setStatus publishes a retained StatusMsg', async () => {
    client = await Inspire.start({
      slug: 'mock-app',
      version: '0.1.0',
      broker: { host: '127.0.0.1', port: harness.port },
      nodeId: 'host-1',
      heartbeatIntervalMs: 60_000,
      reconnectPeriod: 0,
    })
    await client.setStatus('ready', 'all systems online')

    const sub = mqtt.connect({ host: '127.0.0.1', port: harness.port, reconnectPeriod: 0 })
    const got = new Promise<string>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('no status received')), 2000)
      sub.on('connect', () => {
        sub.subscribe('inspire/status/mock-app/host-1', { qos: 1 })
      })
      sub.on('message', (_topic, payload) => {
        clearTimeout(timer)
        resolve(payload.toString('utf8'))
      })
    })
    const payload = await got
    sub.end(true)
    const parsed = JSON.parse(payload)
    expect(parsed.state).toBe('ready')
    expect(parsed.detail).toBe('all systems online')
  })

  it('stop() clears retained presence by publishing empty payload', async () => {
    client = await Inspire.start({
      slug: 'mock-app',
      version: '0.1.0',
      broker: { host: '127.0.0.1', port: harness.port },
      nodeId: 'host-1',
      heartbeatIntervalMs: 60_000,
      reconnectPeriod: 0,
    })

    // Subscribe BEFORE stop so we receive both the original retained and
    // the cleared (empty) one; the broker delivers retained-on-subscribe.
    const sub = mqtt.connect({ host: '127.0.0.1', port: harness.port, reconnectPeriod: 0 })
    await new Promise<void>((resolve) => sub.on('connect', () => resolve()))

    const events: { length: number }[] = []
    const allReceived = new Promise<void>((resolve, reject) => {
      const timer = setTimeout(
        () => reject(new Error(`waited 2s; only saw ${events.length} payloads`)),
        2000,
      )
      sub.on('message', (_topic, payload) => {
        events.push({ length: payload.length })
        // Original retained (>0 bytes) followed by clear (0 bytes).
        if (events.length >= 1 && events[events.length - 1].length === 0) {
          clearTimeout(timer)
          resolve()
        }
      })
    })

    sub.subscribe('inspire/presence/mock-app/host-1', { qos: 1 })
    await client.stop()
    client = null
    await allReceived

    expect(events.some((e) => e.length === 0)).toBe(true)
    sub.end(true)
  })

  it('integration: SDK presence appears within 2s on a fresh subscriber (mirrors AC 4)', async () => {
    // Spec §10 AC 4 asserts "within 2s". This test covers the wire-side
    // half end-to-end: SDK starts, raw subscriber connects after, sees
    // retained presence inside 2s.
    const startedAt = Date.now()
    client = await Inspire.start({
      slug: 'mock-app',
      version: '0.1.0',
      broker: { host: '127.0.0.1', port: harness.port },
      nodeId: 'host-1',
      heartbeatIntervalMs: 60_000,
      reconnectPeriod: 0,
    })

    const sub = mqtt.connect({ host: '127.0.0.1', port: harness.port, reconnectPeriod: 0 })
    const got = new Promise<number>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('AC 4 wire path > 2s')), 2000)
      sub.on('connect', () => {
        sub.subscribe('inspire/presence/+/+', { qos: 1 })
      })
      sub.on('message', () => {
        clearTimeout(timer)
        resolve(Date.now() - startedAt)
      })
    })
    const elapsed = await got
    sub.end(true)
    expect(elapsed).toBeLessThan(2000)
  })
})
