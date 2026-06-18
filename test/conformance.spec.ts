// Cross-language wire conformance: the Node SDK's ACTUAL emitted messages are
// the source of truth for the wire contract; the Python SDK (sdk-python) must
// declare the same field names so atrium can't tell which language produced a
// row. This guards the structural contract — if someone adds/renames a field in
// one SDK and not the other, this fails. (Value-level parity — e.g. rss_mb
// live-vs-peak — is a separate known issue tracked in the design assessment.)
//
// Runs entirely in bun: captures real Node emissions over an in-process broker
// and parses Python's TypedDicts as text — no pytest / Python runtime needed.

import fs from 'node:fs'
import net from 'node:net'
import path from 'node:path'

import { afterEach, beforeEach, expect, it } from 'bun:test'

import Aedes from 'aedes'
import mqtt from 'mqtt'

import { Inspire, type InspireClient } from '../src/index'

const PY_TYPES = path.join(import.meta.dir, '..', 'sdk-python', 'inspire_sdk', '_types.py')

/** Parse Python TypedDict field names from _types.py. Returns class -> field set. */
function parsePythonTypedDicts(src: string): Record<string, Set<string>> {
  const out: Record<string, Set<string>> = {}
  const lines = src.split('\n')
  let current: string | null = null
  for (const line of lines) {
    const cls = line.match(/^class (\w+)\(TypedDict\):/)
    if (cls) {
      current = cls[1]
      out[current] = new Set()
      continue
    }
    if (current) {
      // A field line is indented `    name: type`. Dedent / blank ends the block.
      const field = line.match(/^ {4}(\w+)\s*:/)
      if (field) out[current].add(field[1])
      else if (line.trim() !== '' && !line.startsWith(' ')) current = null
    }
  }
  return out
}

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

let harness: Harness
let client: InspireClient | undefined
let pyTypes: Record<string, Set<string>>

beforeEach(async () => {
  harness = await createBroker()
  pyTypes = parsePythonTypedDicts(fs.readFileSync(PY_TYPES, 'utf8'))
})
afterEach(async () => {
  if (client) await client.stop().catch(() => {})
  client = undefined
  await harness.close()
})

/** Capture the first retained/seen message on a topic filter, as parsed JSON. */
function captureFirst(port: number, filter: string, timeoutMs = 3000): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    const sub = mqtt.connect(`mqtt://127.0.0.1:${port}`)
    const timer = setTimeout(() => {
      sub.end(true)
      reject(new Error(`no message on ${filter} within ${timeoutMs}ms`))
    }, timeoutMs)
    sub.on('connect', () => sub.subscribe(filter))
    sub.on('message', (_t, payload) => {
      if (payload.length === 0) return // skip retained-clear
      clearTimeout(timer)
      sub.end(true)
      resolve(JSON.parse(payload.toString()))
    })
  })
}

it('Python TypedDicts cover every field the Node SDK actually emits', async () => {
  expect(Object.keys(pyTypes).length).toBeGreaterThan(0) // parser sanity

  const presenceP = captureFirst(harness.port, 'inspire/presence/+/+')
  const heartbeatP = captureFirst(harness.port, 'inspire/heartbeat/+/+')
  const manifestP = captureFirst(harness.port, 'inspire/manifest/+/+')

  client = await Inspire.start({
    slug: 'conformance-app',
    version: '1.0.0',
    broker: { host: '127.0.0.1', port: harness.port },
    heartbeatIntervalMs: 150,
  })
  client.onCall('ping', () => 'pong', { description: 'health check' })
  await client.setStatus('ready', 'all good')
  const statusP = captureFirst(harness.port, 'inspire/status/+/+')

  const cases: Array<[string, Record<string, unknown>]> = [
    ['PresenceMsg', await presenceP],
    ['HeartbeatMsg', await heartbeatP],
    ['StatusMsg', await statusP],
    ['CapabilityManifestMsg', await manifestP],
  ]

  for (const [typeName, msg] of cases) {
    const nodeKeys = Object.keys(msg).sort()
    const pyKeys = pyTypes[typeName]
    expect(pyKeys, `Python _types.py is missing TypedDict ${typeName}`).toBeDefined()
    // Every field Node emits must be declared on the Python side.
    const undeclared = nodeKeys.filter((k) => !pyKeys.has(k))
    expect(undeclared, `${typeName}: Python TypedDict missing fields Node emits`).toEqual([])
    // And every required Python field must appear in Node's emission (these
    // four message types have no optional fields).
    const missingFromNode = [...pyKeys].filter((k) => !nodeKeys.includes(k))
    expect(missingFromNode, `${typeName}: Node emission missing fields Python declares`).toEqual([])
  }
})
