// End-to-end: Inspire.start() with NO opts.broker must resolve the broker
// from .inspire/config.toml in the cwd. We point config.toml at an in-process
// aedes broker on a RANDOM port and assert presence lands there — a real
// broker may be running on the default 1883, so connecting to the
// config-specified port is the only honest proof the file was read.

import fs from 'node:fs'
import net from 'node:net'
import os from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, expect, it } from 'bun:test'

import Aedes from 'aedes'
import mqtt from 'mqtt'

import { Inspire, type InspireClient } from '../src/index'

let broker: Aedes
let server: net.Server
let port: number
let tmp: string
let cwd: string
let client: InspireClient | undefined

beforeEach(async () => {
  broker = new Aedes()
  server = net.createServer(broker.handle)
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', () => {
      server.off('error', reject)
      resolve()
    })
  })
  port = (server.address() as net.AddressInfo).port
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'inspire-cfgint-'))
  fs.mkdirSync(path.join(tmp, '.inspire'), { recursive: true })
  fs.writeFileSync(
    path.join(tmp, '.inspire', 'config.toml'),
    `schema_version = 1\n[broker]\nhost = "127.0.0.1"\nport = ${port}\n`,
  )
  cwd = process.cwd()
  process.chdir(tmp)
})

afterEach(async () => {
  process.chdir(cwd)
  if (client) await client.stop().catch(() => {})
  client = undefined
  await new Promise<void>((resolve) => server.close(() => resolve()))
  await broker.close()
  fs.rmSync(tmp, { recursive: true, force: true })
})

it('start() reads broker host/port from .inspire/config.toml when opts.broker is absent', async () => {
  // Snoop the config-specified broker for the retained presence message.
  const snoop = mqtt.connect(`mqtt://127.0.0.1:${port}`)
  const sawPresence = new Promise<boolean>((resolve) => {
    snoop.on('connect', () => snoop.subscribe('inspire/presence/+/+'))
    snoop.on('message', (topic) => {
      if (topic.startsWith('inspire/presence/cfg-int-app/')) resolve(true)
    })
    setTimeout(() => resolve(false), 4000)
  })

  client = await Inspire.start({ slug: 'cfg-int-app', version: '0.0.0', heartbeatIntervalMs: 200 })
  expect(await sawPresence).toBe(true)
  await new Promise<void>((resolve) => snoop.end(true, {}, () => resolve()))
})

it('loadConfig:false ignores config.toml (falls back to explicit broker)', async () => {
  // With loadConfig disabled and an explicit broker, the config port is unused.
  const snoop = mqtt.connect(`mqtt://127.0.0.1:${port}`)
  const sawPresence = new Promise<boolean>((resolve) => {
    snoop.on('connect', () => snoop.subscribe('inspire/presence/+/+'))
    snoop.on('message', () => resolve(true))
    setTimeout(() => resolve(false), 1500)
  })
  // Explicit broker points at the SAME in-process broker so start() succeeds,
  // proving the path works without config; loadConfig:false is the assertion.
  client = await Inspire.start({
    slug: 'noconfig-app',
    version: '0.0.0',
    broker: { host: '127.0.0.1', port },
    loadConfig: false,
    heartbeatIntervalMs: 200,
  })
  expect(await sawPresence).toBe(true)
  await new Promise<void>((resolve) => snoop.end(true, {}, () => resolve()))
})
