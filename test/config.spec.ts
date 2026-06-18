// Unit tests for inspire-sdk config resolution: walk-up discovery of
// .inspire/config.toml, parsing, graceful failure, and the
// opts > env > file > defaults precedence chain.

import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import { afterEach, beforeEach, describe, expect, it } from 'bun:test'

import { findInspireConfigPath, loadInspireConfig, resolveBroker } from '../src/config'

const DEFAULTS = { host: '127.0.0.1', port: 1883 }

let tmp: string
const savedEnv: Record<string, string | undefined> = {}
const ENV_KEYS = ['INSPIRE_BROKER_HOST', 'INSPIRE_BROKER_PORT', 'BROKER_HOST', 'BROKER_PORT']

function writeConfig(dir: string, contents: string): void {
  fs.mkdirSync(path.join(dir, '.inspire'), { recursive: true })
  fs.writeFileSync(path.join(dir, '.inspire', 'config.toml'), contents)
}

beforeEach(() => {
  tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'inspire-cfg-'))
  for (const k of ENV_KEYS) {
    savedEnv[k] = process.env[k]
    delete process.env[k]
  }
})

afterEach(() => {
  fs.rmSync(tmp, { recursive: true, force: true })
  for (const k of ENV_KEYS) {
    if (savedEnv[k] === undefined) delete process.env[k]
    else process.env[k] = savedEnv[k]
  }
})

describe('findInspireConfigPath', () => {
  it('finds .inspire/config.toml in the start dir', () => {
    writeConfig(tmp, '[broker]\nhost = "h"\n')
    expect(findInspireConfigPath(tmp)).toBe(path.join(tmp, '.inspire', 'config.toml'))
  })

  it('walks up parent directories to find it', () => {
    writeConfig(tmp, '[broker]\nhost = "h"\n')
    const nested = path.join(tmp, 'a', 'b', 'c')
    fs.mkdirSync(nested, { recursive: true })
    expect(findInspireConfigPath(nested)).toBe(path.join(tmp, '.inspire', 'config.toml'))
  })

  it('returns undefined when no config exists up to root', () => {
    expect(findInspireConfigPath(tmp)).toBeUndefined()
  })
})

describe('loadInspireConfig', () => {
  it('parses the canonical broker + reporting format', () => {
    writeConfig(
      tmp,
      'schema_version = 1\n[broker]\nhost = "192.168.1.156"\nport = 1884\n[reporting]\nheartbeat_interval_s = 10\n',
    )
    const cfg = loadInspireConfig(tmp)
    expect(cfg.broker).toEqual({ host: '192.168.1.156', port: 1884 })
    expect(cfg.reporting?.heartbeatIntervalMs).toBe(10_000)
  })

  it('tolerates a partial [broker] table (host only)', () => {
    writeConfig(tmp, '[broker]\nhost = "only-host"\n')
    expect(loadInspireConfig(tmp).broker).toEqual({ host: 'only-host', port: undefined })
  })

  it('returns {} when no config file exists', () => {
    expect(loadInspireConfig(tmp)).toEqual({})
  })

  it('returns {} on malformed TOML instead of throwing', () => {
    writeConfig(tmp, '[broker\nhost = busted')
    expect(loadInspireConfig(tmp)).toEqual({})
  })
})

describe('resolveBroker precedence (opts > env > file > defaults)', () => {
  it('uses defaults when nothing is set', () => {
    expect(resolveBroker(undefined, {}, DEFAULTS)).toEqual(DEFAULTS)
  })

  it('file config beats defaults', () => {
    const file = { broker: { host: 'file-host', port: 2000 } }
    expect(resolveBroker(undefined, file, DEFAULTS)).toEqual({ host: 'file-host', port: 2000 })
  })

  it('env beats file config', () => {
    process.env.INSPIRE_BROKER_HOST = 'env-host'
    process.env.INSPIRE_BROKER_PORT = '3000'
    const file = { broker: { host: 'file-host', port: 2000 } }
    expect(resolveBroker(undefined, file, DEFAULTS)).toEqual({ host: 'env-host', port: 3000 })
  })

  it('bare BROKER_* env names also work', () => {
    process.env.BROKER_HOST = 'bare-host'
    process.env.BROKER_PORT = '3100'
    expect(resolveBroker(undefined, {}, DEFAULTS)).toEqual({ host: 'bare-host', port: 3100 })
  })

  it('explicit opts beat everything', () => {
    process.env.INSPIRE_BROKER_HOST = 'env-host'
    const file = { broker: { host: 'file-host', port: 2000 } }
    expect(resolveBroker({ host: 'opt-host', port: 9999 }, file, DEFAULTS)).toEqual({
      host: 'opt-host',
      port: 9999,
    })
  })

  it('ignores invalid env port and falls through', () => {
    process.env.INSPIRE_BROKER_PORT = 'not-a-number'
    const file = { broker: { port: 2500 } }
    expect(resolveBroker(undefined, file, DEFAULTS).port).toBe(2500)
  })
})
