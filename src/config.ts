// inspire-sdk config resolution.
//
// The shared convention (INSPIRE_CONVENTION + sdk README) promises that a
// `.inspire/config.toml` overrides the broker the app connects to. Until now
// the SDK never read it, so every consumer hand-rolled TOML parsing or fell
// back to ad-hoc env vars and the config.toml files sat dead. This module is
// the single place that knows the format, so `Inspire.start()` can default to
// it and consumers can delete their bespoke broker plumbing.
//
// Format (canonical, matches inspire-automation/.inspire/config.toml):
//
//   schema_version = 1
//   [broker]
//   host = "127.0.0.1"
//   port = 1883
//   [reporting]
//   heartbeat_interval_s = 10
//   verbose_default = false

import fs from 'node:fs'
import path from 'node:path'

import { parse as parseToml } from 'smol-toml'

export interface InspireConfig {
  broker?: { host?: string; port?: number }
  reporting?: { heartbeatIntervalMs?: number }
}

/**
 * Walk up from `startDir` to the filesystem root looking for
 * `.inspire/config.toml`. Returns the first match, or undefined.
 */
export function findInspireConfigPath(startDir: string = process.cwd()): string | undefined {
  let dir = path.resolve(startDir)
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const candidate = path.join(dir, '.inspire', 'config.toml')
    if (fs.existsSync(candidate)) return candidate
    const parent = path.dirname(dir)
    if (parent === dir) return undefined // reached root
    dir = parent
  }
}

/**
 * Resolve the inspire config by walking up for `.inspire/config.toml` and
 * parsing the fields the SDK cares about. Never throws: a missing or
 * malformed file resolves to `{}` so a bad config can't take an app down on
 * boot — the caller falls back to env/defaults.
 */
export function loadInspireConfig(startDir: string = process.cwd()): InspireConfig {
  const file = findInspireConfigPath(startDir)
  if (!file) return {}
  let raw: unknown
  try {
    raw = parseToml(fs.readFileSync(file, 'utf8'))
  } catch {
    return {}
  }
  if (typeof raw !== 'object' || raw === null) return {}
  const root = raw as Record<string, unknown>
  const out: InspireConfig = {}

  const broker = root.broker
  if (typeof broker === 'object' && broker !== null) {
    const b = broker as Record<string, unknown>
    const host = typeof b.host === 'string' ? b.host : undefined
    const port = typeof b.port === 'number' ? b.port : undefined
    if (host !== undefined || port !== undefined) out.broker = { host, port }
  }

  const reporting = root.reporting
  if (typeof reporting === 'object' && reporting !== null) {
    const r = reporting as Record<string, unknown>
    if (typeof r.heartbeat_interval_s === 'number') {
      out.reporting = { heartbeatIntervalMs: r.heartbeat_interval_s * 1000 }
    }
  }

  return out
}

/** Parse an env var to a positive integer, or undefined if unset/invalid. */
function envInt(name: string): number | undefined {
  const v = process.env[name]
  if (v === undefined || v === '') return undefined
  const n = Number(v)
  return Number.isFinite(n) && n > 0 ? n : undefined
}

/**
 * Resolve the broker host/port using the full precedence chain:
 *   explicit opts.broker  >  env  >  .inspire/config.toml  >  defaults.
 * Env accepts both the canonical INSPIRE_BROKER_* and the bare BROKER_*
 * names that several apps already use, so adoption needs no env churn.
 */
export function resolveBroker(
  opts: { host?: string; port?: number } | undefined,
  fileConfig: InspireConfig,
  defaults: { host: string; port: number },
): { host: string; port: number } {
  const host =
    opts?.host ??
    process.env.INSPIRE_BROKER_HOST ??
    process.env.BROKER_HOST ??
    fileConfig.broker?.host ??
    defaults.host
  const port =
    opts?.port ??
    envInt('INSPIRE_BROKER_PORT') ??
    envInt('BROKER_PORT') ??
    fileConfig.broker?.port ??
    defaults.port
  return { host, port }
}
