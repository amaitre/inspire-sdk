// Wire-format types for the inspire-* MQTT bus, mirroring spec §4.2 of
// `INSPIRE_ATRIUM_SPEC_ADDENDUM_2.md`. Intentionally duplicated from
// `src/shared/inspire.ts` / `src/main/inspire/types.ts` so this SDK can be
// extracted to its own repo with no source dependency on atrium.
//
// Asymmetry: `AtriumPresenceMsg` lives only in atrium's copy of the types —
// only atrium publishes it. SDK-side apps don't need it.

export interface PresenceMsg {
  v: 1
  app_slug: string
  node_id: string
  version: string
  started_at: string
  pid: number
  service_mode: boolean
}

export interface HeartbeatMsg {
  v: 1
  ts: string
  uptime_s: number
  rss_mb: number
  cpu_pct: number
}

export type StatusState = 'starting' | 'ready' | 'degraded' | 'stopping' | 'error'

export interface StatusMsg {
  v: 1
  state: StatusState
  detail: string
  ts: string
}

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export interface LogMsg {
  v: 1
  ts: string
  level: LogLevel
  msg: string
  fields?: Record<string, unknown>
}

export type CommandVerb = 'set_verbose' | 'shutdown' | 'dump_debug' | 'kill_claude_session'

export interface CommandMsg {
  v: 1
  cmd: CommandVerb
  args: Record<string, unknown>
  request_id: string
}
