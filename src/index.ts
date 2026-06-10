// inspire-sdk — Node SDK for inspire-* apps to participate in the
// inspire-atrium MQTT messaging fabric. Wire format per spec §4.2 of
// `INSPIRE_ATRIUM_SPEC_ADDENDUM_2.md`.
//
// Usage (the 5-line "near boot" snippet from the migration guide):
//
//   import { Inspire } from 'inspire-sdk'
//   const client = await Inspire.start({
//     slug: 'inspire-financial',
//     version: '0.4.2',
//   })
//   await client.setStatus('ready', 'all systems online')
//   process.on('SIGTERM', () => client.stop())
//
// What `start()` does:
//   1. Connects to broker (defaults to `127.0.0.1:1883` per migration guide).
//   2. Configures Last Will & Testament: empty retained payload on
//      `inspire/presence/<slug>/<nodeId>` so atrium auto-cleans on crash
//      (spec §4.1).
//   3. Publishes retained PresenceMsg.
//   4. Starts a 10s heartbeat interval (overridable for tests).
//   5. Subscribes to `inspire/cmd/<slug>/<nodeId>` for atrium → app commands.
//
// What `stop()` does:
//   1. Clears the heartbeat interval.
//   2. Publishes empty retained payload on the presence topic (graceful clear).
//   3. Disconnects.
//
// Slice E will add `forwardClaudeSession()`. NOT in this slice.

import os from 'node:os'

import mqtt, { type IClientOptions, type MqttClient } from 'mqtt'

import type {
  CapabilityManifestMsg,
  CommandMsg,
  HeartbeatMsg,
  LogLevel,
  LogMsg,
  PresenceMsg,
  RpcHandler,
  RpcRequestMsg,
  RpcResponseMsg,
  RpcTarget,
  StatusMsg,
  StatusState,
  VerbSpec,
} from './types'

const DEFAULT_BROKER_HOST = '127.0.0.1'
const DEFAULT_BROKER_PORT = 1883
const DEFAULT_HEARTBEAT_MS = 10_000
const DEFAULT_RPC_TIMEOUT_MS = 8_000

let rpcCounter = 0
/** Process-unique, time-free correlation id (Date.now-free for determinism in tests). */
function newCorrId(): string {
  rpcCounter += 1
  return `${process.pid.toString(36)}-${rpcCounter.toString(36)}`
}

export interface InspireStartOptions {
  slug: string
  version: string
  broker?: { host?: string; port?: number }
  /** Override hostname-based node_id slugifier output. */
  nodeId?: string
  /** Mark `service_mode: true` in PresenceMsg (spec §4.2). */
  serviceMode?: boolean
  /** Test-only override; production should leave at 10000ms (spec §5). */
  heartbeatIntervalMs?: number
  /** Override clientId; defaults to `<slug>-<nodeId>-<pid>`. */
  clientId?: string
  /** mqtt connect options pass-through (advanced). */
  reconnectPeriod?: number
  connectTimeout?: number
}

export interface InspireClient {
  /** Publish a retained StatusMsg. */
  setStatus(state: StatusState, detail: string): Promise<void>
  /** Publish a LogMsg (only routed when atrium has verbose mode on for this app). */
  log(level: LogLevel, msg: string, fields?: Record<string, unknown>): Promise<void>
  /** Register a handler for an inbound CommandMsg verb. */
  onCommand(cmd: string, handler: (msg: CommandMsg) => void): void
  /**
   * Register an RPC verb handler. The handler's return value (or resolved
   * value) is sent back to the caller as the result; a throw becomes an
   * error response. Registering adds the verb to the capability manifest and
   * republishes it.
   */
  onCall(verb: string, handler: RpcHandler, spec?: Omit<VerbSpec, 'name'>): void
  /**
   * Invoke a verb on a (possibly remote, across-bridge) app instance and await
   * the result. Rejects on timeout or on an error response from the target.
   */
  call(
    target: RpcTarget,
    verb: string,
    args?: Record<string, unknown>,
    opts?: { timeoutMs?: number },
  ): Promise<unknown>
  /** The app's current capability manifest (the verbs registered via onCall). */
  readonly verbs: VerbSpec[]
  /** Tear down: clear retained presence + manifest, stop heartbeat, disconnect. */
  stop(): Promise<void>
  /** Resolved node_id used in topics — useful for tests / diagnostics. */
  readonly nodeId: string
  /** Resolved slug. */
  readonly slug: string
}

/** Slugify a hostname per spec §4: lowercase, non-alphanumerics → '-'. */
function slugifyNodeId(hostname: string): string {
  const lowered = (hostname ?? '').toLowerCase()
  const dashed = lowered.replace(/[^a-z0-9]+/g, '-')
  const trimmed = dashed.replace(/^-+|-+$/g, '')
  return trimmed.length > 0 ? trimmed : 'unknown'
}

function topicPresence(slug: string, nodeId: string): string {
  return `inspire/presence/${slug}/${nodeId}`
}
function topicHeartbeat(slug: string, nodeId: string): string {
  return `inspire/heartbeat/${slug}/${nodeId}`
}
function topicStatus(slug: string, nodeId: string): string {
  return `inspire/status/${slug}/${nodeId}`
}
function topicLog(slug: string, nodeId: string): string {
  return `inspire/log/${slug}/${nodeId}`
}
function topicCmd(slug: string, nodeId: string): string {
  return `inspire/cmd/${slug}/${nodeId}`
}
function topicManifest(slug: string, nodeId: string): string {
  return `inspire/manifest/${slug}/${nodeId}`
}
/** Inbound RPC requests for this app instance. */
function topicRpcCall(slug: string, nodeId: string): string {
  return `inspire/rpc/${slug}/${nodeId}/call`
}
/** Reply channel keyed by the caller's reply_to id. Caller subscribes the `+` wildcard. */
function topicRpcReply(replyTo: string, corrId: string): string {
  return `inspire/rpc/_reply/${replyTo}/${corrId}`
}
function topicRpcReplyWildcard(replyTo: string): string {
  return `inspire/rpc/_reply/${replyTo}/+`
}

class InspireClientImpl implements InspireClient {
  readonly slug: string
  readonly nodeId: string
  private readonly client: MqttClient
  private readonly startedAt: string
  private readonly serviceMode: boolean
  private readonly version: string
  private readonly heartbeatIntervalMs: number
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null
  private commandHandlers = new Map<string, (msg: CommandMsg) => void>()
  private commandMessageHandler: ((topic: string, payload: Buffer) => void) | null = null
  private stopped = false
  // RPC + manifest state
  private rpcHandlers = new Map<string, RpcHandler>()
  private verbSpecs = new Map<string, VerbSpec>()
  private rpcServerHandler: ((topic: string, payload: Buffer) => void) | null = null
  /** reply_to id this instance uses when it acts as an RPC caller. */
  private readonly replyTo: string
  /** Pending outbound calls awaiting a reply, keyed by corr_id. */
  private pendingCalls = new Map<string, { resolve: (v: unknown) => void; reject: (e: Error) => void; timer: ReturnType<typeof setTimeout> }>()
  private rpcReplyHandler: ((topic: string, payload: Buffer) => void) | null = null
  private rpcReplySubscribed = false

  constructor(opts: {
    slug: string
    nodeId: string
    version: string
    serviceMode: boolean
    client: MqttClient
    heartbeatIntervalMs: number
  }) {
    this.slug = opts.slug
    this.nodeId = opts.nodeId
    this.version = opts.version
    this.serviceMode = opts.serviceMode
    this.client = opts.client
    this.heartbeatIntervalMs = opts.heartbeatIntervalMs
    this.startedAt = new Date().toISOString()
    this.replyTo = `${opts.slug}-${opts.nodeId}-${process.pid}`
  }

  get verbs(): VerbSpec[] {
    return [...this.verbSpecs.values()]
  }

  /** Internal: publish initial retained presence. */
  private publishPresence(): Promise<void> {
    const msg: PresenceMsg = {
      v: 1,
      app_slug: this.slug,
      node_id: this.nodeId,
      version: this.version,
      started_at: this.startedAt,
      pid: process.pid,
      service_mode: this.serviceMode,
    }
    return new Promise((resolve, reject) => {
      this.client.publish(
        topicPresence(this.slug, this.nodeId),
        JSON.stringify(msg),
        { qos: 1, retain: true },
        (err) => (err ? reject(err) : resolve()),
      )
    })
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) return
    const tick = () => {
      const memMb = Math.round(process.memoryUsage().rss / (1024 * 1024))
      const msg: HeartbeatMsg = {
        v: 1,
        ts: new Date().toISOString(),
        uptime_s: Math.floor(process.uptime()),
        rss_mb: memMb,
        // cpu_pct: cheap approximation. Better implementations will
        // sample process.cpuUsage() deltas across an interval. Slice A
        // doesn't gate on accuracy here.
        cpu_pct: 0,
      }
      this.client.publish(
        topicHeartbeat(this.slug, this.nodeId),
        JSON.stringify(msg),
        { qos: 0, retain: false },
      )
    }
    // Don't fire immediately — the receiver already has the presence msg
    // and atrium uses a freshness window, not a hb count.
    this.heartbeatTimer = setInterval(tick, this.heartbeatIntervalMs)
    if (typeof this.heartbeatTimer.unref === 'function') this.heartbeatTimer.unref()
  }

  private subscribeCommands(): void {
    const cmdTopic = topicCmd(this.slug, this.nodeId)
    this.client.subscribe(cmdTopic, { qos: 1 })
    // Capture the handler so stop() can remove it. Without this, calling
    // Inspire.start() multiple times in one process (e.g. test fixtures)
    // accumulates 'message' listeners that survive each stop().
    const handler = (topic: string, payload: Buffer): void => {
      if (topic !== cmdTopic) return
      try {
        const parsed = JSON.parse(payload.toString('utf8')) as CommandMsg
        const fn = this.commandHandlers.get(parsed.cmd)
        if (fn) fn(parsed)
      } catch {
        /* malformed — ignore */
      }
    }
    this.commandMessageHandler = handler
    this.client.on('message', handler)
  }

  // ── capability manifest ──

  /** Publish the retained capability manifest built from registered verbs. */
  private publishManifest(): Promise<void> {
    const msg: CapabilityManifestMsg = {
      v: 1,
      app_slug: this.slug,
      node_id: this.nodeId,
      version: this.version,
      ts: new Date().toISOString(),
      verbs: this.verbs,
    }
    return new Promise((resolve, reject) => {
      this.client.publish(
        topicManifest(this.slug, this.nodeId),
        JSON.stringify(msg),
        { qos: 1, retain: true },
        (err) => (err ? reject(err) : resolve()),
      )
    })
  }

  // ── RPC server (inbound: this app answers verbs) ──

  /** Subscribe the inbound RPC call topic once; dispatch to registered handlers. */
  private subscribeRpc(): void {
    if (this.rpcServerHandler) return
    const callTopic = topicRpcCall(this.slug, this.nodeId)
    this.client.subscribe(callTopic, { qos: 1 })
    const handler = (topic: string, payload: Buffer): void => {
      if (topic !== callTopic) return
      let req: RpcRequestMsg
      try {
        req = JSON.parse(payload.toString('utf8')) as RpcRequestMsg
      } catch {
        return // malformed — ignore
      }
      void this.dispatchRpc(req)
    }
    this.rpcServerHandler = handler
    this.client.on('message', handler)
  }

  private async dispatchRpc(req: RpcRequestMsg): Promise<void> {
    const replyTopic = topicRpcReply(req.reply_to, req.corr_id)
    const fn = this.rpcHandlers.get(req.verb)
    let res: RpcResponseMsg
    if (!fn) {
      res = {
        v: 1,
        corr_id: req.corr_id,
        ok: false,
        error: { message: `unknown verb: ${req.verb}`, code: 'UNKNOWN_VERB' },
        ts: new Date().toISOString(),
      }
    } else {
      try {
        const result = await fn(req.args ?? {}, req)
        res = { v: 1, corr_id: req.corr_id, ok: true, result, ts: new Date().toISOString() }
      } catch (e) {
        res = {
          v: 1,
          corr_id: req.corr_id,
          ok: false,
          error: { message: e instanceof Error ? e.message : String(e), code: 'HANDLER_ERROR' },
          ts: new Date().toISOString(),
        }
      }
    }
    this.client.publish(replyTopic, JSON.stringify(res), { qos: 1, retain: false })
  }

  onCall(verb: string, handler: RpcHandler, spec?: Omit<VerbSpec, 'name'>): void {
    const isNew = !this.verbSpecs.has(verb)
    this.rpcHandlers.set(verb, handler)
    this.verbSpecs.set(verb, { name: verb, ...spec })
    this.subscribeRpc()
    // Republish the manifest so discovery reflects the new verb. Best-effort:
    // if not yet connected, start() publishes the full manifest anyway.
    if (this.client.connected && (isNew || spec)) {
      void this.publishManifest().catch(() => {})
    }
  }

  // ── RPC client (outbound: this app calls another app's verb) ──

  /** Lazily subscribe this caller's reply wildcard and install the demux handler. */
  private ensureReplySubscription(): void {
    if (this.rpcReplySubscribed) return
    this.rpcReplySubscribed = true
    this.client.subscribe(topicRpcReplyWildcard(this.replyTo), { qos: 1 })
    const handler = (topic: string, payload: Buffer): void => {
      if (!topic.startsWith(`inspire/rpc/_reply/${this.replyTo}/`)) return
      let res: RpcResponseMsg
      try {
        res = JSON.parse(payload.toString('utf8')) as RpcResponseMsg
      } catch {
        return
      }
      const pending = this.pendingCalls.get(res.corr_id)
      if (!pending) return
      clearTimeout(pending.timer)
      this.pendingCalls.delete(res.corr_id)
      if (res.ok) pending.resolve(res.result)
      else pending.reject(new Error(res.error?.message ?? 'rpc error'))
    }
    this.rpcReplyHandler = handler
    this.client.on('message', handler)
  }

  call(
    target: RpcTarget,
    verb: string,
    args: Record<string, unknown> = {},
    opts?: { timeoutMs?: number },
  ): Promise<unknown> {
    this.ensureReplySubscription()
    const corrId = newCorrId()
    const timeoutMs = opts?.timeoutMs ?? DEFAULT_RPC_TIMEOUT_MS
    const req: RpcRequestMsg = {
      v: 1,
      corr_id: corrId,
      reply_to: this.replyTo,
      verb,
      args,
      ts: new Date().toISOString(),
    }
    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingCalls.delete(corrId)
        reject(new Error(`rpc call ${target.slug}/${target.nodeId}.${verb} timed out after ${timeoutMs}ms`))
      }, timeoutMs)
      if (typeof timer.unref === 'function') timer.unref()
      this.pendingCalls.set(corrId, { resolve, reject, timer })
      this.client.publish(
        topicRpcCall(target.slug, target.nodeId),
        JSON.stringify(req),
        { qos: 1, retain: false },
        (err) => {
          if (err) {
            clearTimeout(timer)
            this.pendingCalls.delete(corrId)
            reject(err)
          }
        },
      )
    })
  }

  setStatus(state: StatusState, detail: string): Promise<void> {
    const msg: StatusMsg = { v: 1, state, detail, ts: new Date().toISOString() }
    return new Promise((resolve, reject) => {
      this.client.publish(
        topicStatus(this.slug, this.nodeId),
        JSON.stringify(msg),
        { qos: 1, retain: true },
        (err) => (err ? reject(err) : resolve()),
      )
    })
  }

  log(level: LogLevel, msg: string, fields?: Record<string, unknown>): Promise<void> {
    const payload: LogMsg = { v: 1, ts: new Date().toISOString(), level, msg, fields }
    return new Promise((resolve, reject) => {
      this.client.publish(
        topicLog(this.slug, this.nodeId),
        JSON.stringify(payload),
        { qos: 0, retain: false },
        (err) => (err ? reject(err) : resolve()),
      )
    })
  }

  onCommand(cmd: string, handler: (msg: CommandMsg) => void): void {
    this.commandHandlers.set(cmd, handler)
  }

  async stop(): Promise<void> {
    if (this.stopped) return
    this.stopped = true
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer)
      this.heartbeatTimer = null
    }
    if (this.commandMessageHandler) {
      this.client.off('message', this.commandMessageHandler)
      this.commandMessageHandler = null
    }
    if (this.rpcServerHandler) {
      this.client.off('message', this.rpcServerHandler)
      this.rpcServerHandler = null
    }
    if (this.rpcReplyHandler) {
      this.client.off('message', this.rpcReplyHandler)
      this.rpcReplyHandler = null
    }
    // Fail any in-flight outbound calls so callers don't hang on shutdown.
    for (const [, p] of this.pendingCalls) {
      clearTimeout(p.timer)
      p.reject(new Error('inspire-sdk client stopped'))
    }
    this.pendingCalls.clear()
    // Only attempt the retained-clear publish if we're actually connected.
    // Offline, mqtt's publish callback never fires (it queues), and
    // `client.end(true)` will flush+drop the queue — atrium would then keep
    // showing the row until LWT eventually triggers from the broker side.
    // Skipping the publish when offline is correct: LWT (set up at connect
    // time) handles the cleanup on the broker's reconnect-and-disconnect.
    if (this.client.connected) {
      await new Promise<void>((resolve) => {
        this.client.publish(
          topicPresence(this.slug, this.nodeId),
          Buffer.alloc(0),
          { qos: 1, retain: true },
          () => resolve(),
        )
      })
      // Clear the retained manifest too, so atrium drops the app's API surface
      // on graceful exit (LWT only covers presence).
      await new Promise<void>((resolve) => {
        this.client.publish(
          topicManifest(this.slug, this.nodeId),
          Buffer.alloc(0),
          { qos: 1, retain: true },
          () => resolve(),
        )
      })
    }
    await new Promise<void>((resolve) => {
      this.client.end(true, {}, () => resolve())
    })
  }

  /** Internal: complete startup. Resolves when ready to receive commands. */
  async start(): Promise<void> {
    await this.publishPresence()
    this.startHeartbeat()
    this.subscribeCommands()
    this.subscribeRpc()
    await this.publishManifest()
  }
}

export const Inspire = {
  /**
   * Start an inspire-sdk session: connect to the broker, publish presence
   * retained, start the 10s heartbeat, subscribe to commands. Resolves to
   * an `InspireClient` once the initial presence has been published (the
   * point at which atrium can render the row).
   */
  async start(opts: InspireStartOptions): Promise<InspireClient> {
    const slug = opts.slug
    if (!slug) throw new Error('Inspire.start: slug is required')
    const nodeId = opts.nodeId ?? slugifyNodeId(os.hostname())
    const host = opts.broker?.host ?? DEFAULT_BROKER_HOST
    const port = opts.broker?.port ?? DEFAULT_BROKER_PORT
    const heartbeatIntervalMs = opts.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_MS
    const serviceMode = opts.serviceMode ?? false

    const connectOpts: IClientOptions = {
      host,
      port,
      protocolVersion: 4,
      clean: true,
      clientId: opts.clientId ?? `${slug}-${nodeId}-${process.pid}`,
      reconnectPeriod: opts.reconnectPeriod ?? 2_000,
      connectTimeout: opts.connectTimeout ?? 8_000,
      // LWT — empty retained payload so atrium drops the row on crash.
      will: {
        topic: topicPresence(slug, nodeId),
        payload: Buffer.alloc(0),
        qos: 1,
        retain: true,
      },
    }

    const client = mqtt.connect(connectOpts)
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error('Inspire.start: connect timed out')),
          connectOpts.connectTimeout!,
        )
        client.once('connect', () => {
          clearTimeout(timer)
          resolve()
        })
        client.once('error', (err) => {
          clearTimeout(timer)
          reject(err)
        })
      })
    } catch (err) {
      // Without an explicit end(), mqtt's default reconnectPeriod keeps the
      // failed client retrying in the background while the caller already
      // received a rejection — silent connection leak.
      client.end(true)
      throw err
    }

    const inst = new InspireClientImpl({
      slug,
      nodeId,
      version: opts.version,
      serviceMode,
      client,
      heartbeatIntervalMs,
    })
    await inst.start()
    return inst
  },
}

export type {
  CapabilityManifestMsg,
  CommandMsg,
  HeartbeatMsg,
  LogLevel,
  LogMsg,
  PresenceMsg,
  RpcHandler,
  RpcRequestMsg,
  RpcResponseMsg,
  RpcTarget,
  StatusMsg,
  StatusState,
  VerbSpec,
}
