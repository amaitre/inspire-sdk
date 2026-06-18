// Consumer / hub bus client for inspire-* — the OBSERVER + CALLER role.
//
// Where `Inspire.start()` is for an app that announces itself (presence,
// heartbeat, command/RPC server), `Inspire.observe()` is for a consumer that
// watches the whole bus and invokes verbs on others: it subscribes to every
// app's presence/heartbeat/status/manifest, re-emits them as typed events, and
// can `call()` any app's RPC verb. Optionally it publishes a single retained
// self-presence (with an LWT clear) — used by hubs like atrium.
//
// This is the state machine atrium and inspire-projects each hand-rolled
// (~280 lines apiece, kept in sync with the contract by prose). It now lives
// in the SDK once. Event signatures match atrium's InspireBus so adoption is a
// drop-in swap.

import { EventEmitter } from 'node:events'
import os from 'node:os'

import mqtt, { type IClientOptions, type MqttClient } from 'mqtt'

import { loadInspireConfig, resolveBroker } from './config'
import {
  TOPIC_WILDCARD,
  slugifyNodeId,
  topicCmd,
  topicRpcCall,
  topicRpcReplyWildcard,
} from './topics'
import type {
  CapabilityManifestMsg,
  CommandMsg,
  HeartbeatMsg,
  PresenceMsg,
  RpcRequestMsg,
  RpcResponseMsg,
  StatusMsg,
} from './types'

const DEFAULT_BROKER_HOST = '127.0.0.1'
const DEFAULT_BROKER_PORT = 1883
const DEFAULT_RPC_TIMEOUT_MS = 20_000

interface PendingCall {
  resolve: (value: unknown) => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
}

/** A retained self-presence the consumer publishes on connect, cleared via LWT on crash. */
export interface SelfPresence {
  /** Topic to publish the retained presence on (and clear via LWT). */
  topic: string
  /** Builds the JSON-serializable presence payload (called fresh on each connect). */
  message: () => Record<string, unknown>
}

export interface BusClientOptions {
  broker?: { host?: string; port?: number }
  /** Skip `.inspire/config.toml` resolution (default: false). */
  loadConfig?: boolean
  clientId?: string
  connectTimeout?: number
  /** mqtt reconnect period in ms. 0 disables reconnect. */
  reconnectPeriod?: number
  /** Topic-safe reply_to for RPC calls. Default `consumer-<hostname-slug>`. */
  callerId?: string
  /** Default per-call RPC timeout. */
  rpcTimeoutMs?: number
  /** Optional retained self-presence (for hubs like atrium). */
  selfPresence?: SelfPresence
}

export interface BusClient {
  /** Connect, (optionally) publish self-presence retained, subscribe to the bus. */
  connect(): void
  /** Invoke an RPC verb on a target app instance; resolves with its result. */
  call(
    target: { slug: string; nodeId: string },
    verb: string,
    args?: Record<string, unknown>,
    opts?: { timeoutMs?: number },
  ): Promise<unknown>
  /** Publish a fire-and-forget CommandMsg to one app (QoS 1, not retained). */
  publishCommand(slug: string, nodeId: string, msg: CommandMsg): void
  /** True once at least one broker connect has succeeded. */
  hasEverConnected(): boolean
  /** Disconnect, clear self-presence, fail in-flight calls. Idempotent. */
  stop(): Promise<void>
  // EventEmitter surface (typed events: presence|heartbeat|status|manifest|connect|connecting|disconnect|error)
  on(event: string, listener: (...args: any[]) => void): this
  once(event: string, listener: (...args: any[]) => void): this
  off(event: string, listener: (...args: any[]) => void): this
  emit(event: string, ...args: any[]): boolean
}

class BusClientImpl extends EventEmitter implements BusClient {
  private client: MqttClient | null = null
  private connectedOnce = false
  private shuttingDown = false
  private readonly host: string
  private readonly port: number
  private readonly clientId: string
  private readonly connectTimeout: number
  private readonly reconnectPeriod: number
  private readonly rpcReplyTo: string
  private readonly rpcTimeoutMs: number
  private readonly selfPresence?: SelfPresence
  private rpcCounter = 0
  private readonly pendingCalls = new Map<string, PendingCall>()

  constructor(opts: BusClientOptions) {
    super()
    const fileConfig = opts.loadConfig === false ? {} : loadInspireConfig()
    const resolved = resolveBroker(opts.broker, fileConfig, {
      host: DEFAULT_BROKER_HOST,
      port: DEFAULT_BROKER_PORT,
    })
    this.host = resolved.host
    this.port = resolved.port
    const hostSlug = slugifyNodeId(os.hostname())
    this.clientId = opts.clientId ?? `consumer-${hostSlug}-${process.pid}`
    this.connectTimeout = opts.connectTimeout ?? 8_000
    this.reconnectPeriod = opts.reconnectPeriod ?? 2_000
    this.rpcReplyTo = opts.callerId ?? `consumer-${hostSlug}`
    this.rpcTimeoutMs = opts.rpcTimeoutMs ?? DEFAULT_RPC_TIMEOUT_MS
    this.selfPresence = opts.selfPresence
  }

  connect(): void {
    if (this.client) return // idempotent
    const connectOpts: IClientOptions = {
      host: this.host,
      port: this.port,
      clientId: this.clientId,
      protocolVersion: 4,
      clean: true,
      reconnectPeriod: this.reconnectPeriod,
      connectTimeout: this.connectTimeout,
    }
    if (this.selfPresence) {
      // LWT: empty retained payload clears the self-presence on ungraceful exit.
      connectOpts.will = {
        topic: this.selfPresence.topic,
        payload: Buffer.alloc(0),
        qos: 1,
        retain: true,
      }
    }
    const client = mqtt.connect(connectOpts)
    this.client = client

    client.on('connect', () => {
      this.connectedOnce = true
      this.publishSelfPresence()
      this.subscribeBusTopics()
      this.emit('connect')
    })
    client.on('reconnect', () => this.emit('connecting'))
    client.on('close', () => {
      if (!this.shuttingDown) this.emit('disconnect')
    })
    client.on('offline', () => this.emit('disconnect'))
    client.on('error', (err) => this.emit('error', err))
    client.on('message', (topic, payload) => this.dispatchMessage(topic, payload))
  }

  hasEverConnected(): boolean {
    return this.connectedOnce
  }

  publishCommand(slug: string, nodeId: string, msg: CommandMsg): void {
    if (!this.client) return
    this.client.publish(topicCmd(slug, nodeId), JSON.stringify(msg), { qos: 1, retain: false })
  }

  call(
    target: { slug: string; nodeId: string },
    verb: string,
    args: Record<string, unknown> = {},
    opts?: { timeoutMs?: number },
  ): Promise<unknown> {
    const client = this.client
    if (!client) return Promise.reject(new Error('inspire bus not connected'))
    const corrId = `${process.pid.toString(36)}-${(this.rpcCounter += 1).toString(36)}`
    const timeoutMs = opts?.timeoutMs ?? this.rpcTimeoutMs
    const req: RpcRequestMsg = {
      v: 1,
      corr_id: corrId,
      reply_to: this.rpcReplyTo,
      verb,
      args,
      ts: new Date().toISOString(),
    }
    return new Promise<unknown>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pendingCalls.delete(corrId)
        reject(new Error(`rpc ${target.slug}/${target.nodeId}.${verb} timed out after ${timeoutMs}ms`))
      }, timeoutMs)
      this.pendingCalls.set(corrId, { resolve, reject, timer })
      client.publish(
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

  async stop(): Promise<void> {
    if (this.shuttingDown) return
    this.shuttingDown = true
    for (const [, p] of this.pendingCalls) {
      clearTimeout(p.timer)
      p.reject(new Error('inspire bus shutting down'))
    }
    this.pendingCalls.clear()
    const client = this.client
    this.client = null
    if (!client) return
    if (this.selfPresence && client.connected) {
      await new Promise<void>((resolve) => {
        try {
          client.publish(this.selfPresence!.topic, Buffer.alloc(0), { qos: 1, retain: true }, () => resolve())
        } catch {
          resolve()
        }
      })
    }
    await new Promise<void>((resolve) => client.end(true, {}, () => resolve()))
  }

  // -------------------------------------------------------------- internals

  private publishSelfPresence(): void {
    if (!this.client || !this.selfPresence) return
    this.client.publish(
      this.selfPresence.topic,
      JSON.stringify(this.selfPresence.message()),
      { qos: 1, retain: true },
    )
  }

  private subscribeBusTopics(): void {
    if (!this.client) return
    this.client.subscribe(
      [
        TOPIC_WILDCARD.presence,
        TOPIC_WILDCARD.heartbeat,
        TOPIC_WILDCARD.status,
        TOPIC_WILDCARD.manifest,
        topicRpcReplyWildcard(this.rpcReplyTo),
      ],
      { qos: 1 },
      (err, granted) => {
        if (err) {
          this.emit('error', err)
          return
        }
        const denied = granted?.find((g) => g.qos === 128)
        if (denied) this.emit('error', new Error(`SUBSCRIBE rejected for ${denied.topic}`))
      },
    )
  }

  private handleRpcReply(payload: Buffer): void {
    const res = safeJsonParse<RpcResponseMsg>(payload)
    if (!res) return
    const pending = this.pendingCalls.get(res.corr_id)
    if (!pending) return
    clearTimeout(pending.timer)
    this.pendingCalls.delete(res.corr_id)
    if (res.ok) pending.resolve(res.result)
    else pending.reject(new Error(res.error?.message ?? 'rpc error'))
  }

  private dispatchMessage(topic: string, payload: Buffer): void {
    const isEmpty = payload.length === 0
    const parts = topic.split('/')
    if (parts[0] === 'inspire' && parts[1] === 'rpc' && parts[2] === '_reply') {
      this.handleRpcReply(payload)
      return
    }
    if (parts.length !== 4 || parts[0] !== 'inspire') return
    const kind = parts[1]
    const slug = parts[2]
    const nodeId = parts[3]
    if (!slug || !nodeId) return

    if (kind === 'presence') {
      if (isEmpty) {
        this.emit('presence', null, slug, nodeId)
        return
      }
      const parsed = safeJsonParse<PresenceMsg>(payload)
      if (parsed) this.emit('presence', parsed, slug, nodeId)
      return
    }
    if (kind === 'heartbeat') {
      if (isEmpty) return
      const parsed = safeJsonParse<HeartbeatMsg>(payload)
      if (parsed) this.emit('heartbeat', parsed, slug, nodeId)
      return
    }
    if (kind === 'status') {
      if (isEmpty) return
      const parsed = safeJsonParse<StatusMsg>(payload)
      if (parsed) this.emit('status', parsed, slug, nodeId)
      return
    }
    if (kind === 'manifest') {
      if (isEmpty) {
        this.emit('manifest', null, slug, nodeId)
        return
      }
      const parsed = safeJsonParse<CapabilityManifestMsg>(payload)
      if (parsed) this.emit('manifest', parsed, slug, nodeId)
    }
  }
}

function safeJsonParse<T>(buf: Buffer): T | null {
  try {
    return JSON.parse(buf.toString('utf8')) as T
  } catch {
    return null
  }
}

/** Create a consumer/hub bus client (observer + caller). Call `.connect()` after wiring events. */
export function createBusClient(opts: BusClientOptions = {}): BusClient {
  return new BusClientImpl(opts)
}
