import { type BusClient, type BusClientOptions } from './bus';
import type { CapabilityManifestMsg, CommandMsg, HeartbeatMsg, LogLevel, LogMsg, PresenceMsg, RpcHandler, RpcRequestMsg, RpcResponseMsg, RpcTarget, StatusMsg, StatusState, VerbSpec } from './types';
export interface InspireStartOptions {
    slug: string;
    version: string;
    broker?: {
        host?: string;
        port?: number;
    };
    /** Override hostname-based node_id slugifier output. */
    nodeId?: string;
    /** Mark `service_mode: true` in PresenceMsg (spec §4.2). */
    serviceMode?: boolean;
    /** Test-only override; production should leave at 10000ms (spec §5). */
    heartbeatIntervalMs?: number;
    /** Override clientId; defaults to `<slug>-<nodeId>-<pid>`. */
    clientId?: string;
    /** mqtt connect options pass-through (advanced). */
    reconnectPeriod?: number;
    connectTimeout?: number;
    /** Skip `.inspire/config.toml` resolution (default: false — config is read). */
    loadConfig?: boolean;
}
export interface InspireClient {
    /** Publish a retained StatusMsg. */
    setStatus(state: StatusState, detail: string): Promise<void>;
    /** Publish a LogMsg (only routed when atrium has verbose mode on for this app). */
    log(level: LogLevel, msg: string, fields?: Record<string, unknown>): Promise<void>;
    /** Register a handler for an inbound CommandMsg verb. */
    onCommand(cmd: string, handler: (msg: CommandMsg) => void): void;
    /**
     * Register an RPC verb handler. The handler's return value (or resolved
     * value) is sent back to the caller as the result; a throw becomes an
     * error response. Registering adds the verb to the capability manifest and
     * republishes it.
     */
    onCall(verb: string, handler: RpcHandler, spec?: Omit<VerbSpec, 'name'>): void;
    /**
     * Invoke a verb on a (possibly remote, across-bridge) app instance and await
     * the result. Rejects on timeout or on an error response from the target.
     */
    call(target: RpcTarget, verb: string, args?: Record<string, unknown>, opts?: {
        timeoutMs?: number;
    }): Promise<unknown>;
    /** The app's current capability manifest (the verbs registered via onCall). */
    readonly verbs: VerbSpec[];
    /** Tear down: clear retained presence + manifest, stop heartbeat, disconnect. */
    stop(): Promise<void>;
    /** Resolved node_id used in topics — useful for tests / diagnostics. */
    readonly nodeId: string;
    /** Resolved slug. */
    readonly slug: string;
}
export declare const Inspire: {
    /**
     * Start an inspire-sdk session: connect to the broker, publish presence
     * retained, start the 10s heartbeat, subscribe to commands. Resolves to
     * an `InspireClient` once the initial presence has been published (the
     * point at which atrium can render the row).
     */
    start(opts: InspireStartOptions): Promise<InspireClient>;
    /**
     * Create a consumer/hub bus client (observer + caller): subscribes to every
     * app's presence/heartbeat/status/manifest as typed events and can `call()`
     * any app's RPC verb. Optionally publishes a retained self-presence (hubs).
     * Returns immediately, NOT connected — wire up `.on(...)` then `.connect()`.
     */
    observe(opts?: BusClientOptions): BusClient;
};
export { findInspireConfigPath, loadInspireConfig, resolveBroker } from './config';
export type { InspireConfig } from './config';
export { createBusClient } from './bus';
export type { BusClient, BusClientOptions, SelfPresence } from './bus';
export * as topics from './topics';
export type { CapabilityManifestMsg, CommandMsg, HeartbeatMsg, LogLevel, LogMsg, PresenceMsg, RpcHandler, RpcRequestMsg, RpcResponseMsg, RpcTarget, StatusMsg, StatusState, VerbSpec, };
