import type { CommandMsg } from './types';
/** A retained self-presence the consumer publishes on connect, cleared via LWT on crash. */
export interface SelfPresence {
    /** Topic to publish the retained presence on (and clear via LWT). */
    topic: string;
    /** Builds the JSON-serializable presence payload (called fresh on each connect). */
    message: () => Record<string, unknown>;
}
export interface BusClientOptions {
    broker?: {
        host?: string;
        port?: number;
    };
    /** Skip `.inspire/config.toml` resolution (default: false). */
    loadConfig?: boolean;
    clientId?: string;
    connectTimeout?: number;
    /** mqtt reconnect period in ms. 0 disables reconnect. */
    reconnectPeriod?: number;
    /** Topic-safe reply_to for RPC calls. Default `consumer-<hostname-slug>`. */
    callerId?: string;
    /** Default per-call RPC timeout. */
    rpcTimeoutMs?: number;
    /** Optional retained self-presence (for hubs like atrium). */
    selfPresence?: SelfPresence;
}
export interface BusClient {
    /** Connect, (optionally) publish self-presence retained, subscribe to the bus. */
    connect(): void;
    /** Invoke an RPC verb on a target app instance; resolves with its result. */
    call(target: {
        slug: string;
        nodeId: string;
    }, verb: string, args?: Record<string, unknown>, opts?: {
        timeoutMs?: number;
    }): Promise<unknown>;
    /** Publish a fire-and-forget CommandMsg to one app (QoS 1, not retained). */
    publishCommand(slug: string, nodeId: string, msg: CommandMsg): void;
    /** True once at least one broker connect has succeeded. */
    hasEverConnected(): boolean;
    /** Disconnect, clear self-presence, fail in-flight calls. Idempotent. */
    stop(): Promise<void>;
    on(event: string, listener: (...args: any[]) => void): this;
    once(event: string, listener: (...args: any[]) => void): this;
    off(event: string, listener: (...args: any[]) => void): this;
    emit(event: string, ...args: any[]): boolean;
}
/** Create a consumer/hub bus client (observer + caller). Call `.connect()` after wiring events. */
export declare function createBusClient(opts?: BusClientOptions): BusClient;
