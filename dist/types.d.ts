export interface PresenceMsg {
    v: 1;
    app_slug: string;
    node_id: string;
    version: string;
    started_at: string;
    pid: number;
    service_mode: boolean;
}
export interface HeartbeatMsg {
    v: 1;
    ts: string;
    uptime_s: number;
    rss_mb: number;
    cpu_pct: number;
}
export type StatusState = 'starting' | 'ready' | 'degraded' | 'stopping' | 'error';
export interface StatusMsg {
    v: 1;
    state: StatusState;
    detail: string;
    ts: string;
}
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';
export interface LogMsg {
    v: 1;
    ts: string;
    level: LogLevel;
    msg: string;
    fields?: Record<string, unknown>;
}
export type CommandVerb = 'set_verbose' | 'shutdown' | 'dump_debug' | 'kill_claude_session';
export interface CommandMsg {
    v: 1;
    cmd: CommandVerb;
    args: Record<string, unknown>;
    request_id: string;
}
/** One callable verb in an app's API surface. */
export interface VerbSpec {
    /** Verb name — unique within an app. */
    name: string;
    /** Human/agent-readable description of what the verb does. */
    description?: string;
    /** Example or JSON-schema-ish shape of the args object. */
    input?: Record<string, unknown>;
    /** Example or JSON-schema-ish shape of the result. */
    output?: Record<string, unknown>;
}
/** Retained on `inspire/manifest/<slug>/<nodeId>`; the app's API contract. */
export interface CapabilityManifestMsg {
    v: 1;
    app_slug: string;
    node_id: string;
    version: string;
    ts: string;
    verbs: VerbSpec[];
}
/** Published to `inspire/rpc/<slug>/<nodeId>/call`; invokes one verb. */
export interface RpcRequestMsg {
    v: 1;
    corr_id: string;
    /** Topic-safe caller id; the response is published to inspire/rpc/_reply/<reply_to>/<corr_id>. */
    reply_to: string;
    verb: string;
    args: Record<string, unknown>;
    ts: string;
}
/** Published to `inspire/rpc/_reply/<reply_to>/<corr_id>`; one reply per request. */
export interface RpcResponseMsg {
    v: 1;
    corr_id: string;
    ok: boolean;
    result?: unknown;
    error?: {
        message: string;
        code?: string;
    };
    ts: string;
}
/** Identifies a target app instance for an RPC call. */
export interface RpcTarget {
    slug: string;
    nodeId: string;
}
/** Handler for an inbound RPC verb. Return value (or resolved value) is the result. */
export type RpcHandler = (args: Record<string, unknown>, req: RpcRequestMsg) => unknown | Promise<unknown>;
