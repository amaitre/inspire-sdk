export interface InspireConfig {
    broker?: {
        host?: string;
        port?: number;
    };
    reporting?: {
        heartbeatIntervalMs?: number;
    };
}
/**
 * Walk up from `startDir` to the filesystem root looking for
 * `.inspire/config.toml`. Returns the first match, or undefined.
 */
export declare function findInspireConfigPath(startDir?: string): string | undefined;
/**
 * Resolve the inspire config by walking up for `.inspire/config.toml` and
 * parsing the fields the SDK cares about. Never throws: a missing or
 * malformed file resolves to `{}` so a bad config can't take an app down on
 * boot — the caller falls back to env/defaults.
 */
export declare function loadInspireConfig(startDir?: string): InspireConfig;
/**
 * Resolve the broker host/port using the full precedence chain:
 *   explicit opts.broker  >  env  >  .inspire/config.toml  >  defaults.
 * Env accepts both the canonical INSPIRE_BROKER_* and the bare BROKER_*
 * names that several apps already use, so adoption needs no env churn.
 */
export declare function resolveBroker(opts: {
    host?: string;
    port?: number;
} | undefined, fileConfig: InspireConfig, defaults: {
    host: string;
    port: number;
}): {
    host: string;
    port: number;
};
