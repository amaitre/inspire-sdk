/** Slugify a hostname per spec §4: lowercase, non-alphanumerics → '-'. */
export declare function slugifyNodeId(hostname: string): string;
export declare function topicPresence(slug: string, nodeId: string): string;
export declare function topicHeartbeat(slug: string, nodeId: string): string;
export declare function topicStatus(slug: string, nodeId: string): string;
export declare function topicLog(slug: string, nodeId: string): string;
export declare function topicCmd(slug: string, nodeId: string): string;
export declare function topicManifest(slug: string, nodeId: string): string;
/** Inbound RPC requests for an app instance. */
export declare function topicRpcCall(slug: string, nodeId: string): string;
/** Reply channel keyed by the caller's reply_to id + corr_id. */
export declare function topicRpcReply(replyTo: string, corrId: string): string;
/** A caller subscribes this wildcard to receive all its RPC replies. */
export declare function topicRpcReplyWildcard(replyTo: string): string;
/** Wildcard subscriptions a consumer/hub uses to observe the whole bus. */
export declare const TOPIC_WILDCARD: {
    readonly presence: "inspire/presence/+/+";
    readonly heartbeat: "inspire/heartbeat/+/+";
    readonly status: "inspire/status/+/+";
    readonly manifest: "inspire/manifest/+/+";
};
