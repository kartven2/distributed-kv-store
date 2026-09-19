package com.example.distributedkvstore.cluster;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a single node in the cluster.
 *
 * <p>Identified by a stable UUID {@link #id}, reachable at {@link #host}:{@link #port}.
 * The {@link #alive} flag is updated by {@link GossipFailureDetector} based on heartbeat
 * timestamps; dead nodes are excluded from routing decisions.
 */
public class Node {

    private final String id;
    private final String host;
    private final int port;
    private final AtomicBoolean alive;
    private final AtomicLong lastHeartbeatMs;

    public Node(String id, String host, int port) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.alive = new AtomicBoolean(true);
        this.lastHeartbeatMs = new AtomicLong(System.currentTimeMillis());
    }

    // -------------------------------------------------------------------------
    // Accessors
    // -------------------------------------------------------------------------

    public String getId()   { return id; }
    public String getHost() { return host; }
    public int    getPort() { return port; }

    /** Returns {@code true} if this node is currently considered alive by the failure detector. */
    public boolean isAlive() { return alive.get(); }

    /** Sets the alive status (called by {@link GossipFailureDetector}). */
    public void setAlive(boolean alive) { this.alive.set(alive); }

    /** Returns the epoch-millisecond timestamp of the last received heartbeat. */
    public long getLastHeartbeatMs() { return lastHeartbeatMs.get(); }

    /** Updates the heartbeat timestamp to now (called when a gossip message arrives). */
    public void updateHeartbeat() { lastHeartbeatMs.set(System.currentTimeMillis()); }

    /** Updates the heartbeat timestamp to an explicit value (for gossip state merging). */
    public void updateHeartbeat(long timestampMs) {
        lastHeartbeatMs.updateAndGet(current -> Math.max(current, timestampMs));
    }

    /** Returns the base URL of this node's HTTP API, e.g. {@code http://host:port}. */
    public String baseUrl() {
        return "http://" + host + ":" + port;
    }

    @Override
    public String toString() {
        return String.format("Node{id=%s, address=%s:%d, alive=%b}", id, host, port, isAlive());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Node n)) return false;
        return id.equals(n.id);
    }

    @Override
    public int hashCode() { return id.hashCode(); }
}
