package com.example.distributedkvstore.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.*;

/**
 * GossipFailureDetector — decentralized heartbeat and failure detection.
 *
 * <p>Every {@link #intervalMs} milliseconds this node:
 * <ol>
 *   <li>Updates its own entry in the gossip state table.</li>
 *   <li>Randomly selects {@link #fanout} peers and sends them the full state table
 *       via {@code POST /internal/gossip}.</li>
 *   <li>Marks any node whose heartbeat has not been updated within
 *       {@link #timeoutMs} as <em>dead</em> in the {@link ConsistentHashRing}.</li>
 * </ol>
 *
 * <h2>Gossip message format</h2>
 * Each entry in the table is a {@link NodeState} carrying the node's self-reported
 * {@code id}, {@code host}, {@code port}, and {@code heartbeatMs}. This means peers
 * <em>never</em> derive a node ID from its address — they always use the ID the
 * node itself advertised, preventing ID drift.
 */
@Component
public class GossipFailureDetector {

    private static final Logger log = LoggerFactory.getLogger(GossipFailureDetector.class);

    // -------------------------------------------------------------------------
    // NodeState — the unit of gossip exchange
    // -------------------------------------------------------------------------

    /**
     * Full node identity + heartbeat timestamp, exchanged in every gossip round.
     *
     * <p>Using a record that carries {@code id}, {@code host}, and {@code port}
     * means the receiving node can register a peer in the ring using the peer's
     * <em>own</em> self-reported ID — not one derived from its address.
     */
    public record NodeState(String id, String host, int port, long heartbeatMs) {

        /** Creates a state snapshot for a live node at the current time. */
        public static NodeState of(Node node) {
            return new NodeState(node.getId(), node.getHost(), node.getPort(),
                    System.currentTimeMillis());
        }

        /** Returns the max heartbeat between this and another state for the same node. */
        public NodeState merge(NodeState other) {
            return this.heartbeatMs >= other.heartbeatMs ? this : other;
        }
    }

    // -------------------------------------------------------------------------
    // Fields
    // -------------------------------------------------------------------------

    private final ConsistentHashRing ring;
    private final Node selfNode;
    private final RestTemplate restTemplate;
    private final ScheduledExecutorService scheduler;

    private final long intervalMs;
    private final long timeoutMs;
    private final int fanout;

    /** Gossip state table: nodeId → NodeState (id + host + port + heartbeat). */
    private final ConcurrentHashMap<String, NodeState> stateTable = new ConcurrentHashMap<>();

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    public GossipFailureDetector(
            ConsistentHashRing ring,
            Node selfNode,
            RestTemplate restTemplate,
            @Value("${kv.gossip.interval-ms:1000}") long intervalMs,
            @Value("${kv.gossip.timeout-ms:5000}") long timeoutMs,
            @Value("${kv.gossip.fanout:3}") int fanout) {
        this.ring = ring;
        this.selfNode = selfNode;
        this.restTemplate = restTemplate;
        this.intervalMs = intervalMs;
        this.timeoutMs = timeoutMs;
        this.fanout = fanout;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "gossip-detector");
            t.setDaemon(true);
            return t;
        });
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /** Starts the background gossip loop. Call once at application startup. */
    public void start() {
        scheduler.scheduleAtFixedRate(this::gossipRound, intervalMs, intervalMs,
                TimeUnit.MILLISECONDS);
        log.info("GossipFailureDetector started (interval={}ms, timeout={}ms, fanout={})",
                intervalMs, timeoutMs, fanout);
    }

    /** Stops the gossip loop. */
    public void stop() {
        scheduler.shutdownNow();
    }

    // -------------------------------------------------------------------------
    // Gossip protocol
    // -------------------------------------------------------------------------

    /**
     * Merges a remote gossip state table received from a peer.
     *
     * <p>For each entry in the remote table:
     * <ul>
     *   <li>Keep the entry with the higher heartbeat timestamp.</li>
     *   <li>If this is a node we haven't seen before, register it in the ring
     *       using its <em>self-reported</em> ID — no ID derivation from address.</li>
     * </ul>
     *
     * <p>Called by {@link com.example.distributedkvstore.api.NodeController}
     * when a peer posts to {@code POST /internal/gossip}.
     */
    public void mergeState(Map<String, NodeState> remoteTable) {
        for (Map.Entry<String, NodeState> entry : remoteTable.entrySet()) {
            String nodeId = entry.getKey();
            NodeState remote = entry.getValue();

            // Merge: keep the newer heartbeat
            stateTable.merge(nodeId, remote, NodeState::merge);

            // If this is a brand-new node, add it to the ring using its own ID
            if (!ring.containsNode(nodeId) && !nodeId.equals(selfNode.getId())) {
                Node discovered = new Node(remote.id(), remote.host(), remote.port());
                ring.addNode(discovered);
                log.info("Discovered new node via gossip: {}", discovered);
            }
        }
        updateAliveFlags();
    }

    /** Returns the current gossip state table (sent to peers). */
    public Map<String, NodeState> getStateTable() {
        return Collections.unmodifiableMap(stateTable);
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void gossipRound() {
        try {
            // Step 1: update own state
            stateTable.put(selfNode.getId(), NodeState.of(selfNode));

            // Step 2: fan out to random alive peers
            List<Node> peers = pickRandomPeers();
            Map<String, NodeState> snapshot = new HashMap<>(stateTable);
            for (Node peer : peers) {
                try {
                    restTemplate.postForObject(
                            peer.baseUrl() + "/internal/gossip", snapshot, Void.class);
                } catch (Exception e) {
                    log.debug("Gossip to {} failed: {}", peer.getId(), e.getMessage());
                }
            }

            // Step 3: mark dead nodes
            updateAliveFlags();
        } catch (Exception e) {
            log.warn("Error in gossip round: {}", e.getMessage());
        }
    }

    private void updateAliveFlags() {
        long now = System.currentTimeMillis();
        for (Node node : ring.allNodes()) {
            if (node.getId().equals(selfNode.getId())) continue;
            NodeState state = stateTable.get(node.getId());
            long lastSeen = (state != null) ? state.heartbeatMs() : 0L;
            boolean shouldBeAlive = (now - lastSeen) <= timeoutMs;
            if (node.isAlive() != shouldBeAlive) {
                node.setAlive(shouldBeAlive);
                log.info("Node {} marked {}", node.getId(), shouldBeAlive ? "ALIVE" : "DEAD");
            }
        }
    }

    private List<Node> pickRandomPeers() {
        List<Node> all = new ArrayList<>(ring.allNodes());
        all.removeIf(n -> n.getId().equals(selfNode.getId()) || !n.isAlive());
        Collections.shuffle(all);
        return all.subList(0, Math.min(fanout, all.size()));
    }
}
