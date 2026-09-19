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
 *   <li>Increments its own heartbeat timestamp in the shared state table.</li>
 *   <li>Randomly selects {@link #fanout} peers and sends them the full state table
 *       via {@code POST /internal/gossip}.</li>
 *   <li>Marks any node whose heartbeat has not been updated within
 *       {@link #timeoutMs} as <em>dead</em> in the {@link ConsistentHashRing}.</li>
 * </ol>
 *
 * <p>This achieves O(log N) convergence time for failure propagation across a cluster
 * of N nodes, with no single point of failure.
 */
@Component
public class GossipFailureDetector {

    private static final Logger log = LoggerFactory.getLogger(GossipFailureDetector.class);

    private final ConsistentHashRing ring;
    private final Node selfNode;
    private final RestTemplate restTemplate;
    private final ScheduledExecutorService scheduler;

    private final long intervalMs;
    private final long timeoutMs;
    private final int fanout;

    /** Shared gossip state: nodeId → last known heartbeat epoch-ms. */
    private final ConcurrentHashMap<String, Long> heartbeatTable = new ConcurrentHashMap<>();

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

    /** Called by peers posting their state via {@code POST /internal/gossip}. */
    public void mergeState(Map<String, Long> remoteTable) {
        remoteTable.forEach((nodeId, ts) ->
                heartbeatTable.merge(nodeId, ts, Math::max));
        updateAliveFlags();
    }

    /** Returns the current heartbeat table (sent to peers during gossip). */
    public Map<String, Long> getHeartbeatTable() {
        return Collections.unmodifiableMap(heartbeatTable);
    }

    // -------------------------------------------------------------------------
    // Internal
    // -------------------------------------------------------------------------

    private void gossipRound() {
        try {
            // Step 1: update own heartbeat
            heartbeatTable.put(selfNode.getId(), System.currentTimeMillis());

            // Step 2: fan out to random peers
            List<Node> peers = pickRandomPeers();
            Map<String, Long> snapshot = new HashMap<>(heartbeatTable);
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
            long lastSeen = heartbeatTable.getOrDefault(node.getId(), 0L);
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
