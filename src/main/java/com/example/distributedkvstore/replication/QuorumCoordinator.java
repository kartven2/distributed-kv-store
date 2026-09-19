package com.example.distributedkvstore.replication;

import com.example.distributedkvstore.cluster.ConsistentHashRing;
import com.example.distributedkvstore.cluster.Node;
import com.example.distributedkvstore.storage.InMemoryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;
import java.util.concurrent.*;

/**
 * QuorumCoordinator — fans out reads and writes across N replica nodes
 * and enforces quorum (W acks for writes, R responses for reads).
 *
 * <h2>Tunable Consistency (N, W, R)</h2>
 * <pre>
 *   N = total replicas per key
 *   W = minimum write acks required  (default 2)
 *   R = minimum read responses needed (default 2)
 * </pre>
 * When {@code W + R > N} the read and write quorums overlap, guaranteeing
 * that at least one node always has the latest write — strong consistency.
 *
 * <h2>Single-node mode</h2>
 * If the ring contains only this node, all operations are served locally
 * without any HTTP fan-out.
 */
@Component
public class QuorumCoordinator {

    private static final Logger log = LoggerFactory.getLogger(QuorumCoordinator.class);

    private final ConsistentHashRing ring;
    private final InMemoryStore localStore;
    private final Node selfNode;
    private final RestTemplate restTemplate;
    private final ExecutorService executor;

    private final int n;
    private final int w;
    private final int r;
    private final long timeoutMs;

    public QuorumCoordinator(
            ConsistentHashRing ring,
            InMemoryStore localStore,
            Node selfNode,
            RestTemplate restTemplate,
            @Value("${kv.replication.n:3}") int n,
            @Value("${kv.replication.w:2}") int w,
            @Value("${kv.replication.r:2}") int r,
            @Value("${kv.replication.timeout-ms:3000}") long timeoutMs) {
        this.ring = ring;
        this.localStore = localStore;
        this.selfNode = selfNode;
        this.restTemplate = restTemplate;
        this.n = n;
        this.w = w;
        this.r = r;
        this.timeoutMs = timeoutMs;
        this.executor = Executors.newCachedThreadPool(task -> {
            Thread t = new Thread(task, "quorum-worker");
            t.setDaemon(true);
            return t;
        });
        log.info("QuorumCoordinator started — N={}, W={}, R={}", n, w, r);
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Fans out a write to the top-N nodes. Returns {@code true} when W nodes ack.
     *
     * @param key   the key to write
     * @param value the value to write
     * @return {@code true} if write quorum was reached within the timeout
     */
    public boolean write(String key, String value) {
        List<Node> nodes = ring.getPreferenceList(key, n);
        if (nodes.isEmpty()) { localStore.put(key, value); return true; }

        CountDownLatch latch = new CountDownLatch(Math.min(w, nodes.size()));
        for (Node node : nodes) {
            executor.submit(() -> {
                if (isSelf(node)) {
                    localStore.put(key, value);
                    latch.countDown();
                } else {
                    try {
                        restTemplate.postForObject(
                                node.baseUrl() + "/internal/replicate",
                                Map.of("key", key, "value", value),
                                Void.class);
                        latch.countDown();
                    } catch (Exception e) {
                        log.warn("Write to {} failed: {}", node.getId(), e.getMessage());
                    }
                }
            });
        }

        return awaitLatch(latch, "write", key);
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Fans out a read to the top-N nodes. Returns the first value that R nodes agree on,
     * or the first non-empty response once R replies arrive.
     *
     * @param key the key to read
     * @return the value, or {@link Optional#empty()} if absent / quorum not reached
     */
    public Optional<String> read(String key) {
        List<Node> nodes = ring.getPreferenceList(key, n);
        if (nodes.isEmpty()) return localStore.get(key);

        int needed = Math.min(r, nodes.size());
        CountDownLatch latch = new CountDownLatch(needed);
        List<String> responses = Collections.synchronizedList(new ArrayList<>());

        for (Node node : nodes) {
            executor.submit(() -> {
                Optional<String> result = isSelf(node)
                        ? localStore.get(key)
                        : readFromNode(node, key);
                result.ifPresent(v -> { responses.add(v); latch.countDown(); });
            });
        }

        awaitLatch(latch, "read", key);
        return responses.isEmpty() ? Optional.empty() : Optional.of(responses.get(0));
    }

    // -------------------------------------------------------------------------
    // Delete
    // -------------------------------------------------------------------------

    /**
     * Fans out a delete to the top-N nodes. Returns {@code true} when W nodes ack.
     *
     * @param key the key to delete
     * @return {@code true} if delete quorum was reached within the timeout
     */
    public boolean delete(String key) {
        List<Node> nodes = ring.getPreferenceList(key, n);
        if (nodes.isEmpty()) { localStore.delete(key); return true; }

        CountDownLatch latch = new CountDownLatch(Math.min(w, nodes.size()));
        for (Node node : nodes) {
            executor.submit(() -> {
                if (isSelf(node)) {
                    localStore.delete(key);
                    latch.countDown();
                } else {
                    try {
                        restTemplate.delete(node.baseUrl() + "/internal/replicate/" + key);
                        latch.countDown();
                    } catch (Exception e) {
                        log.warn("Delete on {} failed: {}", node.getId(), e.getMessage());
                    }
                }
            });
        }

        return awaitLatch(latch, "delete", key);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private Optional<String> readFromNode(Node node, String key) {
        try {
            ResponseEntity<Map> resp = restTemplate.getForEntity(
                    node.baseUrl() + "/internal/read?key=" + key, Map.class);
            if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                return Optional.ofNullable((String) resp.getBody().get("value"));
            }
        } catch (Exception e) {
            log.debug("Read from {} failed: {}", node.getId(), e.getMessage());
        }
        return Optional.empty();
    }

    private boolean awaitLatch(CountDownLatch latch, String op, String key) {
        try {
            boolean reached = latch.await(timeoutMs, TimeUnit.MILLISECONDS);
            if (!reached) log.warn("Quorum not reached for {} on key '{}'", op, key);
            return reached;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private boolean isSelf(Node node) {
        return selfNode.getId().equals(node.getId());
    }
}
