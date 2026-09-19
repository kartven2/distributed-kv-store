package com.example.distributedkvstore.cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * ConsistentHashRing — maps data keys to cluster nodes using consistent hashing
 * with virtual nodes.
 *
 * <p>Each physical node occupies {@link #virtualNodeCount} positions on a circular
 * hash space (0 … 2^64 - 1). When routing a key, the ring finds the first virtual
 * node whose hash is ≥ the key's hash — wrapping around if necessary.
 *
 * <p>Virtual nodes ensure even data distribution and minimise data movement when
 * nodes join or leave the cluster.
 */
@Component
public class ConsistentHashRing {

    private static final Logger log = LoggerFactory.getLogger(ConsistentHashRing.class);

    private final int virtualNodeCount;

    /** Ring: hash position → physical Node. */
    private final ConcurrentSkipListMap<Long, Node> ring = new ConcurrentSkipListMap<>();

    /** All physical nodes added to the ring. */
    private final CopyOnWriteArrayList<Node> nodes = new CopyOnWriteArrayList<>();

    public ConsistentHashRing(
            @Value("${kv.ring.virtual-nodes:150}") int virtualNodeCount) {
        this.virtualNodeCount = virtualNodeCount;
    }

    // -------------------------------------------------------------------------
    // Ring management
    // -------------------------------------------------------------------------

    /**
     * Adds a node to the ring by inserting {@link #virtualNodeCount} virtual points.
     *
     * @param node the node to add
     */
    public synchronized void addNode(Node node) {
        nodes.add(node);
        for (int i = 0; i < virtualNodeCount; i++) {
            long hash = hash(node.getId() + "#" + i);
            ring.put(hash, node);
        }
        log.info("Added node {} to ring ({} virtual points)", node.getId(), virtualNodeCount);
    }

    /**
     * Removes a node and all its virtual points from the ring.
     *
     * @param node the node to remove
     */
    public synchronized void removeNode(Node node) {
        nodes.remove(node);
        for (int i = 0; i < virtualNodeCount; i++) {
            long hash = hash(node.getId() + "#" + i);
            ring.remove(hash);
        }
        log.info("Removed node {} from ring", node.getId());
    }

    // -------------------------------------------------------------------------
    // Routing
    // -------------------------------------------------------------------------

    /**
     * Returns the primary node responsible for a key.
     *
     * @param key the data key to route
     * @return the owning node, or {@link Optional#empty()} if the ring is empty
     */
    public Optional<Node> getPrimaryNode(String key) {
        List<Node> list = getPreferenceList(key, 1);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(0));
    }

    /**
     * Returns the top-{@code n} distinct, alive nodes responsible for a key —
     * the "preference list" used by the quorum coordinator.
     *
     * <p>Walks the ring clockwise from the key's hash position, collecting
     * distinct physical nodes, skipping dead ones.
     *
     * @param key the data key
     * @param n   desired number of nodes (may be fewer if the cluster is small or unhealthy)
     * @return ordered list of responsible nodes (primary first)
     */
    public List<Node> getPreferenceList(String key, int n) {
        if (ring.isEmpty()) return Collections.emptyList();

        long keyHash = hash(key);
        List<Node> result = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        // Walk clockwise from keyHash, wrapping around once
        Iterable<Node> candidates = () -> new RingIterator(keyHash);
        for (Node node : candidates) {
            if (result.size() >= n) break;
            if (node.isAlive() && seen.add(node.getId())) {
                result.add(node);
            }
        }
        return result;
    }

    /** Returns all physical nodes registered in the ring. */
    public List<Node> allNodes() { return Collections.unmodifiableList(nodes); }

    /** Returns the number of virtual points currently in the ring. */
    public int ringSize() { return ring.size(); }

    // -------------------------------------------------------------------------
    // Hashing
    // -------------------------------------------------------------------------

    /**
     * Hashes an input string to a {@code long} using the first 8 bytes of MD5.
     * MD5 is used purely as a uniform hash function — not for security.
     */
    static long hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes());
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h = (h << 8) | (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 not available", e);
        }
    }

    // -------------------------------------------------------------------------
    // Internal iterator — walks ring clockwise, wrapping at the end
    // -------------------------------------------------------------------------

    private class RingIterator implements Iterator<Node> {
        private final Iterator<Map.Entry<Long, Node>> tail;
        private final Iterator<Map.Entry<Long, Node>> head;
        private boolean headDone = false;
        private int emitted = 0;

        RingIterator(long startHash) {
            NavigableMap<Long, Node> tailMap = ring.tailMap(startHash);
            this.tail = tailMap.entrySet().iterator();
            this.head = ring.headMap(startHash).entrySet().iterator();
        }

        @Override public boolean hasNext() {
            return emitted < ring.size() && (tail.hasNext() || head.hasNext());
        }

        @Override public Node next() {
            emitted++;
            if (tail.hasNext()) return tail.next().getValue();
            return head.next().getValue();
        }
    }
}
