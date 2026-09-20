package com.example.distributedkvstore.api;

import com.example.distributedkvstore.cluster.GossipFailureDetector;
import com.example.distributedkvstore.storage.InMemoryStore;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

/**
 * NodeController — internal inter-node endpoints (not for external clients).
 *
 * <pre>
 *   POST   /internal/replicate           ← receive a replicated write
 *   DELETE /internal/replicate/{key}     ← receive a replicated delete
 *   GET    /internal/read?key=...        ← serve a read for the quorum fan-out
 *   POST   /internal/gossip              ← receive a gossip heartbeat table
 *   GET    /internal/health              ← liveness check
 * </pre>
 */
@RestController
@RequestMapping("/internal")
public class NodeController {

    private final InMemoryStore store;
    private final GossipFailureDetector gossip;

    public NodeController(InMemoryStore store, GossipFailureDetector gossip) {
        this.store = store;
        this.gossip = gossip;
    }

    /** Receives a replicated write from the quorum coordinator. */
    @PostMapping("/replicate")
    public ResponseEntity<Void> replicate(@RequestBody Map<String, String> body) {
        String key   = body.get("key");
        String value = body.get("value");
        if (key == null || value == null) return ResponseEntity.badRequest().build();
        store.put(key, value);
        return ResponseEntity.ok().build();
    }

    /** Receives a replicated delete from the quorum coordinator. */
    @DeleteMapping("/replicate/{key}")
    public ResponseEntity<Void> replicateDelete(@PathVariable String key) {
        store.delete(key);
        return ResponseEntity.ok().build();
    }

    /** Serves a local read for the quorum fan-out. */
    @GetMapping("/read")
    public ResponseEntity<Map<String, Object>> read(@RequestParam String key) {
        Optional<String> result = store.get(key);
        return result
                .map(v -> ResponseEntity.ok(Map.<String, Object>of("value", v)))
                .orElseGet(() -> ResponseEntity.notFound().<Map<String, Object>>build());
    }

    /** Receives and merges a gossip state table from a peer. */
    @PostMapping("/gossip")
    public ResponseEntity<Void> gossip(
            @RequestBody Map<String, GossipFailureDetector.NodeState> table) {
        gossip.mergeState(table);
        return ResponseEntity.ok().build();
    }

    /** Liveness + basic stats. */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "keys", store.size(),
                "redis", store.isRedisConnected() ? "connected" : "local-only"
        ));
    }
}
