package com.example.distributedkvstore.api;

import com.example.distributedkvstore.replication.QuorumCoordinator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Optional;

/**
 * KVController — client-facing REST API.
 *
 * <pre>
 *   PUT    /kv/{key}   body: { "value": "..." }  → 200 | 503
 *   GET    /kv/{key}                              → 200 { "key", "value" } | 404
 *   DELETE /kv/{key}                              → 200 | 503
 * </pre>
 *
 * <p>Each operation is routed through the {@link QuorumCoordinator}, which fans
 * the request out to the N replica nodes and waits for quorum.
 */
@RestController
@RequestMapping("/kv")
public class KVController {

    private final QuorumCoordinator coordinator;

    public KVController(QuorumCoordinator coordinator) {
        this.coordinator = coordinator;
    }

    @PutMapping("/{key}")
    public ResponseEntity<Map<String, Object>> put(
            @PathVariable String key,
            @RequestBody Map<String, String> body) {

        String value = body.get("value");
        if (value == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "Missing 'value' field"));
        }

        boolean ok = coordinator.write(key, value);
        return ok
                ? ResponseEntity.ok(Map.of("key", key, "value", value))
                : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(Map.of("error", "Write quorum not reached"));
    }

    @GetMapping("/{key}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable String key) {
        Optional<String> result = coordinator.read(key);
        return result
                .map(v -> ResponseEntity.ok(Map.<String, Object>of("key", key, "value", v)))
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "Key not found: " + key)));
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Map<String, Object>> delete(@PathVariable String key) {
        boolean ok = coordinator.delete(key);
        return ok
                ? ResponseEntity.ok(Map.of("key", key, "deleted", true))
                : ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .body(Map.of("error", "Delete quorum not reached"));
    }
}
