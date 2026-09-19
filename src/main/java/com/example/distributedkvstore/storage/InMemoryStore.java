package com.example.distributedkvstore.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * InMemoryStore — write-through key-value store backed by Redis (via Jedis) with a
 * local {@link ConcurrentHashMap} as a fast read layer and fallback.
 *
 * <h2>Write path</h2>
 * <pre>
 *   put / delete → Redis (primary)  +  local map (always)
 * </pre>
 *
 * <h2>Read path</h2>
 * <pre>
 *   get → local map → Redis on miss → populate local map
 * </pre>
 *
 * <h2>Graceful degradation</h2>
 * If {@link JedisPool} is {@code null} (Redis disabled or unreachable at startup),
 * or if a Redis command throws a {@link JedisException} at runtime, the store
 * silently continues with the local map only — ensuring the node stays available.
 */
@Component
public class InMemoryStore {

    private static final Logger log = LoggerFactory.getLogger(InMemoryStore.class);

    /** Fast local read layer and fallback store. */
    private final ConcurrentHashMap<String, String> localMap = new ConcurrentHashMap<>();

    /** Nullable — null when Redis is disabled or failed to connect at startup. */
    private final JedisPool jedisPool;

    public InMemoryStore(@Nullable JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // -------------------------------------------------------------------------
    // Write
    // -------------------------------------------------------------------------

    /**
     * Writes a key-value pair to Redis (primary) and the local map.
     *
     * @param key   the key (must not be {@code null})
     * @param value the value (must not be {@code null})
     */
    public void put(String key, String value) {
        // Always update local map first (fast, never fails)
        localMap.put(key, value);

        // Persist to Redis
        if (jedisPool != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.set(key, value);
            } catch (JedisException e) {
                log.warn("Redis SET failed for key '{}' — local map updated only: {}", key, e.getMessage());
            }
        }
    }

    // -------------------------------------------------------------------------
    // Read
    // -------------------------------------------------------------------------

    /**
     * Reads a value. Checks the local map first; on a miss, falls through to Redis
     * and populates the local map for subsequent reads.
     *
     * @param key the key to look up
     * @return the value, or {@link Optional#empty()} if the key does not exist
     */
    public Optional<String> get(String key) {
        // 1. Local map (fast path)
        String local = localMap.get(key);
        if (local != null) return Optional.of(local);

        // 2. Redis (populate local map on hit)
        if (jedisPool != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                String value = jedis.get(key);
                if (value != null) {
                    localMap.put(key, value); // warm local map
                    return Optional.of(value);
                }
            } catch (JedisException e) {
                log.warn("Redis GET failed for key '{}' — local map only: {}", key, e.getMessage());
            }
        }

        return Optional.empty();
    }

    // -------------------------------------------------------------------------
    // Delete
    // -------------------------------------------------------------------------

    /**
     * Removes a key from Redis and the local map.
     *
     * @param key the key to delete
     * @return {@code true} if the key existed in the local map
     */
    public boolean delete(String key) {
        boolean existed = localMap.remove(key) != null;

        if (jedisPool != null) {
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.del(key);
            } catch (JedisException e) {
                log.warn("Redis DEL failed for key '{}' — removed from local map only: {}", key, e.getMessage());
            }
        }

        return existed;
    }

    // -------------------------------------------------------------------------
    // Utility
    // -------------------------------------------------------------------------

    /** Returns {@code true} if the key exists in the local map. */
    public boolean contains(String key) {
        return localMap.containsKey(key);
    }

    /** Returns the number of entries in the local map. */
    public int size() {
        return localMap.size();
    }

    /** Returns an unmodifiable snapshot of the local map (for debugging). */
    public Map<String, String> snapshot() {
        return Map.copyOf(localMap);
    }

    /** Returns {@code true} if this store is connected to Redis. */
    public boolean isRedisConnected() {
        return jedisPool != null && !jedisPool.isClosed();
    }
}
