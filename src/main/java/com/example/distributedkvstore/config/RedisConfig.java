package com.example.distributedkvstore.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

/**
 * RedisConfig — creates and validates a {@link JedisPool} on startup.
 *
 * <p>If {@code kv.redis.enabled=false} or the connection fails, the bean is
 * {@code null}. {@link com.example.distributedkvstore.storage.InMemoryStore}
 * checks for {@code null} and falls back to its local {@code ConcurrentHashMap}.
 *
 * <h2>Pool settings (tunable via application.properties)</h2>
 * <pre>
 *   kv.redis.host          (default: localhost)
 *   kv.redis.port          (default: 6379)
 *   kv.redis.password      (default: blank = no auth)
 *   kv.redis.timeout-ms    (default: 2000)
 *   kv.redis.pool.max-total (default: 20)
 *   kv.redis.pool.max-idle  (default: 10)
 *   kv.redis.pool.min-idle  (default: 2)
 * </pre>
 */
@Configuration
public class RedisConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);

    @Value("${kv.redis.enabled:true}")    private boolean enabled;
    @Value("${kv.redis.host:localhost}")  private String  host;
    @Value("${kv.redis.port:6379}")       private int     port;
    @Value("${kv.redis.password:}")       private String  password;
    @Value("${kv.redis.timeout-ms:2000}") private int     timeoutMs;
    @Value("${kv.redis.pool.max-total:20}") private int   maxTotal;
    @Value("${kv.redis.pool.max-idle:10}")  private int   maxIdle;
    @Value("${kv.redis.pool.min-idle:2}")   private int   minIdle;

    /**
     * Returns a connected {@link JedisPool}, or {@code null} if Redis is disabled
     * or unreachable (allowing {@code InMemoryStore} to degrade gracefully).
     */
    @Bean
    public JedisPool jedisPool() {
        if (!enabled) {
            log.info("Redis disabled — InMemoryStore will use local ConcurrentHashMap only");
            return null;
        }

        try {
            JedisPoolConfig cfg = new JedisPoolConfig();
            cfg.setMaxTotal(maxTotal);
            cfg.setMaxIdle(maxIdle);
            cfg.setMinIdle(minIdle);
            cfg.setBlockWhenExhausted(true);
            cfg.setMaxWait(Duration.ofSeconds(2));
            cfg.setTestOnBorrow(true);
            cfg.setTestWhileIdle(true);
            cfg.setTimeBetweenEvictionRuns(Duration.ofSeconds(30));

            JedisPool pool = (password != null && !password.isBlank())
                    ? new JedisPool(cfg, host, port, timeoutMs, password)
                    : new JedisPool(cfg, host, port, timeoutMs);

            // Validate eagerly — fail fast on misconfiguration
            try (var jedis = pool.getResource()) {
                log.info("Redis connected — {}:{}, PING={}", host, port, jedis.ping());
            }

            return pool;

        } catch (Exception e) {
            log.warn("Redis unavailable ({}:{}) — falling back to local store: {}", host, port, e.getMessage());
            return null;
        }
    }
}
