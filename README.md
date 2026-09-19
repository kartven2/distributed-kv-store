# Distributed KV Store

A simple distributed key-value store built with Java and Spring Boot, designed for **interview preparation**. It demonstrates core distributed systems concepts clearly and concisely, without unnecessary complexity.

## Architecture Overview

Each node is a standalone Spring Boot application. Nodes form a cluster by discovering each other through a static seed list and maintaining membership via a Gossip protocol.

```
Client
  │
  ▼
KVController  (REST API: PUT / GET / DELETE /kv/{key})
  │
  ▼
QuorumCoordinator  (fans out to N replicas, waits for W/R quorum)
  │
  ├─── self: InMemoryStore
  │       ├── Write-through → Redis (Jedis)  primary, persistent
  │       └── Local ConcurrentHashMap         fast read layer & fallback
  └─── peers: HTTP  →  /internal/replicate | /internal/read

ConsistentHashRing    (maps key → top-N nodes via MD5 + virtual nodes)
GossipFailureDetector (heartbeat exchange; marks dead nodes)
```

---

## Project Structure

```
distributed-kv-store/
├── pom.xml
└── src/main/
    ├── resources/
    │   └── application.properties               # All tunable config (N, W, R, gossip, Redis)
    └── java/com/example/distributedkvstore/
        │
        ├── DistributedKvStoreApplication.java   # Spring Boot entry point
        │
        ├── storage/
        │   └── InMemoryStore.java               # Write-through: Redis primary + ConcurrentHashMap
        │
        ├── cluster/
        │   ├── Node.java                        # Cluster member (id, host, port, alive flag)
        │   ├── ConsistentHashRing.java           # MD5 hash ring with virtual nodes
        │   └── GossipFailureDetector.java        # Heartbeat exchange + dead-node detection
        │
        ├── replication/
        │   └── QuorumCoordinator.java            # N/W/R fan-out with CountDownLatch quorum
        │
        ├── config/
        │   ├── AppConfig.java                   # Wires Node, RestTemplate, seeds, gossip start
        │   └── RedisConfig.java                 # JedisPool bean (null = graceful local fallback)
        │
        └── api/
            ├── KVController.java                # Client API: PUT/GET/DELETE /kv/{key}
            └── NodeController.java              # Internal API: /internal/replicate|read|gossip|health
```

---

## Key Components

### 1. `InMemoryStore` + Redis
Each node persists its shard of data in **Redis** (via Jedis) and keeps a local
`ConcurrentHashMap` as a fast read layer.

| Operation | Behaviour |
|---|---|
| `put(key, value)` | Write to local map immediately; persist to Redis (`SET key value`) |
| `get(key)` | Check local map first (O(1)); on miss, fetch from Redis and warm the local map |
| `delete(key)` | Remove from local map; issue Redis `DEL key` |

**Graceful degradation** — if Redis is disabled (`kv.redis.enabled=false`) or unreachable at
startup, `JedisPool` is `null` and the node continues with the local map only. Redis errors at
runtime are caught and logged as warnings; the operation succeeds locally.

### 2. `ConsistentHashRing`
Maps a key to one or more responsible nodes using **consistent hashing**.
- MD5 hash → position on a circular ring
- **150 virtual nodes** per physical node for even key distribution
- `getPreferenceList(key, n)` returns the top-N nodes for a key
- Adding/removing a node only moves `1/N` of keys — minimal rebalancing

### 3. `GossipFailureDetector`
Decentralized failure detection with no single point of failure.
- Every second, each node increments its own heartbeat timestamp
- Randomly fans out the full heartbeat table to K=3 peers (`POST /internal/gossip`)
- A node is marked **dead** if its heartbeat hasn't been seen within 5 seconds
- Dead nodes are excluded from the `ConsistentHashRing` preference list

### 4. `QuorumCoordinator`
Enforces **tunable consistency** via N, W, R parameters.

| Parameter | Meaning | Default |
|---|---|---|
| N | Total replicas per key | 3 |
| W | Write acks required | 2 |
| R | Read responses required | 2 |

- Operations fan out concurrently using a thread pool
- A `CountDownLatch` blocks until the quorum threshold is met or the timeout elapses
- **Strong consistency** is guaranteed when `W + R > N` (quorums overlap)

### 5. REST API

**Client-facing (`KVController`):**
```
PUT    /kv/{key}   body: { "value": "..." }   → 200 | 503
GET    /kv/{key}                               → 200 { "key", "value" } | 404
DELETE /kv/{key}                               → 200 | 503
```

**Inter-node (`NodeController`):**
```
POST   /internal/replicate           ← receive a replicated write
DELETE /internal/replicate/{key}     ← receive a replicated delete
GET    /internal/read?key=...        ← serve a read for quorum fan-out
POST   /internal/gossip              ← receive heartbeat table from a peer
GET    /internal/health              ← liveness + key count
```

---

## Running Locally (Single Node)

```bash
mvn spring-boot:run
```

```bash
# Write
curl -X PUT localhost:8080/kv/hello \
  -H 'Content-Type: application/json' \
  -d '{"value":"world"}'

# Read
curl localhost:8080/kv/hello

# Delete
curl -X DELETE localhost:8080/kv/hello
```

## Running a 3-Node Cluster

```bash
# Node 1 (port 8080)
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --server.port=8080 \
  --kv.node.port=8080 \
  --kv.cluster.seeds=localhost:8081,localhost:8082"

# Node 2 (port 8081)
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --server.port=8081 \
  --kv.node.port=8081 \
  --kv.cluster.seeds=localhost:8080,localhost:8082"

# Node 3 (port 8082)
mvn spring-boot:run -Dspring-boot.run.arguments="\
  --server.port=8082 \
  --kv.node.port=8082 \
  --kv.cluster.seeds=localhost:8080,localhost:8081"
```

---

## Configuration

| Property | Default | Description |
|---|---|---|
| `kv.node.id` | _(random UUID)_ | Stable node identifier |
| `kv.node.host` | `localhost` | Advertised hostname |
| `kv.node.port` | `8080` | Advertised port |
| `kv.cluster.seeds` | _(empty)_ | Comma-separated `host:port` peers |
| `kv.ring.virtual-nodes` | `150` | Virtual nodes per physical node |
| `kv.replication.n` | `3` | Replication factor |
| `kv.replication.w` | `2` | Write quorum |
| `kv.replication.r` | `2` | Read quorum |
| `kv.replication.timeout-ms` | `3000` | Quorum timeout (ms) |
| `kv.gossip.interval-ms` | `1000` | Gossip period (ms) |
| `kv.gossip.timeout-ms` | `5000` | Node dead threshold (ms) |
| `kv.gossip.fanout` | `3` | Peers contacted per gossip round |
| `kv.redis.enabled` | `true` | Set `false` to run without Redis |
| `kv.redis.host` | `localhost` | Redis host |
| `kv.redis.port` | `6379` | Redis port |
| `kv.redis.password` | _(blank)_ | Redis auth password (omit if none) |
| `kv.redis.timeout-ms` | `2000` | Connection timeout (ms) |
| `kv.redis.pool.max-total` | `20` | Max connections in pool |
| `kv.redis.pool.max-idle` | `10` | Max idle connections |
| `kv.redis.pool.min-idle` | `2` | Min idle connections |
