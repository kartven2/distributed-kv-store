# Distributed-kv-store
Designing a Key-Value (KV) store using a Redis cache, Sorted String Tables (SSTables), and a Bloom filter naturally points to a Log-Structured Merge-tree (LSM Tree) architecture.

The `distributed-kv-store` project is a highly available, Dynamo-style distributed database designed for write-heavy workloads. It leverages a Log-Structured Merge-tree (LSM) architecture for local node storage and a decentralized peer-to-peer network for cluster management.

Here is the detailed breakdown of the system's architecture and its Java components.

## Local Storage Engine

The node-level storage prioritizes fast, concurrent writes and read optimizations to minimize expensive disk I/O. It is managed primarily by the `LsmStorageEngine` and `SSTable` classes.

* **MemTable:** Implemented using a `ConcurrentSkipListMap`, this acts as the in-memory write buffer. It automatically keeps keys sorted, allowing for lock-free concurrent reads and sequential flushing to disk once the size threshold is met.
* **SSTables (Sorted String Tables):** Immutable disk files created when the MemTable fills up. Because the data is flushed sequentially and sorted, the system can perform highly efficient `O(log N)` binary searches on disk to locate keys.
* **Bloom Filter:** Powered by Google Guava, this probabilistic data structure sits in front of the disk storage. It mathematically guarantees whether a key does *not* exist, saving the system from searching disk SSTables for missing keys.
* **Redis Cache:** A fast in-memory cache layer accessed via the Jedis client. Read requests hit Redis first, and successful lookups from disk populate the cache with a Time-To-Live (TTL) to accelerate subsequent reads.

## Cluster Management and Routing

The system is fully decentralized, meaning any node can handle a request. It distributes data and detects failures without relying on a master node.

* **Consistent Hashing (`ConsistentHashRing`):** Maps data keys to specific nodes on a circular hash space. It utilizes "Virtual Nodes" (multiple hash points per physical machine) to ensure even data distribution and to minimize the amount of data that needs to be moved when a server joins or leaves the cluster.
* **Failure Detection (`GossipFailureDetector`):** A background scheduled executor constantly updates a local heartbeat timestamp and randomly exchanges state with other nodes using a Gossip protocol. If a node's heartbeat exceeds a predefined timeout, peers mark it as offline, preventing requests from being routed to dead nodes.

## Replication and Consensus

To guarantee high availability and fault tolerance, data is replicated across multiple machines, governed by strict quorum rules.

* **Quorum Coordinator (`QuorumCoordinator`):** When a client sends a request, the receiving node acts as the coordinator. It uses the Consistent Hash Ring to calculate the "Preference List" (the top *N* nodes responsible for that key) and fans out the reads or writes asynchronously.
* **Tunable Consistency (N, W, R):** The coordinator uses `CountDownLatch` barriers to enforce quorum consensus. A write is only acknowledged as successful when *W* nodes confirm the write. A read is only successful when *R* nodes return a value. As long as `W + R > N`, the system guarantees strong consistency by overlapping the read and write quorums.

## Versioning and Client Interface

In a distributed system, network partitions and concurrent writes to different replicas can create conflicting versions of the same data.

* **Causal Tracking (`VectorClock` & `VersionedValue`):** Instead of relying on physical timestamps (which suffer from clock drift), the system attaches a Vector Clock to every value. This clock tracks the causal history of updates across different nodes.
* **Syntactic Conflict Resolution:** During a read operation, the coordinator fetches values from multiple replicas. By comparing their Vector Clocks, the system can automatically determine if one version strictly supersedes another. If concurrent, conflicting updates are detected, the clocks are merged.
* **Client API (`KVClient`):** A lightweight routing wrapper that formats user payloads, attaches the necessary Vector Clock context, and forwards the HTTP or gRPC requests to an active cluster endpoint.
