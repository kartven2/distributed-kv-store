package com.example.distributedkvstore.config;

import com.example.distributedkvstore.cluster.ConsistentHashRing;
import com.example.distributedkvstore.cluster.GossipFailureDetector;
import com.example.distributedkvstore.cluster.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import java.util.UUID;

/**
 * AppConfig — wires up shared infrastructure beans:
 * <ul>
 *   <li>{@link Node selfNode} — this process's identity</li>
 *   <li>{@link RestTemplate} — HTTP client for quorum fan-out and gossip</li>
 *   <li>Seed peer registration into the {@link ConsistentHashRing}</li>
 *   <li>{@link GossipFailureDetector} startup</li>
 * </ul>
 */
@Configuration
public class AppConfig {

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    @Value("${kv.node.id:}")      private String nodeId;
    @Value("${kv.node.host:localhost}") private String nodeHost;
    @Value("${kv.node.port:8080}") private int nodePort;

    /** Comma-separated seed peers: {@code host:port,host:port} */
    @Value("${kv.cluster.seeds:}") private String seedList;

    @Bean
    public Node selfNode() {
        String id = (nodeId == null || nodeId.isBlank()) ? UUID.randomUUID().toString() : nodeId;
        Node self = new Node(id, nodeHost, nodePort);
        log.info("Self node: {}", self);
        return self;
    }

    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    /** Registers this node + seed peers into the ring, then starts gossip. */
    @Bean
    public Void clusterBootstrap(ConsistentHashRing ring, Node selfNode, GossipFailureDetector gossip) {
        ring.addNode(selfNode);

        if (seedList != null && !seedList.isBlank()) {
            for (String seed : seedList.split(",")) {
                seed = seed.trim();
                String[] parts = seed.split(":");
                if (parts.length != 2) { log.warn("Skipping bad seed: '{}'", seed); continue; }
                try {
                    String peerId = UUID.nameUUIDFromBytes(seed.getBytes()).toString();
                    Node peer = new Node(peerId, parts[0], Integer.parseInt(parts[1]));
                    ring.addNode(peer);
                    log.info("Registered seed peer: {}", peer);
                } catch (NumberFormatException e) {
                    log.warn("Skipping seed '{}' — invalid port", seed);
                }
            }
        }

        gossip.start();
        return null;
    }
}
