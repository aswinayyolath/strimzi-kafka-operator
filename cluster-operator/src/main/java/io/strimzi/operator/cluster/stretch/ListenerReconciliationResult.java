/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Result of listener reconciliation containing all discovered endpoints,
 * generated configurations, and DNS names for use in subsequent
 * reconciliation steps.
 */
public final class ListenerReconciliationResult {

    /**
     * Advertised listeners configuration per broker.
     * Format: nodeId -> "REPLICATION-9091://host:port,..."
     */
    private final Map<Integer, String> advertisedListeners;

    /**
     * Advertised hostnames per broker per listener.
     * Format: nodeId -> listenerId -> hostname
     */
    private final Map<Integer, Map<String, String>> advertisedHostnames;

    /**
     * Advertised ports per broker per listener.
     * Format: nodeId -> listenerId -> port
     */
    private final Map<Integer, Map<String, String>> advertisedPorts;

    /**
     * Controller quorum voters configuration.
     * Format: "0@host:port,1@host:port,..."
     */
    private final String quorumVoters;

    /**
     * DNS names for bootstrap service per cluster.
     * Format: clusterId -> Set of DNS names
     */
    private final Map<String, Set<String>> bootstrapDnsNames;

    /**
     * DNS names for per-broker services.
     * Format: nodeId -> Set of DNS names
     */
    private final Map<Integer, Set<String>> brokerDnsNames;

    /**
     * Constructor.
     *
     * @param advertisedListenersParam Advertised listeners per broker
     * @param advertisedHostnamesParam Advertised hostnames per broker
     *                                 per listener
     * @param advertisedPortsParam Advertised ports per broker per listener
     * @param quorumVotersParam Controller quorum voters configuration
     * @param bootstrapDnsNamesParam DNS names for bootstrap services
     * @param brokerDnsNamesParam DNS names for broker services
     */
    public ListenerReconciliationResult(
            final Map<Integer, String> advertisedListenersParam,
            final Map<Integer, Map<String, String>> advertisedHostnamesParam,
            final Map<Integer, Map<String, String>> advertisedPortsParam,
            final String quorumVotersParam,
            final Map<String, Set<String>> bootstrapDnsNamesParam,
            final Map<Integer, Set<String>> brokerDnsNamesParam) {
        this.advertisedListeners = advertisedListenersParam != null
            ? advertisedListenersParam : new HashMap<>();
        this.advertisedHostnames = advertisedHostnamesParam != null
            ? advertisedHostnamesParam : new HashMap<>();
        this.advertisedPorts = advertisedPortsParam != null
            ? advertisedPortsParam : new HashMap<>();
        this.quorumVoters = quorumVotersParam != null
            ? quorumVotersParam : "";
        this.bootstrapDnsNames = bootstrapDnsNamesParam != null
            ? bootstrapDnsNamesParam : new HashMap<>();
        this.brokerDnsNames = brokerDnsNamesParam != null
            ? brokerDnsNamesParam : new HashMap<>();
    }

    /**
     * Get advertised listeners configuration for a specific broker.
     *
     * @param nodeId Broker node ID
     * @return Advertised listeners string, or empty string if not found
     */
    public String getAdvertisedListeners(final int nodeId) {
        return advertisedListeners.getOrDefault(nodeId, "");
    }

    /**
     * Get all advertised listeners configurations.
     *
     * @return Map of nodeId to advertised listeners string
     */
    public Map<Integer, String> getAllAdvertisedListeners() {
        return new HashMap<>(advertisedListeners);
    }

    /**
     * Get advertised hostnames map.
     *
     * @return Map of nodeId -> listenerId -> hostname
     */
    public Map<Integer, Map<String, String>> getAdvertisedHostnames() {
        return new HashMap<>(advertisedHostnames);
    }

    /**
     * Get advertised ports map.
     *
     * @return Map of nodeId -> listenerId -> port
     */
    public Map<Integer, Map<String, String>> getAdvertisedPorts() {
        return new HashMap<>(advertisedPorts);
    }

    /**
     * Get controller quorum voters configuration.
     *
     * @return Quorum voters string
     */
    public String getQuorumVoters() {
        return quorumVoters;
    }

    /**
     * Get bootstrap DNS names for a specific cluster.
     *
     * @param clusterId Cluster ID
     * @return Set of DNS names, or empty set if not found
     */
    public Set<String> getBootstrapDnsNames(final String clusterId) {
        return new HashSet<>(bootstrapDnsNames.getOrDefault(clusterId,
            new HashSet<>()));
    }

    /**
     * Get all bootstrap DNS names.
     *
     * @return Map of clusterId to DNS names
     */
    public Map<String, Set<String>> getAllBootstrapDnsNames() {
        return new HashMap<>(bootstrapDnsNames);
    }

    /**
     * Get DNS names for a specific broker.
     *
     * @param nodeId Broker node ID
     * @return Set of DNS names, or empty set if not found
     */
    public Set<String> getBrokerDnsNames(final int nodeId) {
        return new HashSet<>(brokerDnsNames.getOrDefault(nodeId,
            new HashSet<>()));
    }

    /**
     * Get all broker DNS names.
     *
     * @return Map of nodeId to DNS names
     */
    public Map<Integer, Set<String>> getAllBrokerDnsNames() {
        return new HashMap<>(brokerDnsNames);
    }

    /**
     * Check if result is empty (no listeners configured).
     *
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return advertisedListeners.isEmpty() && quorumVoters.isEmpty();
    }

    @Override
    public String toString() {
        return "ListenerReconciliationResult{"
                + "advertisedListeners=" + advertisedListeners.size()
                + " brokers, "
                + "quorumVoters='" + (quorumVoters.isEmpty()
                    ? "none" : quorumVoters.length() + " chars") + '\''
                + ", bootstrapDnsNames=" + bootstrapDnsNames.size()
                + " clusters, "
                + ", brokerDnsNames=" + brokerDnsNames.size()
                + " brokers"
                + '}';
    }
}
