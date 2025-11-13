/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.ListenersUtils;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.IngressOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.RouteOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reconciles Kafka listeners for stretch clusters.
 *
 * This class handles:
 * - Creating services (headless, per-broker, bootstrap) across all clusters
 * - Creating Routes for route-type listeners
 * - Creating Ingresses for ingress-type listeners
 * - Discovering actual endpoints (Route hostnames, LoadBalancer IPs, etc.)
 * - Generating advertised.listeners configuration for each broker
 *
 * Unlike the MCS provider which only handles MCS DNS generation,
 * this class handles ALL listener types by reading configuration from
 * Kafka CR.
 */
public final class StretchKafkaListenersReconciler {
    /** Logger for this class. */
    private static final ReconciliationLogger LOGGER =
            ReconciliationLogger.create(
                    StretchKafkaListenersReconciler.class);

    /** HTTPS port for Route and Ingress. */
    private static final int PORT_HTTPS = 443;
    /** HTTP port for Ingress. */
    private static final int PORT_HTTP = 80;
    /** Default replication port. */
    private static final int PORT_REPLICATION = 9091;

    /** Reconciliation context. */
    private final Reconciliation reconciliation;
    /** Kafka custom resource. */
    private final Kafka kafka;
    /** Kafka cluster model. */
    private final KafkaCluster kafkaCluster;
    /** List of KafkaNodePool CRs. */
    private final List<KafkaNodePool> nodePools;
    /** Set of target cluster IDs. */
    private final Set<String> targetClusterIds;
    /** Central cluster ID. */
    private final String centralClusterId;
    /** Resource operator supplier for central cluster. */
    private final ResourceOperatorSupplier centralSupplier;
    /** Resource operator supplier for remote clusters. */
    private final RemoteResourceOperatorSupplier remoteOperatorSupplier;
    /** Namespace for resources. */
    private final String namespace;
    /** Networking provider for stretch clusters. */
    private final StretchNetworkingProvider networkingProvider;

    /**
     * Result of listener reconciliation containing advertised listeners
     * and DNS names.
     */
    public static final class ListenerReconciliationResult {
        /** Advertised hostnames per broker per listener. */
        private final Map<Integer, Map<String, String>> advertisedHostnames;
        /** Advertised ports per broker per listener. */
        private final Map<Integer, Map<String, String>> advertisedPorts;
        /** DNS names for per-broker services. */
        private final Map<Integer, Set<String>> brokerDnsNames;
        /** DNS names for bootstrap services. */
        private final Map<String, Set<String>> bootstrapDnsNames;

        /**
         * Constructor.
         *
         * @param advertisedHostnamesParam Advertised hostnames
         * @param advertisedPortsParam Advertised ports
         * @param brokerDnsNamesParam Broker DNS names
         * @param bootstrapDnsNamesParam Bootstrap DNS names
         */
        public ListenerReconciliationResult(
                final Map<Integer, Map<String, String>>
                    advertisedHostnamesParam,
                final Map<Integer, Map<String, String>>
                    advertisedPortsParam,
                final Map<Integer, Set<String>> brokerDnsNamesParam,
                final Map<String, Set<String>> bootstrapDnsNamesParam) {
            this.advertisedHostnames = advertisedHostnamesParam;
            this.advertisedPorts = advertisedPortsParam;
            this.brokerDnsNames = brokerDnsNamesParam;
            this.bootstrapDnsNames = bootstrapDnsNamesParam;
        }

        /**
         * Get advertised hostnames.
         *
         * @return Advertised hostnames map
         */
        public Map<Integer, Map<String, String>> getAdvertisedHostnames() {
            return advertisedHostnames;
        }

        /**
         * Get advertised ports.
         *
         * @return Advertised ports map
         */
        public Map<Integer, Map<String, String>> getAdvertisedPorts() {
            return advertisedPorts;
        }

        /**
         * Get broker DNS names.
         *
         * @return Broker DNS names map
         */
        public Map<Integer, Set<String>> getBrokerDnsNames() {
            return brokerDnsNames;
        }

        /**
         * Get bootstrap DNS names.
         *
         * @return Bootstrap DNS names map
         */
        public Map<String, Set<String>> getBootstrapDnsNames() {
            return bootstrapDnsNames;
        }
    }

    /**
     * Constructor.
     *
     * @param reconciliationParam Reconciliation context
     * @param kafkaParam Kafka custom resource
     * @param kafkaClusterParam Kafka cluster model
     * @param nodePoolsParam List of KafkaNodePool CRs
     * @param targetClusterIdsParam Target cluster IDs
     * @param centralClusterIdParam Central cluster ID
     * @param centralSupplierParam Central resource operator supplier
     * @param remoteOperatorSupplierParam Remote resource operator supplier
     * @param networkingProviderParam Networking provider for stretch clusters
     */
    public StretchKafkaListenersReconciler(
            final Reconciliation reconciliationParam,
            final Kafka kafkaParam,
            final KafkaCluster kafkaClusterParam,
            final List<KafkaNodePool> nodePoolsParam,
            final Set<String> targetClusterIdsParam,
            final String centralClusterIdParam,
            final ResourceOperatorSupplier centralSupplierParam,
            final RemoteResourceOperatorSupplier remoteOperatorSupplierParam,
            final StretchNetworkingProvider networkingProviderParam) {
        this.reconciliation = reconciliationParam;
        this.kafka = kafkaParam;
        this.kafkaCluster = kafkaClusterParam;
        this.nodePools = nodePoolsParam;
        this.targetClusterIds = targetClusterIdsParam;
        this.centralClusterId = centralClusterIdParam;
        this.centralSupplier = centralSupplierParam;
        this.remoteOperatorSupplier = remoteOperatorSupplierParam;
        this.namespace = kafkaParam.getMetadata().getNamespace();
        this.networkingProvider = networkingProviderParam;

        targetClusterIds.add(centralClusterIdParam);
    }

    /**
     * Reconcile all listener-related resources and generate
     * advertised.listeners configuration.
     *
     * This is the main entry point that orchestrates:
     * 1. Service reconciliation (headless, per-broker, bootstrap)
     * 2. Route reconciliation (for route-type listeners)
     * 3. Ingress reconciliation (for ingress-type listeners)
     * 4. Endpoint discovery (actual hostnames/IPs)
     * 5. Advertised listeners generation
     *
     * @return Future with ListenerReconciliationResult
     */
    public Future<ListenerReconciliationResult> reconcile() {
        LOGGER.infoCr(reconciliation,
                "Starting listener reconciliation for stretch cluster");

        return reconcileServices()
                .compose(v -> reconcileRoutes())
                .compose(v -> reconcileIngresses())
                .compose(v ->
                        discoverEndpointsAndGenerateAdvertisedListeners());
    }

    /**
     * Reconcile services across all clusters.
     * Creates headless, per-broker, and bootstrap services.
     *
     * @return Future that completes when reconciliation is done
     */
    private Future<Void> reconcileServices() {
        LOGGER.infoCr(reconciliation,
                "Reconciling services across {} clusters",
                targetClusterIds.size());

        List<Future<Void>> futures = new ArrayList<>();

        for (String clusterId : targetClusterIds) {
            boolean isCentral = clusterId.equals(centralClusterId);
            ServiceOperator svcOp = getServiceOperator(clusterId);

            if (svcOp == null) {
                return Future.failedFuture(
                        "No ServiceOperator found for cluster: " + clusterId);
            }

            // Generate all services for this cluster
            List<Service> services = new ArrayList<>();

            // 1. Main bootstrap service
            services.add(kafkaCluster.generateService());

            // 2. Headless service (for internal communication and MCS)
            // For remote clusters, generate without owner references
            Service headlessService = kafkaCluster.generateHeadlessService(!isCentral);

            services.add(headlessService);

            // 3. External bootstrap services (for different listener types)
            services.addAll(
                    kafkaCluster.generateExternalBootstrapServices());

            // 4. Per-broker services (for route, cluster-ip,
            // loadbalancer, nodeport listeners)
            Map<String, List<Service>> clusteredPerBrokerServices =
                kafkaCluster.generateClusteredPerPodServices();
            List<Service> perBrokerServices =
                    clusteredPerBrokerServices.getOrDefault(clusterId,
                            Collections.emptyList());
            services.addAll(perBrokerServices);

            LOGGER.infoCr(reconciliation,
                    "Generated {} services for cluster {} "
                    + "(including {} per-broker services)",
                    services.size(), clusterId, perBrokerServices.size());

            // For remote clusters, set StrimziPodSet as owner for all services
            // For central cluster, keep existing ownership (Kafka CR)
            if (!isCentral) {
                String podSetName = getPodSetNameForCluster(clusterId);
                final String finalPodSetName = podSetName;
                final String finalClusterId = clusterId;
                services = services.stream()
                    .map(svc -> {
                        // First remove existing owner references
                        svc = new io.fabric8.kubernetes.api.model.ServiceBuilder(svc)
                            .editMetadata()
                                .withOwnerReferences()  // Empty list
                            .endMetadata()
                            .build();
                        
                        // Then add GC ConfigMap as owner
                        return addGCConfigMapOwner(svc, finalClusterId);
                    })
                    .collect(Collectors.toList());
                
                LOGGER.infoCr(reconciliation,
                    "Set GC ConfigMap as owner for {} services in remote cluster {}",
                    services.size(), clusterId);
            }

            // Batch reconcile all services for this cluster
            futures.add(
                svcOp.batchReconcile(reconciliation, namespace, services,
                        kafkaCluster.getSelectorLabels())
                    .compose(result -> {
                        LOGGER.infoCr(reconciliation,
                                "Services reconciled in cluster {}",
                                clusterId);
                        return Future.succeededFuture();
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Reconcile Routes for route-type listeners across all clusters.
     *
     * @return Future that completes when reconciliation is done
     */
    private Future<Void> reconcileRoutes() {
        // Check if any listeners are of type route
        List<GenericKafkaListener> routeListeners =
                kafka.getSpec().getKafka().getListeners().stream()
                .filter(l -> l.getType() == KafkaListenerType.ROUTE)
                .collect(Collectors.toList());

        if (routeListeners.isEmpty()) {
            LOGGER.debugCr(reconciliation,
                "No route listeners configured, skipping route reconciliation");
            return Future.succeededFuture();
        }

        LOGGER.infoCr(reconciliation, "Reconciling routes for {} route listeners across {} clusters",
                   routeListeners.size(), targetClusterIds.size());

        List<Future<Void>> futures = new ArrayList<>();

        for (String clusterId : targetClusterIds) {
            boolean isCentral = clusterId.equals(centralClusterId);
            RouteOperator routeOp = getRouteOperator(clusterId);

            if (routeOp == null) {
                LOGGER.warnCr(reconciliation,
                    "No RouteOperator found for cluster {}, skipping routes", clusterId);
                continue;
            }

            // Generate routes for this cluster
            List<Route> routes = new ArrayList<>();
            routes.addAll(
                    kafkaCluster.generateExternalBootstrapRoutes(false));

            // Get per-broker routes for this cluster
            Map<String, List<Route>>
                    clusteredRoutes = kafkaCluster.generateClusteredExternalRoutes();
            List<Route> perBrokerRoutes = clusteredRoutes.getOrDefault(clusterId, Collections.emptyList());
            routes.addAll(perBrokerRoutes);

            LOGGER.infoCr(reconciliation,
                "Generated {} routes for cluster {}",
                routes.size(), clusterId);

            // For remote clusters, set GC ConfigMap as owner
            if (!isCentral) {
                final String finalClusterId = clusterId;
                routes = routes.stream()
                    .map(route -> {
                        // First remove existing owner references
                        route = new io.fabric8.openshift.api.model.RouteBuilder(route)
                            .editMetadata()
                                .withOwnerReferences()
                            .endMetadata()
                            .build();
                        // Then add GC ConfigMap as owner
                        return addGCConfigMapOwner(route, finalClusterId);
                    })
                    .collect(Collectors.toList());
                
                LOGGER.infoCr(reconciliation,
                    "Set GC ConfigMap as owner for {} routes in remote cluster {}",
                    routes.size(), clusterId);
            }

            futures.add(
                routeOp.batchReconcile(reconciliation, namespace, routes, kafkaCluster.getSelectorLabels())
                    .compose(result -> {
                        LOGGER.infoCr(reconciliation,
                                "Routes reconciled in cluster {}", clusterId);
                        return Future.succeededFuture();
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Reconcile Ingresses for ingress-type listeners across all clusters.
     *
     * @return Future that completes when reconciliation is done
     */
    private Future<Void> reconcileIngresses() {
        // Check if any listeners are of type ingress
        List<GenericKafkaListener> ingressListeners = kafka.getSpec().getKafka().getListeners().stream()
                .filter(l -> l.getType() == KafkaListenerType.INGRESS)
                .collect(Collectors.toList());

        if (ingressListeners.isEmpty()) {
            LOGGER.debugCr(reconciliation,
                "No ingress listeners configured, skipping ingress reconciliation");
            return Future.succeededFuture();
        }

        LOGGER.infoCr(reconciliation, "Reconciling ingresses for {} ingress listeners across {} clusters",
                   ingressListeners.size(), targetClusterIds.size());

        List<Future<Void>> futures = new ArrayList<>();

        for (String clusterId : targetClusterIds) {
            boolean isCentral = clusterId.equals(centralClusterId);
            IngressOperator ingressOp = getIngressOperator(clusterId);

            if (ingressOp == null) {
                LOGGER.warnCr(reconciliation,
                    "No IngressOperator found for cluster {}, skipping ingresses", clusterId);
                continue;
            }

            // Generate ingresses for this cluster
            List<Ingress> ingresses = new ArrayList<>();
            ingresses.addAll(
                    kafkaCluster.generateExternalBootstrapIngresses());

            // Get per-broker ingresses for this cluster
            Map<String, List<Ingress>>
                    clusteredIngresses = kafkaCluster.generateClusteredExternalIngresses();
            List<Ingress> perBrokerIngresses = clusteredIngresses.getOrDefault(clusterId, Collections.emptyList());
            ingresses.addAll(perBrokerIngresses);

            LOGGER.infoCr(reconciliation,
                "Generated {} ingresses for cluster {}",
                ingresses.size(), clusterId);

            // For remote clusters, set GC ConfigMap as owner
            if (!isCentral) {
                final String finalClusterId = clusterId;
                ingresses = ingresses.stream()
                    .map(ingress -> {
                        // First remove existing owner references
                        ingress = new io.fabric8.kubernetes.api.model.networking.v1.IngressBuilder(ingress)
                            .editMetadata()
                                .withOwnerReferences()
                            .endMetadata()
                            .build();
                        // Then add GC ConfigMap as owner
                        return addGCConfigMapOwner(ingress, finalClusterId);
                    })
                    .collect(Collectors.toList());
                
                LOGGER.infoCr(reconciliation,
                    "Set GC ConfigMap as owner for {} ingresses in remote cluster {}",
                    ingresses.size(), clusterId);
            }

            futures.add(
                ingressOp.batchReconcile(reconciliation, namespace, ingresses, kafkaCluster.getSelectorLabels())
                    .compose(result -> {
                        LOGGER.infoCr(reconciliation,
                                "Ingresses reconciled in cluster {}", clusterId);
                        return Future.succeededFuture();
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Discover actual endpoints and generate advertised.listeners configuration.
     *
     * This method:
     * 1. Discovers actual endpoints for each listener type (Route hostnames, LoadBalancer IPs, etc.)
     * 2. Generates advertised.listeners in the correct format for each broker
     * 3. Generates DNS SANs for TLS certificates
     *
     * @return Future with ListenerReconciliationResult
     */
    private Future<ListenerReconciliationResult> discoverEndpointsAndGenerateAdvertisedListeners() {
        LOGGER.infoCr(reconciliation,
                "Discovering endpoints and generating advertised.listeners");

        // Use LinkedHashMap and LinkedHashSet to maintain deterministic ordering
        // This prevents unnecessary ConfigMap updates due to different iteration order
        Map<Integer, Map<String, String>> advertisedHostnames = new LinkedHashMap<>();
        Map<Integer, Map<String, String>> advertisedPorts = new LinkedHashMap<>();
        Map<Integer, Set<String>> brokerDnsNames = new LinkedHashMap<>();
        Map<String, Set<String>> bootstrapDnsNames = new LinkedHashMap<>();

        List<Future<Void>> futures = new ArrayList<>();

        // Process each broker node
        for (NodeRef node : kafkaCluster.nodes()) {
            if (!node.broker()) {
                continue;  // Skip controller-only nodes
            }

            int nodeId = node.nodeId();
            String clusterId = node.clusterId();
            String podName = node.podName();

            LOGGER.debugCr(reconciliation,
                "Processing broker {} in cluster {}",
                nodeId, clusterId);

            Future<Void> nodeFuture = discoverBrokerEndpoints(node)
                .compose(result -> {
                    advertisedHostnames.put(nodeId, result.hostnames);
                    advertisedPorts.put(nodeId, result.ports);
                    brokerDnsNames.put(nodeId, result.dnsNames);

                    LOGGER.infoCr(reconciliation, "Discovered endpoints for broker {}: hostnames={}, ports={}",
                               nodeId, result.hostnames.keySet(), result.ports.keySet());
                    return Future.succeededFuture();
                });

            futures.add(nodeFuture);
        }

        return Future.join(futures)
            .compose(v -> {
                LOGGER.infoCr(reconciliation, "Endpoint discovery completed for {} brokers",
                           advertisedHostnames.size());

                return Future.succeededFuture(new ListenerReconciliationResult(
                    advertisedHostnames,
                    advertisedPorts,
                    brokerDnsNames,
                    bootstrapDnsNames
                ));
            });
    }

    /**
     * Discover endpoints for a single broker node.
     * Returns hostnames and ports for each listener in envVarIdentifier format.
     *
     * @param node The broker node
     * @return Future with BrokerEndpointResult
     */
    private Future<BrokerEndpointResult> discoverBrokerEndpoints(final NodeRef node) {
        int nodeId = node.nodeId();
        String clusterId = node.clusterId();
        String podName = node.podName();

        // Use LinkedHashMap and LinkedHashSet to maintain deterministic ordering
        // This prevents unnecessary ConfigMap updates due to different iteration order
        Map<String, String> hostnames = new LinkedHashMap<>();
        Map<String, String> ports = new LinkedHashMap<>();
        Set<String> dnsNames = new LinkedHashSet<>();

        List<GenericKafkaListener> listeners = kafka.getSpec().getKafka().getListeners();
        if (listeners == null) {
            listeners = Collections.emptyList();
        }

        List<Future<Void>> listenerFutures = new ArrayList<>();

        // Process each user-defined listener
        for (GenericKafkaListener listener : listeners) {
            String envVarId = ListenersUtils.envVarIdentifier(listener);

            Future<Void> listenerFuture = discoverListenerEndpoint(node, listener)
                .compose(endpoint -> {
                    hostnames.put(envVarId, endpoint.hostname);
                    ports.put(envVarId, String.valueOf(endpoint.port));
                    dnsNames.addAll(endpoint.dnsNames);

                    LOGGER.debugCr(reconciliation, "Discovered endpoint for listener {} on broker {}: {}:{}",
                               listener.getName(), nodeId, endpoint.hostname, endpoint.port);
                    return Future.succeededFuture();
                });

            listenerFutures.add(listenerFuture);
        }

        // Also add REPLICATION listener (internal broker-to-broker)
        Future<Void> replicationFuture = discoverReplicationEndpoint(node)
            .compose(endpoint -> {
                hostnames.put("REPLICATION_9091", endpoint.hostname);
                ports.put("REPLICATION_9091", String.valueOf(endpoint.port));
                dnsNames.addAll(endpoint.dnsNames);
                return Future.succeededFuture();
            });

        listenerFutures.add(replicationFuture);

        return Future.join(listenerFutures)
            .map(v -> new BrokerEndpointResult(hostnames, ports, dnsNames));
    }

    /**
     * Discover endpoint for a specific listener on a broker.
     * Returns the actual hostname/IP and port based on listener type.
     *
     * @param node The broker node
     * @param listener The listener configuration
     * @return Future with ListenerEndpoint
     */
    private Future<ListenerEndpoint> discoverListenerEndpoint(final NodeRef node, final GenericKafkaListener listener) {
        String clusterId = node.clusterId();
        String podName = node.podName();
        int nodeId = node.nodeId();

        KafkaListenerType type = listener.getType();
        String listenerName = listener.getName();
        int port = listener.getPort();

        switch (type) {
            case INTERNAL:
                return discoverInternalEndpoint(node, listener);

            case ROUTE:
                return discoverRouteEndpoint(node, listener);

            case CLUSTER_IP:
                return discoverClusterIpEndpoint(node, listener);

            case LOADBALANCER:
                return discoverLoadBalancerEndpoint(node, listener);

            case NODEPORT:
                return discoverNodePortEndpoint(node, listener);

            case INGRESS:
                return discoverIngressEndpoint(node, listener);

            default:
                LOGGER.warnCr(reconciliation,
                    "Unknown listener type: {}, using default", type);
                Set<String> dnsNames = new LinkedHashSet<>();
                String hostname = podName + "." + listenerName;
                dnsNames.add(hostname);
                return Future.succeededFuture(new ListenerEndpoint(hostname, port, dnsNames));
        }
    }

    /**
     * Discover endpoint for INTERNAL listener type.
     * Uses MCS DNS via headless service.
     *
     * @param node The broker node
     * @param listener The listener configuration
     * @return Future with ListenerEndpoint
     */
    private Future<ListenerEndpoint> discoverInternalEndpoint(final NodeRef node, final GenericKafkaListener listener) {
        String clusterId = node.clusterId();
        String podName = node.podName();
        int port = listener.getPort();

        // Use plugin to generate DNS name
        String hostname = networkingProvider.generatePodDnsName(
            namespace,
            getHeadlessServiceName(),
            podName,
            clusterId
        );

        Set<String> dnsNames = new LinkedHashSet<>();
        dnsNames.add(hostname);

        return Future.succeededFuture(new ListenerEndpoint(hostname, port, dnsNames));
    }

    /**
     * Discover endpoint for ROUTE listener type.
     * Reads the actual hostname from the Route resource.
     *
     * @param node The broker node
     * @param listener The listener configuration
     * @return Future with ListenerEndpoint
     */
    private Future<ListenerEndpoint> discoverRouteEndpoint(final NodeRef node, final GenericKafkaListener listener) {
        String clusterId = node.clusterId();
        String podName = node.podName();
        int nodeId = node.nodeId();
        String listenerName = listener.getName();

        RouteOperator routeOp = getRouteOperator(clusterId);
        if (routeOp == null) {
            LOGGER.warnCr(reconciliation,
                "No RouteOperator for cluster {}, using generated hostname", clusterId);
            return generateRouteHostname(node, listener);
        }

        // Route name format: <cluster-name>-<pool-name>-<listener>-<nodeId>
        String routeName = String.format("%s-%s-%s-%d",
            kafkaCluster.getComponentName(),
            node.poolName(),
            listenerName,
            nodeId);

        return routeOp.getAsync(namespace, routeName)
            .compose(route -> {
                if (route == null || route.getStatus() == null || route.getStatus().getIngress() == null || route.getStatus().getIngress().isEmpty()) {
                    LOGGER.warnCr(reconciliation,
                        "Route {} not ready yet, using generated hostname", routeName);
                    return generateRouteHostname(node, listener);
                }

                // Get hostname from Route status
                String hostname = route.getStatus().getIngress().get(0).getHost();
                if (hostname == null || hostname.isEmpty()) {
                    LOGGER.warnCr(reconciliation,
                        "Route {} has no hostname, using generated", routeName);
                    return generateRouteHostname(node, listener);
                }

                Set<String> dnsNames = new LinkedHashSet<>();
                dnsNames.add(hostname);

                // Routes use HTTPS port (TLS termination at router)
                return Future.succeededFuture(
                        new ListenerEndpoint(hostname, PORT_HTTPS, dnsNames));
            });
    }

    /**
     * Generate Route hostname when actual Route is not available yet.
     *
     * @param node The broker node
     * @param listener The listener configuration
     * @return Future with ListenerEndpoint
     */
    private Future<ListenerEndpoint> generateRouteHostname(final NodeRef node, final GenericKafkaListener listener) {
        String clusterId = node.clusterId();
        int nodeId = node.nodeId();
        String listenerName = listener.getName();

        // Generate expected Route hostname
        // Format: <cluster-name>-<pool>-<listener>-<nodeId>-<namespace>.apps.<domain>
        // Use cluster name + pool name (not kafkaCluster.getComponentName() which is cluster-kafka)
        String hostname = String.format("%s-%s-%s-%d-%s.apps.%s.cp.fyre.ibm.com",
            kafka.getMetadata().getName(),
            node.poolName(),
            listenerName,
            nodeId,
            namespace,
            clusterId);

        Set<String> dnsNames = new LinkedHashSet<>();
        dnsNames.add(hostname);

        return Future.succeededFuture(
                new ListenerEndpoint(hostname, PORT_HTTPS, dnsNames));
    }

    /**
     * Discover endpoint for CLUSTER_IP listener type.
     * Uses per-broker ClusterIP service DNS.
     *
     * @param node The broker node
     * @param listener The listener configuration
     * @return Future with ListenerEndpoint
     */
    private Future<ListenerEndpoint> discoverClusterIpEndpoint(final NodeRef node, final GenericKafkaListener listener) {
        String clusterId = node.clusterId();
        String podName = node.podName();
        int nodeId = node.nodeId();
        String listenerName = listener.getName();
        int port = listener.getPort();

        // Per-broker ClusterIP service name format: <cluster>-<pool>-<listener>-<nodeId>
        // Use cluster name + pool name (not kafkaCluster.getComponentName() which is cluster-kafka)
        String serviceName = String.format("%s-%s-%s-%d",
            kafka.getMetadata().getName(),
            node.poolName(),
            listenerName,
            nodeId);

        // Use MCS DNS for cross-cluster access
        // Format: <clusterId>.<service-name>.<namespace>.svc.clusterset.local
        String hostname = String.format("%s.%s.%s.svc.clusterset.local",
            clusterId, serviceName, namespace);

        Set<String> dnsNames = new LinkedHashSet<>();
        dnsNames.add(hostname);

        // Also add local cluster DNS
        String localDns = String.format("%s.%s.svc.cluster.local", serviceName, namespace);
        dnsNames.add(localDns);

        return Future.succeededFuture(new ListenerEndpoint(hostname, port, dnsNames));
    }

    /**
     * Discover endpoint for LOADBALANCER listener type.
     * Reads the LoadBalancer IP/hostname from Service status.
     *
     * @param node The broker node
     * @param listener The listener configuration
     * @return Future with ListenerEndpoint
     */
    private Future<ListenerEndpoint> discoverLoadBalancerEndpoint(final NodeRef node, final GenericKafkaListener listener) {
        String clusterId = node.clusterId();
        int nodeId = node.nodeId();
        String listenerName = listener.getName();
        int port = listener.getPort();

        ServiceOperator svcOp = getServiceOperator(clusterId);
        if (svcOp == null) {
            LOGGER.warnCr(reconciliation,
                "No ServiceOperator for cluster {}, cannot discover LoadBalancer", clusterId);
            return Future.succeededFuture(new ListenerEndpoint("pending", port, new LinkedHashSet<>()));
        }

        // Service name format: <cluster>-<pool>-<listener>-<nodeId>
        // Use cluster name + pool name (not kafkaCluster.getComponentName() which is cluster-kafka)
        String serviceName = String.format("%s-%s-%s-%d",
            kafka.getMetadata().getName(),
            node.poolName(),
            listenerName,
            nodeId);

        return svcOp.getAsync(namespace, serviceName)
            .compose(service -> {
                if (service == null || service.getStatus() == null || service.getStatus().getLoadBalancer() == null) {
                    LOGGER.debugCr(reconciliation,
                        "LoadBalancer service {} not ready yet", serviceName);
                    return Future.succeededFuture(new ListenerEndpoint("pending", port, new LinkedHashSet<>()));
                }

                var loadBalancer = service.getStatus().getLoadBalancer();
                String hostname = null;

                // Try to get hostname first
                if (loadBalancer.getIngress() != null && !loadBalancer.getIngress().isEmpty()) {
                    var ingress = loadBalancer.getIngress().get(0);
                    hostname = ingress.getHostname();

                    // If no hostname, try IP
                    if (hostname == null || hostname.isEmpty()) {
                        hostname = ingress.getIp();
                    }
                }

                if (hostname == null || hostname.isEmpty()) {
                    LOGGER.debugCr(reconciliation,
                        "LoadBalancer service {} has no external IP/hostname yet", serviceName);
                    return Future.succeededFuture(new ListenerEndpoint("pending", port, new LinkedHashSet<>()));
                }

                Set<String> dnsNames = new LinkedHashSet<>();
                dnsNames.add(hostname);

                return Future.succeededFuture(new ListenerEndpoint(hostname, port, dnsNames));
            });
    }

    /**
     * Discover endpoint for NODEPORT listener type.
     * Uses node IP + NodePort.
     *
     * @param node The broker node
     * @param listener The listener configuration
     * @return Future with ListenerEndpoint
     */
    private Future<ListenerEndpoint> discoverNodePortEndpoint(final NodeRef node, final GenericKafkaListener listener) {
        String clusterId = node.clusterId();
        String podName = node.podName();
        int nodeId = node.nodeId();
        String listenerName = listener.getName();

        ServiceOperator svcOp = getServiceOperator(clusterId);
        if (svcOp == null) {
            LOGGER.warnCr(reconciliation,
                "No ServiceOperator for cluster {}, cannot discover NodePort", clusterId);
            return Future.succeededFuture(new ListenerEndpoint("pending", 0, new LinkedHashSet<>()));
        }

        // Service name format: <cluster>-<pool>-<listener>-<nodeId>
        // Use cluster name + pool name (not kafkaCluster.getComponentName() which is cluster-kafka)
        String serviceName = String.format("%s-%s-%s-%d",
            kafka.getMetadata().getName(),
            node.poolName(),
            listenerName,
            nodeId);

        return svcOp.getAsync(namespace, serviceName)
            .compose(service -> {
                if (service == null || service.getSpec() == null || service.getSpec().getPorts() == null || service.getSpec().getPorts().isEmpty()) {
                    LOGGER.debugCr(reconciliation,
                        "NodePort service {} not ready yet", serviceName);
                    return Future.succeededFuture(new ListenerEndpoint("pending", 0, new LinkedHashSet<>()));
                }

                // Get NodePort from service
                Integer nodePort = service.getSpec().getPorts().get(0).getNodePort();
                if (nodePort == null) {
                    LOGGER.debugCr(reconciliation,
                        "NodePort service {} has no NodePort assigned yet", serviceName);
                    return Future.succeededFuture(new ListenerEndpoint("pending", 0, new LinkedHashSet<>()));
                }

                // For NodePort, we need to discover the node's external IP
                // For now, use a placeholder - in production, this would query the Node resource
                // to get externalIP or internalIP
                String hostname = "node-" + podName;  // Placeholder

                Set<String> dnsNames = new LinkedHashSet<>();
                dnsNames.add(hostname);

                return Future.succeededFuture(new ListenerEndpoint(hostname, nodePort, dnsNames));
            });
    }

    /**
     * Discover endpoint for INGRESS listener type.
     * Reads the hostname from Ingress resource.
     *
     * @param node The broker node
     * @param listener The listener configuration
     * @return Future with ListenerEndpoint
     */
    private Future<ListenerEndpoint> discoverIngressEndpoint(final NodeRef node, final GenericKafkaListener listener) {
        String clusterId = node.clusterId();
        int nodeId = node.nodeId();
        String listenerName = listener.getName();
        int port = listener.getPort();

        IngressOperator ingressOp = getIngressOperator(clusterId);
        if (ingressOp == null) {
            LOGGER.warnCr(reconciliation,
                "No IngressOperator for cluster {}, cannot discover Ingress", clusterId);
            return Future.succeededFuture(new ListenerEndpoint("pending", port, new LinkedHashSet<>()));
        }

        // Ingress name format: <cluster>-<pool>-<listener>-<nodeId>
        // Use cluster name + pool name (not kafkaCluster.getComponentName() which is cluster-kafka)
        String ingressName = String.format("%s-%s-%s-%d",
            kafka.getMetadata().getName(),
            node.poolName(),
            listenerName,
            nodeId);

        return ingressOp.getAsync(namespace, ingressName)
            .compose(ingress -> {
                if (ingress == null || ingress.getSpec() == null || ingress.getSpec().getRules() == null || ingress.getSpec().getRules().isEmpty()) {
                    LOGGER.debugCr(reconciliation,
                        "Ingress {} not ready yet", ingressName);
                    return Future.succeededFuture(new ListenerEndpoint("pending", port, new LinkedHashSet<>()));
                }

                // Get hostname from Ingress rules
                String hostname = ingress.getSpec().getRules().get(0).getHost();
                if (hostname == null || hostname.isEmpty()) {
                    LOGGER.debugCr(reconciliation,
                        "Ingress {} has no hostname", ingressName);
                    return Future.succeededFuture(new ListenerEndpoint("pending", port, new LinkedHashSet<>()));
                }

                Set<String> dnsNames = new LinkedHashSet<>();
                dnsNames.add(hostname);

                // Ingress typically uses HTTPS for TLS or HTTP for non-TLS
                int ingressPort = listener.isTls() ? PORT_HTTPS : PORT_HTTP;

                return Future.succeededFuture(new ListenerEndpoint(hostname, ingressPort, dnsNames));
            });
    }

    /**
     * Discover endpoint for REPLICATION listener (internal broker-to-broker).
     * Always uses MCS DNS via headless service.
     *
     * @param node The broker node
     * @return Future with ListenerEndpoint
     */
    private Future<ListenerEndpoint> discoverReplicationEndpoint(final NodeRef node) {
        String clusterId = node.clusterId();
        String podName = node.podName();

        // Use plugin to generate DNS name
        String hostname = networkingProvider.generatePodDnsName(
            namespace,
            getHeadlessServiceName(),
            podName,
            clusterId
        );

        Set<String> dnsNames = new LinkedHashSet<>();
        dnsNames.add(hostname);

        return Future.succeededFuture(
                new ListenerEndpoint(hostname, PORT_REPLICATION, dnsNames));
    }

    private String getHeadlessServiceName() {
        return KafkaResources.brokersServiceName(kafka.getMetadata().getName());
    }

    private ServiceOperator getServiceOperator(final String clusterId) {
        if (clusterId.equals(centralClusterId)) {
            return centralSupplier.serviceOperations;
        } else {
            return remoteOperatorSupplier.get(clusterId).serviceOperations;
        }
    }

    private RouteOperator getRouteOperator(final String clusterId) {
        if (clusterId.equals(centralClusterId)) {
            return centralSupplier.routeOperations;
        } else {
            return remoteOperatorSupplier.get(clusterId).routeOperations;
        }
    }

    private IngressOperator getIngressOperator(final String clusterId) {
        if (clusterId.equals(centralClusterId)) {
            return centralSupplier.ingressOperations;
        } else {
            return remoteOperatorSupplier.get(clusterId).ingressOperations;
        }
    }

    /**
     * Add GC ConfigMap as owner reference to a resource in remote clusters.
     *
     * @param resource Resource to add owner to
     * @param clusterId Cluster ID
     * @return Resource with owner reference added
     */
    private <T extends HasMetadata> T addGCConfigMapOwner(
            final T resource,
            final String clusterId) {

        // Skip central cluster - no owner reference needed
        if (clusterId.equals(centralClusterId)) {
            return resource;
        }

        // Get GC ConfigMap name
        String gcConfigMapName = kafka.getMetadata().getName() + "-kafka-gc";

        // Get ConfigMapOperator for the cluster
        ConfigMapOperator configMapOp = getConfigMapOperatorForCluster(clusterId);
        if (configMapOp == null) {
            LOGGER.warnCr(reconciliation,
                    "No ConfigMapOperator for cluster {}, cannot set GC owner",
                    clusterId);
            return resource;
        }

        // Fetch GC ConfigMap to get its UID
        ConfigMap gcConfigMap = configMapOp.get(namespace, gcConfigMapName);
        if (gcConfigMap == null || gcConfigMap.getMetadata().getUid() == null) {
            LOGGER.warnCr(reconciliation,
                    "GC ConfigMap {} not found or has no UID in cluster {}, cannot set owner",
                    gcConfigMapName, clusterId);
            return resource;
        }

        String gcUid = gcConfigMap.getMetadata().getUid();
        
        // Add GC ConfigMap as owner reference
        OwnerReference gcOwner = new OwnerReferenceBuilder()
                .withApiVersion("v1")
                .withKind("ConfigMap")
                .withName(gcConfigMapName)
                .withUid(gcUid)
                .withController(false)
                .withBlockOwnerDeletion(false)
                .build();
        
        // Add owner reference to resource
        List<OwnerReference> owners = new ArrayList<>();
        if (resource.getMetadata().getOwnerReferences() != null) {
            owners.addAll(resource.getMetadata().getOwnerReferences());
        }
        owners.add(gcOwner);
        resource.getMetadata().setOwnerReferences(owners);
        
        LOGGER.debugCr(reconciliation,
                "Added GC ConfigMap {} (UID: {}) as owner for {} {} in cluster {}",
                gcConfigMapName, gcUid, resource.getKind(), resource.getMetadata().getName(), clusterId);
        
        return resource;
    }

    /**
     * Get ConfigMap operator for a specific cluster.
     *
     * @param clusterId Cluster ID
     * @return ConfigMapOperator or null if not found
     */
    private ConfigMapOperator getConfigMapOperatorForCluster(
            final String clusterId) {
        if (clusterId.equals(centralClusterId)) {
            return centralSupplier.configMapOperations;
        } else {
            ResourceOperatorSupplier supplier = remoteOperatorSupplier.get(clusterId);
            return supplier != null ? supplier.configMapOperations : null;
        }
    }

    /**
     * Get StrimziPodSet operator for a specific cluster.
     *
     * @param clusterId Cluster ID
     * @return StrimziPodSetOperator or null if not found
     */
    private StrimziPodSetOperator getStrimziPodSetOperatorForCluster(
            final String clusterId) {
        if (clusterId.equals(centralClusterId)) {
            return centralSupplier.strimziPodSetOperator;
        } else {
            return remoteOperatorSupplier.get(clusterId).strimziPodSetOperator;
        }
    }

    /**
     * Get the StrimziPodSet name for a given cluster.
     *
     * @param clusterId Cluster ID
     * @return StrimziPodSet name
     */
    private String getPodSetNameForCluster(final String clusterId) {
        // Find the node pool for this cluster
        for (KafkaNodePool pool : nodePools) {
            String poolClusterId = pool.getMetadata()
                    .getAnnotations()
                    .get("strimzi.io/stretch-cluster-alias");

            if (clusterId.equals(poolClusterId)) {
                return io.strimzi.operator.cluster.model.KafkaPool.componentName(kafka, pool);
            }
        }

        // Fallback: use kafka component name
        return io.strimzi.api.kafka.model.kafka.KafkaResources.kafkaComponentName(
                kafka.getMetadata().getName()
        );
    }

    // Helper classes

    /**
     * Result of broker endpoint discovery.
     *
     * @param hostnames Hostnames per listener
     * @param ports Ports per listener
     * @param dnsNames DNS names for certificates
     */
    private static record BrokerEndpointResult(
            Map<String, String> hostnames,
            Map<String, String> ports,
            Set<String> dnsNames) {
    }

    /**
     * Endpoint information for a single listener.
     *
     * @param hostname The hostname or IP
     * @param port The port number
     * @param dnsNames DNS names for certificates
     */
    private static record ListenerEndpoint(
            String hostname,
            int port,
            Set<String> dnsNames) {
    }

}
