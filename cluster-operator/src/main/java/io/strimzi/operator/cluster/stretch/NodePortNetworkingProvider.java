/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.NodeAddress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.model.Labels;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * NodePort-based networking provider for stretch clusters.
 *
 * Creates NodePort services for each Kafka pod and uses node IPs +
 * NodePort for cross-cluster communication.
 *
 * Advantages:
 * - Works on ANY Kubernetes cluster
 * - Zero external dependencies
 * - Simple to debug
 * - Perfect for testing
 */
public final class NodePortNetworkingProvider
        implements StretchNetworkingProvider {

    /** Logger for this class. */
    private static final Logger LOGGER =
            LogManager.getLogger(NodePortNetworkingProvider.class);

    /** Resource operator supplier for central cluster. */
    private ResourceOperatorSupplier centralSupplier;
    /** Resource operator supplier for remote clusters. */
    private RemoteResourceOperatorSupplier remoteResourceOperatorSupplier;
    /** Node address type to use (ExternalIP or InternalIP). */
    private String nodeAddressType = "ExternalIP";

    @Override
    public Future<Void> init(
            final Map<String, String> config,
            final ResourceOperatorSupplier centralSupplierParam,
            final RemoteResourceOperatorSupplier remoteResourceOperatorSupplierParam) {

        this.centralSupplier = centralSupplierParam;
        this.remoteResourceOperatorSupplier = remoteResourceOperatorSupplierParam;

        // Read configuration
        if (config != null && config.containsKey("nodeAddressType")) {
            this.nodeAddressType = config.get("nodeAddressType");
        }

        LOGGER.info("NodePort provider initialized with nodeAddressType={}",
                nodeAddressType);
        return Future.succeededFuture();
    }

    @Override
    public Future<List<HasMetadata>> createNetworkingResources(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId,
            final Map<String, Integer> ports) {

        LOGGER.debug("{}: Creating NodePort service for pod {} in cluster {}",
                    reconciliation, podName, clusterId);

        ResourceOperatorSupplier supplier = getSupplierForCluster(clusterId);
        if (supplier == null) {
            return Future.failedFuture(
                    "No supplier found for cluster: " + clusterId);
        }

        // Create NodePort service
        String serviceName = podName + "-nodeport";

        List<ServicePort> servicePorts = ports.entrySet().stream()
            .map(entry -> {
                return new io.fabric8.kubernetes.api.model.ServicePortBuilder()
                    .withName(entry.getKey())
                    .withPort(entry.getValue())
                    .withProtocol("TCP")
                    .build();
            })
            .collect(Collectors.toList());

        Service service = new ServiceBuilder()
            .withNewMetadata()
                .withName(serviceName)
                .withNamespace(namespace)
                .addToLabels("app", "strimzi")
                .addToLabels("strimzi.io/cluster", extractClusterName(podName))
                .addToLabels("strimzi.io/kind", "Kafka")
                .addToLabels("strimzi.io/name", podName)
                .addToAnnotations("strimzi.io/stretch-cluster-alias", clusterId)
            .endMetadata()
            .withNewSpec()
                .withType("NodePort")
                .withPorts(servicePorts)
                .addToSelector("statefulset.kubernetes.io/pod-name", podName)
            .endSpec()
            .build();

        return supplier.serviceOperations
            .reconcile(reconciliation, namespace, serviceName, service)
            .map(result -> {
                List<HasMetadata> resources = new ArrayList<>();
                resources.add(service);
                LOGGER.debug("{}: NodePort service {} created/updated",
                        reconciliation, serviceName);
                return resources;
            });
    }

    @Override
    public String generateServiceDnsName(final String namespace,
                                          final String serviceName,
                                          final String clusterId) {
        // NodePort uses standard Kubernetes DNS
        return String.format("%s.%s.svc.cluster.local",
            serviceName,
            namespace);
    }

    @Override
    public String generatePodDnsName(final String namespace,
                                      final String serviceName,
                                      final String podName,
                                      final String clusterId) {
        // NodePort uses standard Kubernetes pod DNS
        return String.format("%s.%s.svc.cluster.local",
            podName,
            namespace);
    }

    @Override
    public Future<String> discoverPodEndpoint(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId,
            final String portName) {

        ResourceOperatorSupplier supplier = getSupplierForCluster(clusterId);
        if (supplier == null) {
            return Future.failedFuture(
                    "No supplier found for cluster: " + clusterId);
        }

        String serviceName = podName + "-nodeport";

        // Get the NodePort service
        return supplier.serviceOperations
            .getAsync(namespace, serviceName)
            .compose(service -> {
                if (service == null) {
                    throw new RuntimeException(
                            "NodePort service not found: " + serviceName);
                }

                // Find the NodePort for the requested port
                ServicePort port = service.getSpec().getPorts().stream()
                    .filter(p -> p.getName().equals(portName))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException(
                            "Port not found: " + portName));

                Integer nodePort = port.getNodePort();

                if (nodePort == null) {
                    return Future.failedFuture(
                            "NodePort not found for port: " + portName);
                }

                // Get a node IP
                return getNodeIp(supplier, clusterId)
                    .map(nodeIp -> nodeIp + ":" + nodePort);
            });
    }

    @Override
    public Future<String> generateAdvertisedListeners(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId,
            final Map<String, String> listeners) {

        List<Future<String>> listenerFutures = new ArrayList<>();

        for (Map.Entry<String, String> entry : listeners.entrySet()) {
            String listenerName = entry.getKey();
            String portName = entry.getValue();

            Future<String> listenerFuture = discoverPodEndpoint(
                reconciliation, namespace, podName, clusterId, portName
            ).map(endpoint -> listenerName + "://" + endpoint);

            listenerFutures.add(listenerFuture);
        }

        return Future.join(listenerFutures)
            .map(result -> {
                List<String> listenerStrings = listenerFutures.stream()
                    .map(Future::result)
                    .collect(Collectors.toList());
                return String.join(",", listenerStrings);
            });
    }

    @Override
    public Future<String> generateQuorumVoters(
            final Reconciliation reconciliation,
            final String namespace,
            final List<ControllerPodInfo> controllerPods,
            final String replicationPortName) {

        List<Future<String>> voterFutures = new ArrayList<>();

        for (ControllerPodInfo controller : controllerPods) {
            Future<String> voterFuture = discoverPodEndpoint(
                reconciliation,
                namespace,
                controller.podName(),
                controller.clusterId(),
                replicationPortName
            ).map(endpoint -> controller.nodeId() + "@" + endpoint);

            voterFutures.add(voterFuture);
        }

        return Future.join(voterFutures)
            .map(result -> {
                List<String> voters = voterFutures.stream()
                    .map(Future::result)
                    .collect(Collectors.toList());
                return String.join(",", voters);
            });
    }

    @Override
    public Future<List<String>> generateCertificateSans(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId) {

        ResourceOperatorSupplier supplier = getSupplierForCluster(clusterId);
        if (supplier == null) {
            return Future.failedFuture(
                    "No supplier found for cluster: " + clusterId);
        }

        // Return node IPs as SANs
        return getNodeIp(supplier, clusterId)
            .map(nodeIp -> {
                List<String> sans = new ArrayList<>();
                sans.add(nodeIp);
                return sans;
            });
    }

    @Override
    public Future<Void> deleteNetworkingResources(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId) {

        ResourceOperatorSupplier supplier = getSupplierForCluster(clusterId);
        if (supplier == null) {
            return Future.failedFuture(
                    "No supplier found for cluster: " + clusterId);
        }

        String serviceName = podName + "-nodeport";

        LOGGER.debug(
                "{}: Deleting NodePort service {} in cluster {}",
                reconciliation, serviceName, clusterId);

        return supplier.serviceOperations
            .reconcile(reconciliation, namespace, serviceName, null)
            .mapEmpty();
    }

    @Override
    public String getProviderName() {
        return "nodeport";
    }

    // Helper methods

    private ResourceOperatorSupplier getSupplierForCluster(
            final String clusterId) {
        return remoteResourceOperatorSupplier.remoteResourceOperators.getOrDefault(clusterId, centralSupplier);
    }

    private Future<String> getNodeIp(
            final ResourceOperatorSupplier supplier,
            final String clusterId) {
        return supplier.nodeOperator
            .listAsync(Labels.EMPTY)
            .map(nodes -> {
                if (nodes == null || nodes.isEmpty()) {
                    throw new RuntimeException(
                            "No nodes found in cluster: " + clusterId);
                }

                // Get first worker node
                Node node = nodes.stream()
                    .filter(n -> !n.getSpec().getTaints()
                        .stream()
                        .anyMatch(t ->
                                "node-role.kubernetes.io/master"
                                        .equals(t.getKey())))
                    .findFirst()
                    .orElse(nodes.get(0));

                // Get node IP based on configured type
                String nodeIp = node.getStatus().getAddresses().stream()
                    .filter(addr -> nodeAddressType.equals(addr.getType()))
                    .map(NodeAddress::getAddress)
                    .findFirst()
                    .orElse(null);

                if (nodeIp == null) {
                    // Fallback to InternalIP if ExternalIP not found
                    nodeIp = node.getStatus().getAddresses().stream()
                        .filter(addr ->
                                "InternalIP".equals(addr.getType()))
                        .map(NodeAddress::getAddress)
                        .findFirst()
                        .orElseThrow(() -> new RuntimeException(
                                "No IP address found for node"));
                }

                return nodeIp;
            });
    }

    private String extractClusterName(final String podName) {
        // Extract cluster name from pod name
        // (e.g., "my-cluster-kafka-0" -> "my-cluster")
        int lastDash = podName.lastIndexOf('-');
        if (lastDash > 0) {
            String withoutIndex = podName.substring(0, lastDash);
            int secondLastDash = withoutIndex.lastIndexOf('-');
            if (secondLastDash > 0) {
                return withoutIndex.substring(0, secondLastDash);
            }
        }
        return podName;
    }
}
