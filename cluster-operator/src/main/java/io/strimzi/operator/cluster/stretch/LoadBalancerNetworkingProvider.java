/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.LoadBalancerIngress;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * LoadBalancer-based networking provider for stretch clusters.
 *
 * Creates LoadBalancer services for each Kafka pod and uses LoadBalancer IPs
 * for cross-cluster communication.
 *
 * Advantages:
 * - Stable IPs (don't change with node replacement)
 * - Standard Kafka ports (not high NodePort range)
 * - Production-ready
 * - Works with MetalLB, cloud providers (AWS ELB, GCP LB, Azure LB)
 */
public final class LoadBalancerNetworkingProvider
        implements StretchNetworkingProvider {

    /**
     * Logger for this class.
     */
    private static final Logger LOGGER =
            LogManager.getLogger(LoadBalancerNetworkingProvider.class);

    /**
     * Resource operator supplier for central cluster.
     */
    private ResourceOperatorSupplier centralSupplier;
    /**
     * Resource operator supplier for remote clusters.
     */
    private RemoteResourceOperatorSupplier remoteResourceOperatorSupplier;
    /**
     * LoadBalancer annotations.
     */
    private Map<String, String> lbAnnotations = new HashMap<>();
    /**
     * LoadBalancer class.
     */
    private String loadBalancerClass;

    @Override
    public Future<Void> init(
            final Map<String, String> config,
            final ResourceOperatorSupplier centralSupplierParam,
            final RemoteResourceOperatorSupplier remoteResourceOperatorSupplierParam) {

        this.centralSupplier = centralSupplierParam;
        this.remoteResourceOperatorSupplier = remoteResourceOperatorSupplierParam;

        // Read configuration
        if (config != null) {
            // Parse LoadBalancer annotations
            // (e.g., "metallb.universe.tf/address-pool=kafka-pool")
            if (config.containsKey("lbAnnotations")) {
                String annotationsStr = config.get("lbAnnotations");
                for (String annotation : annotationsStr.split(",")) {
                    String[] parts = annotation.split("=", 2);
                    if (parts.length == 2) {
                        lbAnnotations.put(parts[0].trim(), parts[1].trim());
                    }
                }
            }

            if (config.containsKey("loadBalancerClass")) {
                this.loadBalancerClass = config.get("loadBalancerClass");
            }
        }

        LOGGER.info(
                "LoadBalancer provider initialized with annotations={}, "
                + "loadBalancerClass={}",
                lbAnnotations, loadBalancerClass);
        return Future.succeededFuture();
    }

    @Override
    public Future<List<HasMetadata>> createNetworkingResources(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId,
            final Map<String, Integer> ports) {

        LOGGER.debug(
                "{}: Creating LoadBalancer service for pod {} in cluster {}",
                reconciliation, podName, clusterId);

        ResourceOperatorSupplier supplier = getSupplierForCluster(clusterId);
        if (supplier == null) {
            return Future.failedFuture(
                    "No supplier found for cluster: " + clusterId);
        }

        // Create LoadBalancer service
        String serviceName = podName + "-loadbalancer";

        List<ServicePort> servicePorts = ports.entrySet().stream()
            .map(entry -> {
                return new io.fabric8.kubernetes.api.model.ServicePortBuilder()
                    .withName(entry.getKey())
                    .withPort(entry.getValue())
                    .withProtocol("TCP")
                    .build();
            })
            .collect(Collectors.toList());

        ServiceBuilder serviceBuilder = new ServiceBuilder()
            .withNewMetadata()
                .withName(serviceName)
                .withNamespace(namespace)
                .addToLabels("app", "strimzi")
                .addToLabels("strimzi.io/cluster", extractClusterName(podName))
                .addToLabels("strimzi.io/kind", "Kafka")
                .addToLabels("strimzi.io/name", podName)
                .addToAnnotations("strimzi.io/stretch-cluster-alias", clusterId)
                // Apply LoadBalancer-specific annotations
                // (e.g., MetalLB pool)
                .addToAnnotations(lbAnnotations)
            .endMetadata()
            .withNewSpec()
                .withType("LoadBalancer")
                .withPorts(servicePorts)
                .addToSelector("statefulset.kubernetes.io/pod-name", podName)
                .withLoadBalancerClass(loadBalancerClass != null
                        && !loadBalancerClass.isEmpty()
                        ? loadBalancerClass : null)
            .endSpec();

        Service service = serviceBuilder.build();

        return supplier.serviceOperations
            .reconcile(reconciliation, namespace, serviceName, service)
            .map(result -> {
                List<HasMetadata> resources = new ArrayList<>();
                resources.add(service);
                LOGGER.debug(
                        "{}: LoadBalancer service {} created/updated",
                        reconciliation, serviceName);
                return resources;
            });
    }

    @Override
    public String generateServiceDnsName(final String namespace,
                                          final String serviceName,
                                          final String clusterId) {
        // LoadBalancer uses standard Kubernetes DNS
        return String.format("%s.%s.svc.cluster.local",
            serviceName,
            namespace);
    }

    @Override
    public String generatePodDnsName(final String namespace,
                                      final String serviceName,
                                      final String podName,
                                      final String clusterId) {
        // LoadBalancer uses standard Kubernetes pod DNS
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

        String serviceName = podName + "-loadbalancer";

        // Get the LoadBalancer service
        return supplier.serviceOperations
            .getAsync(namespace, serviceName)
            .compose(service -> {
                if (service == null) {
                    throw new RuntimeException(
                            "LoadBalancer service not found: " + serviceName);
                }

                // Find the port number for the requested port name
                ServicePort port = service.getSpec().getPorts().stream()
                    .filter(p -> p.getName().equals(portName))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException(
                            "Port not found: " + portName));

                // Get LoadBalancer IP/hostname from ingress
                List<LoadBalancerIngress> ingresses =
                        service.getStatus().getLoadBalancer().getIngress();
                if (ingresses == null || ingresses.isEmpty()) {
                    return Future.failedFuture(
                            "LoadBalancer ingress not ready for service: "
                                    + serviceName);
                }

                LoadBalancerIngress ingress = ingresses.get(0);
                String lbIp = ingress.getIp();
                if (lbIp == null || lbIp.isEmpty()) {
                    // Some cloud providers use hostname instead of IP
                    lbIp = ingress.getHostname();
                }

                if (lbIp == null || lbIp.isEmpty()) {
                    return Future.failedFuture(
                            "LoadBalancer IP/hostname not available "
                                    + "for service: " + serviceName);
                }

                return Future.succeededFuture(lbIp + ":" + port.getPort());
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

        String serviceName = podName + "-loadbalancer";

        // Return LoadBalancer IP as SAN
        return supplier.serviceOperations
            .getAsync(namespace, serviceName)
            .map(service -> {
                List<String> sans = new ArrayList<>();

                if (service != null && service.getStatus() != null
                        && service.getStatus().getLoadBalancer() != null) {
                    List<LoadBalancerIngress> ingresses =
                            service.getStatus().getLoadBalancer()
                                    .getIngress();
                    if (ingresses != null && !ingresses.isEmpty()) {
                        LoadBalancerIngress ingress = ingresses.get(0);
                        if (ingress.getIp() != null) {
                            sans.add(ingress.getIp());
                        }
                        if (ingress.getHostname() != null) {
                            sans.add(ingress.getHostname());
                        }
                    }
                }

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

        String serviceName = podName + "-loadbalancer";

        LOGGER.debug(
                "{}: Deleting LoadBalancer service {} in cluster {}",
                reconciliation, serviceName, clusterId);

        return supplier.serviceOperations
            .reconcile(reconciliation, namespace, serviceName, null)
            .mapEmpty();
    }

    @Override
    public String getProviderName() {
        return "loadbalancer";
    }

    // Helper methods

    private ResourceOperatorSupplier getSupplierForCluster(
            final String clusterId) {
        return remoteResourceOperatorSupplier.remoteResourceOperators.getOrDefault(clusterId, centralSupplier);
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
