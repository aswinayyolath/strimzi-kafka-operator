/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;

/**
 * DnsNameGenerator generates DNS names for services and pods
 *
 * Kubernetes DNS documentation: https://kubernetes.io/docs/concepts/services-networking/dns-pod-service/
 */
public class DnsNameGenerator {
    private static StretchNetworkingProvider stretchProvider;

    private final String namespace;
    private final String serviceName;
    private String clusterId;

    // cluster.local is the default DNS domain for Kubernetes, if modified a user must provide the custom domain
    // via the KUBERNETES_SERVICE_DNS_DOMAIN environment variable
    private static final String KUBERNETES_SERVICE_DNS_DOMAIN =
            System.getenv().getOrDefault("KUBERNETES_SERVICE_DNS_DOMAIN", "cluster.local");

    private DnsNameGenerator(String namespace, String serviceName) {
        this.namespace = namespace;
        this.serviceName = serviceName;
    }

    private DnsNameGenerator(String namespace, String serviceName, String clusterId) {
        this(namespace, serviceName);
        this.clusterId = clusterId;
    }

    /**
     * Creates the DnsNameGenerator instance from namespace and service name
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     *
     * @return  DnsNameGenerator instance
     */
    /**
     * Sets the stretch networking provider to be used for DNS generation
     *
     * @param provider The stretch networking provider
     */
    public static void setStretchProvider(StretchNetworkingProvider provider) {
        stretchProvider = provider;
    }

    /**
     * Gets the current stretch networking provider
     *
     * @return The stretch networking provider, or null if not set
     */
    public static StretchNetworkingProvider getStretchProvider() {
        return stretchProvider;
    }

    /**
     * Creates a DnsNameGenerator instance.
     *
     * @param namespace   The namespace in which the service resides
     * @param serviceName The name of the service
     * @return            DnsNameGenerator instance
     * @throws IllegalArgumentException if any parameter is null or empty
     */
    public static DnsNameGenerator of(String namespace, String serviceName) {
        if (namespace == null || namespace.isEmpty() || serviceName == null || serviceName.isEmpty()) {
            throw new IllegalArgumentException();
        }
        return new DnsNameGenerator(namespace, serviceName);
    }

    /**
     * Creates the DnsNameGenerator instance from Cluster ID, namespace, and service name.
     *
     * @param clusterId   The ID of the remote cluster in a stretched Kafka setup.
     * @param namespace   The namespace in which the service resides.
     * @param serviceName The name of the service.
     * @return            DnsNameGenerator instance.
     * @throws IllegalArgumentException if any parameter is null or empty.
     */
    public static DnsNameGenerator of(String clusterId, String namespace, String serviceName) {
        if (clusterId == null || clusterId.isEmpty()) {
            throw new IllegalArgumentException("Cluster ID must not be null or empty.");
        }
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("Namespace must not be null or empty.");
        }
        if (serviceName == null || serviceName.isEmpty()) {
            throw new IllegalArgumentException("Service name must not be null or empty.");
        }

        return new DnsNameGenerator(namespace, serviceName, clusterId);
    }

    /**
     * Generates the DNS name of the pod including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: my-pod-1.my-service.my-ns.svc.cluster.local
     *
     * Note: Conventionally this would only be used for pods with deterministic names such as StrimziPodSet pods
     *
     * @param podName       Name of the pod
     *
     * @return              DNS name of the pod
     */
    public String podDnsName(String podName) {
        if (stretchProvider != null && clusterId != null) {
            return stretchProvider.generatePodDnsName(namespace, serviceName, podName, clusterId);
        }
        return String.format("%s.%s",
                podName,
                serviceDnsName());
    }

    /**
     * Generates a DNS name of a Pod
     *
     * @param namespace     Namespace of the Pod
     * @param serviceName   Name of the headless service
     * @param podName       Name of the Pod
     *
     * @return  Pod DNS name
     */
    public static String podDnsName(String namespace, String serviceName, String podName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .podDnsName(podName);
    }

    /**
     * Generates a DNS name of a Pod with cluster Id
     *
     * @param clusterId     clusterId of the Pod
     * @param namespace     Namespace of the Pod
     * @param serviceName   Name of the headless service
     * @param podName       Name of the Pod
     *
     * @return  Pod DNS name
     */
    public static String podDnsNameWithClusterId(String clusterId, String namespace, String serviceName, String podName) {
        if (stretchProvider != null) {
            return stretchProvider.generatePodDnsName(namespace, serviceName, podName, clusterId);
        }
        return DnsNameGenerator.of(clusterId, namespace, serviceName)
                .podDnsName(podName);
    }

    /**
     * Generates the DNS name of the pod without the cluster domain suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: my-cluster-pod-1.my-cluster-service.my-ns.svc
     *
     * Note: Conventionally this would only be used for pods with deterministic names such as StrimziPodSet pods
     *
     * @param podName       Name of the pod
     *
     * @return              DNS name of the pod without the cluster domain suffix
     */
    public String podDnsNameWithoutClusterDomain(String podName) {
        return String.format("%s.%s",
                podName,
                serviceDnsNameWithoutClusterDomain());

    }

    /**
     * Generates the pod DNS name without the cluster domain suffix
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     * @param podName       Name of the pod
     * @return              Pod DNS name without the suffix
     */
    public static String podDnsNameWithoutClusterDomain(String namespace, String serviceName, String podName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .podDnsNameWithoutClusterDomain(podName);
    }

    /**
     * Generates the pod DNS name with the cluster domain suffix
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     * @param podName       Name of the pod
     * @return              Pod DNS name including the suffix
     */
    public static String podDnsNameWithClusterDomain(String namespace, String serviceName, String podName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .podDnsName(podName);
    }

    /**
     * Generates the fully qualified DNS name of a pod, including the cluster domain and cluster ID
     *
     * @param clusterId     The ID of the target cluster
     * @param namespace     The namespace of the service.
     * @param serviceName   The name of the service
     * @param podName       The name of the pod
     * @return              Pod DNS name including the suffix
     */
    public static String podDnsNameWithClusterDomainAndClusterId(String clusterId, String namespace, String serviceName, String podName) {
        if (stretchProvider != null) {
            return stretchProvider.generatePodDnsName(namespace, serviceName, podName, clusterId);
        }
        return DnsNameGenerator.of(clusterId, namespace, serviceName)
                .podDnsName(podName);
    }

    /**
     * Generates the DNS name of the service including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: my-service.my-ns.svc.cluster.local
     * If stretch provider is configured, delegates DNS generation to the provider
     * (e.g., MCS plugin returns provider-specific DNS format)
     *
     * @return              DNS name of the service
     */
    public String serviceDnsName() {
        if (stretchProvider != null && clusterId != null) {
            return stretchProvider.generateServiceDnsName(namespace, serviceName, clusterId);
        }
        return String.format("%s.%s.svc.%s",
                serviceName,
                namespace,
                KUBERNETES_SERVICE_DNS_DOMAIN);
    }

    /**
     * Generates the wildcard DNS name of the service without the cluster domain suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: *.my-service.my-ns.svc
     *
     * @return              Wildcard DNS name of the service without the cluster domain suffix
     */
    public String wildcardServiceDnsNameWithoutClusterDomain() {
        return String.format("*.%s.%s.svc",
                serviceName,
                namespace);
    }

    /**
     * Generates the wildcard DNS name of the service including the cluster suffix
     * (i.e. usually with the cluster.local - but can be different on different clusters)
     * Example: *.my-service.my-ns.svc.cluster.local
     *
     * @return              Wildcard DNS name of the service
     */
    public String wildcardServiceDnsName() {
        return String.format("*.%s.%s.svc.%s",
                serviceName,
                namespace,
                KUBERNETES_SERVICE_DNS_DOMAIN);
    }

    /**
     * Generates the DNS name of the service without the cluster domain suffix
     * (i.e. usually without the cluster.local - but can be different on different clusters)
     * Example: my-service.my-ns.svc
     *
     * @return              DNS name of the service without the cluster domain suffix
     */
    public String serviceDnsNameWithoutClusterDomain() {
        return String.format("%s.%s.svc",
                serviceName,
                namespace);
    }

    /**
     * Generates the service name without the cluster domain suffix
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     * @return              Service DNS name without the suffix
     */
    public static String serviceDnsNameWithoutClusterDomain(String namespace, String serviceName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .serviceDnsNameWithoutClusterDomain();
    }

    /**
     * Generates the service name with the cluster domain suffix
     *
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     * @return              Fully qualified DNS name including the suffix
     */
    public static String serviceDnsNameWithClusterDomain(String namespace, String serviceName) {
        return DnsNameGenerator.of(namespace, serviceName)
                .serviceDnsName();
    }

    /**
     * Generates the service name with the cluster Id and cluster domain suffix
     *
     * @param clusterId     Cluster Id of the cluster
     * @param namespace     Namespace of the service
     * @param serviceName   Name of the service
     * @return              Fully qualified DNS name including the suffix
     */
    public static String serviceDnsNameWithClusterDomainAndClusterId(String clusterId, String namespace, String serviceName) {
        return DnsNameGenerator.of(clusterId, namespace, serviceName)
                .serviceDnsName();
    }
}
