/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Class holding Kubernetes clients for remote clusters in a stretch cluster configuration.
 */
public class RemoteClientSupplier {
    private static final Logger LOGGER = LogManager.getLogger(RemoteClientSupplier.class);

    private final Map<String, KubernetesClient> remoteClients;
    /**
     * Constructor.
     *
     * @param namespace      Namespace where the kubeconfig secrets are stored.
     * @param remoteClusters Mapping of cluster IDs to ClusterInfo.
     * @param remoteClients  Mapping of cluster IDs to KubernetesClient instances.
     */
    public RemoteClientSupplier(String namespace, Map<String, ClusterInfo> remoteClusters, Map<String, KubernetesClient> remoteClients) {
        this.remoteClients = remoteClients;
    }

    /**
     * Static factory method to build RemoteClientSupplier using ClusterInfo and kubeconfig secrets.
     *
     * @param namespace      Namespace where kubeconfig secrets reside.
     * @param remoteClusters Map of cluster IDs to ClusterInfo objects.
     * @param secretOperator SecretOperator capable of retrieving kubeconfig secrets.
     * @return A RemoteClientSupplier with initialized KubernetesClients for each remote cluster.
     */
    public static RemoteClientSupplier buildFromClusterInfo(String namespace, Map<String, ClusterInfo> remoteClusters, SecretOperator secretOperator) {
        Map<String, KubernetesClient> remoteClients = new HashMap<>();

        for (Map.Entry<String, ClusterInfo> entry : remoteClusters.entrySet()) {
            String clusterId = entry.getKey();
            ClusterInfo clusterInfo = entry.getValue();

            if (clusterInfo == null) {
                LOGGER.warn("No ClusterInfo found for cluster ID '{}'. Skipping.", clusterId);
                continue;
            }

            try {
                Secret kubeconfigSecret = secretOperator.get(namespace, clusterInfo.secretName());

                if (kubeconfigSecret == null || kubeconfigSecret.getData() == null || !kubeconfigSecret.getData().containsKey("kubeconfig")) {
                    LOGGER.error("Missing kubeconfig for cluster ID '{}', secret '{}'. Skipping cluster.", clusterId, clusterInfo.secretName());
                    continue;
                }

                String kubeconfig = new String(Base64.getDecoder().decode(kubeconfigSecret.getData().get("kubeconfig")));
                Config config = Config.fromKubeconfig(kubeconfig);
                KubernetesClient remoteClient = new KubernetesClientBuilder().withConfig(config).build();

                remoteClients.put(clusterId, remoteClient);
            } catch (Exception e) {
                LOGGER.error("Failed to create Kubernetes client for cluster '{}'", clusterId, e);
            }
        }

        return new RemoteClientSupplier(namespace, remoteClusters, remoteClients);
    }

    /**
     * Get a mapping of remote cluster IDs to Kubernetes clients.
     *
     * @return Map of remote cluster IDs to KubernetesClient instances.
     */
    public Map<String, KubernetesClient> getRemoteClients() {
        return remoteClients;
    }

    /**
     * Get the Kubernetes client for a specific remote cluster.
     *
     * @param clusterId The ID of the target remote cluster.
     * @return KubernetesClient for the given cluster ID, or null if not available.
     */
    public KubernetesClient getRemoteClient(String clusterId) {
        return remoteClients.get(clusterId);
    }
}
