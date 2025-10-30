/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.strimzi.operator.cluster.ClusterInfo;
import io.strimzi.operator.common.InvalidConfigurationException;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Configuration for stretch cluster functionality.
 * This class encapsulates all stretch-cluster-specific configuration
 * to keep the core ClusterOperatorConfig clean.
 */
public class StretchClusterConfig {
    
    // Environment variable names
    /**
     * Environment variable for configuring remote Kubernetes clusters.
     */
    public static final String STRIMZI_REMOTE_KUBE_CONFIG = "STRIMZI_REMOTE_KUBE_CONFIG";

    /**
     * Environment variable for configuring central kubernetes cluster.
     */
    public static final String STRIMZI_CENTRAL_CLUSTER_ID = "STRIMZI_CENTRAL_CLUSTER_ID";

    /**
     * Environment variable for selecting the stretch networking provider.
     */
    public static final String STRIMZI_STRETCH_NETWORK_PROVIDER = "STRIMZI_STRETCH_NETWORK_PROVIDER";

    /**
     * Environment variable for specifying the stretch network configuration ConfigMap.
     */
    public static final String STRIMZI_STRETCH_NETWORK_CONFIG_MAP = "STRIMZI_STRETCH_NETWORK_CONFIG_MAP";

    /**
     * Environment variable for specifying custom plugin class name.
     */
    public static final String STRIMZI_STRETCH_PLUGIN_CLASS_NAME = "STRIMZI_STRETCH_PLUGIN_CLASS_NAME";

    /**
     * Environment variable for specifying custom plugin class path.
     */
    public static final String STRIMZI_STRETCH_PLUGIN_CLASS_PATH = "STRIMZI_STRETCH_PLUGIN_CLASS_PATH";

    private final Map<String, ClusterInfo> remoteClusters;
    private final String centralClusterId;
    private final String networkProvider;
    private final String networkConfigMap;
    private final String pluginClassName;
    private final String pluginClassPath;

    /**
     * Constructor.
     *
     * @param remoteClusters    Map of remote cluster configurations
     * @param centralClusterId  Central cluster ID
     * @param networkProvider   Stretch network provider name
     * @param networkConfigMap  Stretch network config map name
     * @param pluginClassName   Custom plugin class name
     * @param pluginClassPath   Custom plugin class path
     */
    public StretchClusterConfig(
            Map<String, ClusterInfo> remoteClusters,
            String centralClusterId,
            String networkProvider,
            String networkConfigMap,
            String pluginClassName,
            String pluginClassPath) {
        this.remoteClusters = remoteClusters != null ? remoteClusters : new HashMap<>();
        this.centralClusterId = centralClusterId;
        this.networkProvider = networkProvider;
        this.networkConfigMap = networkConfigMap;
        this.pluginClassName = pluginClassName;
        this.pluginClassPath = pluginClassPath;
    }

    /**
     * Gets the remote Kubernetes cluster details.
     *
     * @return Map of remote cluster IDs to their respective Kubernetes configuration info.
     */
    public Map<String, ClusterInfo> getRemoteClusters() {
        return remoteClusters;
    }

    /**
     * Gets the central cluster ID.
     *
     * @return Central cluster ID
     */
    public String getCentralClusterId() {
        return centralClusterId;
    }

    /**
     * Gets the stretch network provider name.
     *
     * @return Stretch network provider name
     */
    public String getNetworkProvider() {
        return networkProvider;
    }

    /**
     * Gets the stretch network config map name.
     *
     * @return Stretch network config map name
     */
    public String getNetworkConfigMap() {
        return networkConfigMap;
    }

    /**
     * Gets the custom plugin class name.
     *
     * @return Custom plugin class name
     */
    public String getPluginClassName() {
        return pluginClassName;
    }

    /**
     * Gets the custom plugin class path.
     *
     * @return Custom plugin class path
     */
    public String getPluginClassPath() {
        return pluginClassPath;
    }

    /**
     * Validates that stretch cluster configuration is valid.
     *
     * @return true if valid stretch cluster configuration is set
     */
    public boolean isValid() {
        boolean hasRemoteConfig = remoteClusters != null && !remoteClusters.isEmpty();
        boolean hasCentralId = centralClusterId != null;
        return hasRemoteConfig && hasCentralId;
    }

    /**
     * Parses remote cluster configurations from environment variable value.
     * 
     * The environment variable is expected to follow the format:
     * <pre>
     * cluster-id-1.url=https://&lt;cluster1-url&gt;
     * cluster-id-1.secret=&lt;secret-name-1&gt;
     * cluster-id-2.url=https://&lt;cluster2-url&gt;
     * cluster-id-2.secret=&lt;secret-name-2&gt;
     * </pre>
     *
     * @param envValue Environment variable value containing remote cluster configuration
     * @return Map of cluster IDs to corresponding ClusterInfo objects
     */
    public static Map<String, ClusterInfo> parseRemoteClusterConfigs(String envValue) {
        Map<String, ClusterInfo> clusters = new HashMap<>();

        if (envValue != null && !envValue.isBlank()) {
            // Temporary storage for cluster data as we parse line by line
            Map<String, String> urls = new HashMap<>();
            Map<String, String> secrets = new HashMap<>();

            for (String line : envValue.split("\n")) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }

                String[] keyValue = line.split("=", 2);
                if (keyValue.length != 2) {
                    throw new InvalidConfigurationException(
                        "Invalid remote cluster configuration line: '" + line + "'. " +
                        "Expected format: 'clusterId.url=<url>' or 'clusterId.secret=<secret>'"
                    );
                }

                String fullKey = keyValue[0].trim();
                String value = keyValue[1].trim();

                int dotIndex = fullKey.indexOf('.');
                if (dotIndex == -1) {
                    throw new InvalidConfigurationException(
                        "Invalid remote cluster configuration line: '" + line + "'. " +
                        "Key must contain a dot separator (e.g., 'cluster2.url')"
                    );
                }

                String clusterId = fullKey.substring(0, dotIndex);
                String field = fullKey.substring(dotIndex + 1);

                if ("url".equals(field)) {
                    urls.put(clusterId, value);
                } else if ("secret".equals(field)) {
                    secrets.put(clusterId, value);
                } else {
                    throw new InvalidConfigurationException(
                        "Invalid field in remote cluster configuration: '" + field + "'. " +
                        "Expected 'url' or 'secret'"
                    );
                }
            }

            // Validate all clusters have both url and secret
            for (String clusterId : urls.keySet()) {
                if (!secrets.containsKey(clusterId)) {
                    throw new InvalidConfigurationException(
                        "Remote cluster '" + clusterId + "' is missing 'secret' configuration"
                    );
                }
            }

            for (String clusterId : secrets.keySet()) {
                if (!urls.containsKey(clusterId)) {
                    throw new InvalidConfigurationException(
                        "Remote cluster '" + clusterId + "' is missing 'url' configuration"
                    );
                }
            }

            // Create ClusterInfo objects
            for (String clusterId : urls.keySet()) {
                clusters.put(clusterId, new ClusterInfo(clusterId, urls.get(clusterId), secrets.get(clusterId)));
            }
        }

        return clusters;
    }

    /**
     * Validates stretch cluster configuration environment variables.
     * All stretch-related variables must be set together or not set at all.
     *
     * @param map Map containing the environment variables.
     */
    public static void validateConfiguration(Map<String, String> map) {
        boolean hasRemoteConfig = map.containsKey(STRIMZI_REMOTE_KUBE_CONFIG);
        boolean hasCentralId = map.containsKey(STRIMZI_CENTRAL_CLUSTER_ID);
        boolean hasNetworkingProvider = map.containsKey(STRIMZI_STRETCH_NETWORK_PROVIDER);

        // Only STRIMZI_REMOTE_KUBE_CONFIG and STRIMZI_CENTRAL_CLUSTER_ID are required
        // STRIMZI_STRETCH_NETWORK_PROVIDER is optional and defaults to "mcs"
        if (hasRemoteConfig || hasCentralId) {
            if (!hasRemoteConfig) {
                throw new InvalidConfigurationException(
                    "STRIMZI_REMOTE_KUBE_CONFIG must be set when configuring stretch clusters. " +
                    "Required variables: STRIMZI_REMOTE_KUBE_CONFIG, STRIMZI_CENTRAL_CLUSTER_ID. " +
                    "Optional: STRIMZI_STRETCH_NETWORK_PROVIDER (defaults to 'mcs')"
                );
            }
            if (!hasCentralId) {
                throw new InvalidConfigurationException(
                    "STRIMZI_CENTRAL_CLUSTER_ID must be set when configuring stretch clusters. " +
                    "Required variables: STRIMZI_REMOTE_KUBE_CONFIG, STRIMZI_CENTRAL_CLUSTER_ID. " +
                    "Optional: STRIMZI_STRETCH_NETWORK_PROVIDER (defaults to 'mcs')"
                );
            }

            // Validate networking provider value if provided
            if (hasNetworkingProvider) {
                String provider = map.get(STRIMZI_STRETCH_NETWORK_PROVIDER);
                if (provider != null && !provider.isEmpty()) {
                    String providerLower = provider.toLowerCase(Locale.ROOT);
                    // Allow built-in providers: nodeport, loadbalancer/lb, mcs/multicluster, custom
                    boolean isBuiltIn = providerLower.equals("nodeport") ||
                                       providerLower.equals("loadbalancer") ||
                                       providerLower.equals("lb") ||
                                       providerLower.equals("mcs") ||
                                       providerLower.equals("multicluster") ||
                                       providerLower.equals("custom");

                    // Allow fully qualified custom class names (contains dots)
                    boolean isCustomProvider = provider.contains(".");

                    if (!isBuiltIn && !isCustomProvider) {
                        throw new InvalidConfigurationException(
                            "Invalid STRIMZI_STRETCH_NETWORK_PROVIDER value: '" + provider + "'. " +
                            "Valid values: nodeport, loadbalancer (or lb), mcs (or multicluster), custom, " +
                            "or a fully qualified custom provider class name"
                        );
                    }
                }
            }
        }
    }

    /**
     * Builds StretchClusterConfig from environment variables.
     *
     * @param map Map containing environment variables
     * @return StretchClusterConfig instance, or null if stretch is not configured
     */
    public static StretchClusterConfig fromEnvironment(Map<String, String> map) {
        validateConfiguration(map);

        String remoteKubeConfig = map.get(STRIMZI_REMOTE_KUBE_CONFIG);
        String centralClusterId = map.get(STRIMZI_CENTRAL_CLUSTER_ID);

        // If neither is set, stretch is not configured
        if (remoteKubeConfig == null && centralClusterId == null) {
            return null;
        }

        Map<String, ClusterInfo> remoteClusters = parseRemoteClusterConfigs(remoteKubeConfig);

        return new StretchClusterConfig(
            remoteClusters,
            centralClusterId,
            map.get(STRIMZI_STRETCH_NETWORK_PROVIDER),
            map.get(STRIMZI_STRETCH_NETWORK_CONFIG_MAP),
            map.get(STRIMZI_STRETCH_PLUGIN_CLASS_NAME),
            map.get(STRIMZI_STRETCH_PLUGIN_CLASS_PATH)
        );
    }
}
