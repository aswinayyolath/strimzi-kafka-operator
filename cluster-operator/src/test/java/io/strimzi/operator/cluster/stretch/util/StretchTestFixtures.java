/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch.util;

import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.strimzi.api.ResourceAnnotations;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.KafkaListenerType;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.ProcessRoles;
import io.strimzi.operator.cluster.ClusterInfo;

import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Test fixtures for Stretch Cluster tests.
 * Provides factory methods to create common test objects.
 * 
 * This class is designed to be independent of Strimzi's test utilities
 * to minimize coupling and maintenance burden.
 */
public class StretchTestFixtures {
    
    // Common test constants
    public static final String TEST_NAMESPACE = "test-namespace";
    public static final String TEST_CLUSTER_NAME = "test-cluster";
    public static final String CENTRAL_CLUSTER_ID = "central";
    public static final String REMOTE_CLUSTER_A_ID = "cluster-a";
    public static final String REMOTE_CLUSTER_B_ID = "cluster-b";
    
    private StretchTestFixtures() {
        // Utility class
    }
    
    /**
     * Creates a basic Kafka CR without stretch cluster configuration.
     */
    public static Kafka createBasicKafka() {
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(TEST_CLUSTER_NAME)
                    .withNamespace(TEST_NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withReplicas(3)
                        .withListeners(
                            new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build()
                        )
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endKafka()
                    .withNewZookeeper()
                        .withReplicas(3)
                        .withNewEphemeralStorage()
                        .endEphemeralStorage()
                    .endZookeeper()
                .endSpec()
                .build();
    }
    
    /**
     * Creates a Kafka CR configured for stretch cluster.
     */
    public static Kafka createStretchKafka() {
        return new KafkaBuilder(createBasicKafka())
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_ENABLE_STRETCH_CLUSTER, "true")
                .endMetadata()
                .build();
    }
    
    /**
     * Creates a Kafka CR with multiple listeners for stretch cluster testing.
     */
    public static Kafka createStretchKafkaWithMultipleListeners() {
        return new KafkaBuilder(createBasicKafka())
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_ENABLE_STRETCH_CLUSTER, "true")
                .endMetadata()
                .editSpec()
                    .editKafka()
                        .withListeners(
                            new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withPort(9092)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(false)
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName("tls")
                                .withPort(9093)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .build(),
                            new GenericKafkaListenerBuilder()
                                .withName("external")
                                .withPort(9094)
                                .withType(KafkaListenerType.LOADBALANCER)
                                .withTls(true)
                                .build()
                        )
                    .endKafka()
                .endSpec()
                .build();
    }
    
    /**
     * Creates a KafkaNodePool for the central cluster.
     */
    public static KafkaNodePool createCentralNodePool(String poolName, int replicas) {
        return createNodePool(poolName, replicas, null);
    }
    
    /**
     * Creates a KafkaNodePool for a remote cluster.
     */
    public static KafkaNodePool createRemoteNodePool(String poolName, int replicas, String clusterId) {
        return createNodePool(poolName, replicas, clusterId);
    }
    
    /**
     * Creates a KafkaNodePool with specified parameters.
     */
    private static KafkaNodePool createNodePool(String poolName, int replicas, String clusterId) {
        KafkaNodePoolBuilder builder = new KafkaNodePoolBuilder()
                .withNewMetadata()
                    .withName(poolName)
                    .withNamespace(TEST_NAMESPACE)
                    .withLabels(Map.of("strimzi.io/cluster", TEST_CLUSTER_NAME))
                .endMetadata()
                .withNewSpec()
                    .withReplicas(replicas)
                    .withRoles(ProcessRoles.BROKER)
                    .withNewEphemeralStorage()
                    .endEphemeralStorage()
                .endSpec();
        
        if (clusterId != null) {
            builder.editMetadata()
                .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_STRETCH_CLUSTER_ALIAS, clusterId)
                .endMetadata();
        }
        
        return builder.build();
    }
    
    /**
     * Creates a ClusterInfo object for testing.
     */
    public static ClusterInfo createClusterInfo(String clusterId, String apiUrl, String secretName) {
        return new ClusterInfo(clusterId, apiUrl, secretName);
    }
    
    /**
     * Creates a kubeconfig secret for testing remote cluster access.
     */
    public static Secret createKubeconfigSecret(String secretName, String namespace, String apiUrl) {
        String kubeconfigContent = createKubeconfigYaml(apiUrl);
        
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(secretName)
                    .withNamespace(namespace)
                .endMetadata()
                .addToData("kubeconfig", Base64.getEncoder().encodeToString(kubeconfigContent.getBytes()))
                .build();
    }
    
    /**
     * Creates a valid kubeconfig YAML for testing.
     */
    private static String createKubeconfigYaml(String apiUrl) {
        return String.format("""
            apiVersion: v1
            kind: Config
            clusters:
            - cluster:
                server: %s
                insecure-skip-tls-verify: true
              name: test-cluster
            contexts:
            - context:
                cluster: test-cluster
                user: test-user
              name: test-context
            current-context: test-context
            users:
            - name: test-user
              user:
                token: test-token
            """, apiUrl);
    }
    
    /**
     * Creates a map of remote cluster configurations for testing.
     */
    public static Map<String, ClusterInfo> createRemoteClusterMap() {
        return Map.of(
            REMOTE_CLUSTER_A_ID, createClusterInfo(
                REMOTE_CLUSTER_A_ID,
                "https://api.cluster-a.example.com:6443",
                "cluster-a-kubeconfig"
            ),
            REMOTE_CLUSTER_B_ID, createClusterInfo(
                REMOTE_CLUSTER_B_ID,
                "https://api.cluster-b.example.com:6443",
                "cluster-b-kubeconfig"
            )
        );
    }
    
    /**
     * Creates a remote cluster configuration string for ClusterOperatorConfig.
     */
    public static String createRemoteClusterConfigString() {
        return String.join("\n",
            REMOTE_CLUSTER_A_ID + ".url=https://api.cluster-a.example.com:6443",
            REMOTE_CLUSTER_A_ID + ".secret=cluster-a-kubeconfig",
            REMOTE_CLUSTER_B_ID + ".url=https://api.cluster-b.example.com:6443",
            REMOTE_CLUSTER_B_ID + ".secret=cluster-b-kubeconfig"
        );
    }
    
    /**
     * Creates a list of node pools for stretch cluster testing.
     * Includes both central and remote node pools.
     */
    public static List<KafkaNodePool> createStretchNodePools() {
        return List.of(
            createCentralNodePool("central-pool", 3),
            createRemoteNodePool("remote-pool-a", 2, REMOTE_CLUSTER_A_ID),
            createRemoteNodePool("remote-pool-b", 2, REMOTE_CLUSTER_B_ID)
        );
    }
    
    /**
     * Creates a Kafka CR with KRaft mode enabled for stretch cluster.
     */
    public static Kafka createStretchKafkaWithKRaft() {
        return new KafkaBuilder(createStretchKafka())
                .editMetadata()
                    .addToAnnotations(ResourceAnnotations.ANNO_STRIMZI_IO_NODE_POOLS, "enabled")
                .endMetadata()
                .editSpec()
                    .withZookeeper(null)
                .endSpec()
                .build();
    }
}
