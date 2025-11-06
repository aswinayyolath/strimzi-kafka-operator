/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.networking.v1.Ingress;
import io.fabric8.openshift.api.model.Route;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaResources;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.cluster.model.ImagePullPolicy;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Stretch Kafka cluster model
 */
public class StretchKafkaCluster {
    /**
     * Component type used by Kubernetes labels
     */
    public static final String COMPONENT_TYPE = "kafka";

    // Stretch Mode Configuration
    private String centralClusterId;
    private Set<String> targetClusterIds;
    private List<String> clusterIds;
    private StretchNetworkingProvider stretchNetworkingProvider;
    private List<KafkaPool> nodePools;
    private Kafka kafka;
    private KafkaCluster kafkaCluster;
    private Reconciliation reconciliation;

    /**
     * Constructor
     *
     * @param reconciliation The reconciliation
     * @param kafka Kafka custom resource
     * @param kafkaCluster The Regular Kafka Cluster Model
     * @param pools Set of node pools used by this cluster
     * @param centralClusterId Cluster Id of the central cluster
     * @param targetClusterIds List of target cluster IDs defined in kafka node pools
     */
    public StretchKafkaCluster(Reconciliation reconciliation,
                                Kafka kafka,
                                KafkaCluster kafkaCluster,
                                List<KafkaPool> pools,
                                String centralClusterId, 
                                Set<String> targetClusterIds) {
        
        this.centralClusterId = centralClusterId;
        this.targetClusterIds = targetClusterIds;
        this.kafka = kafka;
        this.nodePools = pools;
        this.kafkaCluster = kafkaCluster;
        this.reconciliation = reconciliation;
    }

    /**
     * Generates list of references to Kafka nodes for this Kafka cluster, in the specified kube cluster.
     * The references contain both the pod name and the ID of the Kafka node.
     * @param targetClusterId   The cluster Id of the kube cluster where the nodes are deployed
     *
     * @return  Set of Kafka node references
     */
    public Set<NodeRef> nodesAtCluster(String targetClusterId) {
        Set<NodeRef> nodes = new LinkedHashSet<>();
        List<KafkaPool> pools = nodePools.stream()
                                        .filter(x -> x.getTargetCluster().equals(targetClusterId))
                                        .toList();

        for (KafkaPool pool : nodePools) {
            nodes.addAll(pool.nodes());
        }

        return nodes;
    }

    
    /**
     * Generates Map of targetCluster and the list of corresponding
     * per pod services.
     *
     * @return The Map targetCluster and their generated Services
     */
    public Map<String, List<Service>> generateClusteredPerPodServices() {
        Map<String, List<Service>> result = new HashMap<>();

        for (String targetClusterId : targetClusterIds) {
            result.put(targetClusterId, kafkaCluster.generatePerPodServices(targetClusterId));
        }

        return result;
    }

    /**
     * Generates Map of targetCluster and the list of corresponding
     * per pod external Ingresses
     *
     * @return The Map targetCluster and their generated Ingresses
     */
    public Map<String, List<Ingress>> generateClusteredExternalIngresses() {
        Map<String, List<Ingress>> result = new HashMap<>();

        for (String targetClusterId : targetClusterIds) {
            result.put(targetClusterId, kafkaCluster.generateExternalIngresses(targetClusterId));
        }

        return result;
    }

    /**
     * Generates Map of targetCluster and the list of corresponding
     * per pod external Routes
     *
     * @return The Map of targetClusters and their generated Routes
     */
    public Map<String, List<Route>> generateClusteredExternalRoutes() {
        Map<String, List<Route>> result = new HashMap<>();

        for (String targetClusterId : targetClusterIds) {
            result.put(targetClusterId, kafkaCluster.generateExternalRoutes(targetClusterId));
        }

        return result;
    }

    /**
     * Gets the broker service name
     *
     * @return The broker service name
     */
    public String getBrokerServiceName() {
        return KafkaResources.brokersServiceName(kafka.getMetadata().getName());
    }

    /**
     * Generates a clustered StrimziPodSet for the stretched kafka cluster
     *
     * @param isOpenShift            Flags whether we are on OpenShift or not
     * @param imagePullPolicy        Image pull policy which will be used by the pods
     * @param imagePullSecrets       List of image pull secrets
     * @param podAnnotationsProvider Function which provides annotations for given pod based on its broker ID. The
     *                               annotations for each pod are different due to the individual configurations.
     *                               So they need to be dynamically generated though this function instead of just
     *                               passed as Map.
     * @param centralClusterId       Stretch Cluster Central Cluster ID
     *
     * @return List of generated StrimziPodSets with Kafka pods
     */
    public Map<String, List<StrimziPodSet>> generateClusteredPodSets(boolean isOpenShift,
        ImagePullPolicy imagePullPolicy,
        List<LocalObjectReference> imagePullSecrets,
        Function<NodeRef, Map<String, String>> podAnnotationsProvider,
        String centralClusterId) {

        Map<String, List<StrimziPodSet>> podSets = new HashMap<>();

        for (String targetCluster : targetClusterIds) {
            podSets
                .computeIfAbsent(targetCluster, k -> new ArrayList<>())
                .addAll(
                    kafkaCluster.generatePodSets(
                        isOpenShift,
                        imagePullPolicy,
                        imagePullSecrets,
                        podAnnotationsProvider,
                        targetCluster,
                        centralClusterId
                    )
                );
        }

        return podSets;
    }

    /**
     * @return  Returns the central cluster Id of a stretched kafka cluster
     */
    public String getCentralClusterId() {
        return centralClusterId;
    }




    /**
     * @return  Returns target cluster Ids defined in KafkaNodePools of a kafka cluster
     */
    public Set<String> getTargetClusterIds() {
        return targetClusterIds;
    }


    /**
     * @return  Returns the stretch networking provider (can be null for non-stretch clusters)
     */
    public StretchNetworkingProvider getStretchNetworkingProvider() {
        return stretchNetworkingProvider;
    }

}