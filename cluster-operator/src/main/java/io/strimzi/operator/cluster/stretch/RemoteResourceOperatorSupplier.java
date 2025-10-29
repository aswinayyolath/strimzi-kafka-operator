/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.RemoteClientSupplier;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceExportOperator;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.vertx.core.Vertx;
import io.vertx.micrometer.backends.BackendRegistries;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Class holding the various resource operators for remote kubernetes clusters.
 * 
 * <p>This class encapsulates all remote cluster operators and provides a clean
 * interface for accessing them. It is only instantiated when stretch cluster mode
 * is enabled.
 * 
 * <p>Usage:
 * <pre>{@code
 * RemoteResourceOperatorSupplier remoteSupplier = new RemoteResourceOperatorSupplier(
 *     vertx, centralClient, remoteClientSupplier, remotePfas, operatorName, centralClusterId
 * );
 * 
 * // Get operator for a specific cluster
 * ResourceOperatorSupplier clusterSupplier = remoteSupplier.get("cluster2");
 * PodOperator podOp = clusterSupplier.podOperations;
 * }</pre>
 */
public class RemoteResourceOperatorSupplier {
    private static final Logger LOGGER = LogManager.getLogger(RemoteResourceOperatorSupplier.class);

    /**
     * Service Export operator of central cluster.
     */
    public final ServiceExportOperator centralServiceExportOperator;

    /**
     * Service Export operators for all clusters (including central).
     */
    public final Map<String, ServiceExportOperator> serviceExportOperators = new HashMap<>();

    /**
     * Remote resource operator suppliers for each remote cluster.
     */
    public final Map<String, ResourceOperatorSupplier> remoteResourceOperators = new HashMap<>();
    
    /**
     * Constructor.
     * 
     * @param vertx                    Vert.x instance
     * @param centralClient            Kubernetes client of the central cluster
     * @param remoteClientSupplier     Remote Client supplier object
     * @param remotePfas               Remote platform features availability
     * @param operatorName             Operator Name
     * @param centralClusterId         Central Cluster Id
     */
    public RemoteResourceOperatorSupplier(
            Vertx vertx, 
            KubernetesClient centralClient, 
            RemoteClientSupplier remoteClientSupplier, 
            Map<String, PlatformFeaturesAvailability> remotePfas, 
            String operatorName, 
            String centralClusterId) {
        
        if (remoteClientSupplier == null) {
            throw new IllegalArgumentException("remoteClientSupplier cannot be null");
        }
        if (centralClusterId == null || centralClusterId.isEmpty()) {
            throw new IllegalArgumentException("centralClusterId cannot be null or empty");
        }
        
        LOGGER.info("Initializing remote resource operators for {} remote clusters", 
            remoteClientSupplier.getRemoteClients().size());
        
        // Create ServiceExportOperator for central cluster
        this.centralServiceExportOperator = new ServiceExportOperator(vertx, centralClient);
        this.serviceExportOperators.put(centralClusterId, this.centralServiceExportOperator);
        LOGGER.info("Initialized ServiceExportOperator for central cluster '{}'", centralClusterId);
        
        // Create operators for each remote cluster
        for (String targetCluster : remoteClientSupplier.getRemoteClients().keySet()) {
            KubernetesClient stretchClient = remoteClientSupplier.getRemoteClient(targetCluster);

            if (stretchClient == null) {
                LOGGER.error("Remote client for cluster '{}' is null! Skipping operator initialization.", targetCluster);
                continue;
            }

            LOGGER.info("Initializing operators for remote cluster '{}' with API server: {}", 
                targetCluster, stretchClient.getConfiguration().getMasterUrl());

            // Create ServiceExportOperator for this remote cluster
            this.serviceExportOperators.put(targetCluster, new ServiceExportOperator(vertx, stretchClient));

            // Create full ResourceOperatorSupplier for this remote cluster
            this.remoteResourceOperators.put(
                targetCluster, 
                new ResourceOperatorSupplier(
                    vertx,
                    stretchClient,
                    new MicrometerMetricsProvider(BackendRegistries.getDefaultNow()),
                    remotePfas.get(targetCluster),
                    new HashMap<>(),  // No nested remote PFAs
                    operatorName,
                    null  // No nested remote client supplier
                )
            );
            
            LOGGER.info("Successfully initialized operators for remote cluster '{}'", targetCluster);
        }
        
        LOGGER.info("Remote resource operator supplier initialized with {} remote clusters", 
            this.remoteResourceOperators.size());
    }

    /**
     * Get the resource operator supplier for a specific cluster.
     * 
     * @param clusterId Cluster Id 
     * @return Resource operator supplier of the specific cluster, or null if not found
     */
    public ResourceOperatorSupplier get(String clusterId) {
        return remoteResourceOperators.get(clusterId);
    }
}
