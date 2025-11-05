/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaim;
import io.fabric8.kubernetes.api.model.ServiceAccount;
import io.fabric8.kubernetes.api.model.rbac.ClusterRoleBinding;
import io.strimzi.api.kafka.model.common.Condition;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaMetadataState;
import io.strimzi.api.kafka.model.kafka.KafkaStatus;
import io.strimzi.api.kafka.model.kafka.Storage;
import io.strimzi.api.kafka.model.kafka.UsedNodePoolStatus;
import io.strimzi.api.kafka.model.kafka.UsedNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListener;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatus;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolBuilder;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatus;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePoolStatusBuilder;
import io.strimzi.api.kafka.model.serviceexport.ServiceExport;
import io.strimzi.certs.CertManager;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.model.ClusterCa;
import io.strimzi.operator.cluster.model.KafkaCluster;
import io.strimzi.operator.cluster.model.KafkaPool;
import io.strimzi.operator.cluster.model.KafkaVersion;
import io.strimzi.operator.cluster.model.KafkaVersionChange;
import io.strimzi.operator.cluster.model.NodeRef;
import io.strimzi.operator.cluster.model.RestartReasons;
import io.strimzi.operator.cluster.model.SharedEnvironmentProvider;
import io.strimzi.operator.cluster.model.nodepools.NodePoolUtils;
import io.strimzi.operator.cluster.operator.assembly.CaReconciler;
import io.strimzi.operator.cluster.operator.assembly.KafkaNodeUnregistration;
import io.strimzi.operator.cluster.operator.resource.KafkaAgentClientProvider;
import io.strimzi.operator.cluster.operator.resource.KafkaRoller;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ClusterRoleBindingOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ConfigMapOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PvcOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceAccountOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.ServiceExportOperator;
import io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.AdminClientProvider;
import io.strimzi.operator.common.BackOff;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.auth.TlsPemIdentity;
import io.strimzi.operator.common.model.ClientsCa;
import io.strimzi.operator.common.model.Labels;
import io.strimzi.operator.common.model.PasswordGenerator;
import io.strimzi.operator.common.model.StatusDiff;
import io.strimzi.operator.common.model.StatusUtils;
import io.strimzi.operator.common.operator.resource.ReconcileResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.admin.Admin;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Main orchestrator for stretch cluster reconciliation.
 *
 * This class coordinates all stretch cluster operations using the pluggable
 * networking provider. It delegates networking concerns to the provider while
 * handling the overall reconciliation flow.
 *
 * Responsibilities:
 * - Orchestrate reconciliation across multiple clusters
 * - Delegate networking to the provider
 * - Generate Kafka configuration using provider endpoints
 * - Manage certificates with provider-specific SANs
 * - Handle resource creation/deletion across clusters
 */
@SuppressWarnings({"checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity", "checkstyle:FileLength"})
public class StretchClusterReconciler {

    private static final Logger LOGGER = LogManager.getLogger(StretchClusterReconciler.class);
    private static final String STRETCH_CLUSTER_ALIAS_ANNOTATION = "strimzi.io/stretch-cluster-alias";

    private final StretchNetworkingProvider networkingProvider;
    private final ResourceOperatorSupplier centralSupplier;
    private final ClusterOperatorConfig config;
    private final String centralClusterId;
    private final SharedEnvironmentProvider sharedEnvironmentProvider;
    private final Vertx vertx;
    private final CertManager certManager;
    private final PasswordGenerator passwordGenerator;
    private final long operationTimeoutMs;
    private final KafkaAgentClientProvider kafkaAgentClientProvider;
    private final AdminClientProvider adminClientProvider;
    private final StretchClusterValidator validator;
    private TlsPemIdentity coTlsPemIdentity;
    private final RemoteResourceOperatorSupplier remoteOperatorSupplier;

    // Reconciliation context - set once per reconciliation
    /** Current reconciliation context. */
    private Reconciliation reconciliation;
    /** Kafka custom resource being reconciled. */
    private Kafka kafka;
    /** List of KafkaNodePool CRs. */
    private List<KafkaNodePool> nodePools;
    /** Set of target cluster IDs. */
    private Set<String> targetClusterIds;
    /** KafkaCluster model - created once per reconciliation. */
    private KafkaCluster kafkaCluster;

    // Reconciliation results - populated during reconciliation
    /** Cluster CA. */
    private ClusterCa clusterCa;
    /** Clients CA. */
    private ClientsCa clientsCa;
    /** Listener reconciliation result. */
    private ListenerReconciliationResult listenerResult;
    /** Kafka cluster ID. */
    private String clusterId;
    /** Previous node IDs from status. */
    private Set<Integer> previousNodeIds;

    /**
     * Constructor.
     *
     * @param networkingProvider Networking provider to use
     * @param centralSupplier ResourceOperatorSupplier for the central cluster
     * @param remoteResourceOperatorSupplier ResourceOperatorSupplier for remote clusters
     * @param config Cluster operator configuration
     * @param vertx Vertx instance
     * @param certManager CertManager instance
     * @param passwordGenerator PasswordGenerator instance
     */
    public StretchClusterReconciler(
            StretchNetworkingProvider networkingProvider,
            ResourceOperatorSupplier centralSupplier,
            RemoteResourceOperatorSupplier remoteResourceOperatorSupplier,
            ClusterOperatorConfig config,
            Vertx vertx,
            CertManager certManager,
            PasswordGenerator passwordGenerator) {

        this.networkingProvider = networkingProvider;
        this.centralSupplier = centralSupplier;
        this.remoteOperatorSupplier = remoteResourceOperatorSupplier;
        this.config = config;
        this.centralClusterId = config.getCentralClusterId();
        this.sharedEnvironmentProvider = centralSupplier.sharedEnvironmentProvider;
        this.vertx = vertx;
        this.certManager = certManager;
        this.passwordGenerator = passwordGenerator;
        this.operationTimeoutMs = config.getOperationTimeoutMs();
        this.kafkaAgentClientProvider = centralSupplier.kafkaAgentClientProvider;
        this.adminClientProvider = centralSupplier.adminClientProvider;
        
        // Initialize validator with central and remote cluster IDs
        Set<String> remoteClusterIds = remoteOperatorSupplier.remoteResourceOperators.keySet();
        this.validator = new StretchClusterValidator(vertx, centralClusterId, remoteClusterIds);

        LOGGER.info("StretchClusterReconciler initialized with provider: {}",
                   networkingProvider.getProviderName());
    }
    /**
     * Get the cluster ID for a given KafkaNodePool.
     *
     * @param pool The KafkaNodePool
     * @return The cluster ID from the annotation
     */
    private String getClusterIdForPool(KafkaNodePool pool) {
        Map<String, String> annotations = pool.getMetadata().getAnnotations();
        if (annotations != null && annotations.containsKey(STRETCH_CLUSTER_ALIAS_ANNOTATION)) {
            return annotations.get(STRETCH_CLUSTER_ALIAS_ANNOTATION);
        }
        return null;
    }

    /**
     * Extract target cluster IDs from node pools.
     *
     * @param nodePools List of KafkaNodePools
     * @return Set of cluster IDs
     */
    private Set<String> extractTargetClusterIds(List<KafkaNodePool> nodePools) {
        Set<String> clusterIds = new HashSet<>();
        for (KafkaNodePool pool : nodePools) {
            String clusterId = getClusterIdForPool(pool);
            if (clusterId != null && !clusterId.isEmpty()) {
                clusterIds.add(clusterId);
            }
        }
        return clusterIds;
    }

    /**
     * Get PodOperator for a given cluster.
     *
     * @param clusterId Cluster ID
     * @return PodOperator for the cluster, or null if not found
     */
    private PodOperator getPodOperatorForCluster(String clusterId) {
        if (clusterId.equals(centralClusterId)) {
            return centralSupplier.podOperations;
        } else if (remoteOperatorSupplier.remoteResourceOperators.containsKey(clusterId)) {
            return remoteOperatorSupplier.get(clusterId).podOperations;
        } else {
            return null;
        }
    }

    /**
     * Get StrimziPodSetOperator for a given cluster.
     *
     * @param clusterId Cluster ID
     * @return StrimziPodSetOperator for the cluster, or null if not found
     */
    private StrimziPodSetOperator getStrimziPodSetOperatorForCluster(String clusterId) {
        if (clusterId.equals(centralClusterId)) {
            return centralSupplier.strimziPodSetOperator;
        } else if (remoteOperatorSupplier.remoteResourceOperators.containsKey(clusterId)) {
            return remoteOperatorSupplier.get(clusterId).strimziPodSetOperator;
        } else {
            return null;
        }
    }

    /**
     * Get ServiceExportOperator for a given cluster.
     *
     * @param clusterId Cluster ID
     * @return ServiceExportOperator for the cluster
     */
    private ServiceExportOperator getServiceExportOperatorForCluster(String clusterId) {
        LOGGER.debug("Looking for ServiceExportOperator for cluster '{}'. Available clusters: {}",
                    clusterId, remoteOperatorSupplier.serviceExportOperators.keySet());
        return remoteOperatorSupplier.serviceExportOperators.get(clusterId);
    }

    /**
     * Main reconciliation entry point.
     *
     * Orchestrates the entire stretch cluster reconciliation:
     * 1. Validate configuration
     * 2. Create networking resources across clusters
     * 3. Deploy Kafka pods to target clusters
     * 4. Generate and distribute Kafka configuration
     * 5. Handle certificates with provider-specific SANs
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka custom resource
     * @param nodePools List of KafkaNodePool CRs
     * @return Future with KafkaStatus
     */
    @SuppressWarnings("checkstyle:MethodLength")
    public Future<KafkaStatus> reconcile(Reconciliation reconciliation, Kafka kafka, List<KafkaNodePool> nodePools) {

        // Initialize reconciliation context
        this.reconciliation = reconciliation;
        this.kafka = kafka;
        this.nodePools = nodePools;
        this.targetClusterIds = extractTargetClusterIds(nodePools);
        
        LOGGER.info("{}: Starting stretch cluster reconciliation with provider: {}",
                   reconciliation, networkingProvider.getProviderName());

        // Check if reconciliation is paused
        if (io.strimzi.operator.common.Annotations.isReconciliationPausedWithAnnotation(kafka)) {
            return handlePausedReconciliation(reconciliation, kafka);
        }

        LOGGER.debug("{}: Target clusters: {}", reconciliation, targetClusterIds);
        
        // Capture previous node IDs from Kafka status for unregistration tracking
        // This follows the Single cluster Strimzi pattern: compare previous (from status) vs current (from model)
        this.previousNodeIds = kafka.getStatus() != null && kafka.getStatus().getRegisteredNodeIds() != null 
            ? new HashSet<>(kafka.getStatus().getRegisteredNodeIds())
            : new HashSet<>();

        // Step 1: Validate stretch cluster configuration
        StretchClusterValidator.ValidationResult configResult = 
            validator.validateKafkaConfiguration(kafka, nodePools, true);
        
        if (!configResult.isValid()) {
            return handleValidationError(reconciliation, kafka, configResult);
        }
        
        return Future.succeededFuture()

            // Step 2: Reconcile Certificate Authorities (CAs)
            .compose(v -> reconcileCertificateAuthorities(reconciliation, kafka, nodePools)
                .compose(caResult -> {
                    this.clusterCa = caResult.clusterCa();
                    this.clientsCa = caResult.clientsCa();
                    return Future.succeededFuture();
                })
            )

            // Step 2.5: Initialize TLS identity for admin client
            .compose(v -> initClientAuthenticationCertificates(reconciliation))
            
            // Step 2.6: Get cluster ID BEFORE creating ConfigMaps (critical for new pods)
            .compose(v -> getClusterIdEarly(reconciliation, kafka)
                .compose(clusterIdResult -> {
                    this.clusterId = clusterIdResult;
                    LOGGER.debug("{}: Retrieved cluster ID: {}", reconciliation, this.clusterId);
                    
                    // Create KafkaCluster model once, used by all subsequent steps
                    this.kafkaCluster = createKafkaClusterModel(reconciliation, kafka, nodePools, targetClusterIds);
                    LOGGER.debug("{}: Created KafkaCluster model with {} nodes across {} clusters",
                        reconciliation, this.kafkaCluster.nodes().size(), targetClusterIds.size());
                    
                    return Future.succeededFuture();
                })
            )

            // Step 3: Reconcile ServiceAccounts
            .compose(v -> reconcileServiceAccounts(reconciliation, kafka, nodePools, targetClusterIds))

            // Step 4: Create ClusterRoleBindings
            .compose(v -> reconcileClusterRoleBindings(reconciliation, kafka, nodePools, targetClusterIds))

            // Step 5: Create PVCs
            .compose(v -> reconcilePVCs(reconciliation, kafka, nodePools, targetClusterIds))

            // Step 6-7: Reconcile Listeners (Services, Routes, Ingresses, endpoint discovery)
            // Delegates to StretchKafkaListenersReconciler for all listener-related logic
            .compose(v -> {
                // Use the KafkaCluster model created once above
                StretchKafkaListenersReconciler listenersReconciler = new StretchKafkaListenersReconciler(
                    reconciliation, kafka, this.kafkaCluster, nodePools, targetClusterIds, centralClusterId, centralSupplier, remoteOperatorSupplier, networkingProvider
                );
                return listenersReconciler.reconcile();
            })
            .compose(listenerResultParam -> {
                // Store listener result in field
                this.listenerResult = new ListenerReconciliationResult(
                    null,  // advertisedListeners - not used in new implementation
                    listenerResultParam.getAdvertisedHostnames(),
                    listenerResultParam.getAdvertisedPorts(),
                    null,  // quorumVoters - will be generated separately
                    listenerResultParam.getBootstrapDnsNames(),
                    listenerResultParam.getBrokerDnsNames()
                );
                LOGGER.debug("{}: Listener reconciliation completed: {} brokers configured", 
                           reconciliation, listenerResult.getAdvertisedHostnames().size());
                return Future.succeededFuture();
            })

            // Step 8: Create ServiceExports (networking resources)
            .compose(v -> reconcileNetworking(reconciliation, kafka, nodePools, targetClusterIds))

            // Step 9: Create Secrets (certificates) - uses DNS names from listeners
            .compose(v -> reconcileSecrets(reconciliation, kafka, nodePools, targetClusterIds, 
                                          this.clusterCa, this.clientsCa, this.listenerResult))

            // Step 10: Create ConfigMaps (broker configuration) - uses advertised listeners and cluster ID
            .compose(v -> reconcileConfigMaps(reconciliation, kafka, nodePools, targetClusterIds, this.listenerResult, this.clusterId))

            // Step 11: Scale down pods if needed (before creating new PodSets)
            .compose(v -> scaleDown(reconciliation, kafka, nodePools, targetClusterIds))

            // Step 11.5: Unregister removed nodes (KRaft scale-down)
            // Must happen BEFORE creating new PodSets to prevent duplicate node IDs
            // If we create new pods with same IDs before unregistering old ones, we get duplicates!
            .compose(v -> nodeUnregistration(reconciliation, kafka, nodePools, targetClusterIds, this.clusterId, 
                this.previousNodeIds != null ? new ArrayList<>(this.previousNodeIds) : new ArrayList<>()))

            // Step 12: Create/Update StrimziPodSets (handles scale-up automatically)
            .compose(v -> reconcilePodSets(reconciliation, kafka, nodePools, targetClusterIds))

            // Step 12.5: Update owner references on ConfigMaps and Secrets now that PodSets exist
            // ConfigMap OwnerReferece are not getting added properly - There is a bug here
            // We should create an issue in ES Jira and fix this
            .compose(podSetDiffs -> updateOwnerReferencesForRemoteResources(reconciliation, kafka, nodePools, targetClusterIds)
                .map(podSetDiffs))

            // Step 13: Wait for newly added pods to be ready (BEFORE rolling update)
            // This prevents KafkaRoller from encountering unschedulable/pending pods
            .compose(podSetDiffs -> waitForNewPodsReady(reconciliation, kafka, nodePools, targetClusterIds)
                .map(podSetDiffs))

            // Step 14: Rolling update - restart pods if needed (config changes, certs, etc.)
            // Only runs after new pods are ready, so KafkaRoller won't see unschedulable pods
            .compose(podSetDiffs -> rollingUpdate(reconciliation, kafka, nodePools, targetClusterIds, podSetDiffs, this.listenerResult))

            // Step 16: Update node pool statuses
            .compose(v -> updateNodePoolStatuses(reconciliation, kafka, nodePools, targetClusterIds))

            // Step 17: Build and return Kafka status (includes cluster ID)
            .compose(v -> buildStatus(reconciliation, kafka, nodePools, targetClusterIds, this.listenerResult, this.clusterId))

            .onSuccess(status -> {
                LOGGER.info("{}: Stretch cluster reconciliation completed successfully", reconciliation);
            })
            .recover(error -> {
                // Convert exceptions to user-friendly error messages in status
                LOGGER.error("{}: Stretch cluster reconciliation failed: {}", reconciliation, error.getMessage());
                
                KafkaStatus status = new KafkaStatus();
                Set<Condition> conditions = StatusUtils.validate(reconciliation, kafka);
                
                // Add error condition with clear message
                Condition errorCondition = new Condition();
                errorCondition.setType("Ready");
                errorCondition.setStatus("False");
                errorCondition.setReason("ReconciliationFailed");
                errorCondition.setMessage(error.getMessage());
                errorCondition.setLastTransitionTime(StatusUtils.iso8601Now());
                
                conditions.add(errorCondition);
                status.setConditions(new ArrayList<>(conditions));
                status.setObservedGeneration(kafka.getMetadata().getGeneration());
                
                return Future.succeededFuture(status);
            });
    }

    /**
     * Handle paused reconciliation.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @return Future with paused status
     */
    private Future<KafkaStatus> handlePausedReconciliation(Reconciliation reconciliation, Kafka kafka) {
        LOGGER.info("{}: Reconciliation is PAUSED by annotation strimzi.io/pause-reconciliation=true", reconciliation);
        
        KafkaStatus status = new KafkaStatus();
        Set<Condition> conditions = StatusUtils.validate(reconciliation, kafka);
        conditions.add(StatusUtils.getPausedCondition());
        status.setConditions(new ArrayList<>(conditions));
        status.setObservedGeneration(kafka.getMetadata().getGeneration());
        
        return Future.succeededFuture(status);
    }

    /**
     * Reconcile Certificate Authorities (Cluster CA and Clients CA).
     *
     * This creates/updates the CA certificates needed for Kafka cluster security.
     * Step 1: Reconcile CAs in central cluster (creates/updates CA secrets)
     * Step 2: Reconcile CAs in all remote clusters (distributes CA secrets)
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @return Future with CaReconciliationResult containing ClusterCa and ClientsCa
     */
    private Future<CaReconciler.CaReconciliationResult> reconcileCertificateAuthorities(
            Reconciliation reconciliation, Kafka kafka, List<KafkaNodePool> nodePools) {
        LOGGER.info("{}: Reconciling Certificate Authorities", reconciliation);

        // Step 1: Create CaReconciler for central cluster
        CaReconciler centralCaReconciler = new CaReconciler(
                reconciliation,
                kafka,
                config,
                centralSupplier,
                vertx,
                certManager,
                passwordGenerator
        );

        // Reconcile CAs in central cluster first
        return centralCaReconciler.reconcile(Clock.systemUTC())
                .compose(caResult -> {
                    LOGGER.debug("{}: Certificate Authorities reconciled in central cluster", reconciliation);
                    
                    // Step 2: Reconcile CAs in all remote clusters
                    return reconcileCAsInRemoteClusters(reconciliation, kafka, nodePools, caResult)
                            .map(caResult); // Return the CA result
                })
                .onSuccess(result -> {
                    LOGGER.info("{}: Certificate Authorities reconciled successfully in all clusters", reconciliation);
                })
                .onFailure(error -> {
                    LOGGER.error("{}: Failed to reconcile Certificate Authorities", reconciliation, error);
                });
    }

    /**
     * Reconcile Certificate Authorities in all remote clusters.
     * This distributes the CA secrets from central cluster to remote clusters.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param caResult CA reconciliation result from central cluster
     * @return Future that completes when CAs are reconciled in all remote clusters
     */
    private Future<Void> reconcileCAsInRemoteClusters(
            Reconciliation reconciliation, 
            Kafka kafka,
            List<KafkaNodePool> nodePools,
            CaReconciler.CaReconciliationResult caResult) {
        
        // Get all target cluster IDs except central
        Set<String> remoteClusterIds = new HashSet<>();
        for (String clusterId : extractTargetClusterIds(nodePools)) {
            if (!clusterId.equals(centralClusterId)) {
                remoteClusterIds.add(clusterId);
            }
        }

        if (remoteClusterIds.isEmpty()) {
            LOGGER.debug("{}: No remote clusters to reconcile CAs", reconciliation);
            return Future.succeededFuture();
        }

        LOGGER.debug("{}: Reconciling CAs in {} remote clusters: {}", 
                   reconciliation, remoteClusterIds.size(), remoteClusterIds);

        List<Future<Void>> futures = new ArrayList<>();

        for (String targetClusterId : remoteClusterIds) {
            // Get operators for this remote cluster
            io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator remoteSecretOp = 
                remoteOperatorSupplier.get(targetClusterId).secretOperations;
            io.strimzi.operator.cluster.operator.resource.kubernetes.StrimziPodSetOperator remotePodSetOp = 
                remoteOperatorSupplier.get(targetClusterId).strimziPodSetOperator;
            PodOperator remotePodOp = 
                remoteOperatorSupplier.get(targetClusterId).podOperations;

            if (remoteSecretOp == null || remotePodSetOp == null || remotePodOp == null) {
                return Future.failedFuture(
                    "Missing operators for remote cluster: " + targetClusterId);
            }

            // Create a new CaReconciler configured for this remote cluster
            CaReconciler remoteCaReconciler = new CaReconciler(
                    reconciliation,
                    kafka,
                    config,
                    centralSupplier,
                    vertx,
                    certManager,
                    passwordGenerator
            );

            // Configure it for stretch mode with the CAs from central cluster
            remoteCaReconciler.withStretchConfig(
                    remoteSecretOp,
                    remotePodSetOp,
                    remotePodOp,
                    caResult.clusterCa(),
                    caResult.clientsCa(),
                    targetClusterId
            );

            // Reconcile CAs in this remote cluster
            futures.add(
                remoteCaReconciler.reconcile(Clock.systemUTC())
                    .compose(result -> {
                        LOGGER.debug("{}: CAs reconciled in remote cluster {}", 
                                   reconciliation, targetClusterId);
                        return Future.succeededFuture();
                    })
            );
        }
        
        return Future.join(futures).mapEmpty();
    }


    /**
     * Handle validation errors by updating Kafka CR status with error details.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param result Validation result containing error details
     * @return Future with KafkaStatus containing error condition
     */
    private Future<KafkaStatus> handleValidationError(
            Reconciliation reconciliation,
            Kafka kafka,
            StretchClusterValidator.ValidationResult result) {
        
        LOGGER.error("{}: Stretch cluster validation failed: {}", 
            reconciliation, result.getErrorMessage());
        
        KafkaStatus status = new KafkaStatus();
        io.strimzi.api.kafka.model.common.Condition condition = 
            new io.strimzi.api.kafka.model.common.ConditionBuilder()
                .withType("Ready")
                .withStatus("False")
                .withReason(result.getErrorCode())
                .withMessage(result.getErrorMessage())
                .withLastTransitionTime(StatusUtils.iso8601Now())
                .build();
        
        status.setConditions(List.of(condition));
        status.setObservedGeneration(kafka.getMetadata().getGeneration());
        
        return Future.succeededFuture(status);
    }

    /**
     * Reconcile ServiceAccounts across all clusters.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when all ServiceAccounts are reconciled
     */
    private Future<Void> reconcileServiceAccounts(Reconciliation reconciliation, Kafka kafka,
                                                  List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        LOGGER.debug("{}: Reconciling ServiceAccounts across {} clusters",
                reconciliation, targetClusterIds.size());

        List<Future<Void>> futures = new ArrayList<>();
        String namespace = kafka.getMetadata().getNamespace();

        // Use the KafkaCluster model created once in reconcile()
        ServiceAccount baseServiceAccount = this.kafkaCluster.generateServiceAccount();

        for (String clusterId : targetClusterIds) {
            boolean isCentral = clusterId.equals(centralClusterId);
            ServiceAccountOperator saOp;

            // For remote clusters, we need to remove OwnerReferences because the Kafka CR doesn't exist there
            ServiceAccount serviceAccount;
            if (isCentral) {
                saOp = centralSupplier.serviceAccountOperations;
                serviceAccount = baseServiceAccount; // Keep OwnerReference for central cluster
                LOGGER.debug("{}: Using central ServiceAccountOperator for cluster {}", reconciliation, clusterId);
            } else {
                // Use stretch-specific operator that has the remote cluster's client
                saOp = remoteOperatorSupplier.get(clusterId).serviceAccountOperations;
                if (saOp == null) {
                    LOGGER.error("{}: No ServiceAccountOperator found for cluster: {}", reconciliation, clusterId);
                    return Future.failedFuture("No ServiceAccountOperator found for cluster: " + clusterId);
                }

                // Clone the ServiceAccount and remove Kafka CR OwnerReferences for remote clusters
                ServiceAccount remoteServiceAccount = new io.fabric8.kubernetes.api.model.ServiceAccountBuilder(baseServiceAccount)
                    .editMetadata()
                        .withOwnerReferences()  // Empty list removes all owner references
                    .endMetadata()
                    .build();
                
                // Add StrimziPodSet as owner for automatic cleanup
                String podSetName = getPodSetNameForCluster(kafka, nodePools, clusterId);
                serviceAccount = addStrimziPodSetOwner(reconciliation, remoteServiceAccount, podSetName, clusterId, namespace);

                LOGGER.debug("{}: Using stretch ServiceAccountOperator for remote cluster {} (StrimziPodSet owner added)",
                           reconciliation, clusterId);
            }

            final ServiceAccount finalServiceAccount = serviceAccount;
            futures.add(
                saOp.reconcile(reconciliation, namespace, finalServiceAccount.getMetadata().getName(), finalServiceAccount)
                    .compose(result -> {
                        LOGGER.debug("{}: ServiceAccount '{}' reconciled in cluster {}",
                                   reconciliation, finalServiceAccount.getMetadata().getName(), clusterId);
                        return Future.<Void>succeededFuture();
                    })
                    .recover(error -> {
                        LOGGER.error("{}: Failed to reconcile ServiceAccount in cluster {}: {}",
                                    reconciliation, clusterId, error.getMessage());
                        return Future.<Void>failedFuture(error);
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Reconcile ClusterRoleBindings across all clusters.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when all ClusterRoleBindings are reconciled
     */
    private Future<Void> reconcileClusterRoleBindings(Reconciliation reconciliation, Kafka kafka,
                                                      List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        LOGGER.debug("{}: Reconciling ClusterRoleBindings across {} clusters",
                reconciliation, targetClusterIds.size());

        List<Future<Void>> futures = new ArrayList<>();
        String namespace = kafka.getMetadata().getNamespace();

        // Use the KafkaCluster model created once in reconcile()
        ClusterRoleBinding crb = this.kafkaCluster.generateClusterRoleBinding(namespace);

        // ClusterRoleBinding is only needed for rack awareness or NodePort exposure
        // If null, skip ClusterRoleBinding creation
        if (crb == null) {
            LOGGER.debug("{}: ClusterRoleBinding not needed (no rack awareness or NodePort exposure)", reconciliation);
            return Future.succeededFuture();
        }

        for (String clusterId : targetClusterIds) {
            boolean isCentral = clusterId.equals(centralClusterId);
            ClusterRoleBindingOperator crbOp;

            if (isCentral) {
                crbOp = centralSupplier.clusterRoleBindingOperator;
            } else {
                crbOp = remoteOperatorSupplier.get(clusterId).clusterRoleBindingOperator;
                if (crbOp == null) {
                    return Future.failedFuture("No ClusterRoleBindingOperator found for cluster: " + clusterId);
                }
            }

            futures.add(
                crbOp.reconcile(reconciliation, crb.getMetadata().getName(), crb)
                    .compose(result -> {
                        LOGGER.debug("{}: ClusterRoleBinding reconciled in cluster {}", reconciliation, clusterId);
                        return Future.succeededFuture();
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Reconcile PVCs across all clusters.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when all PVCs are reconciled
     */
    private Future<Void> reconcilePVCs(Reconciliation reconciliation, Kafka kafka,
                                      List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        LOGGER.debug("{}: Reconciling PVCs across {} clusters",
                reconciliation, targetClusterIds.size());

        List<Future<Void>> futures = new ArrayList<>();
        String namespace = kafka.getMetadata().getNamespace();

        // Use the KafkaCluster model created once in reconcile()
        for (String clusterId : targetClusterIds) {
            boolean isCentral = clusterId.equals(centralClusterId);
            PvcOperator pvcOp;

            if (isCentral) {
                pvcOp = centralSupplier.pvcOperations;
            } else {
                pvcOp = remoteOperatorSupplier.get(clusterId).pvcOperations;
                if (pvcOp == null) {
                    return Future.failedFuture("No PvcOperator found for cluster: " + clusterId);
                }
            }

            // Generate PVCs for this cluster using KafkaCluster method
            // This reuses existing Strimzi logic for PVC generation
            List<PersistentVolumeClaim> pvcs = this.kafkaCluster.generatePersistentVolumeClaimsAtCluster(clusterId);

            for (PersistentVolumeClaim pvc : pvcs) {
                futures.add(
                    pvcOp.reconcile(reconciliation, namespace, pvc.getMetadata().getName(), pvc)
                        .compose(result -> {
                            LOGGER.debug("{}: PVC {} reconciled in cluster {}",
                                        reconciliation, pvc.getMetadata().getName(), clusterId);
                            return Future.succeededFuture();
                        })
                );
            }
        }

        return Future.join(futures).mapEmpty();
    }

    // Service and listener reconciliation now handled by StretchKafkaListenersReconciler

    /**
     * Reconcile Secrets (certificates) across all clusters.
     *
     * This creates:
     * 1. Cluster CA cert secret (shared)
     * 2. Clients CA cert secret (shared)
     * 3. Per-broker certificate secrets (one per broker)
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @param clusterCa Cluster CA
     * @param clientsCa Clients CA
     * @return Future that completes when all Secrets are reconciled
     */
    private Future<Void> reconcileSecrets(Reconciliation reconciliation, Kafka kafka,
                                         List<KafkaNodePool> nodePools, Set<String> targetClusterIds,
                                         ClusterCa clusterCa, ClientsCa clientsCa,
                                         ListenerReconciliationResult listenerResult) {
        LOGGER.info("{}: Reconciling Secrets across {} clusters",
                reconciliation, targetClusterIds.size());

        List<Future<Void>> futures = new ArrayList<>();
        String namespace = kafka.getMetadata().getNamespace();

        // Use the KafkaCluster model created once in reconcile()

        for (String clusterId : targetClusterIds) {
            boolean isCentral = clusterId.equals(centralClusterId);
            
            // Get the appropriate secret operator
            io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator secretOp;
            if (isCentral) {
                secretOp = centralSupplier.secretOperations;
            } else {
                secretOp = remoteOperatorSupplier.get(clusterId).secretOperations;
                if (secretOp == null) {
                    return Future.failedFuture("No SecretOperator found for cluster: " + clusterId);
                }
            }

            // List existing secrets in this cluster
            futures.add(
                secretOp.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingSecrets -> {
                        LOGGER.info("{}: Found {} existing secrets in cluster {}",
                                   reconciliation, existingSecrets.size(), clusterId);

                        // Generate certificate secrets for this cluster
                        // Use DNS names from listener reconciliation
                        Set<String> bootstrapDnsNames = listenerResult.getBootstrapDnsNames(clusterId);
                        Map<Integer, Set<String>> brokerDnsNames = listenerResult.getAllBrokerDnsNames();
                        
                        LOGGER.debug("{}: Using {} bootstrap DNS names and {} broker DNS entries for cluster {}",
                                   reconciliation, bootstrapDnsNames.size(), brokerDnsNames.size(), clusterId);
                        
                        List<io.fabric8.kubernetes.api.model.Secret> desiredSecrets = 
                            this.kafkaCluster.generateCertificatesSecrets(
                                clusterCa, 
                                clientsCa, 
                                existingSecrets,
                                bootstrapDnsNames,
                                brokerDnsNames,
                                true, // maintenance window satisfied
                                clusterId
                            );

                        LOGGER.info("{}: Generated {} certificate secrets for cluster {}",
                                   reconciliation, desiredSecrets.size(), clusterId);

                        // Build list of desired secret names for this cluster
                        List<String> desiredSecretNames = desiredSecrets.stream()
                            .map(s -> s.getMetadata().getName())
                            .collect(Collectors.toList());
                        
                        LOGGER.debug("{}: Cluster {} - Desired secrets: {}, Existing secrets: {}",
                            reconciliation, clusterId, desiredSecretNames.size(), existingSecrets.size());
                        
                        // Reconcile each secret
                        List<Future<Void>> secretFutures = new ArrayList<>();
                        
                        // Delete secrets that exist but are not desired (belong to removed pods)
                        for (io.fabric8.kubernetes.api.model.Secret existingSecret : existingSecrets) {
                            String secretName = existingSecret.getMetadata().getName();
                            // Only delete broker/controller secrets, not CA or cluster secrets
                            if ((secretName.contains("-broker-") || secretName.contains("-controller-")) 
                                && !desiredSecretNames.contains(secretName)) {
                                LOGGER.info("{}: Deleting orphaned secret {} from cluster {} (pod was scaled down)",
                                    reconciliation, secretName, clusterId);
                                secretFutures.add(
                                    secretOp.deleteAsync(reconciliation, namespace, secretName, true)
                                        .mapEmpty()
                                );
                            }
                        }
                        
                        // Create/update desired secrets
                        for (io.fabric8.kubernetes.api.model.Secret secret : desiredSecrets) {
                            // For remote clusters, preserve existing StrimziPodSet owner references
                            // They are set by updateOwnerReferencesForRemoteResources() in Step 12.5
                            // For central cluster, keep existing ownership (Kafka CR)
                            if (!isCentral) {
                                // Get existing Secret to preserve StrimziPodSet owner references
                                io.fabric8.kubernetes.api.model.Secret existing = secretOp.get(namespace, secret.getMetadata().getName());
                                if (existing != null && existing.getMetadata().getOwnerReferences() != null) {
                                    // Preserve StrimziPodSet owner references from existing Secret
                                    List<OwnerReference> existingRefs = existing.getMetadata().getOwnerReferences();
                                    List<OwnerReference> strimziPodSetRefs = existingRefs.stream()
                                        .filter(ref -> "StrimziPodSet".equals(ref.getKind()))
                                        .toList();
                                    
                                    if (!strimziPodSetRefs.isEmpty()) {
                                        LOGGER.debug("{}: Preserving {} StrimziPodSet owner reference(s) for Secret {}",
                                            reconciliation, strimziPodSetRefs.size(), secret.getMetadata().getName());
                                        secret = new io.fabric8.kubernetes.api.model.SecretBuilder(secret)
                                            .editMetadata()
                                                .withOwnerReferences(strimziPodSetRefs)
                                            .endMetadata()
                                            .build();
                                    }
                                }
                            }

                            final io.fabric8.kubernetes.api.model.Secret finalSecret = secret;
                            secretFutures.add(
                                secretOp.reconcile(reconciliation, namespace, secret.getMetadata().getName(), finalSecret)
                                    .compose(result -> {
                                        LOGGER.debug("{}: Secret {} reconciled in cluster {}",
                                                   reconciliation, finalSecret.getMetadata().getName(), clusterId);
                                        return Future.succeededFuture();
                                    })
                            );
                        }

                        return Future.join(secretFutures).mapEmpty();
                    })
            );
        }

        return Future.join(futures)
            .compose(v -> {
                LOGGER.info("{}: All secrets reconciled successfully across {} clusters",
                           reconciliation, targetClusterIds.size());
                return Future.succeededFuture();
            });
    }

    /**
     * Reconcile ConfigMaps (broker configuration) across all clusters.
     *
     * This creates per-broker ConfigMaps containing:
     * - server.properties (Kafka broker configuration)
     * - log4j.properties (logging configuration)
     * - metrics configuration
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @param listenerResult Listener reconciliation result
     * @param clusterId Cluster ID
     * @return Future that completes when all ConfigMaps are reconciled
     */
    private Future<Void> reconcileConfigMaps(Reconciliation reconciliation, Kafka kafka,
                                            List<KafkaNodePool> nodePools, Set<String> targetClusterIds,
                                            ListenerReconciliationResult listenerResult, String clusterId) {
        LOGGER.info("{}: Reconciling ConfigMaps across {} clusters (cluster ID: {})",
                reconciliation, targetClusterIds.size(), clusterId);

        List<Future<Void>> futures = new ArrayList<>();
        String namespace = kafka.getMetadata().getNamespace();

        // Use the KafkaCluster model created once in reconcile()
        // Create MetricsAndLogging with null ConfigMaps (will use defaults)
        // In a full implementation, we'd fetch the metrics and logging ConfigMaps if referenced
        io.strimzi.operator.cluster.model.MetricsAndLogging metricsAndLogging = 
            new io.strimzi.operator.cluster.model.MetricsAndLogging(null, null);

        for (String targetClusterId : targetClusterIds) {
            boolean isCentral = targetClusterId.equals(centralClusterId);
            ConfigMapOperator cmOp;

            if (isCentral) {
                cmOp = centralSupplier.configMapOperations;
            } else {
                cmOp = remoteOperatorSupplier.get(targetClusterId).configMapOperations;
                if (cmOp == null) {
                    return Future.failedFuture("No ConfigMapOperator found for cluster: " + targetClusterId);
                }
            }

            // Generate per-broker configuration ConfigMaps for this specific cluster
            Map<Integer, Map<String, String>> advertisedHostnames = listenerResult.getAdvertisedHostnames();
            Map<Integer, Map<String, String>> advertisedPorts = listenerResult.getAdvertisedPorts();
            
            LOGGER.debug("{}: Generating ConfigMaps for cluster {}. " +
                       "Advertised listeners - Hostnames: {} brokers, Ports: {} brokers",
                       reconciliation, targetClusterId, advertisedHostnames.size(), advertisedPorts.size());
            
            List<ConfigMap> desiredConfigMaps = kafkaCluster.generatePerBrokerConfigurationConfigMaps(
                metricsAndLogging,
                advertisedHostnames,
                advertisedPorts,
                targetClusterId  // Only generate for nodes in this cluster
            );

            LOGGER.info("{}: Generated {} ConfigMaps for cluster {}",
                       reconciliation, desiredConfigMaps.size(), targetClusterId);

            // List existing ConfigMaps in this cluster and clean up ones that don't belong
            final ConfigMapOperator finalCmOp = cmOp;
            final String currentClusterId = targetClusterId;
            
            futures.add(
                cmOp.listAsync(namespace, kafkaCluster.getSelectorLabels())
                    .compose(existingConfigMaps -> {
                        List<Future<Void>> clusterFutures = new ArrayList<>();
                        
                        // Build list of desired ConfigMap names for this cluster
                        List<String> desiredNames = desiredConfigMaps.stream()
                            .map(cm -> cm.getMetadata().getName())
                            .collect(Collectors.toList());
                        
                        // Add the shared metrics/logging ConfigMap name (we don't want to delete it)
                        desiredNames.add(io.strimzi.api.kafka.model.kafka.KafkaResources.kafkaMetricsAndLogConfigMapName(
                            kafka.getMetadata().getName()));
                        
                        LOGGER.debug("{}: Cluster {} - Desired ConfigMaps: {}, Existing ConfigMaps: {}",
                            reconciliation, currentClusterId, desiredNames.size(), existingConfigMaps.size());
                        
                        // Delete ConfigMaps that exist but are not desired (belong to other clusters)
                        for (ConfigMap existingCm : existingConfigMaps) {
                            String cmName = existingCm.getMetadata().getName();
                            if (!desiredNames.contains(cmName)) {
                                LOGGER.info("{}: Deleting ConfigMap {} from cluster {} (belongs to different cluster)",
                                    reconciliation, cmName, currentClusterId);
                                clusterFutures.add(
                                    finalCmOp.deleteAsync(reconciliation, namespace, cmName, true)
                                        .mapEmpty()
                                );
                            }
                        }
                        
                        // Create/update desired ConfigMaps in this cluster
                        for (ConfigMap cm : desiredConfigMaps) {
                            // For remote clusters, preserve existing StrimziPodSet owner references
                            // They are set by updateOwnerReferencesForRemoteResources() in Step 12.5
                            // For central cluster, keep existing ownership (Kafka CR)
                            if (!isCentral) {
                                // Get existing ConfigMap to preserve StrimziPodSet owner references
                                ConfigMap existing = finalCmOp.get(namespace, cm.getMetadata().getName());
                                if (existing != null && existing.getMetadata().getOwnerReferences() != null) {
                                    // Preserve StrimziPodSet owner references from existing ConfigMap
                                    List<OwnerReference> existingRefs = existing.getMetadata().getOwnerReferences();
                                    List<OwnerReference> strimziPodSetRefs = existingRefs.stream()
                                        .filter(ref -> "StrimziPodSet".equals(ref.getKind()))
                                        .toList();
                                    
                                    if (!strimziPodSetRefs.isEmpty()) {
                                        LOGGER.debug("{}: Preserving {} StrimziPodSet owner reference(s) for ConfigMap {}",
                                            reconciliation, strimziPodSetRefs.size(), cm.getMetadata().getName());
                                        cm = new io.fabric8.kubernetes.api.model.ConfigMapBuilder(cm)
                                            .editMetadata()
                                                .withOwnerReferences(strimziPodSetRefs)
                                            .endMetadata()
                                            .build();
                                    }
                                }
                            }
                            
                            final ConfigMap finalCm = cm;
                            clusterFutures.add(
                                finalCmOp.reconcile(reconciliation, namespace, cm.getMetadata().getName(), finalCm)
                                    .compose(result -> {
                                        LOGGER.debug("{}: ConfigMap {} reconciled in cluster {}",
                                            reconciliation, finalCm.getMetadata().getName(), currentClusterId);
                                        return Future.succeededFuture();
                                    })
                            );
                        }
                        
                        return Future.join(clusterFutures).mapEmpty();
                    })
            );
        }

        return Future.join(futures)
            .compose(v -> {
                LOGGER.info("{}: All ConfigMaps reconciled successfully across {} clusters",
                           reconciliation, targetClusterIds.size());
                return Future.succeededFuture();
            });
    }

    /**
     * Scale down Kafka pods across all clusters if needed.
     * 
     * This method handles broker scale-down by:
     * 1. Getting the desired nodes from the KafkaCluster model
     * 2. For each target cluster, listing existing PodSets
     * 3. Filtering out pods that are no longer needed
     * 4. Updating PodSets with the reduced pod list
     * 
     * Controllers cannot be scaled down (only brokers can be scaled down).
     * Scale-up is handled automatically by reconcilePodSets.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when scale-down is finished
     */
    private Future<Void> scaleDown(Reconciliation reconciliation, Kafka kafka,
                                    List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        LOGGER.debug("{}: Checking if Kafka scale-down is needed", reconciliation);

        // Use the KafkaCluster model created once in reconcile()
        
        List<Future<Void>> futures = new ArrayList<>();

        for (String targetClusterId : targetClusterIds) {
            // Get desired nodes for this cluster
            Set<NodeRef> desiredNodesForCluster = kafkaCluster.nodes().stream()
                .filter(node -> targetClusterId.equals(node.clusterId()))
                .collect(Collectors.toSet());

            Set<String> desiredPodNames = desiredNodesForCluster.stream()
                .map(NodeRef::podName)
                .collect(Collectors.toSet());

            // Get the appropriate operator for this cluster
            StrimziPodSetOperator podSetOperator = getStrimziPodSetOperatorForCluster(targetClusterId);

            // Scale down pods in this cluster
            futures.add(
                podSetOperator.listAsync(reconciliation.namespace(), kafkaCluster.getSelectorLabels())
                    .compose(podSets -> {
                        if (podSets == null || podSets.isEmpty()) {
                            return Future.succeededFuture();
                        }

                        List<Future<Void>> scaleDownOps = new ArrayList<>();

                        for (io.strimzi.api.kafka.model.podset.StrimziPodSet podSet : podSets) {
                            // Filter pods to keep only desired ones
                            List<Map<String, Object>> desiredPods = podSet.getSpec().getPods().stream()
                                .filter(pod -> {
                                    io.fabric8.kubernetes.api.model.Pod p = io.strimzi.operator.cluster.model.PodSetUtils.mapToPod(pod);
                                    return desiredPodNames.contains(p.getMetadata().getName());
                                })
                                .collect(Collectors.toList());

                            // Check if scale-down is needed
                            if (podSet.getSpec().getPods().size() > desiredPods.size()) {
                                LOGGER.info("{}: Scaling down PodSet {} in cluster {} from {} to {} replicas",
                                    reconciliation, podSet.getMetadata().getName(), targetClusterId,
                                    podSet.getSpec().getPods().size(), desiredPods.size());

                                // Create scaled-down PodSet
                                io.strimzi.api.kafka.model.podset.StrimziPodSet scaledDownPodSet =
                                    new io.strimzi.api.kafka.model.podset.StrimziPodSetBuilder(podSet)
                                        .editSpec()
                                            .withPods(desiredPods)
                                        .endSpec()
                                        .build();

                                scaleDownOps.add(
                                    podSetOperator.reconcile(reconciliation, reconciliation.namespace(),
                                        podSet.getMetadata().getName(), scaledDownPodSet)
                                        .mapEmpty()
                                );
                            }
                        }

                        return Future.join(scaleDownOps).mapEmpty();
                    })
            );
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Reconcile StrimziPodSets across all clusters.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when all StrimziPodSets are reconciled
     */
    private Future<Map<String, Map<String, ReconcileResult<io.strimzi.api.kafka.model.podset.StrimziPodSet>>>> reconcilePodSets(
            Reconciliation reconciliation, Kafka kafka,
            List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        LOGGER.info("{}: Reconciling StrimziPodSets across {} clusters",
                reconciliation, targetClusterIds.size());

        // Use the KafkaCluster model created once in reconcile()

        // Log node pool information before generating PodSets
        for (KafkaNodePool pool : nodePools) {
            int specReplicas = pool.getSpec().getReplicas();
            int statusReplicas = pool.getStatus() != null && pool.getStatus().getNodeIds() != null ? 
                pool.getStatus().getNodeIds().size() : 0;
            LOGGER.info("{}: NodePool {} - spec.replicas={}, status.nodeIds.size={}", 
                reconciliation, pool.getMetadata().getName(), specReplicas, statusReplicas);
        }
        
        // Generate clustered PodSets using existing Strimzi logic
        // This creates PodSets per cluster with proper configuration
        Map<String, List<io.strimzi.api.kafka.model.podset.StrimziPodSet>> clusteredPodSets =
            this.kafkaCluster.generateClusteredPodSets(
                false,  // isOpenshift - get from PlatformFeaturesAvailability
                null,   // imagePullPolicy
                null,   // imagePullSecrets
                pod -> new HashMap<>(),  // podAnnotations function
                centralClusterId
            );
        
        // Log generated PodSets
        for (Map.Entry<String, List<io.strimzi.api.kafka.model.podset.StrimziPodSet>> entry : clusteredPodSets.entrySet()) {
            String clusterId = entry.getKey();
            for (io.strimzi.api.kafka.model.podset.StrimziPodSet podSet : entry.getValue()) {
                int podCount = podSet.getSpec() != null && podSet.getSpec().getPods() != null ? 
                    podSet.getSpec().getPods().size() : 0;
                LOGGER.info("{}: Generated PodSet {} for cluster {} with {} pods",
                    reconciliation, podSet.getMetadata().getName(), clusterId, podCount);
            }
        }

        Map<String, Map<String, ReconcileResult<io.strimzi.api.kafka.model.podset.StrimziPodSet>>> clusteredPodSetDiffs = new HashMap<>();
        List<Future<Void>> futures = new ArrayList<>();

        for (Map.Entry<String, List<io.strimzi.api.kafka.model.podset.StrimziPodSet>> entry : clusteredPodSets.entrySet()) {
            String clusterId = entry.getKey();
            List<io.strimzi.api.kafka.model.podset.StrimziPodSet> podSets = entry.getValue();

            boolean isCentral = clusterId.equals(centralClusterId);
            StrimziPodSetOperator podSetOp;

            if (isCentral) {
                podSetOp = centralSupplier.strimziPodSetOperator;
            } else {
                podSetOp = remoteOperatorSupplier.get(clusterId).strimziPodSetOperator;
                if (podSetOp == null) {
                    return Future.failedFuture("No StrimziPodSetOperator found for cluster: " + clusterId);
                }
            }

            // Reconcile PodSets with retry on conflict
            // Kubernetes may reject updates if the resource was modified concurrently
            futures.add(
                reconcilePodSetsWithRetry(
                    reconciliation,
                    podSetOp,
                    podSets,
                    this.kafkaCluster.getSelectorLabels(),
                    clusterId,
                    3  // max retries
                ).compose(result -> {
                    LOGGER.info("{}: {} PodSets reconciled in cluster {}",
                               reconciliation, result.size(), clusterId);
                    clusteredPodSetDiffs.put(clusterId, result);
                    return Future.succeededFuture();
                })
            );
        }

        return Future.join(futures).map(clusteredPodSetDiffs);
    }

    /**
     * Reconcile PodSets with retry logic to handle concurrent modification conflicts.
     * 
     * When multiple reconciliations happen in parallel (e.g., stretch reconciler + PodSet controller),
     * Kubernetes may reject updates with HTTP 409 Conflict. This method retries with exponential backoff.
     *
     * @param reconciliation Reconciliation context
     * @param podSetOp StrimziPodSet operator
     * @param podSets List of desired PodSets
     * @param selector Label selector
     * @param clusterId Cluster ID for logging
     * @param maxRetries Maximum number of retries
     * @return Future with reconciliation results
     */
    private Future<Map<String, ReconcileResult<io.strimzi.api.kafka.model.podset.StrimziPodSet>>> reconcilePodSetsWithRetry(
            Reconciliation reconciliation,
            StrimziPodSetOperator podSetOp,
            List<io.strimzi.api.kafka.model.podset.StrimziPodSet> podSets,
            Labels selector,
            String clusterId,
            int maxRetries) {
        
        return reconcilePodSetsAttempt(reconciliation, podSetOp, podSets, selector, clusterId, 0, maxRetries);
    }

    /**
     * Single attempt to reconcile PodSets with retry logic.
     */
    private Future<Map<String, ReconcileResult<io.strimzi.api.kafka.model.podset.StrimziPodSet>>> reconcilePodSetsAttempt(
            Reconciliation reconciliation,
            StrimziPodSetOperator podSetOp,
            List<io.strimzi.api.kafka.model.podset.StrimziPodSet> podSets,
            Labels selector,
            String clusterId,
            int attempt,
            int maxRetries) {
        
        return podSetOp.batchReconcile(
            reconciliation,
            reconciliation.namespace(),
            podSets,
            selector
        ).recover(error -> {
            // Check if this is a conflict error (HTTP 409)
            boolean isConflict = error instanceof io.fabric8.kubernetes.client.KubernetesClientException &&
                error.getMessage() != null &&
                (error.getMessage().contains("the object has been modified") ||
                 error.getMessage().contains("Conflict") ||
                 error.getMessage().contains("409"));
            
            if (isConflict && attempt < maxRetries) {
                // Exponential backoff: 100ms, 200ms, 400ms
                long delayMs = 100L * (1L << attempt);
                
                LOGGER.warn("{}: PodSet reconciliation conflict in cluster {} (attempt {}/{}), retrying in {}ms: {}",
                    reconciliation, clusterId, attempt + 1, maxRetries, delayMs, 
                    error.getMessage().substring(0, Math.min(200, error.getMessage().length())));
                
                // Wait and retry
                Promise<Map<String, ReconcileResult<io.strimzi.api.kafka.model.podset.StrimziPodSet>>> promise = Promise.promise();
                vertx.setTimer(delayMs, timerId -> {
                    reconcilePodSetsAttempt(reconciliation, podSetOp, podSets, selector, clusterId, attempt + 1, maxRetries)
                        .onComplete(promise);
                });
                return promise.future();
            } else {
                // Not a conflict or max retries reached - fail
                if (isConflict) {
                    LOGGER.error("{}: PodSet reconciliation failed in cluster {} after {} retries due to conflicts",
                        reconciliation, clusterId, maxRetries);
                }
                return Future.failedFuture(error);
            }
        });
    }

    /**
     * Wait for all pods to be ready across all clusters.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when all pods are ready
     */
    private Future<Void> waitForPodsReady(Reconciliation reconciliation, Kafka kafka,
                                         List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        LOGGER.info("{}: Waiting for pods to be ready across {} clusters",
                reconciliation, targetClusterIds.size());

        List<Future<Void>> futures = new ArrayList<>();
        String namespace = kafka.getMetadata().getNamespace();

        // Use the KafkaCluster model created once in reconcile()

        for (String clusterId : targetClusterIds) {
            boolean isCentral = clusterId.equals(centralClusterId);
            PodOperator podOp;

            if (isCentral) {
                podOp = centralSupplier.podOperations;
            } else {
                podOp = remoteOperatorSupplier.get(clusterId).podOperations;
                if (podOp == null) {
                    return Future.failedFuture("No PodOperator found for cluster: " + clusterId);
                }
            }

            // Get nodes for this cluster
            Set<NodeRef> nodes = kafkaCluster.nodesAtCluster(clusterId);

            for (NodeRef node : nodes) {
                String podName = node.podName();
                futures.add(
                    podOp.readiness(reconciliation, namespace, podName, 1000, 300000)
                        .compose(result -> {
                            LOGGER.info("{}: Pod {} is ready in cluster {}",
                                       reconciliation, podName, clusterId);
                            return Future.<Void>succeededFuture();
                        })
                        .recover(error -> {
                            LOGGER.warn("{}: Pod {} not ready in cluster {}: {}",
                                       reconciliation, podName, clusterId, error.getMessage());
                            // Don't fail the whole reconciliation if one pod isn't ready
                            return Future.<Void>succeededFuture();
                        })
                );
            }
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Reconcile networking resources across all clusters.
     *
     * For MCS provider: Creates ServiceExport for the broker service in each cluster.
     * For other providers: They handle their own networking (NodePort, LoadBalancer, etc.)
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when all networking resources are created
     */
    private Future<Void> reconcileNetworking(Reconciliation reconciliation, Kafka kafka,
                                            List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        LOGGER.info("{}: Reconciling networking resources with provider: {}",
                    reconciliation, networkingProvider.getProviderName());

        // Create networking resources (Services, ServiceExports, etc.) for each pod
        List<Future<List<HasMetadata>>> futures = new ArrayList<>();
        
        for (KafkaNodePool pool : nodePools) {
            String poolName = pool.getMetadata().getName();
            int replicas = pool.getSpec().getReplicas();
            
            // Determine which cluster this pool belongs to
            String clusterId = pool.getMetadata().getAnnotations() != null
                ? pool.getMetadata().getAnnotations().get("strimzi.io/stretch-cluster-alias")
                : centralClusterId;
            
            if (clusterId == null) {
                clusterId = centralClusterId;
            }
            
            LOGGER.debug("{}: Creating networking resources for pool {} ({} replicas) in cluster {}",
                        reconciliation, poolName, replicas, clusterId);
            
            // Create networking resources for each pod in the pool
            for (int i = 0; i < replicas; i++) {
                String podName = kafka.getMetadata().getName() + "-" + poolName + "-" + i;
                
                // Define ports for the pod (standard Kafka ports)
                Map<String, Integer> ports = new java.util.HashMap<>();
                ports.put("replication", 9091);
                ports.put("plain", 9092);
                ports.put("tls", 9093);
                ports.put("controlplane-9090", 9090);
                
                LOGGER.debug("{}: Calling plugin.createNetworkingResources for pod {} in cluster {}",
                            reconciliation, podName, clusterId);
                
                Future<List<HasMetadata>> future = networkingProvider.createNetworkingResources(
                    reconciliation,
                    kafka.getMetadata().getNamespace(),
                    podName,
                    clusterId,
                    ports
                );
                
                futures.add(future);
            }
        }
        
        // Wait for all networking resources to be created
        if (futures.isEmpty()) {
            LOGGER.info("{}: No networking resources to create", reconciliation);
            return Future.succeededFuture();
        }
        
        return Future.all(futures)
            .map(v -> {
                LOGGER.info("{}: All networking resources created successfully", reconciliation);
                return null;
            })
            .mapEmpty();
    }
    
    // Removed MCS-specific ServiceExport reconciliation - now handled by MCS plugin itself
    
    /**
     * Placeholder for future networking resource reconciliation if needed.
     */
    @SuppressWarnings("unused")
    private Future<Void> reconcileNetworkingLegacy(Reconciliation reconciliation, Kafka kafka,
                                            List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        // Legacy MCS-specific code removed
        // Providers now handle their own networking resources
        return Future.succeededFuture();
    }
    
    // Removed: Legacy MCS-specific code
    // MCS is now an external plugin and handles its own ServiceExport resources

    /**
     * Create KafkaCluster model for stretch cluster.
     *
     * This helper method creates a KafkaCluster model instance that can be used
     * to generate resources (ServiceAccount, Services, PVCs, etc.).
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return KafkaCluster model instance
     */
    private KafkaCluster createKafkaClusterModel(Reconciliation reconciliation, Kafka kafka,
                                                 List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        return createKafkaClusterModel(reconciliation, kafka, nodePools, targetClusterIds, null);
    }

    private KafkaCluster createKafkaClusterModel(Reconciliation reconciliation, Kafka kafka,
                                                 List<KafkaNodePool> nodePools, Set<String> targetClusterIds, String clusterId) {
        // Convert KafkaNodePool CRs to KafkaPool models
        // For now, use empty old storage and default version change
        
        // Store cluster ID for use in ConfigMap generation
        if (clusterId != null) {
            LOGGER.debug("{}: Creating KafkaCluster model with cluster ID: {}", reconciliation, clusterId);
        }
        Map<String, Storage> oldStorage = new HashMap<>();
        // Create version change with version from Kafka CR
        KafkaVersion defaultVersion = config.versions().defaultVersion();
        String metadataVersion = kafka.getSpec().getKafka().getMetadataVersion();
        KafkaVersionChange versionChange = new KafkaVersionChange(
            defaultVersion,
            defaultVersion,
            null,  // interBrokerProtocolVersion - will use default
            null,  // logMessageFormatVersion - will use default
            metadataVersion  // metadataVersion from Kafka CR
        );

        List<KafkaPool> pools = NodePoolUtils.createKafkaPools(
            reconciliation,
            kafka,
            nodePools,
            oldStorage,
            versionChange,
            sharedEnvironmentProvider
        );

        // Use the provided cluster ID if available, otherwise get/generate one
        // This is Critical: for scale operations, we MUST use the existing cluster ID
        // to prevent INCONSISTENT_CLUSTER_ID errors
        String kafkaClusterId;
        if (clusterId != null && !clusterId.isEmpty()) {
            kafkaClusterId = clusterId;
            LOGGER.debug("{}: Using provided cluster ID: {}", reconciliation, kafkaClusterId);
        } else {
            kafkaClusterId = NodePoolUtils.getOrGenerateKRaftClusterId(kafka, nodePools);
            LOGGER.debug("{}: Generated/retrieved cluster ID: {}", reconciliation, kafkaClusterId);
        }

        // Use the stretch-specific factory method
        return KafkaCluster.forStretchFromCrd(
            reconciliation,
            kafka,
            pools,
            config.versions(),
            versionChange,
            kafkaClusterId,
            sharedEnvironmentProvider,
            centralClusterId,
            new ArrayList<>(targetClusterIds),
            new ArrayList<>(targetClusterIds)  // clusterIds = targetClusterIds for now
        );
    }

    /**
     * Update KafkaNodePool CR statuses with cluster ID, node IDs, roles, etc.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when all node pool statuses are updated
     */
    private Future<Void> updateNodePoolStatuses(Reconciliation reconciliation, Kafka kafka, 
                                                 List<KafkaNodePool> nodePools, Set<String> targetClusterIds) {
        LOGGER.debug("{}: Updating KafkaNodePool statuses", reconciliation);

        // Get cluster ID
        String clusterId = io.strimzi.operator.cluster.model.nodepools.NodePoolUtils.getOrGenerateKRaftClusterId(kafka, nodePools);
        
        // Use the KafkaCluster model created once in reconcile()
        Map<String, KafkaNodePoolStatus> statuses = this.kafkaCluster.nodePoolStatuses();

        List<Future<KafkaNodePool>> statusUpdateFutures = new ArrayList<>();

        for (KafkaNodePool nodePool : nodePools) {
            KafkaNodePoolStatus newStatus = statuses.get(nodePool.getMetadata().getName());
            
            if (newStatus != null) {
                // Add observedGeneration
                KafkaNodePool updatedNodePool = 
                    new KafkaNodePoolBuilder(nodePool)
                        .withStatus(new KafkaNodePoolStatusBuilder(newStatus)
                                .withObservedGeneration(nodePool.getMetadata().getGeneration())
                                .build())
                        .build();

                StatusDiff diff = 
                    new StatusDiff(nodePool.getStatus(), updatedNodePool.getStatus());

                if (!diff.isEmpty()) {
                    LOGGER.info("{}: Updating status for KafkaNodePool {} - replicas: {}, nodeIds: {}, roles: {}",
                        reconciliation,
                        nodePool.getMetadata().getName(),
                        newStatus.getReplicas(),
                        newStatus.getNodeIds(),
                        newStatus.getRoles());
                    statusUpdateFutures.add(centralSupplier.kafkaNodePoolOperator.updateStatusAsync(reconciliation, updatedNodePool));
                } else {
                    LOGGER.debug("{}: Status unchanged for KafkaNodePool {}", reconciliation, nodePool.getMetadata().getName());
                }
            }
        }

        return Future.join(statusUpdateFutures).mapEmpty();
    }

    /**
     * Build Kafka status with cluster information.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @param listenerResult Listener reconciliation results
     * @param clusterId Current cluster ID
     * @return Future with KafkaStatus
     */
    private Future<KafkaStatus> buildStatus(Reconciliation reconciliation, Kafka kafka, 
                                            List<KafkaNodePool> nodePools,
                                            Set<String> targetClusterIds,
                                            ListenerReconciliationResult listenerResult,
                                            String clusterId) {
        LOGGER.debug("{}: Building Kafka status", reconciliation);

        KafkaStatus status = new KafkaStatus();

        // Set observedGeneration
        if (kafka.getMetadata().getGeneration() != null) {
            status.setObservedGeneration(kafka.getMetadata().getGeneration());
        }

        // Set operator version
        status.setOperatorLastSuccessfulVersion(getOperatorVersion());
        
        // Set cluster ID (critical for scale operations)
        if (clusterId != null) {
            status.setClusterId(clusterId);
            LOGGER.debug("{}: Set cluster ID in status: {}", reconciliation, clusterId);
        }

        // Set Kafka version and metadata state
        if (kafka.getSpec() != null && kafka.getSpec().getKafka() != null) {
            if (kafka.getSpec().getKafka().getVersion() != null) {
                status.setKafkaVersion(kafka.getSpec().getKafka().getVersion());
            }
            if (kafka.getSpec().getKafka().getMetadataVersion() != null) {
                status.setKafkaMetadataVersion(kafka.getSpec().getKafka().getMetadataVersion());
            }
            status.setKafkaMetadataState(KafkaMetadataState.KRaft);
        }

        // Generate listener statuses from listener reconciliation results
        if (listenerResult != null && kafka.getSpec() != null && kafka.getSpec().getKafka() != null) {
            List<ListenerStatus> listenerStatuses = 
                generateListenerStatuses(kafka, listenerResult);
            status.setListeners(listenerStatuses);
            LOGGER.debug("{}: Set {} listener statuses", reconciliation, listenerStatuses.size());
        }

        // Set list of node pools
        List<UsedNodePoolStatus> nodePoolStatuses = nodePools.stream()
            .map(pool -> new UsedNodePoolStatusBuilder()
                    .withName(pool.getMetadata().getName())
                    .build())
            .sorted(Comparator.comparing(UsedNodePoolStatus::getName))
            .toList();
        status.setKafkaNodePools(nodePoolStatuses);

        // Set registered node IDs using KafkaCluster model (uses desired state from NodeIdAssignor)
        // This ensures the status reflects the CURRENT state after this reconciliation
        List<Integer> registeredNodeIds = this.kafkaCluster.nodes().stream()
            .map(NodeRef::nodeId)
            .sorted()
            .toList();
        if (!registeredNodeIds.isEmpty()) {
            status.setRegisteredNodeIds(registeredNodeIds);
            LOGGER.info("{}: Set registered node IDs in Kafka status: {}", reconciliation, registeredNodeIds);
        }

        // Add a condition to indicate stretch cluster is active
        status.addCondition(new Condition() {{
                setType("StretchCluster");
                setStatus("True");
                setReason("StretchClusterActive");
                setMessage("Stretch cluster reconciliation completed with provider: "
                        + networkingProvider.getProviderName());
            }});

        return Future.succeededFuture(status);
    }

    /**
     * Get the operator version from the properties file.
     *
     * @return Operator version string
     */
    private String getOperatorVersion() {
        try {
            java.util.Properties properties = new java.util.Properties();
            java.io.InputStream propertiesFile = getClass().getResourceAsStream("/.properties");
            if (propertiesFile != null) {
                properties.load(propertiesFile);
                propertiesFile.close();
                return properties.getProperty("version", "unknown");
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to load operator version from properties file", e);
        }
        return "unknown";
    }

    /**
     * Generate listener statuses with addresses and bootstrap servers.
     * This creates the detailed listener status information shown in the Kafka CR status.
     *
     * @param kafka Kafka CR
     * @param listenerResult Listener reconciliation results
     * @return List of listener statuses
     */
    private List<ListenerStatus> generateListenerStatuses(
            Kafka kafka, ListenerReconciliationResult listenerResult) {
        
        List<ListenerStatus> listenerStatuses = new ArrayList<>();
        String namespace = kafka.getMetadata().getNamespace();
        String clusterName = kafka.getMetadata().getName();
        
        for (GenericKafkaListener listener : kafka.getSpec().getKafka().getListeners()) {
            ListenerStatusBuilder statusBuilder = 
                new ListenerStatusBuilder()
                    .withName(listener.getName());
            
            // Generate bootstrap address based on listener type
            String bootstrapHost = null;
            int bootstrapPort = listener.getPort();
            
            switch (listener.getType()) {
                case INTERNAL:
                    // Internal listeners use the bootstrap service
                    bootstrapHost = String.format("%s-kafka-bootstrap.%s.svc", clusterName, namespace);
                    break;
                    
                case ROUTE:
                    // Route listeners - use first available route hostname from bootstrap DNS names
                    // For stretch clusters, there may be multiple route addresses (one per cluster)
                    Map<String, Set<String>> bootstrapDnsNames = listenerResult.getAllBootstrapDnsNames();
                    for (Set<String> dnsSet : bootstrapDnsNames.values()) {
                        for (String dns : dnsSet) {
                            if (dns.contains(".apps.")) {
                                bootstrapHost = dns;
                                bootstrapPort = 443;
                                break;
                            }
                        }
                        if (bootstrapHost != null) break;
                    }
                    break;
                    
                case CLUSTER_IP:
                    // ClusterIP listeners use the bootstrap service
                    bootstrapHost = String.format("%s-kafka-%s-bootstrap.%s.svc", 
                        clusterName, listener.getName(), namespace);
                    break;
                    
                default:
                    // For other types, use a generic bootstrap service name
                    bootstrapHost = String.format("%s-kafka-%s-bootstrap.%s.svc", 
                        clusterName, listener.getName(), namespace);
                    break;
            }
            
            if (bootstrapHost != null) {
                statusBuilder.withAddresses(
                    new ListenerAddressBuilder()
                        .withHost(bootstrapHost)
                        .withPort(bootstrapPort)
                        .build()
                );
                // Note: bootstrapServers is automatically generated from addresses
            }
            
            listenerStatuses.add(statusBuilder.build());
        }
        
        return listenerStatuses;
    }

    /**
     * Delete stretch cluster resources.
     *
     * Called when Kafka CR is deleted. Cleans up ServiceExports
     * across all clusters (for MCS provider).
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @return Future that completes when cleanup is done
     */
    public Future<Void> delete(Reconciliation reconciliation, Kafka kafka, List<KafkaNodePool> nodePools) {
        LOGGER.info("{}: Deleting stretch cluster resources", reconciliation);

        List<Future<Void>> futures = new ArrayList<>();
        String namespace = kafka.getMetadata().getNamespace();
        Set<String> targetClusterIds = extractTargetClusterIds(nodePools);

        // Step 1: Delete StrimziPodSets in ALL clusters (including remote clusters)
        // This triggers Kubernetes GC to delete all owned resources (Pods, ConfigMaps, Secrets, etc.)
        for (String clusterId : targetClusterIds) {
            // Get all PodSet names for this cluster
            List<String> podSetNames = getPodSetNamesForCluster(kafka, nodePools, clusterId);
            
            for (String podSetName : podSetNames) {
                StrimziPodSetOperator podSetOp = getStrimziPodSetOperatorForCluster(clusterId);
                
                if (podSetOp != null) {
                    LOGGER.info("{}: Deleting StrimziPodSet {} in cluster {}",
                        reconciliation, podSetName, clusterId);
                    
                    futures.add(
                        podSetOp.reconcile(reconciliation, namespace, podSetName, null)  // null = delete
                            .compose(result -> {
                                LOGGER.info("{}: StrimziPodSet {} deleted in cluster {}",
                                    reconciliation, podSetName, clusterId);
                                return Future.<Void>succeededFuture();
                            })
                            .recover(error -> {
                                LOGGER.warn("{}: Failed to delete StrimziPodSet {} in cluster {}: {}",
                                    reconciliation, podSetName, clusterId, error.getMessage());
                                return Future.<Void>succeededFuture();  // Continue even if deletion fails
                            })
                    );
                } else {
                    LOGGER.warn("{}: No StrimziPodSetOperator found for cluster {}", reconciliation, clusterId);
                }
            }
        }

        // Step 2: Delete networking resources (handled by provider)
        // Providers handle their own resource cleanup via deleteNetworkingResources()
        LOGGER.debug("{}: Networking resource deletion handled by provider", reconciliation);

        return Future.succeededFuture();
    }

    /**
     * Initialize the TrustSet and PemAuthIdentity to be used by TLS clients during reconciliation
     *
     * @param reconciliation Reconciliation context
     * @return Future that completes when TLS identity is initialized
     */
    private Future<Void> initClientAuthenticationCertificates(Reconciliation reconciliation) {
        return io.strimzi.operator.cluster.operator.assembly.ReconcilerUtils.coTlsPemIdentity(
            reconciliation,
            centralSupplier.secretOperations
        ).onSuccess(identity -> {
            this.coTlsPemIdentity = identity;
            LOGGER.debug("{}: TLS identity initialized for admin client", reconciliation);
        }).mapEmpty();
    }

    /**
     * Perform rolling update of Kafka pods if needed (config changes, cert updates, etc.)
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @param podSetDiffs PodSet reconciliation results per cluster
     * @param listenerResult Listener reconciliation result with advertised hostnames/ports
     * @return Future that completes when rolling update is done
     */
    private Future<Void> rollingUpdate(
            Reconciliation reconciliation,
            Kafka kafka,
            List<KafkaNodePool> nodePools,
            Set<String> targetClusterIds,
            Map<String, Map<String, ReconcileResult<io.strimzi.api.kafka.model.podset.StrimziPodSet>>> podSetDiffs,
            ListenerReconciliationResult listenerResult) {
        
        LOGGER.info("{}: Checking if rolling update is needed across {} clusters",
                reconciliation, targetClusterIds.size());

        // Use the KafkaCluster model created once in reconcile()
        List<Future<Void>> futures = new ArrayList<>();

        for (String targetClusterId : targetClusterIds) {
            Map<String, ReconcileResult<io.strimzi.api.kafka.model.podset.StrimziPodSet>> clusterPodSetDiffs = 
                podSetDiffs.get(targetClusterId);
            
            if (clusterPodSetDiffs == null || clusterPodSetDiffs.isEmpty()) {
                LOGGER.debug("{}: No PodSets found for cluster {}, skipping rolling update",
                    reconciliation, targetClusterId);
                continue;
            }

            Set<NodeRef> nodesInCluster = kafkaCluster.nodes().stream()
                .filter(node -> targetClusterId.equals(node.clusterId()))
                .collect(Collectors.toSet());

            futures.add(maybeRollKafka(
                reconciliation,
                kafka,
                nodePools,
                nodesInCluster,
                targetClusterId,
                clusterPodSetDiffs,
                listenerResult
            ));
        }

        return Future.join(futures).mapEmpty();
    }

    /**
     * Roll Kafka pods in a specific cluster if needed
     */
    private Future<Void> maybeRollKafka(
            Reconciliation reconciliation,
            Kafka kafka,
            List<KafkaNodePool> nodePools,
            Set<NodeRef> nodes,
            String targetClusterId,
            Map<String, ReconcileResult<io.strimzi.api.kafka.model.podset.StrimziPodSet>> podSetDiffs,
            ListenerReconciliationResult listenerResult) {

        PodOperator podOp = getPodOperatorForCluster(targetClusterId);
        
        // Use the KafkaCluster model created once in reconcile()

        return new KafkaRoller(
                reconciliation,
                vertx,
                podOp,
                1_000,
                operationTimeoutMs,
                () -> new BackOff(250, 2, 10),
                nodes,
                coTlsPemIdentity,
                adminClientProvider,
                kafkaAgentClientProvider,
                brokerId -> this.kafkaCluster.generatePerBrokerConfiguration(
                    brokerId,
                    listenerResult.getAdvertisedHostnames(),
                    listenerResult.getAdvertisedPorts()
                ),
                "", // logging configuration - will be enhanced later
                this.kafkaCluster.getKafkaVersion(),
                true, // allowReconfiguration
                centralSupplier.restartEventsPublisher
            )
            .withStretch(targetClusterId)
            .rollingRestart(pod -> {
                // TODO: Implement proper restart reason detection
                // For now, return empty reasons - pods will only restart if explicitly needed
                return new RestartReasons();
            });
    }

    /**
     * Wait for newly added pods to be ready (not all pods, just new ones from scale-up)
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when new pods are ready
     */
    private Future<Void> waitForNewPodsReady(
            Reconciliation reconciliation,
            Kafka kafka,
            List<KafkaNodePool> nodePools,
            Set<String> targetClusterIds) {
        
        LOGGER.info("{}: Waiting for newly added pods to be ready", reconciliation);

        // Use the KafkaCluster model created once in reconcile()
        List<Future<Void>> futures = new ArrayList<>();

        for (String targetClusterId : targetClusterIds) {
            // Get only the newly added nodes for this cluster
            Set<NodeRef> addedNodes = kafkaCluster.addedNodes().stream()
                .filter(node -> targetClusterId.equals(node.clusterId()))
                .collect(Collectors.toSet());

            if (addedNodes.isEmpty()) {
                LOGGER.debug("{}: No new pods to wait for in cluster {}", reconciliation, targetClusterId);
                continue;
            }

            List<String> newPodNames = addedNodes.stream()
                .map(NodeRef::podName)
                .collect(Collectors.toList());

            LOGGER.info("{}: Waiting for {} new pods in cluster {}: {}",
                reconciliation, newPodNames.size(), targetClusterId, newPodNames);

            PodOperator podOp = getPodOperatorForCluster(targetClusterId);
            
            // Wait for each pod individually
            for (String podName : newPodNames) {
                futures.add(
                    podOp.readiness(reconciliation, reconciliation.namespace(), podName, 1_000, operationTimeoutMs)
                        .compose(v -> Future.<Void>succeededFuture())
                        .recover(error -> {
                            LOGGER.warn("{}: Pod {} readiness check failed in cluster {}: {}",
                                reconciliation, podName, targetClusterId, error.getMessage());
                            return Future.<Void>failedFuture(error);
                        })
                );
            }
        }

        if (futures.isEmpty()) {
            LOGGER.info("{}: No new pods to wait for across all clusters", reconciliation);
            return Future.succeededFuture();
        }

        return Future.join(futures)
            .compose(v -> {
                LOGGER.info("{}: All new pods are now ready and running", reconciliation);
                return Future.succeededFuture();
            })
            .mapEmpty();
    }

    /**
     * Get the Kafka cluster ID early (before creating ConfigMaps).
     * This retrieves the cluster ID from the existing cluster or from status.
     * The cluster ID MUST be available before creating ConfigMaps so new pods
     * start with the correct cluster ID and don't get INCONSISTENT_CLUSTER_ID errors.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @return Future with cluster ID (or null if cluster doesn't exist yet)
     */
    private Future<String> getClusterIdEarly(Reconciliation reconciliation, Kafka kafka) {
        LOGGER.debug("{}: Retrieving Kafka cluster ID early", reconciliation);

        // First, check if cluster ID is already in the status
        if (kafka.getStatus() != null && kafka.getStatus().getClusterId() != null) {
            String existingClusterId = kafka.getStatus().getClusterId();
            LOGGER.info("{}: Using existing cluster ID from status: {}", reconciliation, existingClusterId);
            return Future.succeededFuture(existingClusterId);
        }

        // If not in status, try to get it from the running cluster
        String bootstrapHostname = kafka.getMetadata().getName() + "-kafka-bootstrap." +
            kafka.getMetadata().getNamespace() + ".svc:" + KafkaCluster.REPLICATION_PORT;

        return vertx.executeBlocking(() -> {
            Admin kafkaAdmin = null;
            try {
                LOGGER.debug("{}: Creating AdminClient for cluster ID using {}", reconciliation, bootstrapHostname);
                kafkaAdmin = adminClientProvider.createAdminClient(
                    bootstrapHostname,
                    coTlsPemIdentity.pemTrustSet(),
                    coTlsPemIdentity.pemAuthIdentity()
                );
                String clusterId = kafkaAdmin.describeCluster().clusterId().get();
                LOGGER.info("{}: Retrieved Kafka cluster ID from running cluster: {}", reconciliation, clusterId);
                return clusterId;
            } catch (Exception e) {
                // This is expected for new clusters - they don't have a cluster ID yet
                LOGGER.debug("{}: Could not get cluster ID (expected for new clusters): {}", 
                    reconciliation, e.getMessage());
                return null;
            } finally {
                if (kafkaAdmin != null) {
                    kafkaAdmin.close();
                }
            }
        });
    }

    /**
     * Unregister removed nodes from KRaft cluster.
     * This follows the Strimzi pattern from KafkaReconciler.nodeUnregistration():
     * - Compare previousNodeIds (from Kafka status at START) vs currentNodeIds (from KafkaCluster model NOW)
     * - Unregister nodes that were in previous but not in current
     * - Use KafkaNodeUnregistration utility class for the actual unregistration
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @param clusterId Current cluster ID
     * @param previousNodeIds Node IDs from Kafka status at the start of reconciliation
     * @return Future that completes when unregistration is done
     */
    private Future<Void> nodeUnregistration(
            Reconciliation reconciliation,
            Kafka kafka,
            List<KafkaNodePool> nodePools,
            Set<String> targetClusterIds,
            String clusterId,
            List<Integer> previousNodeIds) {
        
        if (clusterId == null) {
            LOGGER.debug("{}: Cluster ID not available, skipping node unregistration", reconciliation);
            return Future.succeededFuture();
        }

        // Get current node IDs from KafkaCluster model (uses NodeIdAssignor.desired())
        // This is the correct set of nodes that SHOULD exist after this reconciliation
        List<Integer> currentNodeIds = this.kafkaCluster.nodes().stream()
            .map(NodeRef::nodeId)
            .sorted()
            .toList();
        
        LOGGER.info("{}: Current node IDs from KafkaCluster model: {}", reconciliation, currentNodeIds);
        LOGGER.info("{}: Previous node IDs from Kafka status: {}", reconciliation, previousNodeIds);

        // Check if we need to unregister any nodes
        // This follows Strimzi's pattern: if previous contains nodes not in current, unregister them
        if (previousNodeIds != null && !new HashSet<>(currentNodeIds).containsAll(previousNodeIds)) {
            // Calculate which nodes need to be unregistered
            List<Integer> nodeIdsToUnregister = new ArrayList<>(previousNodeIds);
            nodeIdsToUnregister.removeAll(currentNodeIds);

            LOGGER.info("{}: Kafka nodes {} were removed from the Kafka cluster and will be unregistered", 
                reconciliation, nodeIdsToUnregister);

            // Use Strimzi's KafkaNodeUnregistration utility class
            // This handles all the complexity of unregistration including error handling
            return KafkaNodeUnregistration.unregisterNodes(
                reconciliation,
                vertx,
                adminClientProvider,
                coTlsPemIdentity.pemTrustSet(),
                coTlsPemIdentity.pemAuthIdentity(),
                nodeIdsToUnregister
            ).onComplete(res -> {
                if (res.succeeded()) {
                    LOGGER.info("{}: Kafka nodes {} were successfully unregistered from the Kafka cluster", 
                        reconciliation, nodeIdsToUnregister);
                } else {
                    LOGGER.warn("{}: Failed to unregister Kafka nodes {} from the Kafka cluster", 
                        reconciliation, nodeIdsToUnregister);
                }
            }).mapEmpty();
        } else {
            // No nodes to unregister
            LOGGER.debug("{}: No nodes need to be unregistered", reconciliation);
            return Future.succeededFuture();
        }
    }

    /**
     * Add StrimziPodSet as owner for resources in remote clusters.
     * This enables automatic cleanup when StrimziPodSet is deleted.
     * 
     * IMPORTANT: Only applies to remote clusters. Central cluster resources
     * keep their existing ownership (Kafka CR).
     *
     * @param reconciliation Reconciliation context
     * @param resource The resource to add owner to
     * @param podSetName Name of the StrimziPodSet
     * @param clusterId Cluster ID
     * @param namespace Namespace
     * @return Resource with owner reference added
     */
    private <T extends HasMetadata> T addStrimziPodSetOwner(
            final Reconciliation reconciliation,
            final T resource,
            final String podSetName,
            final String clusterId,
            final String namespace) {

        String resourceName = resource.getMetadata().getName();
        LOGGER.info("{}: addStrimziPodSetOwner called for {} in cluster {}, PodSet: {}",
                reconciliation, resourceName, clusterId, podSetName);

        // Get the StrimziPodSet from the remote cluster to get its UID
        StrimziPodSetOperator podSetOp = getStrimziPodSetOperatorForCluster(clusterId);

        if (podSetOp == null) {
            LOGGER.warn("{}: No StrimziPodSetOperator for cluster {}, cannot set owner",
                    reconciliation, clusterId);
            return resource;
        }

        io.strimzi.api.kafka.model.podset.StrimziPodSet podSet = podSetOp.get(namespace, podSetName);

        if (podSet == null) {
            LOGGER.warn("{}: StrimziPodSet {} not found in cluster {}, cannot set owner",
                    reconciliation, podSetName, clusterId);
            return resource;
        }

        String podSetUid = podSet.getMetadata().getUid();
        LOGGER.info("{}: Found StrimziPodSet {} in cluster {} with UID: {}",
                reconciliation, podSetName, clusterId, podSetUid);

        // Create the StrimziPodSet owner reference
        OwnerReference podSetOwner = new OwnerReferenceBuilder()
                .withApiVersion("core.strimzi.io/v1beta2")
                .withKind("StrimziPodSet")
                .withName(podSetName)
                .withUid(podSetUid)
                .withController(true)
                .withBlockOwnerDeletion(false)
                .build();

        // Add or replace owner reference
        List<OwnerReference> existingOwners = resource.getMetadata().getOwnerReferences();
        if (existingOwners == null || existingOwners.isEmpty()) {
            // No existing owners, set new list
            LOGGER.debug("{}: {} has NO existing owners, setting StrimziPodSet {} as owner",
                    reconciliation, resourceName, podSetName);
            resource.getMetadata().setOwnerReferences(List.of(podSetOwner));
        } else {
            // Log existing owners
            String existingOwnerInfo = existingOwners.stream()
                .map(ref -> ref.getKind() + "/" + ref.getName())
                .collect(Collectors.joining(", "));
            LOGGER.debug("{}: {} has existing owners: [{}]",
                    reconciliation, resourceName, existingOwnerInfo);
            
            // Remove any existing StrimziPodSet owners and add the new one
            List<OwnerReference> filteredOwners = existingOwners.stream()
                .filter(ref -> !"StrimziPodSet".equals(ref.getKind()))
                .toList();
            
            if (filteredOwners.size() < existingOwners.size()) {
                LOGGER.debug("{}: Removed {} existing StrimziPodSet owner(s) from {}",
                        reconciliation, existingOwners.size() - filteredOwners.size(), resourceName);
            }
            
            List<OwnerReference> newOwners = new ArrayList<>(filteredOwners);
            newOwners.add(podSetOwner);
            resource.getMetadata().setOwnerReferences(newOwners);
            
            LOGGER.debug("{}: Set new owner references for {}: [{}]",
                    reconciliation, resourceName, 
                    newOwners.stream().map(ref -> ref.getKind() + "/" + ref.getName()).collect(Collectors.joining(", ")));
        }

        LOGGER.debug("{}: ✅ Successfully set StrimziPodSet {} (uid={}) as owner for {} in cluster {}",
                reconciliation, podSetName, podSetUid, resourceName, clusterId);

        return resource;
    }


    /**
     * Get the StrimziPodSet name for a given cluster.
     * For now, assumes one pool per cluster. May need to be enhanced
     * for multiple pools per cluster.
     *
     * @param kafka Kafka CR
     * @param nodePools List of node pools
     * @param clusterId Cluster ID
     * @return StrimziPodSet name
     */
    private String getPodSetNameForCluster(
            final Kafka kafka,
            final List<KafkaNodePool> nodePools,
            final String clusterId) {
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

    /**
     * Get all StrimziPodSet names for a Kafka cluster in a specific cluster.
     *
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param clusterId Cluster ID
     * @return List of StrimziPodSet names
     */
    private List<String> getPodSetNamesForCluster(
            final Kafka kafka,
            final List<KafkaNodePool> nodePools,
            final String clusterId) {

        List<String> podSetNames = new ArrayList<>();

        for (KafkaNodePool pool : nodePools) {
            String poolClusterId = pool.getMetadata()
                    .getAnnotations()
                    .get("strimzi.io/stretch-cluster-alias");

            if (clusterId.equals(poolClusterId)) {
                String podSetName = io.strimzi.operator.cluster.model.KafkaPool.componentName(kafka, pool);
                podSetNames.add(podSetName);
            }
        }

        return podSetNames;
    }

    /**
     * Update owner references on ConfigMaps, Secrets, and ServiceAccounts in remote clusters.
     * This runs AFTER PodSets are created, so the PodSet UIDs are available.
     * 
     * For remote clusters only - central cluster resources keep Kafka CR ownership.
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param targetClusterIds Set of target cluster IDs
     * @return Future that completes when all owner references are updated
     */
    private Future<Void> updateOwnerReferencesForRemoteResources(
            Reconciliation reconciliation,
            Kafka kafka,
            List<KafkaNodePool> nodePools,
            Set<String> targetClusterIds) {
        
        LOGGER.debug("{}: ========== UPDATING OWNER REFERENCES FOR REMOTE CLUSTERS ==========", reconciliation);
        LOGGER.debug("{}: Target clusters: {}", reconciliation, targetClusterIds);
        
        List<Future<Void>> futures = new ArrayList<>();
        String namespace = kafka.getMetadata().getNamespace();
        
        for (String clusterId : targetClusterIds) {
            // Handle central cluster separately - set Kafka CR as owner for ServiceExport
            if (clusterId.equals(centralClusterId)) {
                LOGGER.debug("{}: Processing central cluster {} (setting Kafka CR as ServiceExport owner)", 
                        reconciliation, clusterId);
                
                // Update ServiceExport in central cluster with Kafka CR as owner
                ServiceExportOperator centralServiceExportOp = remoteOperatorSupplier.centralServiceExportOperator;
                if (centralServiceExportOp != null) {
                    String serviceExportName = kafka.getMetadata().getName() + "-kafka-brokers";
                    ServiceExport serviceExport = centralServiceExportOp.get(namespace, serviceExportName);
                    
                    if (serviceExport != null) {
                        // Set Kafka CR as owner
                        io.fabric8.kubernetes.api.model.OwnerReference kafkaOwner = 
                            new io.fabric8.kubernetes.api.model.OwnerReferenceBuilder()
                                .withApiVersion(kafka.getApiVersion())
                                .withKind(kafka.getKind())
                                .withName(kafka.getMetadata().getName())
                                .withUid(kafka.getMetadata().getUid())
                                .withController(true)
                                .withBlockOwnerDeletion(false)
                                .build();
                        
                        ServiceExport updatedServiceExport = new io.strimzi.api.kafka.model.serviceexport.ServiceExportBuilder(serviceExport)
                            .editMetadata()
                                .withOwnerReferences(kafkaOwner)
                            .endMetadata()
                            .build();
                        
                        futures.add(
                            centralServiceExportOp.reconcile(reconciliation, namespace, serviceExportName, updatedServiceExport)
                                .compose(result -> {
                                    LOGGER.debug("{}: Set Kafka CR as owner for ServiceExport {} in central cluster",
                                        reconciliation, serviceExportName);
                                    return Future.<Void>succeededFuture();
                                })
                                .recover(error -> {
                                    LOGGER.error("{}: Failed to update ServiceExport owner in central cluster: {}",
                                        reconciliation, error.getMessage(), error);
                                    return Future.<Void>succeededFuture();
                                })
                        );
                    } else {
                        LOGGER.warn("{}: ServiceExport {} not found in central cluster",
                            reconciliation, serviceExportName);
                    }
                }
                continue;
            }
            
            LOGGER.debug("{}: Processing remote cluster: {}", reconciliation, clusterId);
            
            // Get all PodSet names for this cluster
            List<String> podSetNames = getPodSetNamesForCluster(kafka, nodePools, clusterId);
            if (podSetNames.isEmpty()) {
                LOGGER.warn("{}: No PodSets found for cluster {}, skipping owner reference update",
                    reconciliation, clusterId);
                continue;
            }
            
            String kafkaName = kafka.getMetadata().getName();
            
            // Update ConfigMaps - match each ConfigMap to its corresponding PodSet
            ConfigMapOperator cmOp = remoteOperatorSupplier.get(clusterId).configMapOperations;
            if (cmOp != null) {
                futures.add(updateResourceOwnerReferencesWithMatching(reconciliation, cmOp, namespace, podSetNames, clusterId, "ConfigMap", kafkaName));
            } else {
                LOGGER.error("{}: No ConfigMapOperator found for cluster {}!", reconciliation, clusterId);
            }
            
            // Update Secrets - match each Secret to its corresponding PodSet
            io.strimzi.operator.cluster.operator.resource.kubernetes.SecretOperator secretOp = 
                remoteOperatorSupplier.get(clusterId).secretOperations;
            if (secretOp != null) {
                futures.add(updateResourceOwnerReferencesWithMatching(reconciliation, secretOp, namespace, podSetNames, clusterId, "Secret", kafkaName));
            }
            
            // Update ServiceAccount - use any broker PodSet as owner (ServiceAccount is shared)
            ServiceAccountOperator saOp = remoteOperatorSupplier.get(clusterId).serviceAccountOperations;
            if (saOp != null) {
                // Prefer broker PodSet for ServiceAccount ownership
                String podSetName = podSetNames.stream()
                    .filter(name -> name.contains("-broker"))
                    .findFirst()
                    .orElse(podSetNames.get(0));
                String saName = kafka.getMetadata().getName() + "-kafka";
                futures.add(updateSingleResourceOwnerReference(reconciliation, saOp, namespace, saName, podSetName, clusterId, "ServiceAccount"));
            }
            
            // Update ServiceExport - use any broker PodSet as owner (ServiceExport is shared across all brokers)
            ServiceExportOperator serviceExportOp = remoteOperatorSupplier.serviceExportOperators.get(clusterId);
            if (serviceExportOp != null) {
                // Prefer broker PodSet for ServiceExport ownership
                String podSetName = podSetNames.stream()
                    .filter(name -> name.contains("-broker"))
                    .findFirst()
                    .orElse(podSetNames.get(0));
                String serviceExportName = kafka.getMetadata().getName() + "-kafka-brokers";
                futures.add(updateSingleResourceOwnerReference(reconciliation, serviceExportOp, namespace, serviceExportName, podSetName, clusterId, "ServiceExport"));
            }
        }
        
        return Future.join(futures).mapEmpty();
    }

    /**
     * Update owner references for all resources, matching each resource to its corresponding PodSet.
     * For example, my-cluster-stretch1-broker-2 ConfigMap should be owned by my-cluster-stretch1-broker PodSet.
     */
    private <T extends HasMetadata> Future<Void> updateResourceOwnerReferencesWithMatching(
            Reconciliation reconciliation,
            io.strimzi.operator.cluster.operator.resource.kubernetes.AbstractNamespacedResourceOperator<?, T, ?, ?> operator,
            String namespace,
            List<String> podSetNames,
            String clusterId,
            String resourceType,
            String kafkaName) {
        
        // Use Kafka cluster selector labels to filter resources
        Labels selectorLabels = Labels.forStrimziCluster(kafkaName);
        
        return operator.listAsync(namespace, selectorLabels)
            .compose(resources -> {
                List<Future<Void>> updateFutures = new ArrayList<>();
                
                LOGGER.debug("{}: Found {} {}s for Kafka cluster {} in cluster {}",
                    reconciliation, resources.size(), resourceType, kafkaName, clusterId);
                
                for (T resource : resources) {
                    String resourceName = resource.getMetadata().getName();
                    
                    // Check if resource already has owner reference
                    List<io.fabric8.kubernetes.api.model.OwnerReference> ownerRefs = 
                        resource.getMetadata().getOwnerReferences();
                    
                    // Log current owner references for debugging
                    if (ownerRefs == null || ownerRefs.isEmpty()) {
                        LOGGER.debug("{}: {} {} in cluster {} has NO owner references",
                            reconciliation, resourceType, resourceName, clusterId);
                    } else {
                        String ownerInfo = ownerRefs.stream()
                            .map(ref -> ref.getKind() + "/" + ref.getName() + " (uid=" + ref.getUid() + ")")
                            .collect(Collectors.joining(", "));
                        LOGGER.debug("{}: {} {} in cluster {} has owner references: [{}]",
                            reconciliation, resourceType, resourceName, clusterId, ownerInfo);
                    }
                    
                    boolean hasStrimziPodSetOwner = ownerRefs != null && ownerRefs.stream()
                        .anyMatch(ref -> "StrimziPodSet".equals(ref.getKind()));
                    
                    if (!hasStrimziPodSetOwner) {
                        // Find the matching PodSet for this resource
                        // Match pattern: my-cluster-stretch1-broker-2 -> my-cluster-stretch1-broker
                        String matchingPodSetName = findMatchingPodSet(resourceName, podSetNames);
                        
                        if (matchingPodSetName != null) {
                            LOGGER.debug("{}: Adding StrimziPodSet {} owner reference to {} {} in cluster {}",
                                reconciliation, matchingPodSetName, resourceType, resourceName, clusterId);
                            
                            T updatedResource = addStrimziPodSetOwner(reconciliation, resource, matchingPodSetName, clusterId, namespace);
                            
                            // Log the owner references after adding
                            List<io.fabric8.kubernetes.api.model.OwnerReference> newOwnerRefs = 
                                updatedResource.getMetadata().getOwnerReferences();
                            String newOwnerInfo = newOwnerRefs.stream()
                                .map(ref -> ref.getKind() + "/" + ref.getName() + " (uid=" + ref.getUid() + ")")
                                .collect(Collectors.joining(", "));
                            LOGGER.debug("{}: {} {} will be updated with owner references: [{}]",
                                reconciliation, resourceType, resourceName, newOwnerInfo);
                            
                            updateFutures.add(
                                operator.reconcile(reconciliation, namespace, resourceName, updatedResource)
                                    .compose(result -> {
                                        LOGGER.debug("{}: Updated owner reference for {} {} in cluster {}",
                                            reconciliation, resourceType, resourceName, clusterId);
                                        return Future.<Void>succeededFuture();
                                    })
                                    .recover(error -> {
                                        LOGGER.error("{}: Failed to update owner reference for {} {} in cluster {}: {}",
                                            reconciliation, resourceType, resourceName, clusterId, error.getMessage(), error);
                                        return Future.<Void>succeededFuture();
                                    })
                            );
                        } else {
                            LOGGER.warn("{}: Could not find matching PodSet for {} {} in cluster {}. Available PodSets: {}",
                                reconciliation, resourceType, resourceName, clusterId, podSetNames);
                        }
                    } else {
                        LOGGER.debug("{}: Skipping {} {} - already has StrimziPodSet owner",
                            reconciliation, resourceType, resourceName);
                    }
                }
                
                return Future.join(updateFutures).mapEmpty();
            });
    }

    /**
     * Find the matching PodSet name for a resource.
     * Example: my-cluster-stretch1-broker-2 -> my-cluster-stretch1-broker
     *          my-cluster-stretch1-controller-3 -> my-cluster-stretch1-controller
     *          my-cluster-kafka-config -> my-cluster-stretch1-broker (shared resource, prefer broker)
     */
    private String findMatchingPodSet(String resourceName, List<String> podSetNames) {
        // Try to find a PodSet whose name is a prefix of the resource name
        for (String podSetName : podSetNames) {
            if (resourceName.startsWith(podSetName)) {
                return podSetName;
            }
        }
        
        // If no exact match, try to match by role (broker/controller)
        if (resourceName.contains("-broker-")) {
            return podSetNames.stream()
                .filter(name -> name.contains("-broker"))
                .findFirst()
                .orElse(null);
        } else if (resourceName.contains("-controller-")) {
            return podSetNames.stream()
                .filter(name -> name.contains("-controller"))
                .findFirst()
                .orElse(null);
        }
        
        // Fallback for shared resources (like my-cluster-kafka-config): prefer broker PodSet
        return podSetNames.stream()
            .filter(name -> name.contains("-broker"))
            .findFirst()
            .orElse(podSetNames.isEmpty() ? null : podSetNames.get(0));
    }

    /**
     * Update owner references for all resources of a given type in a cluster.
     */
    private <T extends HasMetadata> Future<Void> updateResourceOwnerReferences(
            Reconciliation reconciliation,
            io.strimzi.operator.cluster.operator.resource.kubernetes.AbstractNamespacedResourceOperator<?, T, ?, ?> operator,
            String namespace,
            String podSetName,
            String clusterId,
            String resourceType,
            String kafkaName) {
        
        // Use Kafka cluster selector labels to filter resources
        Labels selectorLabels = Labels.forStrimziCluster(kafkaName);
        
        return operator.listAsync(namespace, selectorLabels)
            .compose(resources -> {
                List<Future<Void>> updateFutures = new ArrayList<>();
                
                LOGGER.debug("{}: Found {} {}s for Kafka cluster {} in cluster {}",
                    reconciliation, resources.size(), resourceType, kafkaName, clusterId);
                
                for (T resource : resources) {
                    String resourceName = resource.getMetadata().getName();
                    
                    // Check if resource already has owner reference
                    List<io.fabric8.kubernetes.api.model.OwnerReference> ownerRefs = 
                        resource.getMetadata().getOwnerReferences();
                    boolean hasStrimziPodSetOwner = ownerRefs != null && ownerRefs.stream()
                        .anyMatch(ref -> "StrimziPodSet".equals(ref.getKind()));
                    
                    if (!hasStrimziPodSetOwner) {
                        LOGGER.info("{}: Adding StrimziPodSet owner reference to {} {} in cluster {}",
                            reconciliation, resourceType, resourceName, clusterId);
                        
                        T updatedResource = addStrimziPodSetOwner(reconciliation, resource, podSetName, clusterId, namespace);
                        updateFutures.add(
                            operator.reconcile(reconciliation, namespace, resourceName, updatedResource)
                                .compose(result -> {
                                    LOGGER.debug("{}: Updated owner reference for {} {} in cluster {}",
                                        reconciliation, resourceType, resourceName, clusterId);
                                    return Future.succeededFuture();
                                })
                        );
                    } else {
                        LOGGER.debug("{}: {} {} already has StrimziPodSet owner reference in cluster {}",
                            reconciliation, resourceType, resourceName, clusterId);
                    }
                }
                
                return Future.join(updateFutures).mapEmpty();
            });
    }

    /**
     * Update owner reference for a single resource.
     */
    private <T extends HasMetadata> Future<Void> updateSingleResourceOwnerReference(
            Reconciliation reconciliation,
            io.strimzi.operator.cluster.operator.resource.kubernetes.AbstractNamespacedResourceOperator<?, T, ?, ?> operator,
            String namespace,
            String resourceName,
            String podSetName,
            String clusterId,
            String resourceType) {
        
        return operator.getAsync(namespace, resourceName)
            .compose(resource -> {
                if (resource == null) {
                    LOGGER.debug("{}: {} {} not found in cluster {}, skipping owner reference update",
                        reconciliation, resourceType, resourceName, clusterId);
                    return Future.succeededFuture();
                }
                
                T updatedResource = addStrimziPodSetOwner(reconciliation, resource, podSetName, clusterId, namespace);
                return operator.reconcile(reconciliation, namespace, resourceName, updatedResource)
                    .compose(result -> {
                        LOGGER.debug("{}: Updated owner reference for {} {} in cluster {}",
                            reconciliation, resourceType, resourceName, clusterId);
                        return Future.succeededFuture();
                    });
            });
    }

    /**
     * Delete stretch cluster resources when Kafka CR is deleted.
     *
     * This method deletes StrimziPodSets in remote clusters, which triggers
     * Kubernetes garbage collection to delete all owned resources (Services,
     * ConfigMaps, Secrets, etc.).
     *
     * Central cluster resources are NOT deleted here - they are handled by
     * Kubernetes GC when the Kafka CR is deleted (via existing OwnerReferences).
     *
     * @param reconciliation Reconciliation context
     * @param kafka Kafka CR being deleted
     * @param nodePools List of KafkaNodePool CRs
     * @return Future that completes when deletion is done
     */
    public Future<Void> deleteStretchClusterResources(
            final Reconciliation reconciliation,
            final Kafka kafka,
            final List<KafkaNodePool> nodePools) {

        LOGGER.info("{}: Deleting stretch cluster resources in remote clusters", reconciliation);

        String namespace = kafka.getMetadata().getNamespace();
        Set<String> targetClusterIds = extractTargetClusterIds(nodePools);

        List<Future<Void>> deletionFutures = new ArrayList<>();

        // Delete StrimziPodSets in each remote cluster
        for (String clusterId : targetClusterIds) {
            // Skip central cluster - handled by K8s GC when Kafka CR is deleted
            if (clusterId.equals(centralClusterId)) {
                LOGGER.debug("{}: Skipping central cluster {} (handled by K8s GC)",
                        reconciliation, clusterId);
                continue;
            }

            StrimziPodSetOperator podSetOp = getStrimziPodSetOperatorForCluster(clusterId);

            if (podSetOp == null) {
                LOGGER.warn("{}: No StrimziPodSetOperator for cluster {}, skipping deletion",
                        reconciliation, clusterId);
                continue;
            }

            // Get all StrimziPodSets for this Kafka cluster in the remote cluster
            List<String> podSetNames = getPodSetNamesForCluster(kafka, nodePools, clusterId);

            for (String podSetName : podSetNames) {
                LOGGER.info("{}: Deleting StrimziPodSet {} in remote cluster {}",
                        reconciliation, podSetName, clusterId);

                deletionFutures.add(
                        podSetOp.deleteAsync(reconciliation, namespace, podSetName, true)
                                .compose(v -> {
                                    LOGGER.info("{}: Successfully deleted StrimziPodSet {} in cluster {}",
                                            reconciliation, podSetName, clusterId);
                                    return Future.<Void>succeededFuture();
                                })
                                .recover(error -> {
                                    LOGGER.error("{}: Failed to delete StrimziPodSet {} in cluster {}: {}",
                                            reconciliation, podSetName, clusterId, error.getMessage());

                                    // Log manual cleanup instructions
                                    LOGGER.error("{}: MANUAL CLEANUP REQUIRED: " +
                                                    "Delete StrimziPodSet {}/{} in cluster {} manually",
                                            reconciliation, namespace, podSetName, clusterId);

                                    // Continue with other deletions even if one fails
                                    return Future.<Void>succeededFuture();
                                })
                );
            }
        }

        if (deletionFutures.isEmpty()) {
            LOGGER.info("{}: No remote cluster resources to delete", reconciliation);
            return Future.succeededFuture();
        }

        return Future.join(deletionFutures)
                .compose(v -> {
                    LOGGER.info("{}: Stretch cluster resource deletion completed", reconciliation);
                    return Future.succeededFuture();
                })
                .mapEmpty();
    }
}
