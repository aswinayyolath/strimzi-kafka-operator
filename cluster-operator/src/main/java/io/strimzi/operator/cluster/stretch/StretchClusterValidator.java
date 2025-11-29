/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.nodepool.KafkaNodePool;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Validator for stretch cluster configuration and runtime connectivity.
 * Provides comprehensive validation with user-friendly error messages.
 */
public class StretchClusterValidator {
    /** Logger instance. */
    private static final ReconciliationLogger LOGGER =
        ReconciliationLogger.create(StretchClusterValidator.class);

    /** Vert.x instance. */
    private final Vertx vertx;
    /** Central cluster ID. */
    private final String centralClusterId;
    /** Set of remote cluster IDs. */
    private final Set<String> remoteClusterIds;

    /**
     * Validation result containing success/failure status and error
     * details.
     */
    public static final class ValidationResult {
        /** Validation status. */
        private final boolean valid;
        /** Error message. */
        private final String errorMessage;
        /** Error code. */
        private final String errorCode;

        private ValidationResult(final boolean validParam,
                                 final String errorMessageParam,
                                 final String errorCodeParam) {
            this.valid = validParam;
            this.errorMessage = errorMessageParam;
            this.errorCode = errorCodeParam;
        }

        /**
         * Create a successful validation result.
         *
         * @return ValidationResult indicating success
         */
        public static ValidationResult success() {
            return new ValidationResult(true, null, null);
        }

        /**
         * Create a failed validation result with error code and message.
         *
         * @param errorCode Error code identifying the type of validation
         *                  failure
         * @param errorMessage Detailed error message describing the
         *                     validation failure
         * @return ValidationResult indicating failure with error details
         */
        public static ValidationResult error(final String errorCode,
                                             final String errorMessage) {
            return new ValidationResult(false, errorMessage, errorCode);
        }

        /**
         * Check if validation passed.
         *
         * @return true if validation passed, false otherwise
         */
        public boolean isValid() {
            return valid;
        }

        /**
         * Get the error message.
         *
         * @return Error message, or null if validation passed
         */
        public String getErrorMessage() {
            return errorMessage;
        }

        /**
         * Get the error code.
         *
         * @return Error code, or null if validation passed
         */
        public String getErrorCode() {
            return errorCode;
        }
    }

    /**
     * Constructor.
     *
     * @param vertxInstance Vert.x instance
     * @param centralClusterIdParam Central cluster ID
     * @param remoteClusterIdsParam Set of remote cluster IDs
     */
    public StretchClusterValidator(final Vertx vertxInstance,
                                    final String centralClusterIdParam,
                                    final Set<String> remoteClusterIdsParam) {
        this.vertx = vertxInstance;
        this.centralClusterId = centralClusterIdParam;
        this.remoteClusterIds = remoteClusterIdsParam;
    }

    /**
     * Validate that operator is properly configured for stretch clusters.
     * Consolidates validation from KafkaClusterCreator.
     *
     * @param isStretchMode Whether stretch mode is enabled on Kafka CR
     * @param isOperatorConfigured Whether operator has stretch cluster env
     *                             vars configured
     * @param kafkaName Name of the Kafka CR (for error messages)
     * @return ValidationResult indicating success or failure with error
     *         details
     */
    public ValidationResult validateOperatorConfiguration(
            final boolean isStretchMode,
            final boolean isOperatorConfigured,
            final String kafkaName) {
        if (isStretchMode && !isOperatorConfigured) {
            return ValidationResult.error(
                "OperatorNotConfigured",
                String.format("Stretch mode is enabled for Kafka cluster "
                    + "'%s', but required environment variables are not "
                    + "properly configured. Required: "
                    + "STRIMZI_REMOTE_KUBE_CONFIG, "
                    + "STRIMZI_CENTRAL_CLUSTER_ID. "
                    + "STRIMZI_STRETCH_PLUGIN_CLASS_NAME"
                    + "STRIMZI_STRETCH_PLUGIN_CLASS_PATH",
                    kafkaName)
            );
        }
        return ValidationResult.success();
    }

    /**
     * Validate Kafka CR and KafkaNodePool configuration.
     * Consolidates validation from StretchClusterReconciler and
     * KafkaClusterCreator.
     *
     * @param kafka Kafka CR
     * @param nodePools List of KafkaNodePool CRs
     * @param isStretchMode Whether stretch mode is enabled (from Kafka CR
     *                      annotation)
     * @return ValidationResult indicating success or failure with error
     *         details
     */
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public ValidationResult validateKafkaConfiguration(
            final Kafka kafka,
            final List<KafkaNodePool> nodePools,
            final boolean isStretchMode) {
        String kafkaName = kafka.getMetadata().getName();

        // Check if Kafka CR has stretch annotation
        // The correct annotation is: strimzi.io/enable-stretch-cluster: "true"
        boolean stretchEnabled = kafka.getMetadata().getAnnotations()
            != null
            && "true".equals(kafka.getMetadata().getAnnotations()
                .get("strimzi.io/enable-stretch-cluster"));

        // Validate each KafkaNodePool and collect info
        Set<String> usedClusterIds = new HashSet<>();
        Set<String> poolsWithoutTarget = new HashSet<>();
        Set<String> poolsWithTarget = new HashSet<>();
        Set<String> invalidClusterIds = new HashSet<>();

        for (KafkaNodePool pool : nodePools) {
            String poolName = pool.getMetadata().getName();
            // The correct annotation is: strimzi.io/stretch-cluster-alias
            String targetCluster = pool.getMetadata().getAnnotations()
                != null
                ? pool.getMetadata().getAnnotations()
                    .get("strimzi.io/stretch-cluster-alias")
                : null;

            // Track pools with and without target cluster annotation
            if (targetCluster == null || targetCluster.isEmpty()) {
                poolsWithoutTarget.add(poolName);
            } else {
                poolsWithTarget.add(poolName);

                // Check if target cluster ID is valid
                if (!targetCluster.equals(centralClusterId)
                    && !remoteClusterIds.contains(targetCluster)) {
                    invalidClusterIds.add(targetCluster);
                }

                usedClusterIds.add(targetCluster);
            }
        }

        // PROBLEM 1 FIX: Check if KNPs have stretch annotations but
        // operator is not configured for stretch mode
        if (!isStretchMode && !poolsWithTarget.isEmpty()) {
            return ValidationResult.error(
                "OperatorNotConfigured",
                String.format("Configuration error: The following node "
                    + "pools have 'strimzi.io/stretch-cluster-alias' "
                    + "annotations but the operator is not configured for "
                    + "stretch clusters: %s. Please ensure "
                    + "STRIMZI_REMOTE_KUBE_CONFIG and "
                    + "STRIMZI_CENTRAL_CLUSTER_ID environment variables are "
                    + "set on the operator, or remove the stretch annotations "
                    + "from node pools.",
                    String.join(", ", poolsWithTarget))
            );
        }

        // PROBLEM 1 FIX: Check if KNPs have stretch annotations but Kafka
        // CR doesn't have stretch annotation
        if (isStretchMode && !stretchEnabled && !poolsWithTarget.isEmpty()) {
            return ValidationResult.error(
                "MissingStretchAnnotation",
                String.format("Kafka CR '%s' is missing required "
                    + "annotation 'strimzi.io/enable-stretch-cluster: true' "
                    + "but the following node pools have stretch cluster "
                    + "annotations: %s. Either add the annotation to the "
                    + "Kafka CR or remove 'strimzi.io/stretch-cluster-alias' "
                    + "annotations from all node pools.",
                    kafkaName, String.join(", ", poolsWithTarget))
            );
        }

        // Check if stretch mode is enabled but Kafka CR doesn't have
        // annotation
        if (isStretchMode && !stretchEnabled && poolsWithTarget.isEmpty()) {
            return ValidationResult.error(
                "MissingStretchAnnotation",
                String.format("Kafka CR '%s' is missing required "
                    + "annotation 'strimzi.io/enable-stretch-cluster: true'. "
                    + "Stretch cluster mode must be explicitly enabled on "
                    + "the Kafka CR.", kafkaName)
            );
        }

        // If stretch mode is enabled, ALL pools must have target-cluster
        // annotation (including central cluster pools)
        if (isStretchMode && stretchEnabled && !poolsWithoutTarget.isEmpty()) {
            return ValidationResult.error(
                "MissingTargetCluster",
                String.format("Stretch cluster validation failed: The "
                    + "following node pools are missing required annotation "
                    + "'strimzi.io/stretch-cluster-alias': %s. All node pools "
                    + "must specify a target cluster when stretch mode is "
                    + "enabled.",
                    String.join(", ", poolsWithoutTarget))
            );
        }

        // Report invalid cluster IDs
        if (!invalidClusterIds.isEmpty()) {
            Set<String> validClusterIds = new HashSet<>();
            validClusterIds.add(centralClusterId);
            validClusterIds.addAll(remoteClusterIds);

            return ValidationResult.error(
                "InvalidTargetCluster",
                String.format("Stretch cluster validation failed: The "
                    + "following target cluster IDs are not configured: %s. "
                    + "Valid cluster IDs are: %s. Please check "
                    + "STRIMZI_CENTRAL_CLUSTER_ID and "
                    + "STRIMZI_REMOTE_KUBE_CONFIG.",
                    String.join(", ", invalidClusterIds),
                    String.join(", ", validClusterIds))
            );
        }

        return ValidationResult.success();
    }

    /**
     * Validate runtime connectivity to remote clusters.
     *
     * @param clusterClients Map of cluster ID to KubernetesClient
     * @return Future with ValidationResult
     */
    public Future<ValidationResult> validateRuntimeConnectivity(
            final Map<String, KubernetesClient> clusterClients) {

        List<Future<ValidationResult>> futures = new ArrayList<>();

        for (Map.Entry<String, KubernetesClient> entry
                : clusterClients.entrySet()) {
            String clusterId = entry.getKey();
            KubernetesClient client = entry.getValue();

            futures.add(validateClusterConnectivity(
                clusterId, client));
        }

        return Future.join(futures)
            .map(compositeFuture -> {
                // Return first error found, or success if all passed
                for (int i = 0; i < compositeFuture.size(); i++) {
                    ValidationResult result = compositeFuture.resultAt(i);
                    if (!result.isValid()) {
                        return result;
                    }
                }
                return ValidationResult.success();
            });
    }

    /**
     * Validates network latency between central and remote clusters.
     *
     * Uses the StretchNetworkingProvider to perform a realistic latency test
     * that simulates the actual path Kafka pods will use for communication.
     *
     * This validation should only be performed on initial deployment to avoid
     * overhead during normal reconciliation.
     *
     * @param reconciliation Reconciliation context
     * @param networkingProvider Configured networking provider
     * @param remoteSupplier Remote resource operator supplier
     * @param namespace Namespace for test resources
     * @param kafkaName Kafka cluster name (for error messages)
     * @param maxLatencyMs Maximum acceptable latency in milliseconds
     * @param warningLatencyMs Warning threshold for latency
     * @return Future with ValidationResult
     */
    public Future<ValidationResult> validateNetworkLatency(
            final Reconciliation reconciliation,
            final StretchNetworkingProvider networkingProvider,
            final RemoteResourceOperatorSupplier remoteSupplier,
            final String namespace,
            final String kafkaName,
            final int maxLatencyMs,
            final int warningLatencyMs) {

        // Validate inputs
        if (networkingProvider == null) {
            LOGGER.warnCr(reconciliation, "Network latency validation skipped - networking provider not initialized");
            return Future.succeededFuture(ValidationResult.success());
        }

        if (remoteSupplier == null || remoteSupplier.remoteResourceOperators.isEmpty()) {
            LOGGER.debugCr(reconciliation, "No remote clusters to validate latency");
            return Future.succeededFuture(ValidationResult.success());
        }

        LOGGER.infoCr(reconciliation, "Validating network latency for stretch cluster '{}'", kafkaName);

        // Measure latency to each remote cluster
        List<Future<LatencyMeasurement>> latencyChecks = new ArrayList<>();
        for (String clusterId : remoteSupplier.remoteResourceOperators.keySet()) {
            latencyChecks.add(measureClusterLatency(
                reconciliation, networkingProvider, namespace, clusterId
            ));
        }

        return Future.join(latencyChecks)
            .map(results -> {
                List<LatencyMeasurement> measurements = new ArrayList<>();
                for (int i = 0; i < results.size(); i++) {
                    measurements.add(results.resultAt(i));
                }
                return validateLatencyMeasurements(
                    reconciliation, measurements, kafkaName, maxLatencyMs, warningLatencyMs
                );
            })
            .recover(error -> {
                LOGGER.errorCr(reconciliation, "Network latency validation failed: {}", error.getMessage());
                return Future.succeededFuture(ValidationResult.error(
                    "LatencyCheckFailed",
                    String.format("Failed to validate network latency for Kafka cluster '%s': %s. " +
                        "This may indicate network connectivity issues between clusters.",
                        kafkaName, error.getMessage())
                ));
            });
    }

    /**
     * Measures network latency to a specific remote cluster.
     *
     * Creates a temporary test service using the networking provider, measures
     * the time to create and discover the endpoint, then cleans up.
     * This simulates the actual path Kafka will use.
     *
     * @param reconciliation Reconciliation context
     * @param provider Networking provider
     * @param namespace Namespace for test resources
     * @param clusterId Target cluster ID
     * @return Future with LatencyMeasurement
     */
    private Future<LatencyMeasurement> measureClusterLatency(
            final Reconciliation reconciliation,
            final StretchNetworkingProvider provider,
            final String namespace,
            final String clusterId) {

        // Use clusterId in name to prevent collisions
        String testPodName = "strimzi-latency-test-" + clusterId + "-" + System.currentTimeMillis();
        Map<String, Integer> testPorts = new HashMap<>();
        testPorts.put("test", 9999);

        long startTime = System.nanoTime();

        LOGGER.debugCr(reconciliation, "Measuring latency to cluster '{}' using test pod '{}'",
                      clusterId, testPodName);

        // Step 1: Create networking resources (simulates what happens for Kafka pods)
        return provider.createNetworkingResources(
                reconciliation, namespace, testPodName, clusterId, testPorts
            )
            .compose(resources -> {
                // Step 2: Extract actual service name from created resources
                String serviceName = extractServiceName(resources, testPodName);

                if (serviceName == null) {
                    return Future.failedFuture(new RuntimeException(
                        "No Service found in networking resources for test pod: " + testPodName +
                        ". Provider may not have created a Service resource."
                    ));
                }

                LOGGER.debugCr(reconciliation,
                    "Found service '{}' for latency test to cluster '{}'",
                    serviceName, clusterId);

                // Step 3: Discover endpoint (simulates endpoint discovery during reconciliation)
                return provider.discoverPodEndpoint(
                    reconciliation, namespace, serviceName, clusterId, "test"
                );
            })
            .compose(endpoint -> {
                // Calculate total latency
                long endTime = System.nanoTime();
                long latencyMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);

                LOGGER.debugCr(reconciliation,
                    "Latency to cluster '{}': {}ms (endpoint: {})",
                    clusterId, latencyMs, endpoint);

                // Step 4: Clean up test resources
                return provider.deleteNetworkingResources(
                        reconciliation, namespace, testPodName, clusterId
                    )
                    .map(v -> new LatencyMeasurement(clusterId, latencyMs, endpoint, true));
            })
            .recover(error -> {
                // If measurement fails, try to clean up and return failure measurement
                LOGGER.warnCr(reconciliation,
                    "Failed to measure latency to cluster '{}': {}",
                    clusterId, error.getMessage());

                // Attempt cleanup (best effort)
                return provider.deleteNetworkingResources(
                        reconciliation, namespace, testPodName, clusterId
                    )
                    .recover(cleanupError -> {
                        LOGGER.warnCr(reconciliation,
                            "Failed to cleanup latency test resources for cluster '{}': {}. " +
                            "Manual cleanup may be required for test resources with name pattern: {}*",
                            clusterId, cleanupError.getMessage(), testPodName);
                        return Future.succeededFuture();
                    })
                    .map(v -> new LatencyMeasurement(
                        clusterId, Long.MAX_VALUE, "unavailable", false
                    ));
            });
    }

    /**
     * Extracts service name from created networking resources.
     *
     * @param resources List of resources created by the networking provider
     * @param testPodName Name of the test pod (for logging)
     * @return Service name, or null if no Service found
     */
    private String extractServiceName(List<io.fabric8.kubernetes.api.model.HasMetadata> resources, String testPodName) {
        for (io.fabric8.kubernetes.api.model.HasMetadata resource : resources) {
            if (resource instanceof io.fabric8.kubernetes.api.model.Service) {
                return resource.getMetadata().getName();
            }
        }
        return null;
    }

    /**
     * Validates collected latency measurements against thresholds.
     *
     * @param reconciliation Reconciliation context
     * @param measurements Collected latency measurements
     * @param kafkaName Kafka cluster name
     * @param maxLatencyMs Maximum acceptable latency
     * @param warningLatencyMs Warning threshold
     * @return ValidationResult
     */
    private ValidationResult validateLatencyMeasurements(
            final Reconciliation reconciliation,
            final List<LatencyMeasurement> measurements,
            final String kafkaName,
            final int maxLatencyMs,
            final int warningLatencyMs) {

        List<String> errors = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        StringBuilder detailedInfo = new StringBuilder();

        for (LatencyMeasurement m : measurements) {
            if (!m.isSuccessful()) {
                errors.add(String.format(
                    "Cluster '%s': Failed to measure latency - network provider test failed",
                    m.getClusterId()
                ));
                continue;
            }

            String latencyInfo = String.format(
                "%s <-> %s: %dms (endpoint: %s)",
                centralClusterId, m.getClusterId(), m.getLatencyMs(), m.getEndpoint()
            );

            if (m.getLatencyMs() > maxLatencyMs) {
                errors.add(String.format(
                    "Cluster '%s': %dms latency exceeds maximum %dms",
                    m.getClusterId(), m.getLatencyMs(), maxLatencyMs
                ));
                detailedInfo.append(latencyInfo).append(" - EXCEEDS LIMIT; ");
            } else if (m.getLatencyMs() > warningLatencyMs) {
                warnings.add(String.format(
                    "Cluster '%s': %dms latency exceeds warning threshold %dms",
                    m.getClusterId(), m.getLatencyMs(), warningLatencyMs
                ));
                detailedInfo.append(latencyInfo).append(" - WARNING; ");
            } else {
                detailedInfo.append(latencyInfo).append(" - OK; ");
            }
        }

        LOGGER.infoCr(reconciliation, "Latency measurements: {}", detailedInfo.toString());

        if (!errors.isEmpty()) {
            String errorMessage = String.format(
                "Kafka cluster '%s' failed network latency validation:%n%s%n%n" +
                "Stretch clusters require same-datacenter deployment with network latency " +
                "below %dms. Cross-region or high-latency deployments are not supported.%n%n" +
                "Deployment blocked to prevent performance issues and instability.%n%n" +
                "To fix this issue:%n" +
                "1. Deploy all Kubernetes clusters in the same datacenter/region%n" +
                "2. Ensure network infrastructure supports low-latency pod-to-pod communication%n" +
                "3. Verify network path between clusters (check firewalls, routing, etc.)%n" +
                "4. If using cloud providers, ensure all clusters are in the same region/zone%n%n" +
                "Measured latencies: %s",
                kafkaName,
                String.join("\n", errors),
                maxLatencyMs,
                detailedInfo.toString()
            );

            return ValidationResult.error("NetworkLatencyTooHigh", errorMessage);
        }

        // Log warnings but allow deployment
        if (!warnings.isEmpty()) {
            LOGGER.warnCr(reconciliation, 
                "Kafka cluster '{}' has elevated network latency: {}",
                kafkaName, String.join("; ", warnings));
            LOGGER.warnCr(reconciliation,
                "Consider deploying closer together for optimal performance. " +
                "Current latencies: {}", detailedInfo.toString());
        }

        return ValidationResult.success();
    }

    /**
     * Represents a network latency measurement to a cluster.
     */
    private static class LatencyMeasurement {
        private final String clusterId;
        private final long latencyMs;
        private final String endpoint;
        private final boolean successful;

        LatencyMeasurement(String clusterId, long latencyMs, String endpoint, boolean successful) {
            this.clusterId = clusterId;
            this.latencyMs = latencyMs;
            this.endpoint = endpoint;
            this.successful = successful;
        }

        public String getClusterId() {
            return clusterId;
        }

        public long getLatencyMs() {
            return latencyMs;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public boolean isSuccessful() {
            return successful;
        }
    }

    /**
     * Validate connectivity to a single cluster.
     *
     * @param clusterId Cluster ID
     * @param client KubernetesClient for the cluster
     * @return Future with ValidationResult
     */
    private Future<ValidationResult> validateClusterConnectivity(
            final String clusterId,
            final KubernetesClient client) {

        return vertx.executeBlocking(() -> {
            try {
                // Check API server reachable by getting version
                client.getKubernetesVersion();

                // Check required CRDs exist
                boolean strimziPodSetCrdExists = client.apiextensions().v1()
                    .customResourceDefinitions()
                    .withName("strimzipodsets.core.strimzi.io").get()
                    != null;

                if (!strimziPodSetCrdExists) {
                    return ValidationResult.error(
                        "MissingCRD",
                        String.format("Required CRD "
                            + "'strimzipodsets.core.strimzi.io' not found in "
                            + "cluster '%s'. Please ensure Strimzi CRDs are "
                            + "installed in all target clusters.", clusterId)
                    );
                }

                return ValidationResult.success();

            } catch (Exception e) {
                return ValidationResult.error(
                    "ConnectivityError",
                    String.format("Cannot connect to cluster '%s': %s. "
                        + "Please check kubeconfig and network connectivity.",
                        clusterId, e.getMessage())
                );
            }
        });
    }
}
