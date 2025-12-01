/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExecListener;
import io.fabric8.kubernetes.client.dsl.ExecWatch;
import io.strimzi.operator.cluster.operator.resource.kubernetes.PodOperator;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.operator.common.ReconciliationLogger;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Helper class for performing actual pod-to-pod network latency tests.
 * Deploys test pods and executes real network commands to measure data plane latency.
 */
public class NetworkLatencyTester {
    private static final ReconciliationLogger LOGGER = ReconciliationLogger.create(NetworkLatencyTester.class);
    
    // Test pod configuration
    private static final String TEST_IMAGE = "busybox:latest";
    private static final String TEST_PORT_NAME = "test";
    private static final int TEST_PORT = 8080;
    private static final int POD_READY_TIMEOUT_SECONDS = 60;
    private static final int LATENCY_TEST_TIMEOUT_SECONDS = 30;
    private static final int LATENCY_SAMPLES = 5;
    private static final int SAMPLE_DELAY_MS = 100;
    
    // Pattern to extract connection time from nc output
    // Example: "Connection to 10.0.0.1 8080 port [tcp/*] succeeded!"
    private static final Pattern NC_SUCCESS_PATTERN = Pattern.compile("succeeded");
    
    private final Vertx vertx;
    
    /**
     * Constructor.
     *
     * @param vertx Vert.x instance
     */
    public NetworkLatencyTester(Vertx vertx) {
        this.vertx = vertx;
    }
    
    /**
     * Creates a test pod specification.
     *
     * @param namespace Namespace
     * @param podName Pod name
     * @param clusterId Cluster ID for labeling
     * @return Pod specification
     */
    public Pod createTestPodSpec(String namespace, String podName, String clusterId) {
        Container container = new ContainerBuilder()
            .withName("latency-test")
            .withImage(TEST_IMAGE)
            .withCommand("sh", "-c", "nc -l -p " + TEST_PORT + " -k")  // Listen mode
            .withNewResources()
                .addToRequests("cpu", io.fabric8.kubernetes.api.model.Quantity.parse("10m"))
                .addToRequests("memory", io.fabric8.kubernetes.api.model.Quantity.parse("32Mi"))
                .addToLimits("cpu", io.fabric8.kubernetes.api.model.Quantity.parse("100m"))
                .addToLimits("memory", io.fabric8.kubernetes.api.model.Quantity.parse("64Mi"))
            .endResources()
            .build();
        
        return new PodBuilder()
            .withNewMetadata()
                .withName(podName)
                .withNamespace(namespace)
                .addToLabels("app", "strimzi-latency-test")
                .addToLabels("strimzi.io/cluster-id", clusterId)
                .addToLabels("strimzi.io/kind", "latency-test")
            .endMetadata()
            .withNewSpec()
                .withContainers(container)
                .withRestartPolicy("Never")
            .endSpec()
            .build();
    }
    
    /**
     * Deploys a test pod and waits for it to be ready.
     *
     * @param reconciliation Reconciliation context
     * @param podOperator Pod operator
     * @param namespace Namespace
     * @param podName Pod name
     * @param clusterId Cluster ID
     * @return Future that completes when pod is ready
     */
    public Future<Pod> deployTestPod(
            Reconciliation reconciliation,
            PodOperator podOperator,
            String namespace,
            String podName,
            String clusterId) {
        
        LOGGER.debugCr(reconciliation, "Deploying latency test pod '{}' in namespace '{}' for cluster '{}'",
            podName, namespace, clusterId);
        
        Pod podSpec = createTestPodSpec(namespace, podName, clusterId);
        
        return podOperator.reconcile(reconciliation, namespace, podName, podSpec)
            .compose(v -> waitForPodReady(reconciliation, podOperator, namespace, podName))
            .onSuccess(pod -> LOGGER.debugCr(reconciliation, 
                "Test pod '{}' is ready in cluster '{}'", podName, clusterId))
            .onFailure(error -> LOGGER.warnCr(reconciliation,
                "Failed to deploy test pod '{}' in cluster '{}': {}", 
                podName, clusterId, error.getMessage()));
    }
    
    /**
     * Waits for a pod to be ready.
     *
     * @param reconciliation Reconciliation context
     * @param podOperator Pod operator
     * @param namespace Namespace
     * @param podName Pod name
     * @return Future with the ready pod
     */
    private Future<Pod> waitForPodReady(
            Reconciliation reconciliation,
            PodOperator podOperator,
            String namespace,
            String podName) {
        
        Promise<Pod> promise = Promise.promise();
        long startTime = System.currentTimeMillis();
        
        vertx.setPeriodic(1000, timerId -> {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > POD_READY_TIMEOUT_SECONDS * 1000L) {
                vertx.cancelTimer(timerId);
                promise.fail(new RuntimeException(
                    "Timeout waiting for test pod '" + podName + "' to be ready after " + 
                    POD_READY_TIMEOUT_SECONDS + " seconds"));
                return;
            }
            
            podOperator.getAsync(namespace, podName).onComplete(ar -> {
                if (ar.succeeded() && ar.result() != null) {
                    Pod pod = ar.result();
                    if (isPodReady(pod)) {
                        vertx.cancelTimer(timerId);
                        promise.complete(pod);
                    }
                }
            });
        });
        
        return promise.future();
    }
    
    /**
     * Checks if a pod is ready.
     *
     * @param pod Pod to check
     * @return true if pod is ready
     */
    private boolean isPodReady(Pod pod) {
        if (pod.getStatus() == null || pod.getStatus().getPhase() == null) {
            return false;
        }
        
        String phase = pod.getStatus().getPhase();
        return "Running".equals(phase);
    }
    
    /**
     * Measures network latency from source pod to target endpoint.
     * Executes multiple samples and returns median latency.
     *
     * @param reconciliation Reconciliation context
     * @param client Kubernetes client for source cluster
     * @param namespace Namespace
     * @param sourcePodName Source pod name
     * @param targetEndpoint Target endpoint (host:port)
     * @param targetClusterId Target cluster ID (for logging)
     * @return Future with latency measurement result
     */
    public Future<LatencyResult> measureLatency(
            Reconciliation reconciliation,
            KubernetesClient client,
            String namespace,
            String sourcePodName,
            String targetEndpoint,
            String targetClusterId) {
        
        LOGGER.debugCr(reconciliation, 
            "Measuring latency from pod '{}' to endpoint '{}' (cluster '{}')",
            sourcePodName, targetEndpoint, targetClusterId);
        
        // Parse endpoint
        String[] parts = targetEndpoint.split(":");
        if (parts.length != 2) {
            return Future.failedFuture(new IllegalArgumentException(
                "Invalid endpoint format: " + targetEndpoint + ". Expected host:port"));
        }
        
        String host = parts[0];
        String port = parts[1];
        
        // Collect multiple samples
        List<Future<Long>> sampleFutures = new ArrayList<>();
        for (int i = 0; i < LATENCY_SAMPLES; i++) {
            final int sampleNum = i;
            Future<Long> sampleFuture = vertx.executeBlocking(() -> {
                // Add delay between samples (except first)
                if (sampleNum > 0) {
                    try {
                        Thread.sleep(SAMPLE_DELAY_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                
                return executeSingleLatencyTest(client, namespace, sourcePodName, host, port);
            });
            sampleFutures.add(sampleFuture);
        }
        
        // Wait for all samples and calculate statistics
        return Future.all(sampleFutures)
            .map(composite -> {
                List<Long> samples = new ArrayList<>();
                for (int i = 0; i < composite.size(); i++) {
                    samples.add(composite.resultAt(i));
                }
                
                Collections.sort(samples);
                long min = samples.get(0);
                long max = samples.get(samples.size() - 1);
                long median = samples.get(samples.size() / 2);
                long avg = (long) samples.stream().mapToLong(Long::longValue).average().orElse(0);
                
                LOGGER.debugCr(reconciliation,
                    "Latency to cluster '{}': min={}ms, median={}ms, avg={}ms, max={}ms",
                    targetClusterId, min, median, avg, max);
                
                return new LatencyResult(targetEndpoint, min, median, avg, max, true);
            })
            .recover(error -> {
                LOGGER.warnCr(reconciliation,
                    "Failed to measure latency to cluster '{}': {}",
                    targetClusterId, error.getMessage());
                return Future.succeededFuture(
                    new LatencyResult(targetEndpoint, -1, -1, -1, -1, false));
            });
    }
    
    /**
     * Executes a single latency test using nc (netcat).
     *
     * @param client Kubernetes client
     * @param namespace Namespace
     * @param podName Pod name
     * @param host Target host
     * @param port Target port
     * @return Latency in milliseconds
     */
    private long executeSingleLatencyTest(
            KubernetesClient client,
            String namespace,
            String podName,
            String host,
            String port) {
        
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        
        CompletableFuture<Integer> exitCodeFuture = new CompletableFuture<>();
        
        long startTime = System.nanoTime();
        
        try (ExecWatch watch = client.pods()
                .inNamespace(namespace)
                .withName(podName)
                .writingOutput(out)
                .writingError(err)
                .usingListener(new ExecListener() {
                    public void onOpen() {
                        // Connection opened
                    }
                    
                    public void onFailure(Throwable t, io.fabric8.kubernetes.client.http.HttpResponse<?> response) {
                        exitCodeFuture.completeExceptionally(t);
                    }
                    
                    public void onClose(int code, String reason) {
                        exitCodeFuture.complete(code);
                    }
                })
                .exec("sh", "-c", "nc -zv -w 5 " + host + " " + port)) {
            
            // Wait for command to complete
            Integer exitCode = exitCodeFuture.get(LATENCY_TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            long endTime = System.nanoTime();
            long latencyMs = TimeUnit.NANOSECONDS.toMillis(endTime - startTime);
            
            String output = out.toString(StandardCharsets.UTF_8);
            String errorOutput = err.toString(StandardCharsets.UTF_8);
            
            // Check if connection succeeded
            if (exitCode == 0 || NC_SUCCESS_PATTERN.matcher(output + errorOutput).find()) {
                return latencyMs;
            } else {
                throw new RuntimeException("Connection failed: " + errorOutput);
            }
            
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute latency test: " + e.getMessage(), e);
        }
    }
    
    /**
     * Deletes a test pod.
     *
     * @param reconciliation Reconciliation context
     * @param podOperator Pod operator
     * @param namespace Namespace
     * @param podName Pod name
     * @return Future that completes when pod is deleted
     */
    public Future<Void> deleteTestPod(
            Reconciliation reconciliation,
            PodOperator podOperator,
            String namespace,
            String podName) {
        
        LOGGER.debugCr(reconciliation, "Deleting latency test pod '{}' in namespace '{}'",
            podName, namespace);
        
        return podOperator.reconcile(reconciliation, namespace, podName, null)
            .mapEmpty();
    }
    
    /**
     * Result of a latency measurement.
     */
    public static class LatencyResult {
        private final String endpoint;
        private final long minMs;
        private final long medianMs;
        private final long avgMs;
        private final long maxMs;
        private final boolean successful;
        
        /**
         * Constructor.
         *
         * @param endpoint Target endpoint
         * @param minMs Minimum latency
         * @param medianMs Median latency
         * @param avgMs Average latency
         * @param maxMs Maximum latency
         * @param successful Whether measurement was successful
         */
        public LatencyResult(String endpoint, long minMs, long medianMs, long avgMs, long maxMs, boolean successful) {
            this.endpoint = endpoint;
            this.minMs = minMs;
            this.medianMs = medianMs;
            this.avgMs = avgMs;
            this.maxMs = maxMs;
            this.successful = successful;
        }
        
        /**
         * Gets the endpoint.
         * @return endpoint
         */
        public String getEndpoint() {
            return endpoint;
        }
        
        /**
         * Gets minimum latency.
         * @return min latency in ms
         */
        public long getMinMs() {
            return minMs;
        }
        
        /**
         * Gets median latency.
         * @return median latency in ms
         */
        public long getMedianMs() {
            return medianMs;
        }
        
        /**
         * Gets average latency.
         * @return average latency in ms
         */
        public long getAvgMs() {
            return avgMs;
        }
        
        /**
         * Gets maximum latency.
         * @return max latency in ms
         */
        public long getMaxMs() {
            return maxMs;
        }
        
        /**
         * Checks if measurement was successful.
         * @return true if successful
         */
        public boolean isSuccessful() {
            return successful;
        }
    }
}
