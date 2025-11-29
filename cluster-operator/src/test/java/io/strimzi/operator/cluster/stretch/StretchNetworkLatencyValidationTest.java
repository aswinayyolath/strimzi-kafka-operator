/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.strimzi.operator.cluster.RemoteClientSupplier;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit tests for network latency validation in StretchClusterValidator.
 * 
 * Tests validate:
 * - Input validation (null checks)
 * - Latency measurement logic
 * - Threshold validation (max and warning)
 * - Error handling and cleanup
 * - Multiple cluster scenarios
 */
@ExtendWith(VertxExtension.class)
public class StretchNetworkLatencyValidationTest {
    
    private static final String CENTRAL_CLUSTER_ID = "central";
    private static final String REMOTE_CLUSTER_A = "cluster-a";
    private static final String REMOTE_CLUSTER_B = "cluster-b";
    private static final String NAMESPACE = "test-namespace";
    private static final String KAFKA_NAME = "my-cluster";
    
    private static Vertx vertx;
    private StretchClusterValidator validator;
    private Reconciliation reconciliation;
    
    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
    }
    
    @AfterAll
    public static void afterAll() {
        if (vertx != null) {
            vertx.close();
        }
    }
    
    @BeforeEach
    public void setUp() {
        validator = new StretchClusterValidator(
            vertx,
            CENTRAL_CLUSTER_ID,
            Set.of(REMOTE_CLUSTER_A, REMOTE_CLUSTER_B)
        );
        reconciliation = new Reconciliation("test", "Kafka", NAMESPACE, KAFKA_NAME);
    }
    
    // ========== Input Validation Tests ==========
    
    @Test
    public void testValidationSkipsWhenProviderIsNull(VertxTestContext context) {
        validator.validateNetworkLatency(
                reconciliation,
                null,  // null provider
                null,
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Should succeed when provider is null", result.isValid(), is(true));
                assertThat("Error code should be null", result.getErrorCode(), is(nullValue()));
                assertThat("Error message should be null", result.getErrorMessage(), is(nullValue()));
                context.completeNow();
            })));
    }
    
    @Test
    public void testValidationSkipsWhenRemoteSupplierIsNull(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(2);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                null,  // null remote supplier
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Should succeed when remote supplier is null", result.isValid(), is(true));
                context.completeNow();
            })));
    }
    
    @Test
    public void testValidationSkipsWhenNoRemoteClusters(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(2);
        RemoteResourceOperatorSupplier emptySupplier = createEmptyRemoteSupplier();
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                emptySupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Should succeed when no remote clusters", result.isValid(), is(true));
                context.completeNow();
            })));
    }
    
    // ========== Low Latency Tests (Should Pass) ==========
    
    @Test
    public void testLowLatencyPasses(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(2); // 2ms latency
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,  // max: 10ms
                5    // warning: 5ms
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Low latency should pass validation", result.isValid(), is(true));
                assertThat("Should have no error code", result.getErrorCode(), is(nullValue()));
                context.completeNow();
            })));
    }
    
    @Test
    public void testMultipleClustersWithLowLatency(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(3); // 3ms latency
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A, REMOTE_CLUSTER_B);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Multiple clusters with low latency should pass", result.isValid(), is(true));
                context.completeNow();
            })));
    }
    
    // ========== Elevated Latency Tests (Should Warn but Pass) ==========
    
    @Test
    public void testElevatedLatencyWarnsButPasses(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(7); // 7ms - above warning, below max
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,  // max: 10ms
                5    // warning: 5ms
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Elevated latency should still pass", result.isValid(), is(true));
                assertThat("Should have no error code", result.getErrorCode(), is(nullValue()));
                // Note: Warning is logged, not returned in ValidationResult
                context.completeNow();
            })));
    }
    
    // ========== High Latency Tests (Should Block) ==========
    
    @Test
    public void testHighLatencyBlocks(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(95); // 95ms - way over max
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,  // max: 10ms
                5    // warning: 5ms
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("High latency should fail validation", result.isValid(), is(false));
                assertThat("Error code should be NetworkLatencyTooHigh", 
                          result.getErrorCode(), is("NetworkLatencyTooHigh"));
                assertThat("Error message should mention latency exceeds maximum", 
                          result.getErrorMessage(), containsString("latency exceeds maximum 10ms"));
                assertThat("Error message should mention cluster", 
                          result.getErrorMessage(), containsString(REMOTE_CLUSTER_A));
                assertThat("Error message should have fix instructions", 
                          result.getErrorMessage(), containsString("same-datacenter deployment"));
                context.completeNow();
            })));
    }
    
    @Test
    public void testBelowMaxThresholdPasses(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(2); // Well below 10ms
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                100,  // max: 100ms (high threshold to account for test overhead)
                50
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Below threshold should pass", 
                          result.isValid(), is(true));
                context.completeNow();
            })));
    }
    
    @Test
    public void testOneOverMaxThresholdBlocks(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(11); // 11ms - 1 over max
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,  // max: 10ms
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("1ms over threshold should fail", result.isValid(), is(false));
                assertThat("Error code should be NetworkLatencyTooHigh", 
                          result.getErrorCode(), is("NetworkLatencyTooHigh"));
                context.completeNow();
            })));
    }
    
    // ========== Multiple Clusters with Mixed Latency ==========
    
    @Test
    public void testMultipleClustersOneHighLatencyBlocks(VertxTestContext context) {
        // Provider returns different latencies for different clusters
        TestNetworkingProvider provider = new TestNetworkingProvider(2, 95); // cluster-a: 2ms, cluster-b: 95ms
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A, REMOTE_CLUSTER_B);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Should fail when any cluster has high latency", result.isValid(), is(false));
                assertThat("Error message should mention the bad cluster", 
                          result.getErrorMessage(), containsString(REMOTE_CLUSTER_B));
                context.completeNow();
            })));
    }
    
    // ========== Error Handling Tests ==========
    
    @Test
    public void testProviderFailureHandled(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(true); // Simulate failure
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Should fail when provider fails", result.isValid(), is(false));
                assertThat("Error code should indicate failure", 
                          result.getErrorCode(), is("NetworkLatencyTooHigh"));
                context.completeNow();
            })));
    }
    
    @Test
    public void testNoServiceInResourcesHandled(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(2, false); // No service in resources
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Should fail when no Service found", result.isValid(), is(false));
                context.completeNow();
            })));
    }
    
    @Test
    public void testCleanupIsAttemptedOnFailure(VertxTestContext context) {
        TestNetworkingProvider provider = new TestNetworkingProvider(true); // Simulate failure
        RemoteResourceOperatorSupplier remoteSupplier = createRemoteSupplier(REMOTE_CLUSTER_A);
        
        validator.validateNetworkLatency(
                reconciliation,
                provider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Cleanup should be attempted", provider.deleteCallCount.get() > 0, is(true));
                context.completeNow();
            })));
    }
    
    // ========== Helper Methods ==========
    
    private RemoteResourceOperatorSupplier createRemoteSupplier(String... clusterIds) {
        Map<String, ResourceOperatorSupplier> remoteOps = new HashMap<>();
        for (String clusterId : clusterIds) {
            remoteOps.put(clusterId, null); // We don't actually use the ResourceOperatorSupplier in tests
        }
        
        // Create a mock RemoteClientSupplier
        RemoteClientSupplier mockRemoteClientSupplier = 
            new RemoteClientSupplier(NAMESPACE, new HashMap<>(), new HashMap<>());
        
        // Create a minimal RemoteResourceOperatorSupplier for testing
        return new RemoteResourceOperatorSupplier(vertx, null, mockRemoteClientSupplier, new HashMap<>(), "test-operator", CENTRAL_CLUSTER_ID) {
            {
                // Use reflection to set the public final field
                this.remoteResourceOperators.putAll(remoteOps);
            }
        };
    }
    
    private RemoteResourceOperatorSupplier createEmptyRemoteSupplier() {
        RemoteClientSupplier mockRemoteClientSupplier = 
            new RemoteClientSupplier(NAMESPACE, new HashMap<>(), new HashMap<>());
        return new RemoteResourceOperatorSupplier(vertx, null, mockRemoteClientSupplier, new HashMap<>(), "test-operator", CENTRAL_CLUSTER_ID);
    }
    
    // ========== Test Networking Provider ==========
    
    /**
     * Standalone test implementation of StretchNetworkingProvider that simulates latency.
     * Does NOT extend the real TestNetworkingProvider to avoid interference.
     */
    private static class TestNetworkingProvider implements StretchNetworkingProvider {
        private final long[] latenciesMs;
        private final boolean simulateFailure;
        private final boolean includeService;
        private final AtomicInteger callIndex = new AtomicInteger(0);
        final AtomicInteger deleteCallCount = new AtomicInteger(0);
        
        // Constructor for single latency
        TestNetworkingProvider(long latencyMs) {
            this(latencyMs, true);
        }
        
        // Constructor with service control
        TestNetworkingProvider(long latencyMs, boolean includeService) {
            this.latenciesMs = new long[]{latencyMs};
            this.simulateFailure = false;
            this.includeService = includeService;
        }
        
        // Constructor for multiple latencies
        TestNetworkingProvider(long... latenciesMs) {
            this.latenciesMs = latenciesMs;
            this.simulateFailure = false;
            this.includeService = true;
        }
        
        // Constructor for failure simulation
        TestNetworkingProvider(boolean simulateFailure) {
            this.latenciesMs = new long[]{0};
            this.simulateFailure = simulateFailure;
            this.includeService = true;
        }
        
        @Override
        public Future<List<HasMetadata>> createNetworkingResources(
                Reconciliation reconciliation,
                String namespace,
                String podName,
                String targetClusterId,
                Map<String, Integer> ports) {
            
            if (simulateFailure) {
                return Future.failedFuture(new RuntimeException("Simulated provider failure"));
            }
            
            int index = callIndex.getAndIncrement();
            long latency = latenciesMs[Math.min(index, latenciesMs.length - 1)];
            
            // Simulate latency with timer
            Promise<Long> promise = Promise.promise();
            vertx.setTimer(latency, promise::complete);
            
            return promise.future()
                .compose(v -> {
                    List<HasMetadata> resources = new ArrayList<>();
                    if (includeService) {
                        Service service = new ServiceBuilder()
                            .withNewMetadata()
                                .withName(podName + "-svc")
                                .withNamespace(namespace)
                            .endMetadata()
                            .build();
                        resources.add(service);
                    }
                    return Future.succeededFuture(resources);
                });
        }
        
        @Override
        public Future<String> discoverPodEndpoint(
                Reconciliation reconciliation,
                String namespace,
                String serviceName,
                String targetClusterId,
                String portName) {
            if (simulateFailure) {
                return Future.failedFuture(new RuntimeException("Simulated discovery failure"));
            }
            return Future.succeededFuture("test-endpoint:9999");
        }
        
        @Override
        public Future<Void> deleteNetworkingResources(
                Reconciliation reconciliation,
                String namespace,
                String podName,
                String targetClusterId) {
            deleteCallCount.incrementAndGet();
            return Future.succeededFuture();
        }
        
        @Override
        public Future<Void> init(Map<String, String> config,
                                ResourceOperatorSupplier centralSupplier,
                                RemoteResourceOperatorSupplier remoteSupplier) {
            return Future.succeededFuture();
        }
        
        @Override
        public Future<String> generateAdvertisedListeners(
                Reconciliation reconciliation,
                String namespace,
                String podName,
                String targetClusterId,
                Map<String, String> listenerPorts) {
            return Future.succeededFuture("PLAINTEXT://test:9092");
        }
        
        @Override
        public Future<String> generateQuorumVoters(
                Reconciliation reconciliation,
                String namespace,
                List<io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider.ControllerPodInfo> controllerPods,
                String targetClusterId) {
            return Future.succeededFuture("1@test:9093");
        }
        
        @Override
        public String generatePodDnsName(
                String namespace,
                String podName,
                String targetClusterId,
                String serviceName) {
            return podName + "." + serviceName + ".svc.cluster.local";
        }
        
        @Override
        public String generateServiceDnsName(
                String namespace,
                String serviceName,
                String targetClusterId) {
            return serviceName + "." + namespace + ".svc.cluster.local";
        }
        
        @Override
        public String getProviderName() {
            return "test-provider";
        }
    }
}
