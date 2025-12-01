/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.MicrometerMetricsProvider;
import io.strimzi.operator.common.Reconciliation;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Integration tests for network latency validation with real multi-cluster setup.
 * Uses MockKube3 to simulate multiple Kubernetes clusters.
 */
@ExtendWith(VertxExtension.class)
public class StretchNetworkLatencyValidationIT {
    
    private static final String CENTRAL_CLUSTER_ID = "central";
    private static final String REMOTE_CLUSTER_A = "cluster-a";
    private static final String REMOTE_CLUSTER_B = "cluster-b";
    private static final String NAMESPACE = "test-namespace";
    private static final String KAFKA_NAME = "my-cluster";
    
    private static Vertx vertx;
    private static MockKube3 mockKubeCentral;
    private static MockKube3 mockKubeRemoteA;
    private static MockKube3 mockKubeRemoteB;
    
    private StretchClusterValidator validator;
    private Reconciliation reconciliation;
    private TestNetworkingProvider networkingProvider;
    private RemoteResourceOperatorSupplier remoteSupplier;
    
    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
        
        // Create separate mock Kubernetes clusters
        mockKubeCentral = new MockKube3.MockKube3Builder().build();
        mockKubeCentral.start();
        
        mockKubeRemoteA = new MockKube3.MockKube3Builder().build();
        mockKubeRemoteA.start();
        
        mockKubeRemoteB = new MockKube3.MockKube3Builder().build();
        mockKubeRemoteB.start();
    }
    
    @AfterAll
    public static void afterAll() {
        if (mockKubeCentral != null) {
            mockKubeCentral.stop();
        }
        if (mockKubeRemoteA != null) {
            mockKubeRemoteA.stop();
        }
        if (mockKubeRemoteB != null) {
            mockKubeRemoteB.stop();
        }
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
        networkingProvider = new TestNetworkingProvider();
        
        // Create remote resource operator supplier
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(
            false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        MicrometerMetricsProvider metricsProvider = new MicrometerMetricsProvider(
            new io.micrometer.core.instrument.composite.CompositeMeterRegistry());
        
        Map<String, ResourceOperatorSupplier> remoteOps = new HashMap<>();
        remoteOps.put(REMOTE_CLUSTER_A, new ResourceOperatorSupplier(
            vertx, mockKubeRemoteA.client(), metricsProvider, pfa, "test-operator"));
        remoteOps.put(REMOTE_CLUSTER_B, new ResourceOperatorSupplier(
            vertx, mockKubeRemoteB.client(), metricsProvider, pfa, "test-operator"));
        
        remoteSupplier = new RemoteResourceOperatorSupplier(
            vertx,
            mockKubeCentral.client(),
            null,
            null,
            "test-operator",
            CENTRAL_CLUSTER_ID
        );
        // Inject remote operators
        remoteSupplier.remoteResourceOperators.putAll(remoteOps);
    }
    
    @AfterEach
    public void tearDown() {
        // Clean up test resources
        mockKubeCentral.client().pods().inNamespace(NAMESPACE).delete();
        mockKubeRemoteA.client().pods().inNamespace(NAMESPACE).delete();
        mockKubeRemoteB.client().pods().inNamespace(NAMESPACE).delete();
    }
    
    @Test
    public void testValidationSucceedsWithLowLatency(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();
        
        // Deploy test pods that will be ready quickly
        deployReadyTestPods();
        
        validator.validateNetworkLatency(
                reconciliation,
                networkingProvider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                100,  // High threshold to ensure success
                50
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Validation should succeed", result.isValid(), is(true));
                assertThat("Error code should be null", result.getErrorCode(), is(nullValue()));
                checkpoint.flag();
            })));
    }
    
    @Test
    public void testValidationFailsWithHighLatency(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();
        
        // Deploy test pods
        deployReadyTestPods();
        
        // Use very low threshold to force failure
        validator.validateNetworkLatency(
                reconciliation,
                networkingProvider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                1,  // Very low threshold
                1
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Validation should fail", result.isValid(), is(false));
                assertThat("Error code should be NetworkLatencyTooHigh", 
                    result.getErrorCode(), is("NetworkLatencyTooHigh"));
                assertThat("Error message should contain latency info", 
                    result.getErrorMessage().contains("latency exceeds"), is(true));
                checkpoint.flag();
            })));
    }
    
    @Test
    public void testValidationSkipsWhenProviderIsNull(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();
        
        validator.validateNetworkLatency(
                reconciliation,
                null,  // null provider
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Validation should succeed when provider is null", 
                    result.isValid(), is(true));
                checkpoint.flag();
            })));
    }
    
    @Test
    public void testValidationSkipsWhenRemoteSupplierIsNull(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();
        
        validator.validateNetworkLatency(
                reconciliation,
                networkingProvider,
                null,  // null remote supplier
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                assertThat("Validation should succeed when remote supplier is null", 
                    result.isValid(), is(true));
                checkpoint.flag();
            })));
    }
    
    @Test
    public void testValidationCleansUpResourcesOnFailure(VertxTestContext context) {
        Checkpoint checkpoint = context.checkpoint();
        
        // Don't deploy pods - this will cause validation to fail
        
        validator.validateNetworkLatency(
                reconciliation,
                networkingProvider,
                remoteSupplier,
                NAMESPACE,
                KAFKA_NAME,
                10,
                5
            )
            .onComplete(context.succeeding(result -> context.verify(() -> {
                // Validation should handle failure gracefully
                assertThat("Result should not be null", result, is(notNullValue()));
                
                // Check that no test pods are left behind
                List<Pod> centralPods = mockKubeCentral.client()
                    .pods()
                    .inNamespace(NAMESPACE)
                    .withLabel("app", "strimzi-latency-test")
                    .list()
                    .getItems();
                
                List<Pod> remotePods = mockKubeRemoteA.client()
                    .pods()
                    .inNamespace(NAMESPACE)
                    .withLabel("app", "strimzi-latency-test")
                    .list()
                    .getItems();
                
                assertThat("No test pods should remain in central cluster", 
                    centralPods.size(), is(0));
                assertThat("No test pods should remain in remote cluster", 
                    remotePods.size(), is(0));
                
                checkpoint.flag();
            })));
    }
    
    /**
     * Helper method to deploy test pods that are immediately ready.
     */
    private void deployReadyTestPods() {
        // Deploy pods in central cluster
        Pod centralPod = new PodBuilder()
            .withNewMetadata()
                .withName("test-central-pod")
                .withNamespace(NAMESPACE)
                .addToLabels("app", "strimzi-latency-test")
            .endMetadata()
            .withNewSpec()
                .addNewContainer()
                    .withName("test")
                    .withImage("busybox")
                .endContainer()
            .endSpec()
            .withNewStatus()
                .withPhase("Running")
            .endStatus()
            .build();
        
        mockKubeCentral.client().pods().inNamespace(NAMESPACE).resource(centralPod).create();
        
        // Deploy pods in remote clusters
        Pod remotePodA = new PodBuilder()
            .withNewMetadata()
                .withName("test-remote-pod-" + REMOTE_CLUSTER_A)
                .withNamespace(NAMESPACE)
                .addToLabels("app", "strimzi-latency-test")
                .addToLabels("strimzi.io/cluster-id", REMOTE_CLUSTER_A)
            .endMetadata()
            .withNewSpec()
                .addNewContainer()
                    .withName("test")
                    .withImage("busybox")
                .endContainer()
            .endSpec()
            .withNewStatus()
                .withPhase("Running")
            .endStatus()
            .build();
        
        Pod remotePodB = new PodBuilder()
            .withNewMetadata()
                .withName("test-remote-pod-" + REMOTE_CLUSTER_B)
                .withNamespace(NAMESPACE)
                .addToLabels("app", "strimzi-latency-test")
                .addToLabels("strimzi.io/cluster-id", REMOTE_CLUSTER_B)
            .endMetadata()
            .withNewSpec()
                .addNewContainer()
                    .withName("test")
                    .withImage("busybox")
                .endContainer()
            .endSpec()
            .withNewStatus()
                .withPhase("Running")
            .endStatus()
            .build();
        
        mockKubeRemoteA.client().pods().inNamespace(NAMESPACE).resource(remotePodA).create();
        mockKubeRemoteB.client().pods().inNamespace(NAMESPACE).resource(remotePodB).create();
    }
    
    /**
     * Test implementation of StretchNetworkingProvider for integration testing.
     */
    private static class TestNetworkingProvider implements StretchNetworkingProvider {
        
        public String getProviderName() {
            return "test-provider";
        }
        
        public Future<Void> init(
                Map<String, String> config,
                ResourceOperatorSupplier centralSupplier,
                RemoteResourceOperatorSupplier remoteSupplier) {
            return Future.succeededFuture();
        }
        
        public Future<List<HasMetadata>> createNetworkingResources(
                Reconciliation reconciliation,
                String namespace,
                String podName,
                String clusterId,
                Map<String, Integer> ports) {
            
            // Create a simple ClusterIP service
            Service service = new ServiceBuilder()
                .withNewMetadata()
                    .withName(podName + "-svc")
                    .withNamespace(namespace)
                .endMetadata()
                .withNewSpec()
                    .withType("ClusterIP")
                    .addNewPort()
                        .withPort(8080)
                        .withName("test")
                    .endPort()
                .endSpec()
                .build();
            
            List<HasMetadata> resources = new ArrayList<>();
            resources.add(service);
            return Future.succeededFuture(resources);
        }
        
        public Future<String> discoverPodEndpoint(
                Reconciliation reconciliation,
                String namespace,
                String serviceName,
                String clusterId,
                String portName) {
            
            // Return a mock endpoint
            return Future.succeededFuture("10.0.0.1:8080");
        }
        
        public Future<Void> deleteNetworkingResources(
                Reconciliation reconciliation,
                String namespace,
                String podName,
                String clusterId) {
            
            return Future.succeededFuture();
        }
        
        public String generateServiceDnsName(String namespace, String serviceName, String clusterId) {
            return serviceName + "." + namespace + ".svc.cluster.local";
        }
        
        public String generatePodDnsName(String namespace, String serviceName, String podName, String clusterId) {
            return podName + "." + serviceName + "." + namespace + ".svc.cluster.local";
        }
        
        public Future<String> generateAdvertisedListeners(
                Reconciliation reconciliation,
                String namespace,
                String podName,
                String clusterId,
                Map<String, String> listeners) {
            return Future.succeededFuture("REPLICATION://broker-0:9091");
        }
        
        public Future<String> generateQuorumVoters(
                Reconciliation reconciliation,
                String namespace,
                List<StretchNetworkingProvider.ControllerPodInfo> controllerPods,
                String clusterId) {
            return Future.succeededFuture("0@controller-0:9093");
        }
    }
}
