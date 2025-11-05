/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.ClusterOperatorConfig;
import io.strimzi.operator.cluster.KafkaVersionTestUtils;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.RemoteClientSupplier;
import io.strimzi.operator.cluster.ResourceUtils;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.InvalidConfigurationException;
import io.strimzi.platform.KubernetesVersion;
import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import io.strimzi.test.mockkube3.MockKube3;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for StretchNetworkingProviderFactory.
 * 
 * This test class validates:
 * - Built-in provider creation (NodePort, LoadBalancer)
 * - Default provider selection
 * - Provider name normalization (case-insensitive, aliases)
 * - Error handling for invalid provider names
 * - Error handling for custom provider configuration
 * 
 * Test Strategy:
 * - Tests factory pattern behavior
 * - Tests configuration validation
 * - Tests error messages are user-friendly
 * - Does NOT test custom plugin loading (requires external JARs)
 * 
 * @see StretchNetworkingProviderFactory
 */
@Tag("stretch-cluster")
@Tag("stretch-unit")
@ParallelSuite
@ExtendWith(VertxExtension.class)
public class StretchNetworkingProviderFactoryTest {
    
    private static final String CENTRAL_CLUSTER_ID = "central";
    private static final String REMOTE_CLUSTER_A_ID = "cluster-a";
    
    private static Vertx vertx;
    private static MockKube3 mockKubeCentral;
    private static MockKube3 mockKubeRemoteA;
    
    private KubernetesClient centralClient;
    private KubernetesClient remoteClientA;
    private ResourceOperatorSupplier centralSupplier;
    private RemoteResourceOperatorSupplier remoteSupplier;
    
    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
        
        // Create mock Kubernetes clusters once for all tests
        mockKubeCentral = new MockKube3.MockKube3Builder().build();
        mockKubeCentral.start();
        
        mockKubeRemoteA = new MockKube3.MockKube3Builder().build();
        mockKubeRemoteA.start();
    }
    
    @AfterAll
    public static void afterAll() {
        if (mockKubeCentral != null) {
            mockKubeCentral.stop();
        }
        if (mockKubeRemoteA != null) {
            mockKubeRemoteA.stop();
        }
        if (vertx != null) {
            vertx.close();
        }
    }
    
    @BeforeEach
    public void setUp() {
        // Get fresh clients for each test
        centralClient = mockKubeCentral.client();
        remoteClientA = mockKubeRemoteA.client();
        
        // Create suppliers
        PlatformFeaturesAvailability pfa = new PlatformFeaturesAvailability(
            false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION);
        centralSupplier = new ResourceOperatorSupplier(vertx, centralClient, 
            new io.strimzi.operator.common.MicrometerMetricsProvider(new io.micrometer.core.instrument.composite.CompositeMeterRegistry()), 
            pfa, "test-operator");
        
        Map<String, KubernetesClient> remoteClients = new HashMap<>();
        remoteClients.put(REMOTE_CLUSTER_A_ID, remoteClientA);
        
        Map<String, io.strimzi.operator.cluster.ClusterInfo> remoteClusters = new HashMap<>();
        remoteClusters.put(REMOTE_CLUSTER_A_ID, 
            new io.strimzi.operator.cluster.ClusterInfo(REMOTE_CLUSTER_A_ID, "https://api.cluster-a.example.com:6443", "cluster-a-kubeconfig"));
        
        RemoteClientSupplier remoteClientSupplier = new RemoteClientSupplier("test-ns", remoteClusters, remoteClients);
        
        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        remotePfas.put(REMOTE_CLUSTER_A_ID, pfa);
        
        remoteSupplier = new RemoteResourceOperatorSupplier(
            vertx, centralClient, remoteClientSupplier, remotePfas, "test-operator", CENTRAL_CLUSTER_ID
        );
    }
    
    // ========== Built-in Provider Tests ==========
    
    @ParallelTest
    public void testCreateNodePortProvider() {

        ClusterOperatorConfig config = createOperatorConfig("nodeport");
        Map<String, String> providerConfig = new HashMap<>();
        

        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, providerConfig, centralSupplier, remoteSupplier
        );
        

        assertThat("Provider should be created", provider, is(notNullValue()));
        assertThat("Provider should be NodePortNetworkingProvider", 
                  provider, instanceOf(NodePortNetworkingProvider.class));
        assertThat("Provider name should be nodeport", 
                  provider.getProviderName(), is("nodeport"));
    }
    
    @ParallelTest
    public void testCreateLoadBalancerProvider() {

        ClusterOperatorConfig config = createOperatorConfig("loadbalancer");
        Map<String, String> providerConfig = new HashMap<>();
        

        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, providerConfig, centralSupplier, remoteSupplier
        );
        

        assertThat("Provider should be created", provider, is(notNullValue()));
        assertThat("Provider should be LoadBalancerNetworkingProvider", 
                  provider, instanceOf(LoadBalancerNetworkingProvider.class));
        assertThat("Provider name should be loadbalancer", 
                  provider.getProviderName(), is("loadbalancer"));
    }
    
    @ParallelTest
    public void testCreateLoadBalancerProviderWithAlias() {
        // Arrange - "lb" is an alias for "loadbalancer"
        ClusterOperatorConfig config = createOperatorConfig("lb");
        Map<String, String> providerConfig = new HashMap<>();
        

        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, providerConfig, centralSupplier, remoteSupplier
        );
        

        assertThat("Provider should be created", provider, is(notNullValue()));
        assertThat("Provider should be LoadBalancerNetworkingProvider", 
                  provider, instanceOf(LoadBalancerNetworkingProvider.class));
    }
    
    // ========== Default Provider Tests ==========
    
    @ParallelTest
    public void testCreateWithNullProviderUsesDefault() {

        ClusterOperatorConfig config = createOperatorConfig(null);
        Map<String, String> providerConfig = new HashMap<>();
        

        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, providerConfig, centralSupplier, remoteSupplier
        );
        

        assertThat("Provider should be created", provider, is(notNullValue()));
        assertThat("Should default to NodePort provider", 
                  provider, instanceOf(NodePortNetworkingProvider.class));
    }
    
    @ParallelTest
    public void testCreateWithEmptyProviderUsesDefault() {

        ClusterOperatorConfig config = createOperatorConfig("");
        Map<String, String> providerConfig = new HashMap<>();
        

        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, providerConfig, centralSupplier, remoteSupplier
        );
        

        assertThat("Provider should be created", provider, is(notNullValue()));
        assertThat("Should default to NodePort provider", 
                  provider, instanceOf(NodePortNetworkingProvider.class));
    }
    
    // ========== Case Sensitivity Tests ==========
    
    @ParallelTest
    public void testProviderNameIsCaseInsensitive() {

        ClusterOperatorConfig config = createOperatorConfig("NODEPORT");
        Map<String, String> providerConfig = new HashMap<>();
        

        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, providerConfig, centralSupplier, remoteSupplier
        );
        

        assertThat("Provider should be created with uppercase name", 
                  provider, is(notNullValue()));
        assertThat("Provider should be NodePortNetworkingProvider", 
                  provider, instanceOf(NodePortNetworkingProvider.class));
    }
    
    @ParallelTest
    public void testProviderNameWithMixedCase() {

        ClusterOperatorConfig config = createOperatorConfig("LoadBalancer");
        Map<String, String> providerConfig = new HashMap<>();
        

        StretchNetworkingProvider provider = StretchNetworkingProviderFactory.create(
            config, providerConfig, centralSupplier, remoteSupplier
        );
        

        assertThat("Provider should be created with mixed case name", 
                  provider, is(notNullValue()));
        assertThat("Provider should be LoadBalancerNetworkingProvider", 
                  provider, instanceOf(LoadBalancerNetworkingProvider.class));
    }
    
    // ========== Error Handling Tests ==========
    
    @Test
    public void testCreateWithInvalidProviderName() {
        // Arrange & Act & Assert
        // Invalid provider name is rejected during config creation, not factory creation
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            createOperatorConfig("invalid-provider");
        });
        
        assertThat("Exception message should mention invalid provider", 
                  exception.getMessage(), containsString("Invalid STRIMZI_STRETCH_NETWORK_PROVIDER"));
        assertThat("Exception message should mention the invalid name", 
                  exception.getMessage(), containsString("invalid-provider"));
        assertThat("Exception message should list supported providers", 
                  exception.getMessage(), containsString("nodeport"));
    }
    
    @Test
    public void testCreateCustomProviderWithoutClassName() {

        ClusterOperatorConfig config = createOperatorConfigForCustomProvider(null, null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(
                config, providerConfig, centralSupplier, remoteSupplier
            );
        });
        
        assertThat("Exception message should mention missing class name", 
                  exception.getMessage(), containsString("STRIMZI_STRETCH_PLUGIN_CLASS_NAME"));
        assertThat("Exception message should provide example", 
                  exception.getMessage(), containsString("Example:"));
    }
    
    @Test
    public void testCreateCustomProviderWithEmptyClassName() {

        ClusterOperatorConfig config = createOperatorConfigForCustomProvider("", null);
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(
                config, providerConfig, centralSupplier, remoteSupplier
            );
        });
        
        assertThat("Exception message should mention missing class name", 
                  exception.getMessage(), containsString("STRIMZI_STRETCH_PLUGIN_CLASS_NAME"));
    }
    
    @Test
    public void testCreateCustomProviderWithNonExistentClass() {

        ClusterOperatorConfig config = createOperatorConfigForCustomProvider(
            "com.example.NonExistentProvider", null
        );
        Map<String, String> providerConfig = new HashMap<>();
        
        // Act & Assert
        InvalidConfigurationException exception = assertThrows(InvalidConfigurationException.class, () -> {
            StretchNetworkingProviderFactory.create(
                config, providerConfig, centralSupplier, remoteSupplier
            );
        });
        
        assertThat("Exception message should mention class not found", 
                  exception.getMessage(), containsString("class not found"));
        assertThat("Exception message should mention the class name", 
                  exception.getMessage(), containsString("NonExistentProvider"));
    }
    
    // ========== Helper Methods ==========
    
    /**
     * Creates a ClusterOperatorConfig with the specified network provider.
     */
    private ClusterOperatorConfig createOperatorConfig(String networkProvider) {
        ClusterOperatorConfig.ClusterOperatorConfigBuilder builder = 
            new ClusterOperatorConfig.ClusterOperatorConfigBuilder(
                ResourceUtils.dummyClusterOperatorConfig(), 
                KafkaVersionTestUtils.getKafkaVersionLookup())
            .with("STRIMZI_CENTRAL_CLUSTER_ID", CENTRAL_CLUSTER_ID)
            .with("STRIMZI_REMOTE_KUBE_CONFIG", 
                REMOTE_CLUSTER_A_ID + ".url=https://api.cluster-a.example.com:6443\n" +
                REMOTE_CLUSTER_A_ID + ".secret=cluster-a-kubeconfig");
        
        if (networkProvider != null) {
            builder.with("STRIMZI_STRETCH_NETWORK_PROVIDER", networkProvider);
        }
        
        return builder.build();
    }
    
    /**
     * Creates a ClusterOperatorConfig for custom provider testing.
     */
    private ClusterOperatorConfig createOperatorConfigForCustomProvider(String className, String classPath) {
        ClusterOperatorConfig.ClusterOperatorConfigBuilder builder = 
            new ClusterOperatorConfig.ClusterOperatorConfigBuilder(
                ResourceUtils.dummyClusterOperatorConfig(), 
                KafkaVersionTestUtils.getKafkaVersionLookup())
            .with("STRIMZI_CENTRAL_CLUSTER_ID", CENTRAL_CLUSTER_ID)
            .with("STRIMZI_REMOTE_KUBE_CONFIG", 
                REMOTE_CLUSTER_A_ID + ".url=https://api.cluster-a.example.com:6443\n" +
                REMOTE_CLUSTER_A_ID + ".secret=cluster-a-kubeconfig")
            .with("STRIMZI_STRETCH_NETWORK_PROVIDER", "custom");
        
        if (className != null) {
            builder.with("STRIMZI_STRETCH_PLUGIN_CLASS_NAME", className);
        }
        if (classPath != null) {
            builder.with("STRIMZI_STRETCH_PLUGIN_CLASS_PATH", classPath);
        }
        
        return builder.build();
    }
}
