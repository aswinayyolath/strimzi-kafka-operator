/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.operator.cluster.PlatformFeaturesAvailability;
import io.strimzi.operator.cluster.RemoteClientSupplier;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.platform.KubernetesVersion;
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for RemoteResourceOperatorSupplier.
 * 
 * This test class validates:
 * - Constructor parameter validation
 * - ResourceOperatorSupplier creation for remote clusters
 * - Retrieval of operators by cluster ID
 * 
 * Test Strategy:
 * - Uses MockKube3 for Kubernetes client mocking
 * - Tests constructor validation
 * - Tests operator initialization
 * - Tests operator retrieval
 * 
 * @see RemoteResourceOperatorSupplier
 */
@Tag("stretch-cluster")
@Tag("stretch-unit")

@ExtendWith(VertxExtension.class)
public class RemoteResourceOperatorSupplierTest {
    
    private static final String CENTRAL_CLUSTER_ID = "central";
    private static final String REMOTE_CLUSTER_A_ID = "cluster-a";
    private static final String REMOTE_CLUSTER_B_ID = "cluster-b";
    private static final String OPERATOR_NAME = "test-operator";
    
    private static Vertx vertx;
    private static MockKube3 mockKubeCentral;
    private static MockKube3 mockKubeRemoteA;
    private static MockKube3 mockKubeRemoteB;
    
    private KubernetesClient centralClient;
    private KubernetesClient remoteClientA;
    private KubernetesClient remoteClientB;
    
    @BeforeAll
    public static void beforeAll() {
        vertx = Vertx.vertx();
        
        // Create mock Kubernetes clusters once for all tests
        mockKubeCentral = new MockKube3.MockKube3Builder().build();
        mockKubeCentral.start();
        
        mockKubeRemoteA = new MockKube3.MockKube3Builder().build();
        mockKubeRemoteA.start();
        
        mockKubeRemoteB = new MockKube3.MockKube3Builder().build();
        mockKubeRemoteB.start();
    }
    
    @AfterAll
    public static void afterAll() {
        // Stop mock clusters
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
        // Get fresh clients for each test
        centralClient = mockKubeCentral.client();
        remoteClientA = mockKubeRemoteA.client();
        remoteClientB = mockKubeRemoteB.client();
    }
    
    // ========== Constructor Validation Tests ==========
    
    @Test
    public void testConstructorWithNullRemoteClientSupplier() {
 
        RemoteClientSupplier remoteClientSupplier = null;
        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new RemoteResourceOperatorSupplier(
                vertx,
                centralClient,
                remoteClientSupplier,
                remotePfas,
                OPERATOR_NAME,
                CENTRAL_CLUSTER_ID
            );
        });
        
        assertThat("Exception message should mention remoteClientSupplier",
                  exception.getMessage().contains("remoteClientSupplier"));
    }
    
    @Test
    public void testConstructorWithNullCentralClusterId() {
 
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        Map<String, PlatformFeaturesAvailability> remotePfas = createRemotePfas();
        

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new RemoteResourceOperatorSupplier(
                vertx,
                centralClient,
                remoteClientSupplier,
                remotePfas,
                OPERATOR_NAME,
                null
            );
        });
        
        assertThat("Exception message should mention centralClusterId",
                  exception.getMessage().contains("centralClusterId"));
    }
    
    @Test
    public void testConstructorWithEmptyCentralClusterId() {
 
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        Map<String, PlatformFeaturesAvailability> remotePfas = createRemotePfas();
        

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            new RemoteResourceOperatorSupplier(
                vertx,
                centralClient,
                remoteClientSupplier,
                remotePfas,
                OPERATOR_NAME,
                ""
            );
        });
        
        assertThat("Exception message should mention centralClusterId",
                  exception.getMessage().contains("centralClusterId"));
    }
    
    // ========== Initialization Tests ==========
    
    
    public void testSuccessfulInitialization() {
 
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        Map<String, PlatformFeaturesAvailability> remotePfas = createRemotePfas();
        
 
        RemoteResourceOperatorSupplier supplier = new RemoteResourceOperatorSupplier(
            vertx,
            centralClient,
            remoteClientSupplier,
            remotePfas,
            OPERATOR_NAME,
            CENTRAL_CLUSTER_ID
        );
        

        assertThat("Supplier should be created", supplier, is(notNullValue()));
        assertThat("RemoteResourceOperators map should contain 2 entries", 
                  supplier.remoteResourceOperators.size(), is(2));
    }
    
    
    public void testRemoteResourceOperatorsCreated() {
 
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        Map<String, PlatformFeaturesAvailability> remotePfas = createRemotePfas();
        
 
        RemoteResourceOperatorSupplier supplier = new RemoteResourceOperatorSupplier(
            vertx,
            centralClient,
            remoteClientSupplier,
            remotePfas,
            OPERATOR_NAME,
            CENTRAL_CLUSTER_ID
        );
        

        assertThat("RemoteResourceOperators should contain cluster A", 
                  supplier.remoteResourceOperators.containsKey(REMOTE_CLUSTER_A_ID), is(true));
        assertThat("RemoteResourceOperators should contain cluster B", 
                  supplier.remoteResourceOperators.containsKey(REMOTE_CLUSTER_B_ID), is(true));
        assertThat("ResourceOperatorSupplier for cluster A should not be null", 
                  supplier.remoteResourceOperators.get(REMOTE_CLUSTER_A_ID), is(notNullValue()));
        assertThat("ResourceOperatorSupplier for cluster B should not be null", 
                  supplier.remoteResourceOperators.get(REMOTE_CLUSTER_B_ID), is(notNullValue()));
    }
    
    
    public void testInitializationWithNoRemoteClusters() {
 
        RemoteClientSupplier emptyRemoteClientSupplier = new RemoteClientSupplier("test-ns", new HashMap<>(), new HashMap<>());
        Map<String, PlatformFeaturesAvailability> remotePfas = new HashMap<>();
        
 
        RemoteResourceOperatorSupplier supplier = new RemoteResourceOperatorSupplier(
            vertx,
            centralClient,
            emptyRemoteClientSupplier,
            remotePfas,
            OPERATOR_NAME,
            CENTRAL_CLUSTER_ID
        );
        

        assertThat("Supplier should be created", supplier, is(notNullValue()));
        assertThat("RemoteResourceOperators should be empty", 
                  supplier.remoteResourceOperators.size(), is(0));
    }
    
    // ========== Retrieval Tests ==========
    
    
    public void testGetRemoteResourceOperatorSupplier() {
 
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        Map<String, PlatformFeaturesAvailability> remotePfas = createRemotePfas();
        RemoteResourceOperatorSupplier supplier = new RemoteResourceOperatorSupplier(
            vertx,
            centralClient,
            remoteClientSupplier,
            remotePfas,
            OPERATOR_NAME,
            CENTRAL_CLUSTER_ID
        );
        
 
        ResourceOperatorSupplier clusterASupplier = supplier.get(REMOTE_CLUSTER_A_ID);
        ResourceOperatorSupplier clusterBSupplier = supplier.get(REMOTE_CLUSTER_B_ID);
        

        assertThat("Cluster A supplier should not be null", clusterASupplier, is(notNullValue()));
        assertThat("Cluster B supplier should not be null", clusterBSupplier, is(notNullValue()));
        assertThat("Cluster A supplier should have pod operations", 
                  clusterASupplier.podOperations, is(notNullValue()));
        assertThat("Cluster B supplier should have service operations", 
                  clusterBSupplier.serviceOperations, is(notNullValue()));
    }
    
    
    public void testGetNonExistentCluster() {
 
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        Map<String, PlatformFeaturesAvailability> remotePfas = createRemotePfas();
        RemoteResourceOperatorSupplier supplier = new RemoteResourceOperatorSupplier(
            vertx,
            centralClient,
            remoteClientSupplier,
            remotePfas,
            OPERATOR_NAME,
            CENTRAL_CLUSTER_ID
        );
        
 
        ResourceOperatorSupplier nonExistent = supplier.get("non-existent-cluster");
        

        assertThat("Non-existent cluster should return null", nonExistent, is(nullValue()));
    }
    
    
    public void testGetCentralCluster() {
 
        RemoteClientSupplier remoteClientSupplier = createRemoteClientSupplier();
        Map<String, PlatformFeaturesAvailability> remotePfas = createRemotePfas();
        RemoteResourceOperatorSupplier supplier = new RemoteResourceOperatorSupplier(
            vertx,
            centralClient,
            remoteClientSupplier,
            remotePfas,
            OPERATOR_NAME,
            CENTRAL_CLUSTER_ID
        );
        
 
        ResourceOperatorSupplier centralSupplier = supplier.get(CENTRAL_CLUSTER_ID);
        

        // Central cluster is not in remoteResourceOperators, only in serviceExportOperators
        assertThat("Central cluster should not be in remote operators", 
                  centralSupplier, is(nullValue()));
    }
    
    // ========== Helper Methods ==========
    
    /**
     * Creates a RemoteClientSupplier with two remote clusters.
     */
    private RemoteClientSupplier createRemoteClientSupplier() {
        Map<String, KubernetesClient> remoteClients = new HashMap<>();
        remoteClients.put(REMOTE_CLUSTER_A_ID, remoteClientA);
        remoteClients.put(REMOTE_CLUSTER_B_ID, remoteClientB);
        
        Map<String, io.strimzi.operator.cluster.ClusterInfo> remoteClusters = new HashMap<>();
        remoteClusters.put(REMOTE_CLUSTER_A_ID, 
            new io.strimzi.operator.cluster.ClusterInfo(REMOTE_CLUSTER_A_ID, "https://api.cluster-a.example.com:6443", "cluster-a-kubeconfig"));
        remoteClusters.put(REMOTE_CLUSTER_B_ID, 
            new io.strimzi.operator.cluster.ClusterInfo(REMOTE_CLUSTER_B_ID, "https://api.cluster-b.example.com:6443", "cluster-b-kubeconfig"));
        
        return new RemoteClientSupplier("test-ns", remoteClusters, remoteClients);
    }
    
    /**
     * Creates PlatformFeaturesAvailability map for remote clusters.
     */
    private Map<String, PlatformFeaturesAvailability> createRemotePfas() {
        Map<String, PlatformFeaturesAvailability> pfas = new HashMap<>();
        pfas.put(REMOTE_CLUSTER_A_ID, 
                new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));
        pfas.put(REMOTE_CLUSTER_B_ID, 
                new PlatformFeaturesAvailability(false, KubernetesVersion.MINIMAL_SUPPORTED_VERSION));
        return pfas;
    }
}
