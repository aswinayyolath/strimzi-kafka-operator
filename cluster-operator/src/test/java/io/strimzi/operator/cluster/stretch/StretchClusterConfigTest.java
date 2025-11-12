/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.strimzi.operator.cluster.ClusterInfo;
import io.strimzi.operator.common.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for StretchClusterConfig parsing and validation.
 * These tests were moved from ClusterOperatorConfigTest to keep stretch-specific tests isolated.
 */
public class StretchClusterConfigTest {

    private static final String VALID_CLUSTER_A_URL = "https://api.cluster-a.com";
    private static final String VALID_CLUSTER_B_URL = "https://api.cluster-b.com";
    private static final String VALID_CLUSTER_A_SECRET = "secret-a";
    private static final String VALID_CLUSTER_B_SECRET = "secret-b";

    @Test
    public void testValidRemoteClusterConfigParsing() {
        String kubeConfig = String.join("\n",
            "cluster-a.url=" + VALID_CLUSTER_A_URL,
            "cluster-a.secret=" + VALID_CLUSTER_A_SECRET,
            "cluster-b.url=" + VALID_CLUSTER_B_URL,
            "cluster-b.secret=" + VALID_CLUSTER_B_SECRET
        );

        Map<String, ClusterInfo> remoteClusters = 
            StretchClusterConfig.parseRemoteClusterConfigs(kubeConfig);

        assertEquals(2, remoteClusters.size());

        ClusterInfo clusterA = remoteClusters.get("cluster-a");
        assertNotNull(clusterA);
        assertEquals(VALID_CLUSTER_A_URL, clusterA.apiUrl());
        assertEquals(VALID_CLUSTER_A_SECRET, clusterA.secretName());

        ClusterInfo clusterB = remoteClusters.get("cluster-b");
        assertNotNull(clusterB);
        assertEquals(VALID_CLUSTER_B_URL, clusterB.apiUrl());
        assertEquals(VALID_CLUSTER_B_SECRET, clusterB.secretName());
    }

    @Test
    public void testEmptyRemoteClusterConfig() {
        Map<String, ClusterInfo> remoteClusters = 
            StretchClusterConfig.parseRemoteClusterConfigs("");
        assertTrue(remoteClusters.isEmpty());
    }

    @Test
    public void testMissingRemoteClusterConfig() {
        Map<String, ClusterInfo> remoteClusters = 
            StretchClusterConfig.parseRemoteClusterConfigs(null);
        assertTrue(remoteClusters.isEmpty());
    }

    @Test
    public void testInvalidSingleLine() {
        String invalidConfig = "invalid-line-without-equals";
        assertThrows(InvalidConfigurationException.class, () -> {
            StretchClusterConfig.parseRemoteClusterConfigs(invalidConfig);
        });
    }

    @Test
    public void testMissingSecretForCluster() {
        String config = "cluster-a.url=" + VALID_CLUSTER_A_URL;
        assertThrows(InvalidConfigurationException.class, () -> 
            StretchClusterConfig.parseRemoteClusterConfigs(config));
    }

    @Test
    public void testMissingUrlForCluster() {
        String config = "cluster-a.secret=" + VALID_CLUSTER_A_SECRET;
        assertThrows(InvalidConfigurationException.class, () -> 
            StretchClusterConfig.parseRemoteClusterConfigs(config));
    }

    @Test
    public void testMultipleEqualsSignsInLine() {
        String invalidConfig = "cluster-a.url=https://api.cluster-a.com=extra";
        assertThrows(InvalidConfigurationException.class, () -> 
            StretchClusterConfig.parseRemoteClusterConfigs(invalidConfig));
    }

    @Test
    public void testMissingDotInKey() {
        String invalidConfig = "clusteraurl=https://api.cluster-a.com";
        assertThrows(InvalidConfigurationException.class, () -> 
            StretchClusterConfig.parseRemoteClusterConfigs(invalidConfig));
    }

    @Test
    public void testClusterIdWithDot() {
        String invalidConfig = "cluster.a.url=https://api.cluster-a.com";
        assertThrows(InvalidConfigurationException.class, () -> 
            StretchClusterConfig.parseRemoteClusterConfigs(invalidConfig));
    }

    @Test
    public void testInvalidUrlFormat() {
        String invalidConfig = String.join("\n",
            "cluster-a.url=not-a-valid-url",
            "cluster-a.secret=" + VALID_CLUSTER_A_SECRET
        );
        assertThrows(InvalidConfigurationException.class, () -> 
            StretchClusterConfig.parseRemoteClusterConfigs(invalidConfig));
    }

    @Test
    public void testUnknownFieldInCluster() {
        String invalidConfig = String.join("\n",
            "cluster-a.unknown=value",
            "cluster-a.url=" + VALID_CLUSTER_A_URL,
            "cluster-a.secret=" + VALID_CLUSTER_A_SECRET
        );
        assertThrows(InvalidConfigurationException.class, () -> 
            StretchClusterConfig.parseRemoteClusterConfigs(invalidConfig));
    }

    @Test
    public void testDuplicateClusterKeyThrows() {
        String invalidConfig = String.join("\n",
            "cluster-a.url=" + VALID_CLUSTER_A_URL,
            "cluster-a.secret=" + VALID_CLUSTER_A_SECRET,
            "cluster-a.secret=another-secret"
        );
        assertThrows(InvalidConfigurationException.class, () -> 
            StretchClusterConfig.parseRemoteClusterConfigs(invalidConfig));
    }

    @Test
    public void testPartialConfigsForMultipleClustersThrows() {
        String invalidConfig = String.join("\n",
            "cluster-a.url=" + VALID_CLUSTER_A_URL,
            "cluster-b.secret=" + VALID_CLUSTER_B_SECRET
        );
        assertThrows(InvalidConfigurationException.class, () -> 
            StretchClusterConfig.parseRemoteClusterConfigs(invalidConfig));
    }

    @Test
    public void testConfigWithExtraWhitespace() {
        String config = String.join("\n",
            "   cluster-a.url   =   " + VALID_CLUSTER_A_URL + "   ",
            "   cluster-a.secret   =   " + VALID_CLUSTER_A_SECRET + "   "
        );
        
        Map<String, ClusterInfo> clusters = 
            StretchClusterConfig.parseRemoteClusterConfigs(config);
        assertEquals(1, clusters.size());

        ClusterInfo clusterA = clusters.get("cluster-a");
        assertNotNull(clusterA);
        assertEquals(VALID_CLUSTER_A_URL, clusterA.apiUrl());
        assertEquals(VALID_CLUSTER_A_SECRET, clusterA.secretName());
    }
}
