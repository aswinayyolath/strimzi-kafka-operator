/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.stretch;

import io.strimzi.test.annotations.ParallelSuite;
import io.strimzi.test.annotations.ParallelTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for ListenerReconciliationResult.
 * 
 * This test class validates:
 * - Constructor with various parameter combinations
 * - Null parameter handling (should use empty defaults)
 * - Getter methods for all fields
 * - isEmpty() logic
 * - toString() output
 * - Defensive copying (returned collections should be copies)
 * 
 * Test Strategy:
 * - Tests data class behavior
 * - Tests null safety
 * - Tests immutability guarantees
 * 
 * @see ListenerReconciliationResult
 */
@Tag("stretch-cluster")
@Tag("stretch-unit")
@ParallelSuite
public class ListenerReconciliationResultTest {
    
    // ========== Constructor Tests ==========
    
    @ParallelTest
    public void testConstructorWithAllParameters() {

        Map<Integer, String> advertisedListeners = createAdvertisedListeners();
        Map<Integer, Map<String, String>> advertisedHostnames = createAdvertisedHostnames();
        Map<Integer, Map<String, String>> advertisedPorts = createAdvertisedPorts();
        String quorumVoters = "0@broker-0:9091,1@broker-1:9091";
        Map<String, Set<String>> bootstrapDnsNames = createBootstrapDnsNames();
        Map<Integer, Set<String>> brokerDnsNames = createBrokerDnsNames();
        
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            advertisedListeners,
            advertisedHostnames,
            advertisedPorts,
            quorumVoters,
            bootstrapDnsNames,
            brokerDnsNames
        );
        
        assertThat("Advertised listeners should be set", 
                  result.getAllAdvertisedListeners().size(), is(2));
        assertThat("Advertised hostnames should be set", 
                  result.getAdvertisedHostnames().size(), is(2));
        assertThat("Advertised ports should be set", 
                  result.getAdvertisedPorts().size(), is(2));
        assertThat("Quorum voters should be set", 
                  result.getQuorumVoters(), is(quorumVoters));
        assertThat("Bootstrap DNS names should be set", 
                  result.getAllBootstrapDnsNames().size(), is(2));
        assertThat("Broker DNS names should be set", 
                  result.getAllBrokerDnsNames().size(), is(2));
    }
    
    @ParallelTest
    public void testConstructorWithNullParameters() {
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, null, null
        );
        
        assertThat("Advertised listeners should be empty", 
                  result.getAllAdvertisedListeners(), is(anEmptyMap()));
        assertThat("Advertised hostnames should be empty", 
                  result.getAdvertisedHostnames(), is(anEmptyMap()));
        assertThat("Advertised ports should be empty", 
                  result.getAdvertisedPorts(), is(anEmptyMap()));
        assertThat("Quorum voters should be empty string", 
                  result.getQuorumVoters(), is(""));
        assertThat("Bootstrap DNS names should be empty", 
                  result.getAllBootstrapDnsNames(), is(anEmptyMap()));
        assertThat("Broker DNS names should be empty", 
                  result.getAllBrokerDnsNames(), is(anEmptyMap()));
    }
    
    @ParallelTest
    public void testConstructorWithMixedNullParameters() {
        Map<Integer, String> advertisedListeners = createAdvertisedListeners();
        String quorumVoters = "0@broker-0:9091";
        
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            advertisedListeners,
            null,  // null hostnames
            null,  // null ports
            quorumVoters,
            null,  // null bootstrap DNS
            null   // null broker DNS
        );
        
        assertThat("Advertised listeners should be set", 
                  result.getAllAdvertisedListeners().size(), is(2));
        assertThat("Advertised hostnames should be empty", 
                  result.getAdvertisedHostnames(), is(anEmptyMap()));
        assertThat("Advertised ports should be empty", 
                  result.getAdvertisedPorts(), is(anEmptyMap()));
        assertThat("Quorum voters should be set", 
                  result.getQuorumVoters(), is(quorumVoters));
        assertThat("Bootstrap DNS names should be empty", 
                  result.getAllBootstrapDnsNames(), is(anEmptyMap()));
        assertThat("Broker DNS names should be empty", 
                  result.getAllBrokerDnsNames(), is(anEmptyMap()));
    }
    
    // ========== Advertised Listeners Tests ==========
    
    @ParallelTest
    public void testGetAdvertisedListenersForExistingNode() {
        
        Map<Integer, String> advertisedListeners = createAdvertisedListeners();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            advertisedListeners, null, null, null, null, null
        );
        

        String listeners = result.getAdvertisedListeners(0);
        

        assertThat("Should return listeners for node 0", 
                  listeners, is("REPLICATION-9091://broker-0:9091,PLAIN-9092://broker-0:9092"));
    }
    
    @ParallelTest
    public void testGetAdvertisedListenersForNonExistentNode() {
        
        Map<Integer, String> advertisedListeners = createAdvertisedListeners();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            advertisedListeners, null, null, null, null, null
        );
        

        String listeners = result.getAdvertisedListeners(999);
        

        assertThat("Should return empty string for non-existent node", 
                  listeners, is(""));
    }
    
    @ParallelTest
    public void testGetAllAdvertisedListeners() {
        
        Map<Integer, String> advertisedListeners = createAdvertisedListeners();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            advertisedListeners, null, null, null, null, null
        );
        
        
        Map<Integer, String> allListeners = result.getAllAdvertisedListeners();
        

        assertThat("Should return all listeners", allListeners.size(), is(2));
        assertThat("Should contain node 0", allListeners.containsKey(0), is(true));
        assertThat("Should contain node 1", allListeners.containsKey(1), is(true));
    }
    
    @ParallelTest
    public void testGetAllAdvertisedListenersReturnsDefensiveCopy() {
        
        Map<Integer, String> advertisedListeners = createAdvertisedListeners();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            advertisedListeners, null, null, null, null, null
        );
        
        
        Map<Integer, String> copy1 = result.getAllAdvertisedListeners();
        Map<Integer, String> copy2 = result.getAllAdvertisedListeners();
        copy1.put(999, "modified");
        

        assertThat("Should return different instances", copy1, is(not(copy2)));
        assertThat("Original should not be modified", 
                  result.getAllAdvertisedListeners().containsKey(999), is(false));
    }
    
    // ========== Advertised Hostnames Tests ==========
    
    @ParallelTest
    public void testGetAdvertisedHostnames() {
        
        Map<Integer, Map<String, String>> advertisedHostnames = createAdvertisedHostnames();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, advertisedHostnames, null, null, null, null
        );
        
        
        Map<Integer, Map<String, String>> hostnames = result.getAdvertisedHostnames();
        

        assertThat("Should return hostnames", hostnames.size(), is(2));
        assertThat("Should contain node 0 hostnames", 
                  hostnames.get(0).get("plain"), is("broker-0.example.com"));
    }
    
    // ========== Advertised Ports Tests ==========
    
    @ParallelTest
    public void testGetAdvertisedPorts() {
        
        Map<Integer, Map<String, String>> advertisedPorts = createAdvertisedPorts();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, advertisedPorts, null, null, null
        );
        
        
        Map<Integer, Map<String, String>> ports = result.getAdvertisedPorts();
        

        assertThat("Should return ports", ports.size(), is(2));
        assertThat("Should contain node 0 ports", 
                  ports.get(0).get("plain"), is("9092"));
    }
    
    // ========== Quorum Voters Tests ==========
    
    @ParallelTest
    public void testGetQuorumVoters() {
        
        String quorumVoters = "0@broker-0:9091,1@broker-1:9091,2@broker-2:9091";
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, quorumVoters, null, null
        );
        
        
        String voters = result.getQuorumVoters();
        

        assertThat("Should return quorum voters", voters, is(quorumVoters));
    }
    
    @ParallelTest
    public void testGetQuorumVotersWhenNull() {
        
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, null, null
        );
        
        
        String voters = result.getQuorumVoters();
        

        assertThat("Should return empty string when null", voters, is(""));
    }
    
    // ========== Bootstrap DNS Names Tests ==========
    
    @ParallelTest
    public void testGetBootstrapDnsNamesForExistingCluster() {
        
        Map<String, Set<String>> bootstrapDnsNames = createBootstrapDnsNames();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, bootstrapDnsNames, null
        );
        
        
        Set<String> dnsNames = result.getBootstrapDnsNames("central");
        

        assertThat("Should return DNS names for central cluster", dnsNames, hasSize(2));
        assertThat("Should contain expected DNS name", 
                  dnsNames.contains("kafka-bootstrap.central.svc.clusterset.local"), is(true));
    }
    
    @ParallelTest
    public void testGetBootstrapDnsNamesForNonExistentCluster() {
        
        Map<String, Set<String>> bootstrapDnsNames = createBootstrapDnsNames();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, bootstrapDnsNames, null
        );
        
        
        Set<String> dnsNames = result.getBootstrapDnsNames("non-existent");
        

        assertThat("Should return empty set for non-existent cluster", dnsNames, is(empty()));
    }
    
    @ParallelTest
    public void testGetAllBootstrapDnsNames() {
        
        Map<String, Set<String>> bootstrapDnsNames = createBootstrapDnsNames();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, bootstrapDnsNames, null
        );
        
        
        Map<String, Set<String>> allDnsNames = result.getAllBootstrapDnsNames();
        

        assertThat("Should return all bootstrap DNS names", allDnsNames.size(), is(2));
        assertThat("Should contain central cluster", allDnsNames.containsKey("central"), is(true));
        assertThat("Should contain cluster-a", allDnsNames.containsKey("cluster-a"), is(true));
    }
    
    // ========== Broker DNS Names Tests ==========
    
    @ParallelTest
    public void testGetBrokerDnsNamesForExistingBroker() {
        
        Map<Integer, Set<String>> brokerDnsNames = createBrokerDnsNames();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, null, brokerDnsNames
        );
        
        
        Set<String> dnsNames = result.getBrokerDnsNames(0);
        

        assertThat("Should return DNS names for broker 0", dnsNames, hasSize(2));
        assertThat("Should contain expected DNS name", 
                  dnsNames.contains("broker-0.kafka.svc.clusterset.local"), is(true));
    }
    
    @ParallelTest
    public void testGetBrokerDnsNamesForNonExistentBroker() {
        
        Map<Integer, Set<String>> brokerDnsNames = createBrokerDnsNames();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, null, brokerDnsNames
        );
        
        
        Set<String> dnsNames = result.getBrokerDnsNames(999);
        

        assertThat("Should return empty set for non-existent broker", dnsNames, is(empty()));
    }
    
    @ParallelTest
    public void testGetAllBrokerDnsNames() {
        
        Map<Integer, Set<String>> brokerDnsNames = createBrokerDnsNames();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, null, brokerDnsNames
        );
        
        
        Map<Integer, Set<String>> allDnsNames = result.getAllBrokerDnsNames();
        

        assertThat("Should return all broker DNS names", allDnsNames.size(), is(2));
        assertThat("Should contain broker 0", allDnsNames.containsKey(0), is(true));
        assertThat("Should contain broker 1", allDnsNames.containsKey(1), is(true));
    }
    
    // ========== isEmpty() Tests ==========
    
    @ParallelTest
    public void testIsEmptyWhenEmpty() {
        
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, null, null
        );
        

        assertThat("Should be empty when all parameters are null", result.isEmpty(), is(true));
    }
    
    @ParallelTest
    public void testIsEmptyWhenHasAdvertisedListeners() {
        
        Map<Integer, String> advertisedListeners = createAdvertisedListeners();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            advertisedListeners, null, null, null, null, null
        );
        

        assertThat("Should not be empty when has advertised listeners", 
                  result.isEmpty(), is(false));
    }
    
    @ParallelTest
    public void testIsEmptyWhenHasQuorumVoters() {
        
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, "0@broker-0:9091", null, null
        );
        

        assertThat("Should not be empty when has quorum voters", result.isEmpty(), is(false));
    }
    
    @ParallelTest
    public void testIsEmptyWhenHasBoth() {
        
        Map<Integer, String> advertisedListeners = createAdvertisedListeners();
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            advertisedListeners, null, null, "0@broker-0:9091", null, null
        );
        

        assertThat("Should not be empty when has both", result.isEmpty(), is(false));
    }
    
    // ========== toString() Tests ==========
    
    @Test
    public void testToString() {
        
        Map<Integer, String> advertisedListeners = createAdvertisedListeners();
        String quorumVoters = "0@broker-0:9091,1@broker-1:9091";
        Map<String, Set<String>> bootstrapDnsNames = createBootstrapDnsNames();
        Map<Integer, Set<String>> brokerDnsNames = createBrokerDnsNames();
        
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            advertisedListeners, null, null, quorumVoters, bootstrapDnsNames, brokerDnsNames
        );
        
        
        String str = result.toString();
        

        assertThat("Should contain class name", str, containsString("ListenerReconciliationResult"));
        assertThat("Should contain advertised listeners count", str, containsString("2 brokers"));
        assertThat("Should contain quorum voters info", str, containsString("chars"));
        assertThat("Should contain bootstrap DNS count", str, containsString("2 clusters"));
        assertThat("Should contain broker DNS count", str, containsString("2 brokers"));
    }
    
    @Test
    public void testToStringWhenEmpty() {
        
        ListenerReconciliationResult result = new ListenerReconciliationResult(
            null, null, null, null, null, null
        );
        
        
        String str = result.toString();
        

        assertThat("Should contain class name", str, containsString("ListenerReconciliationResult"));
        assertThat("Should indicate no quorum voters", str, containsString("none"));
    }
    
    // ========== Helper Methods ==========
    
    private Map<Integer, String> createAdvertisedListeners() {
        Map<Integer, String> listeners = new HashMap<>();
        listeners.put(0, "REPLICATION-9091://broker-0:9091,PLAIN-9092://broker-0:9092");
        listeners.put(1, "REPLICATION-9091://broker-1:9091,PLAIN-9092://broker-1:9092");
        return listeners;
    }
    
    private Map<Integer, Map<String, String>> createAdvertisedHostnames() {
        Map<Integer, Map<String, String>> hostnames = new HashMap<>();
        Map<String, String> node0 = new HashMap<>();
        node0.put("plain", "broker-0.example.com");
        node0.put("replication", "broker-0.internal");
        hostnames.put(0, node0);
        
        Map<String, String> node1 = new HashMap<>();
        node1.put("plain", "broker-1.example.com");
        node1.put("replication", "broker-1.internal");
        hostnames.put(1, node1);
        
        return hostnames;
    }
    
    private Map<Integer, Map<String, String>> createAdvertisedPorts() {
        Map<Integer, Map<String, String>> ports = new HashMap<>();
        Map<String, String> node0 = new HashMap<>();
        node0.put("plain", "9092");
        node0.put("replication", "9091");
        ports.put(0, node0);
        
        Map<String, String> node1 = new HashMap<>();
        node1.put("plain", "9092");
        node1.put("replication", "9091");
        ports.put(1, node1);
        
        return ports;
    }
    
    private Map<String, Set<String>> createBootstrapDnsNames() {
        Map<String, Set<String>> dnsNames = new HashMap<>();
        
        Set<String> centralDns = new HashSet<>();
        centralDns.add("kafka-bootstrap.central.svc.clusterset.local");
        centralDns.add("kafka-bootstrap.central.svc.cluster.local");
        dnsNames.put("central", centralDns);
        
        Set<String> clusterADns = new HashSet<>();
        clusterADns.add("kafka-bootstrap.cluster-a.svc.clusterset.local");
        clusterADns.add("kafka-bootstrap.cluster-a.svc.cluster.local");
        dnsNames.put("cluster-a", clusterADns);
        
        return dnsNames;
    }
    
    private Map<Integer, Set<String>> createBrokerDnsNames() {
        Map<Integer, Set<String>> dnsNames = new HashMap<>();
        
        Set<String> broker0Dns = new HashSet<>();
        broker0Dns.add("broker-0.kafka.svc.clusterset.local");
        broker0Dns.add("broker-0.kafka.svc.cluster.local");
        dnsNames.put(0, broker0Dns);
        
        Set<String> broker1Dns = new HashSet<>();
        broker1Dns.add("broker-1.kafka.svc.clusterset.local");
        broker1Dns.add("broker-1.kafka.svc.cluster.local");
        dnsNames.put(1, broker1Dns);
        
        return dnsNames;
    }
}
