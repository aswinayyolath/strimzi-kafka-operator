/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.kafka;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the status of a stretch cluster deployment
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"enabled", "networkingProvider", "centralClusterId", "targetClusters"})
@EqualsAndHashCode
@ToString
public class StretchClusterStatus implements UnknownPropertyPreserving, Serializable {
    private static final long serialVersionUID = 1L;

    private Boolean enabled;
    private String networkingProvider;
    private String centralClusterId;
    private String targetClusters;
    private Map<String, Object> additionalProperties;

    @Description("Indicates whether this is a stretch cluster deployment")
    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    @Description("The networking provider used for cross-cluster communication (e.g., 'mcs', 'nodeport')")
    public String getNetworkingProvider() {
        return networkingProvider;
    }

    public void setNetworkingProvider(String networkingProvider) {
        this.networkingProvider = networkingProvider;
    }

    @Description("The ID of the central cluster where the Kafka CR is deployed")
    public String getCentralClusterId() {
        return centralClusterId;
    }

    public void setCentralClusterId(String centralClusterId) {
        this.centralClusterId = centralClusterId;
    }

    @Description("Comma-separated list of target cluster IDs where Kafka brokers are deployed")
    public String getTargetClusters() {
        return targetClusters;
    }

    public void setTargetClusters(String targetClusters) {
        this.targetClusters = targetClusters;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : new HashMap<>(0);
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(1);
        }
        this.additionalProperties.put(name, value);
    }
}
