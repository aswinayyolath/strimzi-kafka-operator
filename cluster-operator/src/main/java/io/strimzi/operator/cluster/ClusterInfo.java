/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster;

/**
 * Represents information about a Kubernetes cluster.
 * Includes the cluster URL and the secret containing credentials for access.
 */
public class ClusterInfo {
    private String url;     // Kubernetes cluster URL
    private String secret;  // Secret containing the credentials

    /**
     * Constructs a new ClusterInfo instance with the specified URL and secret.
     *
     * @param url    The URL of the Kubernetes cluster.
     * @param secret The name of the secret containing the credentials for accessing the cluster.
     */
    public ClusterInfo(String url, String secret) {
        this.url = url;
        this.secret = secret;
    }

    /**
     * Gets the Kubernetes cluster URL.
     *
     * @return The URL of the Kubernetes cluster.
     */
    public String getUrl() {
        return url;
    }

    /**
     * Gets the name of the secret containing the credentials for accessing the cluster.
     *
     * @return The name of the secret.
     */
    public String getSecret() {
        return secret;
    }
}