/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.kubernetes;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.serviceexport.ServiceExport;
import io.strimzi.api.kafka.model.serviceexport.ServiceExportList;
import io.vertx.core.Vertx;

/**
 * Operator for {@code ServiceExport}s
 */
public class ServiceExportOperator extends CrdOperator<KubernetesClient, ServiceExport, ServiceExportList> {

    /**
     * Constructs the ServiceExport operator
     *
     * @param vertx  The Vertx instance.
     * @param client The Kubernetes client.
     */
    public ServiceExportOperator(Vertx vertx, KubernetesClient client) {
        super(vertx, client, ServiceExport.class, ServiceExportList.class, ServiceExport.RESOURCE_KIND);
    }

}
