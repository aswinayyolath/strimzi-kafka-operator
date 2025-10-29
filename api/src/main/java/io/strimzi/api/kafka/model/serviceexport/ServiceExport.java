/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.serviceexport;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.fabric8.kubernetes.api.model.Namespaced;
import io.fabric8.kubernetes.client.CustomResource;
import io.fabric8.kubernetes.model.annotation.Group;
import io.fabric8.kubernetes.model.annotation.Version;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.crdgenerator.annotations.Crd;
import io.sundr.builder.annotations.Buildable;
import io.sundr.builder.annotations.BuildableReference;

import java.util.List;


@JsonDeserialize
@Crd(
    spec = @Crd.Spec(
        names = @Crd.Spec.Names(
            kind = ServiceExport.RESOURCE_KIND,
            plural = ServiceExport.RESOURCE_PLURAL,
            shortNames = {ServiceExport.SHORT_NAME}
        ),
        group = ServiceExport.RESOURCE_GROUP,
        scope = ServiceExport.SCOPE,
        versions = {
            @Crd.Spec.Version(name = ServiceExport.V1ALPHA1, served = true, storage = false)
        },
        subresources = @Crd.Spec.Subresources(
            status = @Crd.Spec.Subresources.Status()
        ),
        additionalPrinterColumns = {
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Ready",
                description = "The state of the custom resource",
                jsonPath = ".status.conditions[?(@.type==\"Ready\")].status",
                type = "string"),
            @Crd.Spec.AdditionalPrinterColumn(
                name = "Valid",
                description = "Validity of the custom resource",
                jsonPath = ".status.conditions[?(@.type==\"Valid\")].status",
                type = "string")
        }
    )
)
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API,
        refs = {@BuildableReference(CustomResource.class)}
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@Version(ServiceExport.VERSION)
@Group(ServiceExport.GROUP)
public class ServiceExport extends CustomResource<ServiceExportSpec, ServiceExportStatus> implements Namespaced {
    public static final String GROUP = "multicluster.x-k8s.io";
    public static final String VERSION = "v1alpha1";
    public static final String V1ALPHA1 = Constants.V1ALPHA1;

    public static final String SCOPE = "Namespaced";
    public static final String RESOURCE_KIND = "ServiceExport";
    public static final String RESOURCE_LIST_KIND = RESOURCE_KIND + "List";
    public static final String RESOURCE_GROUP = Constants.RESOURCE_GROUP_NAME;
    public static final String RESOURCE_PLURAL = "serviceexports";
    public static final String RESOURCE_SINGULAR = "serviceexport";
    public static final String CRD_NAME = RESOURCE_PLURAL + "." + RESOURCE_GROUP;
    public static final String SHORT_NAME = "svcex";
    public static final List<String> RESOURCE_SHORTNAMES = List.of(SHORT_NAME);

}
