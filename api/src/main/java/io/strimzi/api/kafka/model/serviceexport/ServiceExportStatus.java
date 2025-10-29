/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.serviceexport;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.strimzi.api.kafka.model.kafka.Status;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class ServiceExportStatus extends Status {
    
}
