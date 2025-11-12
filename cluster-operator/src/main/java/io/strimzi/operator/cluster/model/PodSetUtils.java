/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.fabric8.kubernetes.api.model.Pod;
import io.strimzi.api.kafka.model.podset.StrimziPodSet;
import io.strimzi.operator.common.Annotations;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Shared methods for working with StrimziPodSet resources
 */
public class PodSetUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> POD_TYPE = new TypeReference<>() { };

    /**
     * Converts Pod to Map for storing it in StrimziPodSets
     *
     * @param pod   Pod which should be converted
     *
     * @return      Map representing the pod
     */
    public static Map<String, Object> podToMap(Pod pod) {
        return MAPPER.convertValue(pod, POD_TYPE);
    }

    /**
     * Converts Pod to String
     *
     * @param pod   Pod which should be converted
     *
     * @throws JsonProcessingException  Throws JsonProcessingException when the conversion to String fails
     *
     * @return      String with the Pod definition
     */
    public static String podToString(Pod pod) throws JsonProcessingException {
        return MAPPER.writeValueAsString(pod);
    }

    /**
     * Converts List of Pods to List of Maps which can be used in StrimziPodSets
     *
     * @param pods  List of Pods which should be converted
     *
     * @return      List of Maps with the Pod structures
     */
    public static List<Map<String, Object>> podsToMaps(List<Pod> pods)  {
        return pods.stream().map(p -> podToMap(p)).collect(Collectors.toList());
    }

    /**
     * Converts Map to Pod for decoding of StrimziPodSets
     *
     * @param map   Pod represented as Map which should be decoded
     *
     * @return      Pod object decoded from the map
     */
    public static Pod mapToPod(Map<String, Object> map) {
        return MAPPER.convertValue(map, Pod.class);
    }

    /**
     * Converts a PdoSet to a List of Pods. This is useful when extracting information from the Pods in a PodSet
     *
     * @param podSet  PodSet with the Pods
     *
     * @return      List of Pods
     */
    public static List<Pod> podSetToPods(StrimziPodSet podSet)  {
        if (podSet != null
                && podSet.getSpec() != null
                && podSet.getSpec().getPods() != null)   {
            return podSet.getSpec().getPods().stream().map(m -> mapToPod(m)).toList();
        } else {
            return List.of();
        }
    }

    /**
     * Extracts Pod names from a PodSet
     *
     * @param podSet    PodSet to extract the pod names from
     *
     * @return  List of pod names
     */
    public static List<String> podNames(StrimziPodSet podSet)   {
        return podSetToPods(podSet).stream().map(pod -> pod.getMetadata().getName()).toList();
    }

    /**
     * Check whether the Pod reached one of its terminal phases: Succeeded or Failed. This is checked based on
     * the .status.phase field.
     *
     * @param pod   The Pod object
     *
     * @return  True if the Pod is in terminal phase. False otherwise.
     */
    public static boolean isInTerminalState(Pod pod)   {
        return pod.getStatus() != null
                && ("Failed".equals(pod.getStatus().getPhase()) || "Succeeded".equals(pod.getStatus().getPhase()));
    }

    /**
     * Check if the podset is considered "remote" based on specific stretch cluster conditions.
     * A PodSet is remote if:
     *
     * - Stretch cluster env vars (e.g., STRIMZI_REMOTE_KUBE_CONFIG, STRIMZI_CENTRAL_CLUSTER_ID) are NOT configured in the Cluster Operator.
     * - The PodSet has the `strimzi.io/stretch-cluster-alias` annotation.
     * - The value of the `strimzi.io/stretch-cluster-alias` annotation does not match the provided central cluster ID.
     *
     *
     * @param podset The StrimziPodSet object to check.
     * @param isStretchConfigured True if stretch cluster env vars are present and configured in the Cluster Operator.
     * @param centralClusterId The ID of the central cluster from the environment/configuration.
     *
     * @return True if the PodSet is identified as remote under these conditions, false otherwise.
     */
    public static boolean isRemotePodSet(StrimziPodSet podset, boolean isStretchConfigured, String centralClusterId) {
        // Condition 1: Stretch env vars are NOT configured
        if (isStretchConfigured) {
            return false;
        }

        // Condition 2: Stretch cluster alias is defined in PodSet annotation
        if (!Annotations.hasAnnotation(podset, Annotations.ANNO_STRIMZI_STRETCH_CLUSTER_ALIAS)) {
            return false;
        }

        // Condition 3: Central cluster ID does not match Stretch cluster alias
        // We retrieve the alias only if we've confirmed it exists.
        String stretchClusterAlias = ModelUtils.getTargetClusterAlias(podset);

        // At this point, stretchClusterAlias is guaranteed not to be null because Annotations.hasAnnotation returned true,
        // and ModelUtils.getTargetClusterAlias returns null only if the annotation isn't found.
        // Ensure centralClusterId is not null before comparison to prevent NPE.
        if (centralClusterId == null) {
            return true;
        }

        return !stretchClusterAlias.equals(centralClusterId);
    }
}
