/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.exporter.labels;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.config.ClusterSpecValidator;
import io.karasu.core.spi.MetricLabelEnricher;
import io.karasu.protocol.model.ClusterTargetKey;
import io.prometheus.metrics.model.snapshots.Labels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * Merges and normalizes Karasu's 3-layer extensible Prometheus label model:
 * <ol>
 *   <li>Mandatory built-in labels: {@code k8s_cluster}, {@code kafka_cluster}</li>
 *   <li>L1 global {@code common-labels}</li>
 *   <li>L2 per-cluster {@code labels}</li>
 *   <li>L3 programmatic {@link MetricLabelEnricher} SPI beans</li>
 * </ol>
 * Guarantees a uniform label key schema across every data point in a Prometheus metric family.
 */
public final class LabelNormalizer {

    private final Map<String, String> commonLabels;
    private final List<String> declaredExtraLabelKeys;
    private final int maxExtraLabels;
    private final List<MetricLabelEnricher> enrichers;

    public LabelNormalizer(Map<String, String> commonLabels,
                           List<String> declaredExtraLabelKeys,
                           int maxExtraLabels,
                           List<MetricLabelEnricher> enrichers) {
        this.commonLabels = commonLabels == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(commonLabels));
        this.declaredExtraLabelKeys = declaredExtraLabelKeys == null ? List.of() : List.copyOf(declaredExtraLabelKeys);
        this.maxExtraLabels = Math.max(0, maxExtraLabels);
        this.enrichers = enrichers == null ? List.of() : List.copyOf(enrichers);

        ClusterSpecValidator.validateExtraLabels(this.commonLabels, this.maxExtraLabels, "karasu.metrics.common-labels");
        for (String declared : this.declaredExtraLabelKeys) {
            ClusterSpecValidator.validateExtraLabels(Map.of(declared, ""), this.maxExtraLabels, "karasu.metrics.declared-extra-label-keys");
        }
    }

    /**
     * Resolves merged extra labels for one cluster (L1 < L2 < L3) and validates against reserved collisions.
     */
    public Map<String, String> resolveClusterExtraLabels(ClusterSpec spec) {
        Map<String, String> merged = new LinkedHashMap<>(commonLabels);
        merged.putAll(spec.labels());
        for (MetricLabelEnricher enricher : enrichers) {
            Map<String, String> dynamic = enricher.enrichClusterLabels(spec);
            if (dynamic != null && !dynamic.isEmpty()) {
                merged.putAll(dynamic);
            }
        }
        ClusterSpecValidator.validateExtraLabels(merged, maxExtraLabels, spec.targetKey().canonicalId());
        return merged;
    }

    /**
     * Resolves merged extra labels for a specific consumer group (cluster labels + L3 group enrichers).
     */
    public Map<String, String> resolveGroupExtraLabels(ClusterSpec spec,
                                                       Map<String, String> clusterExtra,
                                                       String consumerGroup) {
        if (enrichers.isEmpty()) {
            return clusterExtra;
        }
        Map<String, String> merged = null;
        for (MetricLabelEnricher enricher : enrichers) {
            Map<String, String> dynamic = enricher.enrichGroupLabels(spec, consumerGroup);
            if (dynamic != null && !dynamic.isEmpty()) {
                if (merged == null) {
                    merged = new LinkedHashMap<>(clusterExtra);
                }
                merged.putAll(dynamic);
            }
        }
        if (merged == null) {
            return clusterExtra;
        }
        ClusterSpecValidator.validateExtraLabels(merged, maxExtraLabels, spec.targetKey().canonicalId() + "/" + consumerGroup);
        return merged;
    }

    /**
     * Computes the deterministic sorted union of declared and observed extra label keys across snapshots
     * so every data point in a metric family shares an identical label schema.
     */
    public List<String> computeUnifiedExtraKeys(List<Map<String, String>> observedMaps) {
        TreeSet<String> keys = new TreeSet<>(declaredExtraLabelKeys);
        for (Map<String, String> map : observedMaps) {
            keys.addAll(map.keySet());
        }
        return new ArrayList<>(keys);
    }

    /**
     * Builds immutable Prometheus {@link Labels} combining mandatory {@code k8s_cluster}, {@code kafka_cluster},
     * fixed metric-specific pairs, and normalized extra labels (filling {@code ""} when absent).
     */
    public Labels buildLabels(ClusterTargetKey targetKey,
                              String[] fixedNames,
                              String[] fixedValues,
                              List<String> unifiedExtraKeys,
                              Map<String, String> extraValues) {
        int total = 2 + fixedNames.length + unifiedExtraKeys.size();
        String[] names = new String[total];
        String[] values = new String[total];

        int idx = 0;
        names[idx] = "k8s_cluster";
        values[idx++] = targetKey.k8sCluster();
        names[idx] = "kafka_cluster";
        values[idx++] = targetKey.kafkaCluster();

        for (int i = 0; i < fixedNames.length; i++) {
            names[idx] = fixedNames[i];
            values[idx++] = fixedValues[i];
        }
        for (String extraKey : unifiedExtraKeys) {
            names[idx] = extraKey;
            values[idx++] = extraValues.getOrDefault(extraKey, "");
        }
        return Labels.of(names, values);
    }
}
