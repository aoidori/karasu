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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Guarantees uniform label schemas across every data point in a metric family, supports pre-activation
 * merged validation (#12), and isolates per-cluster runtime enricher errors so one cluster never breaks scrape.
 */
public final class LabelNormalizer {

    private static final Logger log = LoggerFactory.getLogger(LabelNormalizer.class);

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
     * Validates merged L1 + L2 + L3 cluster labels BEFORE a cluster spec is activated (#12).
     * Throws {@link IllegalArgumentException} if merged label count exceeds {@code maxExtraLabels}
     * or any key is invalid/reserved.
     */
    public void validateMergedClusterSpec(ClusterSpec spec) {
        resolveClusterExtraLabelsStrict(spec);
    }

    /**
     * Strict merged cluster label resolution (throws on limit/validation violation).
     */
    public Map<String, String> resolveClusterExtraLabelsStrict(ClusterSpec spec) {
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
     * Scrape-safe cluster label resolution (#12): if a dynamic enricher or unvalidated spec ever exceeds
     * label limits at scrape time, falls back to validated {@code commonLabels} instead of aborting the whole scrape.
     */
    public Map<String, String> resolveClusterExtraLabelsSafe(ClusterSpec spec) {
        try {
            return resolveClusterExtraLabelsStrict(spec);
        } catch (RuntimeException ex) {
            log.error("Isolating label validation failure for cluster {} during scrape (falling back to commonLabels): {}",
                    spec.targetKey().canonicalId(), ex.getMessage());
            return commonLabels;
        }
    }

    /**
     * Scrape-safe group label resolution (#12).
     */
    public Map<String, String> resolveGroupExtraLabelsSafe(ClusterSpec spec,
                                                           Map<String, String> clusterExtra,
                                                           String consumerGroup) {
        if (enrichers.isEmpty()) {
            return clusterExtra;
        }
        try {
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
        } catch (RuntimeException ex) {
            log.error("Isolating group label validation failure for {}/{} during scrape (falling back to cluster labels): {}",
                    spec.targetKey().canonicalId(), consumerGroup, ex.getMessage());
            return clusterExtra;
        }
    }

    public List<String> computeUnifiedExtraKeys(List<Map<String, String>> observedMaps) {
        TreeSet<String> keys = new TreeSet<>(declaredExtraLabelKeys);
        for (Map<String, String> map : observedMaps) {
            keys.addAll(map.keySet());
        }
        return new ArrayList<>(keys);
    }

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
