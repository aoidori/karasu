/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.snapshot;

import io.karasu.core.config.ClusterSpec;
import io.karasu.protocol.model.ClusterHealthSnapshot;
import io.karasu.protocol.model.ClusterTargetKey;
import io.karasu.protocol.model.GroupEvaluation;

import java.util.List;
import java.util.Map;

/**
 * Immutable point-in-time snapshot published atomically by each {@code ClusterRuntime}.
 * Prometheus scrape threads read these snapshots without acquiring any storage or evaluation lock.
 */
public record ClusterMetricsSnapshot(
        ClusterTargetKey targetKey,
        ClusterSpec spec,
        ClusterHealthSnapshot health,
        List<GroupEvaluation> groups,
        Map<String, long[]> topicPartitionOffsets,
        Map<DecodeErrorKey, Long> decodeErrorCounts,
        Map<String, Long> reconcileErrorCounts,
        DurationHistogramSnapshot snapshotBuildDurationHistogram,
        long builtAtEpochMs
) {
    public record DecodeErrorKey(String recordType, String reason) {
    }

    /**
     * Immutable Prometheus classic histogram snapshot (upperBounds + non-cumulative bucket counts + sum).
     */
    public record DurationHistogramSnapshot(
            double[] upperBounds,
            long[] bucketCounts,
            double sumSeconds
    ) {
        public static final double[] DEFAULT_BUCKETS = new double[]{
                0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, Double.POSITIVE_INFINITY
        };

        public static DurationHistogramSnapshot empty() {
            return new DurationHistogramSnapshot(DEFAULT_BUCKETS, new long[DEFAULT_BUCKETS.length], 0.0);
        }
    }
}
