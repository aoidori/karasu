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
        double lastSnapshotBuildDurationSeconds,
        long builtAtEpochMs
) {
    public record DecodeErrorKey(String recordType, String reason) {
    }
}
