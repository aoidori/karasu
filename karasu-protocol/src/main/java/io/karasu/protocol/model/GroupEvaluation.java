/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.model;

import java.util.List;

/**
 * Aggregated evaluation result for a consumer group across all tracked partitions.
 * {@code lagTotalComplete} is {@code false} if any committed partition for this group was dropped
 * by cardinality limits, lacks broker head-offset metadata, or the group has no committed partitions yet.
 */
public record GroupEvaluation(
        ClusterTargetKey targetKey,
        String group,
        StatusCode status,
        float complete,
        List<PartitionEvaluation> partitions,
        int totalPartitions,
        PartitionEvaluation maxLag,
        long totalLag,
        boolean lagTotalComplete
) {
}
