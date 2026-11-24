/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.model;

/**
 * Evaluation result for a single consumed topic-partition.
 */
public record PartitionEvaluation(
        String topic,
        int partition,
        String owner,
        String clientId,
        StatusCode status,
        CommitSample start,
        CommitSample end,
        long currentLag,
        float complete
) {
}
