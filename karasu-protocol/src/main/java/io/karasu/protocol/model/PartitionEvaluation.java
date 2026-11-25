/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.model;

/**
 * Evaluation result for a single consumed topic-partition.
 * When {@code hasCommit} is {@code false}, no committed offset has been observed for this partition
 * (e.g., only member assignment metadata exists) and {@code currentLag} is {@value #UNKNOWN_LAG}.
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
        float complete,
        boolean hasCommit
) {
    public static final long UNKNOWN_LAG = -1L;

    public boolean hasKnownLag() {
        return hasCommit && currentLag >= 0L;
    }
}
