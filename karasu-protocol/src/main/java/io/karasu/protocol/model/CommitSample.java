/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.protocol.model;

/**
 * Single consumer offset commit sample stored in a partition sliding window.
 * A negative {@code commitLag} represents an absent (null) lag at commit time (e.g. historical backfill).
 */
public record CommitSample(
        long offset,
        long order,
        long commitTimestampMs,
        long observedTimestampMs,
        long commitLag
) {
    public static final long NO_LAG = -1L;

    public boolean hasCommitLag() {
        return commitLag >= 0L;
    }
}
