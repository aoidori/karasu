/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.storage;

import io.karasu.protocol.model.CommitSample;

/**
 * Compact primitive-array ring buffer storing consumer offset commit history for one topic-partition.
 * Zero heap allocations on the hot append path; preserves Burrow's exact order-based insertion
 * and min-distance merge semantics.
 *
 * <p>Thread safety: callers must hold the owning consumer group lock.
 */
final class PartitionCommitRing {

    private final int capacity;
    private final long[] offsets;
    private final long[] orders;
    private final long[] commitTimestamps;
    private final long[] observedTimestamps;
    private final long[] commitLags;

    private int start;
    private int count;
    private String owner = "";
    private String clientId = "";
    private java.util.UUID topicId;

    PartitionCommitRing(int capacity) {
        this.capacity = capacity;
        this.offsets = new long[capacity];
        this.orders = new long[capacity];
        this.commitTimestamps = new long[capacity];
        this.observedTimestamps = new long[capacity];
        this.commitLags = new long[capacity];
    }

    /**
     * Inserts a commit sample ordered by {@code order} (its {@code __consumer_offsets} record offset).
     *
     * @return {@code true} if the sample was appended as the newest commit (caller should update group lastCommit)
     */
    boolean addCommit(long offset,
                      long order,
                      long commitTimestampMs,
                      long observedTimestampMs,
                      long brokerOffset,
                      long minDistanceMs) {
        // Fast path: empty ring or strictly newer than latest sample
        if (count == 0 || order > orders[phys(count - 1)]) {
            long lag = brokerOffset >= 0L ? Math.max(0L, brokerOffset - offset) : CommitSample.NO_LAG;
            if (count > 0 && minDistanceMs > 0L) {
                int lastPhys = phys(count - 1);
                if ((commitTimestampMs - commitTimestamps[lastPhys]) < minDistanceMs) {
                    // Merge frequent commit into previous slot while keeping original commitTimestamp (Burrow semantics)
                    offsets[lastPhys] = offset;
                    orders[lastPhys] = order;
                    observedTimestamps[lastPhys] = observedTimestampMs;
                    commitLags[lastPhys] = lag;
                    return true;
                }
            }
            append(offset, order, commitTimestampMs, observedTimestampMs, lag);
            return true;
        }

        // Duplicate of latest
        if (order == orders[phys(count - 1)]) {
            return false;
        }

        // Full ring and older than or equal to oldest entry -> drop
        if (count == capacity && order <= orders[phys(0)]) {
            return false;
        }

        // Find logical insertion index ins such that orders[ins-1] < order < orders[ins]
        int ins = count - 1;
        while (ins > 0) {
            long prevOrder = orders[phys(ins - 1)];
            if (prevOrder == order) {
                return false;
            }
            if (prevOrder < order) {
                break;
            }
            ins--;
        }
        if (ins == 0 && orders[phys(0)] == order) {
            return false;
        }

        // Check min-distance against predecessor (ins - 1)
        if (ins > 0 && minDistanceMs > 0L) {
            int predPhys = phys(ins - 1);
            if ((commitTimestampMs - commitTimestamps[predPhys]) < minDistanceMs) {
                offsets[predPhys] = offset;
                orders[predPhys] = order;
                observedTimestamps[predPhys] = observedTimestampMs;
                commitLags[predPhys] = CommitSample.NO_LAG;
                return false;
            }
        }

        // Out-of-order/backfill commits do not record a commit-time lag against current broker LEO
        if (count < capacity) {
            if (ins == 0) {
                start = (start - 1 + capacity) % capacity;
                count++;
                writeSlot(start, offset, order, commitTimestampMs, observedTimestampMs, CommitSample.NO_LAG);
            } else {
                // Shift [ins .. count-1] right by 1
                count++;
                for (int i = count - 1; i > ins; i--) {
                    copySlot(phys(i - 1), phys(i));
                }
                writeSlot(phys(ins), offset, order, commitTimestampMs, observedTimestampMs, CommitSample.NO_LAG);
            }
        } else {
            // Ring is full and ins > 0 (since ins == 0 was rejected above):
            // Drop oldest (index 0) and shift [1 .. ins-1] left by 1, then write at ins - 1
            for (int i = 0; i < ins - 1; i++) {
                copySlot(phys(i + 1), phys(i));
            }
            writeSlot(phys(ins - 1), offset, order, commitTimestampMs, observedTimestampMs, CommitSample.NO_LAG);
        }
        return false;
    }

    int count() {
        return count;
    }

    int capacity() {
        return capacity;
    }

    String owner() {
        return owner;
    }

    String clientId() {
        return clientId;
    }

    void setOwner(String owner, String clientId) {
        this.owner = owner == null ? "" : owner;
        this.clientId = clientId == null ? "" : clientId;
    }

    void clearOwner() {
        this.owner = "";
        this.clientId = "";
    }

    java.util.UUID topicId() {
        return topicId;
    }

    void setTopicId(java.util.UUID topicId) {
        this.topicId = topicId;
    }

    CommitSample[] snapshotCommits() {
        CommitSample[] result = new CommitSample[count];
        for (int i = 0; i < count; i++) {
            int p = phys(i);
            result[i] = new CommitSample(
                    offsets[p],
                    orders[p],
                    commitTimestamps[p],
                    observedTimestamps[p],
                    commitLags[p]
            );
        }
        return result;
    }

    private void append(long offset, long order, long commitTs, long observedTs, long lag) {
        if (count < capacity) {
            int p = phys(count);
            writeSlot(p, offset, order, commitTs, observedTs, lag);
            count++;
        } else {
            writeSlot(start, offset, order, commitTs, observedTs, lag);
            start = (start + 1) % capacity;
        }
    }

    private void writeSlot(int p, long offset, long order, long commitTs, long observedTs, long lag) {
        offsets[p] = offset;
        orders[p] = order;
        commitTimestamps[p] = commitTs;
        observedTimestamps[p] = observedTs;
        commitLags[p] = lag;
    }

    private void copySlot(int from, int to) {
        offsets[to] = offsets[from];
        orders[to] = orders[from];
        commitTimestamps[to] = commitTimestamps[from];
        observedTimestamps[to] = observedTimestamps[from];
        commitLags[to] = commitLags[from];
    }

    private int phys(int logicalIdx) {
        return (start + logicalIdx) % capacity;
    }
}
