/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.storage;

/**
 * Compact primitive ring buffer storing recent broker log-end-offset (LEO) samples for one partition.
 * All mutations must be externally synchronized by the owning topic lock.
 */
final class BrokerOffsetRing {

    private final long[] offsets;
    private final long[] timestamps;
    private int start;
    private int count;

    BrokerOffsetRing(int capacity) {
        this.offsets = new long[capacity];
        this.timestamps = new long[capacity];
    }

    void add(long offset, long timestampMs) {
        if (count < offsets.length) {
            int idx = (start + count) % offsets.length;
            offsets[idx] = offset;
            timestamps[idx] = timestampMs;
            count++;
        } else {
            offsets[start] = offset;
            timestamps[start] = timestampMs;
            start = (start + 1) % offsets.length;
        }
    }

    boolean isEmpty() {
        return count == 0;
    }

    long latestOffset() {
        if (count == 0) {
            return -1L;
        }
        return offsets[(start + count - 1) % offsets.length];
    }

    /**
     * Returns broker offsets in chronological order (oldest -> newest).
     */
    long[] snapshotOffsets() {
        long[] out = new long[count];
        for (int i = 0; i < count; i++) {
            out[i] = offsets[(start + i) % offsets.length];
        }
        return out;
    }
}
