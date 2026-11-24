/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.evaluator;

import io.karasu.protocol.model.CommitSample;
import io.karasu.protocol.model.StatusCode;

/**
 * Pure, stateless Burrow-compatible sliding-window consumer status evaluation engine.
 * Preserves the exact rule precedence and edge-case behavior of LinkedIn Burrow's
 * {@code CachingEvaluator} with additional clock-skew protection.
 */
public final class PartitionStatusCalculator {

    private static final long CLOCK_SKEW_TOLERANCE_MS = 60_000L;

    private PartitionStatusCalculator() {
    }

    public static StatusCode calculatePartitionStatus(CommitSample[] offsets,
                                                      long[] brokerOffsets,
                                                      long currentLag,
                                                      long timeNowSec,
                                                      long allowedLag) {
        if (offsets == null || offsets.length == 0) {
            return StatusCode.OK;
        }
        if (currentLag > allowedLag) {
            if (isOffsetsStopped(offsets, timeNowSec) && !isRecentLagZero(offsets, brokerOffsets)) {
                return StatusCode.STOP;
            }

            int rewindIndex = findRewindIndex(offsets);
            if (rewindIndex > 0 && !isRewindRecovered(offsets, rewindIndex)) {
                return StatusCode.REWIND;
            }

            if (isLagAlwaysNotZero(offsets, allowedLag)) {
                if (isOffsetsStalled(offsets)) {
                    return StatusCode.STALL;
                }
                if (isLagNotDecreasing(offsets)) {
                    return StatusCode.WARN;
                }
            }
        }
        return StatusCode.OK;
    }

    /**
     * Rule 1: Returns true iff no commit sample in the window had commitLag <= allowedLag.
     */
    public static boolean isLagAlwaysNotZero(CommitSample[] offsets, long allowedLag) {
        for (CommitSample sample : offsets) {
            if (sample != null && sample.hasCommitLag() && sample.commitLag() <= allowedLag) {
                return false;
            }
        }
        return true;
    }

    /**
     * Rule 2a: Returns the first index where offset decreased from the previous commit, or -1 if none.
     */
    public static int findRewindIndex(CommitSample[] offsets) {
        for (int i = 1; i < offsets.length; i++) {
            if (offsets[i].offset() < offsets[i - 1].offset()) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Rule 2b: Returns true if the consumer caught back up to or past the offset prior to the rewind.
     */
    public static boolean isRewindRecovered(CommitSample[] offsets, int resetIndex) {
        long previousProgress = offsets[resetIndex - 1].offset();
        for (int i = resetIndex; i < offsets.length; i++) {
            if (offsets[i].offset() >= previousProgress) {
                return true;
            }
        }
        return false;
    }

    /**
     * Rule 3: Returns true if (now - lastTimestamp) > (lastTimestamp - firstTimestamp).
     * Falls back to observedTimestamp when commitTimestamp exhibits severe clock skew.
     */
    public static boolean isOffsetsStopped(CommitSample[] offsets, long timeNowSec) {
        long nowMs = timeNowSec * 1000L;
        long firstTs = offsets[0].commitTimestampMs();
        long lastTs = offsets[offsets.length - 1].commitTimestampMs();

        // Clock-skew safeguard: if commit timestamps are inverted or far in the future and valid observed timestamps exist
        boolean skewed = (lastTs < firstTs) || ((lastTs - nowMs) > CLOCK_SKEW_TOLERANCE_MS);
        if (skewed) {
            long obsFirst = offsets[0].observedTimestampMs();
            long obsLast = offsets[offsets.length - 1].observedTimestampMs();
            if (obsFirst > 0L && obsLast >= obsFirst) {
                firstTs = obsFirst;
                lastTs = obsLast;
            }
        }
        return (nowMs - lastTs) > (lastTs - firstTs);
    }

    /**
     * Rule 4: Returns true if every commit in the window committed the exact same offset.
     */
    public static boolean isOffsetsStalled(CommitSample[] offsets) {
        for (int i = 1; i < offsets.length; i++) {
            if (offsets[i].offset() != offsets[i - 1].offset()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Rule 5: Returns true if lag never decreased between consecutive samples with known commitLag.
     */
    public static boolean isLagNotDecreasing(CommitSample[] offsets) {
        long lastLag = CommitSample.NO_LAG;
        for (CommitSample sample : offsets) {
            if (sample != null && sample.hasCommitLag()) {
                long lag = sample.commitLag();
                if (lastLag != CommitSample.NO_LAG && lag < lastLag) {
                    return false;
                }
                lastLag = lag;
            }
        }
        return true;
    }

    /**
     * Returns true if any recent broker LEO sample was <= the consumer's last committed offset.
     */
    public static boolean isRecentLagZero(CommitSample[] offsets, long[] brokerOffsets) {
        if (brokerOffsets == null || brokerOffsets.length == 0) {
            return false;
        }
        long lastOffset = offsets[offsets.length - 1].offset();
        for (long brokerOffset : brokerOffsets) {
            if (brokerOffset <= lastOffset) {
                return true;
            }
        }
        return false;
    }
}
