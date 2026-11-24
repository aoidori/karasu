/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.evaluator;

import io.karasu.protocol.model.CommitSample;
import io.karasu.protocol.model.StatusCode;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden test suite ported verbatim from LinkedIn Burrow's {@code caching_test.go} (16 cases)
 * plus Karasu clock-skew protection tests.
 */
class PartitionStatusCalculatorTest {

    private record GoldenCase(
            int index,
            CommitSample[] offsets,
            long[] brokerOffsets,
            long currentLag,
            long timeNowSec,
            long allowedLag,
            boolean isLagAlwaysNotZero,
            int rewindIndex,
            boolean isStopped,
            boolean isStalled,
            boolean isLagNotDecreasing,
            boolean isRecentLagZero,
            StatusCode expectedStatus
    ) {
    }

    private static CommitSample s(long offset, long order, long tsMs, long lag) {
        return new CommitSample(offset, order, tsMs, 0L, lag);
    }

    private static final List<GoldenCase> BURROW_GOLDEN_CASES = List.of(
            // 0 - OK because there is zero lag somewhere
            new GoldenCase(0,
                    new CommitSample[]{s(1000, 1, 100000, 0), s(2000, 2, 200000, 50), s(3000, 3, 300000, 100), s(4000, 4, 400000, 150), s(5000, 5, 500000, 200)},
                    new long[]{5200}, 200, 600, 0,
                    false, -1, false, false, true, false, StatusCode.OK),

            // 1 - OK because time since last commit (400s) equals diff (400s), not strictly greater
            new GoldenCase(1,
                    new CommitSample[]{s(1000, 1, 100000, 0), s(2000, 2, 200000, 50), s(3000, 3, 300000, 100), s(4000, 4, 400000, 150), s(5000, 5, 500000, 200)},
                    new long[]{5200}, 200, 900, 0,
                    false, -1, false, false, true, false, StatusCode.OK),

            // 2 - STOP because time since last commit (500s) > diff (400s), even with zero lag at start
            new GoldenCase(2,
                    new CommitSample[]{s(1000, 1, 100000, 0), s(2000, 2, 200000, 50), s(3000, 3, 300000, 100), s(4000, 4, 400000, 150), s(5000, 5, 500000, 200)},
                    new long[]{5200}, 200, 1000, 0,
                    false, -1, true, false, true, false, StatusCode.STOP),

            // 3 - STOP when lag is always non-zero as well
            new GoldenCase(3,
                    new CommitSample[]{s(1000, 1, 100000, 50), s(2000, 2, 200000, 100), s(3000, 3, 300000, 150), s(4000, 4, 400000, 200), s(5000, 5, 500000, 250)},
                    new long[]{5250}, 250, 1000, 0,
                    true, -1, true, false, true, false, StatusCode.STOP),

            // 4 - OK because of zero lag at start even though offsets are identical
            new GoldenCase(4,
                    new CommitSample[]{s(1000, 1, 100000, 0), s(1000, 2, 200000, 50), s(1000, 3, 300000, 100), s(1000, 4, 400000, 150), s(1000, 5, 500000, 200)},
                    new long[]{1200}, 200, 600, 0,
                    false, -1, false, true, true, false, StatusCode.OK),

            // 5 - STALL because lag is always non-zero and offsets are identical
            new GoldenCase(5,
                    new CommitSample[]{s(1000, 1, 100000, 100), s(1000, 2, 200000, 150), s(1000, 3, 300000, 200), s(1000, 4, 400000, 250), s(1000, 5, 500000, 300)},
                    new long[]{1300}, 300, 600, 0,
                    true, -1, false, true, true, false, StatusCode.STALL),

            // 6 - STALL even when lag stays constant
            new GoldenCase(6,
                    new CommitSample[]{s(1000, 1, 100000, 100), s(1000, 2, 200000, 100), s(1000, 3, 300000, 100), s(1000, 4, 400000, 100), s(1000, 5, 500000, 100)},
                    new long[]{1100}, 100, 600, 0,
                    true, -1, false, true, true, false, StatusCode.STALL),

            // 7 - OK because rewind recovered and lag decreased
            new GoldenCase(7,
                    new CommitSample[]{s(1000, 1, 100000, 100), s(2000, 2, 200000, 150), s(3000, 3, 300000, 200), s(2000, 4, 400000, 1250), s(4000, 5, 500000, 300)},
                    new long[]{4300}, 300, 600, 0,
                    true, 3, false, false, false, false, StatusCode.OK),

            // 8 - OK because currentLag is 0 even though timestamps look stopped
            new GoldenCase(8,
                    new CommitSample[]{s(1000, 1, 100000, 50), s(2000, 2, 200000, 100), s(3000, 3, 300000, 150), s(4000, 4, 400000, 200), s(5000, 5, 500000, 250)},
                    new long[]{5250}, 0, 1000, 0,
                    true, -1, true, false, true, false, StatusCode.OK),

            // 9 - STOP because currentLag > 0 and recent broker offsets never <= lastOffset
            new GoldenCase(9,
                    new CommitSample[]{
                            s(792748079, 1, 1512224618356L, 0), s(792748080, 2, 1512224619362L, 0),
                            s(792748081, 3, 1512224620366L, 0), s(792748082, 4, 1512224621367L, 0),
                            s(792748083, 5, 1512224622370L, 0), s(792748084, 6, 1512224623373L, 0),
                            s(792748085, 7, 1512224624378L, 0), s(792748086, 8, 1512224625379L, 0),
                            s(792748087, 9, 1512224626383L, 0), s(792748088, 10, 1512224627383L, 0),
                            s(792748089, 11, 1512224628383L, 0), s(792748090, 12, 1512224629388L, 0),
                            s(792748091, 13, 1512224630391L, 0), s(792748092, 14, 1512224631394L, 0),
                            s(792748093, 15, 1512224632397L, 0)
                    },
                    new long[]{792749024, 792749000, 792748800, 792748600, 792748500}, 931, 1512224650L, 0,
                    false, -1, true, false, true, false, StatusCode.STOP),

            // 10 - OK because recent broker offset window shows zero lag recently
            new GoldenCase(10,
                    new CommitSample[]{
                            s(792748079, 1, 1512224618356L, 0), s(792748080, 2, 1512224619362L, 0),
                            s(792748081, 3, 1512224620366L, 0), s(792748082, 4, 1512224621367L, 0),
                            s(792748083, 5, 1512224622370L, 0), s(792748084, 6, 1512224623373L, 0),
                            s(792748085, 7, 1512224624378L, 0), s(792748086, 8, 1512224625379L, 0),
                            s(792748087, 9, 1512224626383L, 0), s(792748088, 10, 1512224627383L, 0),
                            s(792748089, 11, 1512224628383L, 0), s(792748090, 12, 1512224629388L, 0),
                            s(792748091, 13, 1512224630391L, 0), s(792748092, 14, 1512224631394L, 0),
                            s(792748093, 15, 1512224632397L, 0)
                    },
                    new long[]{792748094, 792748093, 792748093, 792748093}, 1, 1512224650L, 0,
                    false, -1, true, false, true, true, StatusCode.OK),

            // 11 - Nil/absent lag entries interleaved should not change result
            new GoldenCase(11,
                    new CommitSample[]{
                            s(1000, 10, 100000, 0), s(1500, 15, 150000, CommitSample.NO_LAG),
                            s(2000, 20, 200000, 50), s(2500, 25, 250000, CommitSample.NO_LAG),
                            s(3000, 30, 300000, 100), s(3500, 35, 350000, CommitSample.NO_LAG),
                            s(4000, 40, 400000, 150), s(4500, 45, 450000, CommitSample.NO_LAG),
                            s(5000, 50, 500000, 200), s(5500, 55, 550000, CommitSample.NO_LAG)
                    },
                    new long[]{5700}, 200, 600, 0,
                    false, -1, false, false, true, false, StatusCode.OK),

            // 12 - OK because currentLag <= allowedLag
            new GoldenCase(12,
                    new CommitSample[]{s(1000, 1, 100000, 2), s(2000, 2, 200000, 1), s(2000, 2, 300000, 1)},
                    new long[]{2001}, 1, 550, 1,
                    false, -1, true, false, false, false, StatusCode.OK),

            // 13 - STALL with allowedLag=99
            new GoldenCase(13,
                    new CommitSample[]{s(1000, 1, 100000, 100), s(1000, 2, 200000, 150), s(1000, 3, 300000, 200), s(1000, 4, 400000, 250), s(1000, 5, 500000, 300)},
                    new long[]{1300}, 300, 600, 99,
                    true, -1, false, true, true, false, StatusCode.STALL),

            // 14 - OK after recovered rewind with allowedLag=99
            new GoldenCase(14,
                    new CommitSample[]{s(1000, 1, 100000, 100), s(2000, 2, 200000, 150), s(3000, 3, 300000, 200), s(2000, 4, 400000, 1250), s(4000, 5, 500000, 300)},
                    new long[]{4300}, 300, 600, 99,
                    true, 3, false, false, false, false, StatusCode.OK),

            // 15 - REWIND not yet caught up
            new GoldenCase(15,
                    new CommitSample[]{s(1000, 1, 100000, 100), s(2000, 2, 200000, 150), s(3000, 3, 300000, 200), s(2000, 4, 400000, 1250), s(2500, 5, 500000, 300)},
                    new long[]{4300}, 300, 600, 0,
                    true, 3, false, false, false, false, StatusCode.REWIND)
    );

    @Test
    void matchesAllSixteenBurrowGoldenTestCases() {
        for (GoldenCase tc : BURROW_GOLDEN_CASES) {
            assertThat(PartitionStatusCalculator.isLagAlwaysNotZero(tc.offsets, tc.allowedLag))
                    .as("Case %d isLagAlwaysNotZero", tc.index).isEqualTo(tc.isLagAlwaysNotZero);
            assertThat(PartitionStatusCalculator.findRewindIndex(tc.offsets))
                    .as("Case %d findRewindIndex", tc.index).isEqualTo(tc.rewindIndex);
            assertThat(PartitionStatusCalculator.isOffsetsStopped(tc.offsets, tc.timeNowSec))
                    .as("Case %d isOffsetsStopped", tc.index).isEqualTo(tc.isStopped);
            assertThat(PartitionStatusCalculator.isOffsetsStalled(tc.offsets))
                    .as("Case %d isOffsetsStalled", tc.index).isEqualTo(tc.isStalled);
            assertThat(PartitionStatusCalculator.isLagNotDecreasing(tc.offsets))
                    .as("Case %d isLagNotDecreasing", tc.index).isEqualTo(tc.isLagNotDecreasing);
            assertThat(PartitionStatusCalculator.isRecentLagZero(tc.offsets, tc.brokerOffsets))
                    .as("Case %d isRecentLagZero", tc.index).isEqualTo(tc.isRecentLagZero);
            assertThat(PartitionStatusCalculator.calculatePartitionStatus(tc.offsets, tc.brokerOffsets, tc.currentLag, tc.timeNowSec, tc.allowedLag))
                    .as("Case %d calculatePartitionStatus", tc.index).isEqualTo(tc.expectedStatus);
        }
    }

    @Test
    void returnsWarnWhenAdvancingButLagNeverDecreases() {
        CommitSample[] offsets = {
                s(100, 1, 100_000, 50),
                s(200, 2, 200_000, 80),
                s(300, 3, 300_000, 120)
        };
        StatusCode status = PartitionStatusCalculator.calculatePartitionStatus(offsets, new long[]{420}, 120, 350, 0);
        assertThat(status).isEqualTo(StatusCode.WARN);
    }

    @Test
    void protectsAgainstFutureClockSkewUsingObservedTimestamp() {
        CommitSample[] skewed = {
                new CommitSample(100, 1, 9_999_999_000_000L, 100_000L, 50),
                new CommitSample(200, 2, 9_999_999_100_000L, 200_000L, 60)
        };
        // Local time is 500s (500_000ms) -> 300s since last observed > 100s window => STOP
        assertThat(PartitionStatusCalculator.isOffsetsStopped(skewed, 500L)).isTrue();
    }
}
