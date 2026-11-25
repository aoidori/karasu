/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.collector;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.snapshot.ClusterMetricsSnapshot;
import io.karasu.core.snapshot.ClusterMetricsSnapshot.DurationHistogramSnapshot;
import io.karasu.protocol.model.ClusterHealthSnapshot;
import io.karasu.protocol.model.ClusterPhase;
import io.karasu.protocol.model.ClusterTargetKey;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive regression suite covering ClusterManager findings #03, #06, #07, #10, #11, #12, #13, #14.
 */
class ClusterManagerTest {

    private static ClusterSpec spec(String k8s, String kafka, String broker, int intervals,
                                    long allowedLag, String deny, int maxParts, ClusterSpec.SwapPolicy swapPolicy) {
        return new ClusterSpec(
                new ClusterTargetKey(k8s, kafka),
                broker == null ? List.of() : List.of(broker),
                Map.of("tenant", "team-a"),
                ClusterSpec.AuthSpec.PLAINTEXT_NONE,
                ClusterSpec.TlsSpec.DISABLED,
                new ClusterSpec.CollectionSpec("__consumer_offsets", 15, 60, 10,
                        ClusterSpec.StartMode.EARLIEST_THEN_LIVE, ClusterSpec.IsolationLevelConfig.READ_COMMITTED,
                        1, 0, swapPolicy, 5000, 1024 * 1024, 500),
                new ClusterSpec.EvaluationSpec(intervals, 1, 3600, allowedLag, 1.0f, 10),
                new ClusterSpec.FilterSpec("", deny, "", "", true),
                new ClusterSpec.LimitsSpec(100, maxParts),
                Map.of()
        );
    }

    private static final class FakeRuntime extends ClusterRuntime {
        final AtomicBoolean fakeReady = new AtomicBoolean(false);
        final AtomicBoolean fakeStopped = new AtomicBoolean(false);
        final AtomicBoolean fakeHotPatched = new AtomicBoolean(false);
        final AtomicBoolean fakeCardinalityReached = new AtomicBoolean(false);
        volatile ClusterSpec fakeSpec;

        FakeRuntime(ClusterSpec spec, long gen, String fp, ScheduledExecutorService cs, ScheduledExecutorService ss, boolean ready) {
            super(spec, gen, fp, cs, ss);
            this.fakeSpec = spec;
            this.fakeReady.set(ready);
        }

        @Override
        public void start() {
        }

        @Override
        public synchronized void hotPatch(ClusterSpec updatedSpec) {
            this.fakeSpec = updatedSpec;
            this.fakeHotPatched.set(true);
        }

        @Override
        public ClusterSpec spec() {
            return fakeSpec;
        }

        @Override
        public boolean isDataReady() {
            return fakeReady.get();
        }

        @Override
        public boolean isCardinalityLimitReached() {
            return fakeCardinalityReached.get();
        }

        @Override
        public ClusterMetricsSnapshot currentSnapshot() {
            ClusterHealthSnapshot h = new ClusterHealthSnapshot(
                    fakeReady.get() ? ClusterPhase.READY : ClusterPhase.REPLAYING,
                    fakeReady.get(), fakeReady.get(), fakeReady.get(), true, true, 1700000000L, 0L, 0, 0,
                    fakeCardinalityReached.get(), generation(), "");
            return new ClusterMetricsSnapshot(targetKey(), fakeSpec, h, List.of(), Map.of(), Map.of(), Map.of(),
                    DurationHistogramSnapshot.empty(), System.currentTimeMillis());
        }

        @Override
        public void stop() {
            fakeStopped.set(true);
        }
    }

    @Test
    void retainsRunningClusterWhenInvalidConfigOrMergedLabelLimitFails() {
        // Regression test for findings #06 and #12
        List<FakeRuntime> created = new CopyOnWriteArrayList<>();
        try (ClusterManager manager = new ClusterManager(
                8, Duration.ofMillis(400), Duration.ofMillis(200),
                (s, gen, fp, cs, ss) -> {
                    FakeRuntime rt = new FakeRuntime(s, gen, fp, cs, ss, true);
                    created.add(rt);
                    return rt;
                },
                s -> "digest")) {

            manager.setExtraSpecValidator(s -> {
                if (s.labels().size() > 1) {
                    throw new IllegalArgumentException("merged labels exceeded");
                }
            });

            ClusterSpec valid = spec("k8s-a", "kafka-1", "broker-1:9092", 10, 0L, "", 100, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(valid));
            assertThat(manager.activeTargets()).containsExactly(valid.targetKey());

            // 1. Invalid spec (empty bootstrapServers) must NOT delete active cluster!
            ClusterSpec badBrokers = spec("k8s-a", "kafka-1", null, 10, 0L, "", 100, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(badBrokers));
            assertThat(manager.activeTargets()).containsExactly(valid.targetKey());
            assertThat(created.get(0).fakeStopped.get()).isFalse();
            assertThat(manager.currentSnapshots().get(0).reconcileErrorCounts().get("validation")).isEqualTo(1L);

            // 2. Merged label overflow rejected before activation, keeping old active runtime intact!
            ClusterSpec tooManyLabels = new ClusterSpec(
                    valid.targetKey(), List.of("broker-1:9092"), Map.of("tenant", "a", "extra", "b"),
                    valid.auth(), valid.tls(), valid.collection(), valid.evaluation(), valid.filters(), valid.limits(), Map.of());
            manager.reconcile(List.of(tooManyLabels));
            assertThat(manager.activeTargets()).containsExactly(valid.targetKey());
            assertThat(manager.currentSnapshots().get(0).reconcileErrorCounts().get("validation")).isEqualTo(2L);
        }
    }

    @Test
    void cancelsInFlightBlueGreenCandidateWhenRolledBackAToBToA() throws Exception {
        // Regression test for finding #07
        List<FakeRuntime> created = new CopyOnWriteArrayList<>();
        try (ClusterManager manager = new ClusterManager(
                8, Duration.ofSeconds(3), Duration.ofSeconds(5),
                (s, gen, fp, cs, ss) -> {
                    boolean ready = created.isEmpty(); // A starts ready, B starts NOT ready
                    FakeRuntime rt = new FakeRuntime(s, gen, fp, cs, ss, ready);
                    created.add(rt);
                    return rt;
                },
                s -> "digest")) {

            ClusterSpec specA = spec("k8s-a", "kafka-1", "broker-a:9092", 10, 0L, "", 100, ClusterSpec.SwapPolicy.BLUE_GREEN);
            ClusterSpec specB = spec("k8s-a", "kafka-1", "broker-b:9092", 10, 0L, "", 100, ClusterSpec.SwapPolicy.BLUE_GREEN);

            // A active
            manager.reconcile(List.of(specA));
            assertThat(created).hasSize(1);

            // A -> B (B is replaying / not yet ready)
            manager.reconcile(List.of(specB));
            assertThat(created).hasSize(2);
            FakeRuntime runtimeA = created.get(0);
            FakeRuntime runtimeB = created.get(1);

            // Rollback B -> A while B is still in flight
            manager.reconcile(List.of(specA));

            // Even if B later becomes ready, it MUST NOT activate!
            runtimeB.fakeReady.set(true);
            Thread.sleep(500L);

            assertThat(runtimeB.fakeStopped.get()).isTrue();
            assertThat(runtimeA.fakeStopped.get()).isFalse();
            assertThat(manager.currentSnapshots().get(0).spec().bootstrapServers()).containsExactly("broker-a:9092");
        }
    }

    @Test
    void retriesTimedOutBlueGreenSwapAutomaticallyOnceCandidateRecovers() throws Exception {
        // Regression test for finding #14
        List<FakeRuntime> created = new CopyOnWriteArrayList<>();
        AtomicInteger callCount = new AtomicInteger(0);
        try (ClusterManager manager = new ClusterManager(
                8, Duration.ofMillis(200), Duration.ofMillis(200),
                (s, gen, fp, cs, ss) -> {
                    int n = callCount.incrementAndGet();
                    // 1st (A): ready, 2nd (B attempt 0): NOT ready (times out), 3rd (B retry attempt 1): ready!
                    boolean ready = (n != 2);
                    FakeRuntime rt = new FakeRuntime(s, gen, fp, cs, ss, ready);
                    created.add(rt);
                    return rt;
                },
                s -> "digest")) {

            ClusterSpec specA = spec("k8s-a", "kafka-1", "broker-a:9092", 10, 0L, "", 100, ClusterSpec.SwapPolicy.BLUE_GREEN);
            ClusterSpec specB = spec("k8s-a", "kafka-1", "broker-b:9092", 10, 0L, "", 100, ClusterSpec.SwapPolicy.BLUE_GREEN);

            manager.reconcile(List.of(specA));
            manager.reconcile(List.of(specB));

            // Wait for attempt 0 timeout + automatic retry attempt 1 to succeed without re-calling reconcile()!
            long deadline = System.currentTimeMillis() + 3000L;
            while (System.currentTimeMillis() < deadline
                    && !manager.currentSnapshots().get(0).spec().bootstrapServers().contains("broker-b:9092")) {
                Thread.sleep(50L);
            }

            assertThat(created).hasSize(3);
            assertThat(created.get(1).fakeStopped.get()).isTrue(); // timed out attempt 0 stopped
            assertThat(created.get(0).fakeStopped.get()).isTrue(); // old A stopped after retry succeeded
            assertThat(manager.currentSnapshots().get(0).spec().bootstrapServers()).containsExactly("broker-b:9092");
        }
    }

    @Test
    void triggersRebuildOnIntervalsChangeLimitExpansionAfterCapAndFilterLoosening() throws Exception {
        // Regression tests for findings #03 (recovery), #10 (loosening), #11 (intervals)
        List<FakeRuntime> created = new CopyOnWriteArrayList<>();
        try (ClusterManager manager = new ClusterManager(
                8, Duration.ofMillis(500), Duration.ofMillis(200),
                (s, gen, fp, cs, ss) -> {
                    FakeRuntime rt = new FakeRuntime(s, gen, fp, cs, ss, true);
                    created.add(rt);
                    return rt;
                },
                s -> "digest")) {

            // 1. Start with intervals=2
            ClusterSpec s1 = spec("k8s-a", "kafka-1", "broker-a:9092", 2, 0L, "", 10, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(s1));
            assertThat(created).hasSize(1);

            // 2. Change intervals 2 -> 10 triggers Blue-Green rebuild (#11)
            ClusterSpec s2 = spec("k8s-a", "kafka-1", "broker-a:9092", 10, 0L, "", 10, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(s2));
            Thread.sleep(250L);
            assertThat(created).hasSize(2);
            assertThat(manager.currentSnapshots().get(0).spec().evaluation().intervals()).isEqualTo(10);

            // 3. Tighten denylist ("") -> ("^drop-") hot-patches in place (#10)
            ClusterSpec s3 = spec("k8s-a", "kafka-1", "broker-a:9092", 10, 0L, "^drop-", 10, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(s3));
            assertThat(created).hasSize(2);
            assertThat(created.get(1).fakeHotPatched.get()).isTrue();

            // 4. Loosen denylist ("^drop-") -> ("") triggers Blue-Green rebuild to recover excluded groups (#10)
            ClusterSpec s4 = spec("k8s-a", "kafka-1", "broker-a:9092", 10, 0L, "", 10, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(s4));
            Thread.sleep(250L);
            assertThat(created).hasSize(3);

            // 5. Simulate cardinality limit hit, then raise maxGroupPartitions -> triggers rebuild (#03)
            created.get(2).fakeCardinalityReached.set(true);
            ClusterSpec s5 = spec("k8s-a", "kafka-1", "broker-a:9092", 10, 0L, "", 1000, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(s5));
            Thread.sleep(250L);
            assertThat(created).hasSize(4);
        }
    }

    @Test
    void mergesTargetsAcrossMultipleProvidersWithoutDeletingEachOther() {
        // Regression test for finding #13
        try (ClusterManager manager = new ClusterManager(
                8, Duration.ofMillis(400), Duration.ofMillis(200),
                (s, gen, fp, cs, ss) -> new FakeRuntime(s, gen, fp, cs, ss, true),
                s -> "digest")) {

            ClusterSpec a = spec("k8s-a", "kafka-a", "a:9092", 10, 0L, "", 100, ClusterSpec.SwapPolicy.BLUE_GREEN);
            ClusterSpec b = spec("k8s-b", "kafka-b", "b:9092", 10, 0L, "", 100, ClusterSpec.SwapPolicy.BLUE_GREEN);

            manager.reconcileFromProvider("provider-yaml", List.of(a));
            manager.reconcileFromProvider("provider-crd", List.of(b));

            assertThat(manager.activeTargets()).containsExactlyInAnyOrder(a.targetKey(), b.targetKey());

            // Updating provider-yaml must keep provider-crd's target untouched
            ClusterSpec aUpdated = spec("k8s-a", "kafka-a", "a:9092", 10, 25L, "", 100, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcileFromProvider("provider-yaml", List.of(aUpdated));
            assertThat(manager.activeTargets()).containsExactlyInAnyOrder(a.targetKey(), b.targetKey());
        }
    }
}
