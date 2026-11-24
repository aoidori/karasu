/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.collector;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.snapshot.ClusterMetricsSnapshot;
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

import static org.assertj.core.api.Assertions.assertThat;

class ClusterManagerTest {

    private static ClusterSpec spec(String k8s, String kafka, String broker, long allowedLag, ClusterSpec.SwapPolicy swapPolicy) {
        return new ClusterSpec(
                new ClusterTargetKey(k8s, kafka),
                List.of(broker),
                Map.of("tenant", "team-a"),
                ClusterSpec.AuthSpec.PLAINTEXT_NONE,
                ClusterSpec.TlsSpec.DISABLED,
                new ClusterSpec.CollectionSpec("__consumer_offsets", 15, 60, 10,
                        ClusterSpec.StartMode.EARLIEST_THEN_LIVE, ClusterSpec.IsolationLevelConfig.READ_COMMITTED,
                        1, 0, swapPolicy, 5000, 1024 * 1024, 500),
                new ClusterSpec.EvaluationSpec(10, 1, 3600, allowedLag, 1.0f, 10),
                ClusterSpec.FilterSpec.DEFAULT,
                ClusterSpec.LimitsSpec.DEFAULT,
                Map.of()
        );
    }

    private static final class FakeRuntime extends ClusterRuntime {
        final AtomicBoolean fakeReady = new AtomicBoolean(false);
        final AtomicBoolean fakeStopped = new AtomicBoolean(false);
        final AtomicBoolean fakeHotPatched = new AtomicBoolean(false);
        volatile ClusterSpec fakeSpec;

        FakeRuntime(ClusterSpec spec, long gen, String fp, ScheduledExecutorService cs, ScheduledExecutorService ss, boolean ready) {
            super(spec, gen, fp, cs, ss);
            this.fakeSpec = spec;
            this.fakeReady.set(ready);
        }

        @Override
        public void start() {
            // No real Kafka I/O
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
        public ClusterMetricsSnapshot currentSnapshot() {
            ClusterHealthSnapshot h = new ClusterHealthSnapshot(
                    fakeReady.get() ? ClusterPhase.READY : ClusterPhase.REPLAYING,
                    fakeReady.get(), fakeReady.get(), fakeReady.get(), true, true, 1700000000L, 0L, 0, 0, false, generation(), "");
            return new ClusterMetricsSnapshot(targetKey(), fakeSpec, h, List.of(), Map.of(), Map.of(), Map.of(), 0.001, 1700000000000L);
        }

        @Override
        public void stop() {
            fakeStopped.set(true);
        }
    }

    @Test
    void isolatesMultiClusterHotPatchBlueGreenSwapAndDelete() throws Exception {
        List<FakeRuntime> created = new CopyOnWriteArrayList<>();
        AtomicBoolean nextCandidateReady = new AtomicBoolean(true);

        try (ClusterManager manager = new ClusterManager(
                8,
                Duration.ofMillis(600),
                (spec, gen, fp, cs, ss) -> {
                    FakeRuntime rt = new FakeRuntime(spec, gen, fp, cs, ss, nextCandidateReady.get());
                    created.add(rt);
                    return rt;
                },
                spec -> "fixed-digest")) {

            // 1. Add two identically named Kafka clusters across two different K8s clusters
            ClusterSpec a1 = spec("k8s-east", "kafka-core", "broker-a:9092", 0L, ClusterSpec.SwapPolicy.BLUE_GREEN);
            ClusterSpec b1 = spec("k8s-west", "kafka-core", "broker-b:9092", 0L, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(a1, b1));
            assertThat(manager.activeTargets()).hasSize(2);
            assertThat(manager.currentSnapshots()).hasSize(2);

            // 2. Non-connection change (allowedLag) hot-patches without creating a new runtime
            ClusterSpec aHot = spec("k8s-east", "kafka-core", "broker-a:9092", 50L, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(aHot, b1));
            assertThat(created).hasSize(2);
            assertThat(created.get(0).fakeHotPatched.get()).isTrue();
            assertThat(created.get(0).spec().evaluation().allowedLag()).isEqualTo(50L);

            // 3. Connection change with failing candidate retains old healthy runtime!
            nextCandidateReady.set(false);
            ClusterSpec aBadSwap = spec("k8s-east", "kafka-core", "broker-bad:9092", 50L, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(aBadSwap, b1));
            Thread.sleep(900L);
            assertThat(created).hasSize(3);
            assertThat(created.get(0).fakeStopped.get()).isFalse(); // old healthy runtime retained
            assertThat(created.get(2).fakeStopped.get()).isTrue();  // failed candidate stopped

            // 4. Connection change with healthy candidate swaps atomically and stops old runtime
            nextCandidateReady.set(true);
            ClusterSpec aGoodSwap = spec("k8s-east", "kafka-core", "broker-new:9092", 50L, ClusterSpec.SwapPolicy.BLUE_GREEN);
            manager.reconcile(List.of(aGoodSwap, b1));
            Thread.sleep(400L);
            assertThat(created).hasSize(4);
            assertThat(created.get(0).fakeStopped.get()).isTrue();
            assertThat(created.get(3).fakeStopped.get()).isFalse();
            assertThat(created.get(1).fakeStopped.get()).isFalse(); // cluster B completely untouched

            // 5. Delete cluster A immediately unpublishes its snapshot while B remains intact
            manager.reconcile(List.of(b1));
            assertThat(manager.activeTargets()).containsExactly(b1.targetKey());
            assertThat(manager.currentSnapshots()).extracting(ClusterMetricsSnapshot::targetKey)
                    .containsExactly(b1.targetKey());
            assertThat(created.get(3).fakeStopped.get()).isTrue();
            assertThat(created.get(1).fakeStopped.get()).isFalse();
        }
    }
}
