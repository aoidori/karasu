/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.collector;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.snapshot.ClusterMetricsSnapshot;
import io.karasu.protocol.model.ClusterPhase;
import io.karasu.protocol.model.ClusterTargetKey;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for finding #15 (snapshot build exception actively publishes DEGRADED fallback).
 */
class ClusterRuntimeTest {

    @Test
    void publishesDegradedSnapshotWhenBuildSnapshotThrows() {
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        try {
            ClusterSpec spec = new ClusterSpec(
                    new ClusterTargetKey("k8s-a", "kafka-1"),
                    List.of("127.0.0.1:19092"),
                    Map.of(), null, null, null, null, null, null, null);
            AtomicBoolean failNow = new AtomicBoolean(false);
            ClusterRuntime runtime = new ClusterRuntime(spec, 1L, "fp", exec, exec) {
                @Override
                protected ClusterMetricsSnapshot buildSnapshot() {
                    if (failNow.get()) {
                        throw new IllegalStateException("simulated snapshot failure");
                    }
                    return super.buildSnapshot();
                }
            };

            failNow.set(true);
            runtime.refreshSnapshotSafe();
            ClusterMetricsSnapshot snap = runtime.currentSnapshot();
            assertThat(snap.health().phase()).isEqualTo(ClusterPhase.DEGRADED);
            assertThat(snap.health().dataReady()).isFalse();
            assertThat(snap.health().lastErrorMessage()).contains("snapshot_build_failed");
        } finally {
            exec.shutdownNow();
        }
    }
}
