/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.collector;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.storage.ClusterStore;
import io.karasu.protocol.model.ClusterTargetKey;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for finding #04 (per-worker independent health & runtime backlog stall detection).
 */
class OffsetsTopicIngestorTest {

    private static ClusterSpec multiWorkerSpec(int workers) {
        return new ClusterSpec(
                new ClusterTargetKey("k8s-a", "kafka-1"),
                List.of("127.0.0.1:19092"),
                Map.of(),
                ClusterSpec.AuthSpec.PLAINTEXT_NONE,
                ClusterSpec.TlsSpec.DISABLED,
                new ClusterSpec.CollectionSpec("__consumer_offsets", 15, 60, 10,
                        ClusterSpec.StartMode.EARLIEST_THEN_LIVE, ClusterSpec.IsolationLevelConfig.READ_COMMITTED,
                        workers, 0, ClusterSpec.SwapPolicy.BLUE_GREEN, 5000, 1024 * 1024, 500),
                ClusterSpec.EvaluationSpec.DEFAULT,
                ClusterSpec.FilterSpec.DEFAULT,
                ClusterSpec.LimitsSpec.DEFAULT,
                Map.of()
        );
    }

    @Test
    void healthyWorkerDoesNotMaskUnhealthyOrReplayingWorker() {
        ClusterSpec spec = multiWorkerSpec(2);
        ClusterStore store = new ClusterStore(spec);
        BrokerOffsetCollector brokerCollector = new BrokerOffsetCollector(spec, 1L, store, () -> {});
        OffsetsTopicIngestor ingestor = new OffsetsTopicIngestor(spec, 1L, store, brokerCollector, g -> {});
        long now = 1_700_000_000_000L;

        // Worker 0 is caught up and healthy; worker 1 errored/reconnecting
        ingestor.markWorkerStateForTest(0, true, true, now, "");
        ingestor.markWorkerStateForTest(1, false, false, now - 1000L, "broker_disconnected");

        assertThat(ingestor.isHealthy(now)).isFalse();
        assertThat(ingestor.isCaughtUp(now)).isFalse();
        assertThat(ingestor.lastError()).contains("broker_disconnected");

        // Both workers healthy and caught up -> overall healthy & caught up
        ingestor.markWorkerStateForTest(1, true, true, now, "");
        assertThat(ingestor.isHealthy(now)).isTrue();
        assertThat(ingestor.isCaughtUp(now)).isTrue();

        // Worker 1 poll stops advancing beyond freshness threshold -> unhealthy
        assertThat(ingestor.isHealthy(now + 40_000L)).isFalse();
    }

    @Test
    void detectsRuntimeInternalOffsetsBacklogStall() {
        ClusterSpec spec = multiWorkerSpec(1);
        ClusterStore store = new ClusterStore(spec);
        BrokerOffsetCollector brokerCollector = new BrokerOffsetCollector(spec, 1L, store, () -> {});
        OffsetsTopicIngestor ingestor = new OffsetsTopicIngestor(spec, 1L, store, brokerCollector, g -> {});
        long t0 = 1_700_000_000_000L;

        ingestor.markWorkerStateForTest(0, true, true, t0, "");
        store.recordBrokerOffset("__consumer_offsets", 0, 1, 100L, t0);
        ingestor.updatePartitionPositionForTest(0, 50L);

        // First observation records watermark (head=100 at t0): not yet stalled
        assertThat(ingestor.isCaughtUp(t0)).isTrue();
        assertThat(ingestor.computeInternalOffsetsLag()).isEqualTo(50L);

        // 65 seconds later, position is still stuck at 50 (< watermark 100) -> stalled!
        assertThat(ingestor.isCaughtUp(t0 + 65_000L)).isFalse();

        // Once consumer catches up past watermark 100 -> caught up again
        ingestor.updatePartitionPositionForTest(0, 100L);
        assertThat(ingestor.isCaughtUp(t0 + 66_000L)).isTrue();
    }
}
