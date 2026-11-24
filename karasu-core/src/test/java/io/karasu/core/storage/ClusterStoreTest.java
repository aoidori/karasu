/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.core.storage;

import io.karasu.core.config.ClusterSpec;
import io.karasu.core.evaluator.LagEvaluator;
import io.karasu.protocol.decode.DecodedRecord.TopicIdPartitions;
import io.karasu.protocol.model.ClusterTargetKey;
import io.karasu.protocol.model.CommitSample;
import io.karasu.protocol.model.GroupEvaluation;
import io.karasu.protocol.model.StatusCode;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterStoreTest {

    private static ClusterSpec specWith(int intervals, long minDistSec, long expireSec, int maxGroups, int maxParts) {
        return new ClusterSpec(
                new ClusterTargetKey("k8s-a", "kafka-1"),
                List.of("localhost:9092"),
                Map.of(),
                ClusterSpec.AuthSpec.PLAINTEXT_NONE,
                ClusterSpec.TlsSpec.DISABLED,
                ClusterSpec.CollectionSpec.DEFAULT,
                new ClusterSpec.EvaluationSpec(intervals, minDistSec, expireSec, 0L, 1.0f, 10),
                ClusterSpec.FilterSpec.DEFAULT,
                new ClusterSpec.LimitsSpec(maxGroups, maxParts),
                Map.of()
        );
    }

    @Test
    void preservesOrderMinDistanceAndWrapAroundInPrimitiveRing() {
        ClusterStore store = new ClusterStore(specWith(3, 5, 3600, 100, 1000));
        long baseTs = 1_700_000_000_000L;
        store.updateTopicMetadata("orders", UUID.randomUUID(), 1);
        store.recordBrokerOffset("orders", 0, 1, 1000L, baseTs);

        // First commit at T+0
        store.recordConsumerOffset("g1", "orders", 0, 100L, 10L, baseTs, baseTs);
        // Frequent commit at T+2s (< minDistance=5s): merges into slot 0 keeping timestamp=baseTs
        store.recordConsumerOffset("g1", "orders", 0, 150L, 11L, baseTs + 2_000L, baseTs + 2_000L);
        // Normal commits at T+10s, T+20s, T+30s -> ring capacity 3 wraps around dropping oldest
        store.recordConsumerOffset("g1", "orders", 0, 200L, 12L, baseTs + 10_000L, baseTs + 10_000L);
        store.recordConsumerOffset("g1", "orders", 0, 300L, 13L, baseTs + 20_000L, baseTs + 20_000L);
        store.recordConsumerOffset("g1", "orders", 0, 400L, 14L, baseTs + 30_000L, baseTs + 30_000L);

        Map<String, List<ClusterStore.PartitionSnapshot>> fetched = store.fetchConsumer("g1", baseTs + 31_000L);
        assertThat(fetched).containsKey("orders");
        CommitSample[] commits = fetched.get("orders").get(0).commits();
        assertThat(commits).hasSize(3);
        assertThat(commits[0].offset()).isEqualTo(200L);
        assertThat(commits[1].offset()).isEqualTo(300L);
        assertThat(commits[2].offset()).isEqualTo(400L);
        assertThat(fetched.get("orders").get(0).currentLag()).isEqualTo(600L);
    }

    @Test
    void handlesOffsetTombstoneAndGroupTombstoneAndProactiveExpiry() {
        ClusterStore store = new ClusterStore(specWith(3, 0, 10, 100, 1000));
        long t0 = 1_700_000_000_000L;
        store.updateTopicMetadata("orders", UUID.randomUUID(), 2);
        store.recordBrokerOffset("orders", 0, 2, 100L, t0);
        store.recordBrokerOffset("orders", 1, 2, 200L, t0);
        store.recordConsumerOffset("g1", "orders", 0, 50L, 1L, t0, t0);
        store.recordConsumerOffset("g1", "orders", 1, 80L, 2L, t0, t0);
        assertThat(store.trackedPartitionCount()).isEqualTo(2);

        // Offset tombstone removes single partition
        store.deleteConsumerPartition("g1", "orders", 0);
        assertThat(store.trackedPartitionCount()).isEqualTo(1);
        assertThat(store.fetchConsumer("g1", t0 + 1_000L).get("orders")).hasSize(1);

        // Removing last partition removes group completely
        store.deleteConsumerPartition("g1", "orders", 1);
        assertThat(store.trackedGroupCount()).isZero();
        assertThat(store.trackedPartitionCount()).isZero();

        // Proactive expiration purges stale group
        store.recordConsumerOffset("g2", "orders", 0, 10L, 3L, t0, t0);
        List<String> purged = store.purgeExpiredGroups(t0 + 15_000L);
        assertThat(purged).containsExactly("g2");
        assertThat(store.trackedGroupCount()).isZero();
    }

    @Test
    void resolvesKip848OwnerRegardlessOfArrivalOrder() {
        ClusterStore store = new ClusterStore(specWith(2, 0, 3600, 100, 1000));
        long t0 = 1_700_000_000_000L;
        UUID topicUuid = UUID.randomUUID();

        // 1. Broker knows topic name/offset before TopicId is bound, consumer commits offset
        store.recordBrokerOffset("events", 0, 1, 50L, t0);
        store.recordConsumerOffset("modern-g", "events", 0, 40L, 1L, t0, t0);
        store.recordConsumerOffset("modern-g", "events", 0, 45L, 2L, t0 + 1000L, t0 + 1000L);

        // 2. Key 8 (assignment with UUID) arrives BEFORE Key 5 (member host/clientId) and BEFORE TopicId metadata!
        store.updateKip848Assignment("modern-g", "m-1", List.of(new TopicIdPartitions(topicUuid, new int[]{0})));
        // 3. Key 5 arrives
        store.upsertKip848Member("modern-g", "m-1", "modern-client-1", "/10.2.3.4");
        // 4. Metadata refresh delivers TopicId -> "events"
        store.updateTopicMetadata("events", topicUuid, 1);

        LagEvaluator evaluator = new LagEvaluator(
                specWith(2, 0, 3600, 100, 1000),
                store,
                Clock.fixed(Instant.ofEpochMilli(t0 + 2_000L), ZoneOffset.UTC));
        GroupEvaluation eval = evaluator.evaluateGroup("modern-g");
        assertThat(eval).isNotNull();
        assertThat(eval.status()).isEqualTo(StatusCode.OK);
        assertThat(eval.totalLag()).isEqualTo(5L);
        assertThat(eval.partitions()).hasSize(1);
        assertThat(eval.partitions().get(0).owner()).isEqualTo("/10.2.3.4");
        assertThat(eval.partitions().get(0).clientId()).isEqualTo("modern-client-1");
    }

    @Test
    void enforcesCardinalityGuardrailAndHandlesTopicRecreationSafely() {
        ClusterStore store = new ClusterStore(specWith(2, 0, 3600, 1, 1));
        long t0 = 1_700_000_000_000L;
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        store.updateTopicMetadata("t1", id1, 2);
        store.recordBrokerOffset("t1", 0, 2, 100L, t0);
        store.recordBrokerOffset("t1", 1, 2, 100L, t0);

        store.recordConsumerOffset("g1", "t1", 0, 10L, 1L, t0, t0);
        // Exceeds maxGroupPartitions=1 and maxGroups=1
        store.recordConsumerOffset("g1", "t1", 1, 10L, 2L, t0, t0);
        store.recordConsumerOffset("g2", "t1", 0, 10L, 3L, t0, t0);

        assertThat(store.isCardinalityLimitReached()).isTrue();
        assertThat(store.trackedGroupCount()).isEqualTo(1);
        assertThat(store.trackedPartitionCount()).isEqualTo(1);

        // Recreate topic with fewer partitions -> fetchConsumer safely omits incomplete snapshot instead of throwing
        store.updateTopicMetadata("t1", id2, 1);
        assertThat(store.fetchConsumer("g1", t0 + 1000L)).isEmpty();
    }
}
