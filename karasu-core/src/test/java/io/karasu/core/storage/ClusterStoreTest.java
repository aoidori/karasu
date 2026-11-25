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
import io.karasu.protocol.model.PartitionEvaluation;
import io.karasu.protocol.model.StatusCode;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterStoreTest {

    private static ClusterSpec specWith(int intervals, long minDistSec, long expireSec, int maxGroups, int maxParts) {
        return specWithFilters(intervals, minDistSec, expireSec, maxGroups, maxParts, "", "");
    }

    private static ClusterSpec specWithFilters(int intervals, long minDistSec, long expireSec,
                                               int maxGroups, int maxParts, String allow, String deny) {
        return new ClusterSpec(
                new ClusterTargetKey("k8s-a", "kafka-1"),
                List.of("localhost:9092"),
                Map.of(),
                ClusterSpec.AuthSpec.PLAINTEXT_NONE,
                ClusterSpec.TlsSpec.DISABLED,
                ClusterSpec.CollectionSpec.DEFAULT,
                new ClusterSpec.EvaluationSpec(intervals, minDistSec, expireSec, 0L, 1.0f, 10),
                new ClusterSpec.FilterSpec(allow, deny, "", "", true),
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

        store.recordConsumerOffset("g1", "orders", 0, 100L, 10L, baseTs, baseTs);
        store.recordConsumerOffset("g1", "orders", 0, 150L, 11L, baseTs + 2_000L, baseTs + 2_000L);
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
    void retainsFirstCommitForNewTopicBeforeMetadataArrivesAndComputesLagOnceDiscovered() {
        // Regression test for finding #01
        AtomicInteger refreshRequests = new AtomicInteger(0);
        ClusterSpec spec = specWith(3, 0, 3600, 100, 1000);
        ClusterStore store = new ClusterStore(spec, refreshRequests::incrementAndGet);
        long t0 = 1_700_000_000_000L;

        // Commit arrives BEFORE broker metadata knows about "brand-new-topic"
        UUID topicId = UUID.randomUUID();
        store.recordConsumerOffset("new-topic-cg", "brand-new-topic", 0, 5L, 1L, t0, t0, topicId);
        assertThat(refreshRequests.get()).isEqualTo(1);

        // Before metadata arrives, group exists but lag total is incomplete
        LagEvaluator evaluator = new LagEvaluator(spec, store, Clock.fixed(Instant.ofEpochMilli(t0 + 1000L), ZoneOffset.UTC));
        GroupEvaluation beforeMeta = evaluator.evaluateGroup("new-topic-cg");
        assertThat(beforeMeta).isNotNull();
        assertThat(beforeMeta.lagTotalComplete()).isFalse();

        // Next metadata refresh discovers topic and records LEO=20; NO second commit ever occurs!
        store.updateTopicMetadata("brand-new-topic", topicId, 1);
        store.recordBrokerOffset("brand-new-topic", 0, 1, 20L, t0 + 2000L);
        evaluator.invalidateAll();

        GroupEvaluation afterMeta = evaluator.evaluateGroup("new-topic-cg");
        assertThat(afterMeta).isNotNull();
        assertThat(afterMeta.lagTotalComplete()).isTrue();
        assertThat(afterMeta.totalLag()).isEqualTo(15L);
        assertThat(afterMeta.partitions().get(0).hasKnownLag()).isTrue();
        assertThat(afterMeta.partitions().get(0).currentLag()).isEqualTo(15L);
    }

    @Test
    void preservesCommittedOffsetsAcrossClassicToKip848OnlineMigration() {
        // Regression test for finding #02
        ClusterSpec spec = specWith(3, 0, 3600, 100, 1000);
        ClusterStore store = new ClusterStore(spec);
        long t0 = 1_700_000_000_000L;
        UUID topicUuid = UUID.randomUUID();
        store.updateTopicMetadata("orders", topicUuid, 2);
        store.recordBrokerOffset("orders", 0, 2, 12L, t0);
        store.recordBrokerOffset("orders", 1, 2, 12L, t0);

        // Classic group commits 4 on both partitions -> total lag = 16
        store.recordConsumerOffset("migrating-cg", "orders", 0, 4L, 1L, t0, t0, topicUuid);
        store.recordConsumerOffset("migrating-cg", "orders", 1, 4L, 2L, t0, t0, topicUuid);
        store.setConsumerOwner("migrating-cg", "orders", 0, "/10.0.0.1", "classic-client");

        // Kafka 4.2 writes Key-2 Classic tombstone during online upgrade, followed by Key-5/Key-8 KIP-848 metadata, NO new commits
        store.clearClassicGroupMetadata("migrating-cg");
        store.upsertKip848Member("migrating-cg", "m-848", "modern-client", "/10.0.0.9");
        store.updateKip848Assignment("migrating-cg", "m-848", List.of(new TopicIdPartitions(topicUuid, new int[]{0, 1})));

        LagEvaluator evaluator = new LagEvaluator(spec, store, Clock.fixed(Instant.ofEpochMilli(t0 + 1000L), ZoneOffset.UTC));
        GroupEvaluation eval = evaluator.evaluateGroup("migrating-cg");
        assertThat(eval).isNotNull();
        assertThat(eval.lagTotalComplete()).isTrue();
        assertThat(eval.totalLag()).isEqualTo(16L);
        assertThat(eval.partitions()).allSatisfy(p -> {
            assertThat(p.currentLag()).isEqualTo(8L);
            assertThat(p.owner()).isEqualTo("/10.0.0.9");
            assertThat(p.clientId()).isEqualTo("modern-client");
        });
    }

    @Test
    void marksGroupIncompleteWhenCardinalityLimitTruncatesPartitions() {
        // Regression test for finding #03
        ClusterSpec spec = specWith(3, 0, 3600, 10, 1);
        ClusterStore store = new ClusterStore(spec);
        long t0 = 1_700_000_000_000L;
        store.updateTopicMetadata("orders", UUID.randomUUID(), 2);
        store.recordBrokerOffset("orders", 0, 2, 12L, t0);
        store.recordBrokerOffset("orders", 1, 2, 12L, t0);

        store.recordConsumerOffset("capped-cg", "orders", 0, 4L, 1L, t0, t0);
        store.recordConsumerOffset("capped-cg", "orders", 1, 4L, 2L, t0, t0); // rejected by maxGroupPartitions=1

        assertThat(store.isCardinalityLimitReached()).isTrue();
        LagEvaluator evaluator = new LagEvaluator(spec, store, Clock.fixed(Instant.ofEpochMilli(t0 + 1000L), ZoneOffset.UTC));
        GroupEvaluation eval = evaluator.evaluateGroup("capped-cg");
        assertThat(eval).isNotNull();
        // Must NOT claim totalLag=8 is a complete group total!
        assertThat(eval.lagTotalComplete()).isFalse();
    }

    @Test
    void resetsConsumerOffsetsAndFencesStaleCommitsOnSameNameTopicRecreation() {
        // Regression test for finding #05
        ClusterSpec spec = specWith(3, 0, 3600, 100, 1000);
        ClusterStore store = new ClusterStore(spec);
        long t0 = 1_700_000_000_000L;
        UUID oldId = UUID.randomUUID();
        UUID newId = UUID.randomUUID();

        store.updateTopicMetadata("recreated-topic", oldId, 1);
        store.recordBrokerOffset("recreated-topic", 0, 1, 100L, t0);
        store.recordConsumerOffset("cg-1", "recreated-topic", 0, 80L, 1L, t0, t0, oldId);

        // Topic deleted and recreated under the same name with new UUID and LEO=10
        boolean recreated = store.updateTopicMetadata("recreated-topic", newId, 1);
        assertThat(recreated).isTrue();
        store.recordBrokerOffset("recreated-topic", 0, 1, 10L, t0 + 5000L);

        // Stale v4 replay record carrying oldId must be fenced/dropped
        store.recordConsumerOffset("cg-1", "recreated-topic", 0, 85L, 2L, t0 + 6000L, t0 + 6000L, oldId);

        LagEvaluator evaluator = new LagEvaluator(spec, store, Clock.fixed(Instant.ofEpochMilli(t0 + 7000L), ZoneOffset.UTC));
        GroupEvaluation afterRecreate = evaluator.evaluateGroup("cg-1");
        assertThat(afterRecreate == null || afterRecreate.partitions().isEmpty()).isTrue();

        // Commit on new topic identity works cleanly
        store.recordConsumerOffset("cg-1", "recreated-topic", 0, 3L, 3L, t0 + 8000L, t0 + 8000L, newId);
        evaluator.invalidateAll();
        GroupEvaluation fresh = evaluator.evaluateGroup("cg-1");
        assertThat(fresh).isNotNull();
        assertThat(fresh.lagTotalComplete()).isTrue();
        assertThat(fresh.totalLag()).isEqualTo(7L);
    }

    @Test
    void doesNotReportZeroLagForUncommittedOwnerOnlyPartition() {
        // Regression test for finding #09
        ClusterSpec spec = specWith(3, 0, 3600, 100, 1000);
        ClusterStore store = new ClusterStore(spec);
        long t0 = 1_700_000_000_000L;
        store.updateTopicMetadata("orders", UUID.randomUUID(), 1);
        store.recordBrokerOffset("orders", 0, 1, 50L, t0);
        // Only owner assigned, zero commits
        store.setConsumerOwner("uncommitted-cg", "orders", 0, "/10.0.0.1", "c1");

        LagEvaluator evaluator = new LagEvaluator(spec, store, Clock.fixed(Instant.ofEpochMilli(t0 + 1000L), ZoneOffset.UTC));
        GroupEvaluation eval = evaluator.evaluateGroup("uncommitted-cg");
        assertThat(eval).isNotNull();
        assertThat(eval.lagTotalComplete()).isFalse();
        assertThat(eval.partitions()).hasSize(1);
        assertThat(eval.partitions().get(0).hasCommit()).isFalse();
        assertThat(eval.partitions().get(0).hasKnownLag()).isFalse();
        assertThat(eval.partitions().get(0).currentLag()).isEqualTo(PartitionEvaluation.UNKNOWN_LAG);
    }

    @Test
    void immediatelyPurgesExcludedGroupsWhenFilterTightened() {
        // Regression test for finding #10
        ClusterSpec initial = specWithFilters(3, 0, 3600, 100, 1000, "", "");
        ClusterStore store = new ClusterStore(initial);
        LagEvaluator evaluator = new LagEvaluator(initial, store);
        long t0 = System.currentTimeMillis();
        store.updateTopicMetadata("orders", UUID.randomUUID(), 1);
        store.recordBrokerOffset("orders", 0, 1, 100L, t0);
        store.recordConsumerOffset("keep-g", "orders", 0, 90L, 1L, t0, t0);
        store.recordConsumerOffset("drop-g", "orders", 0, 90L, 2L, t0, t0);
        assertThat(evaluator.evaluateAllGroups()).extracting(GroupEvaluation::group)
                .containsExactlyInAnyOrder("keep-g", "drop-g");

        // Tighten denylist to exclude drop-g
        ClusterSpec tightened = specWithFilters(3, 0, 3600, 100, 1000, "", "^drop-.*");
        List<String> purged = store.applyPolicy(tightened);
        evaluator.applyPolicy(tightened);
        assertThat(purged).containsExactly("drop-g");
        assertThat(evaluator.evaluateAllGroups()).extracting(GroupEvaluation::group)
                .containsExactly("keep-g");
        assertThat(evaluator.evaluateGroup("drop-g")).isNull();
    }

    @Test
    void handlesOffsetTombstoneProactiveExpiryAndIndexedKip848Resolution() {
        // Covers tombstone cleanup, expiry, and finding #16 indexed KIP-848 reconciliation
        ClusterSpec spec = specWith(2, 0, 10, 100, 1000);
        ClusterStore store = new ClusterStore(spec);
        long t0 = 1_700_000_000_000L;
        UUID topicUuid = UUID.randomUUID();

        store.recordBrokerOffset("events", 0, 1, 50L, t0);
        store.recordConsumerOffset("modern-g", "events", 0, 40L, 1L, t0, t0);
        store.recordConsumerOffset("modern-g", "events", 0, 45L, 2L, t0 + 1000L, t0 + 1000L);

        // Key 8 before TopicId metadata and Key 5
        store.updateKip848Assignment("modern-g", "m-1", List.of(new TopicIdPartitions(topicUuid, new int[]{0})));
        store.upsertKip848Member("modern-g", "m-1", "modern-client-1", "/10.2.3.4");
        store.updateTopicMetadata("events", topicUuid, 1);

        LagEvaluator evaluator = new LagEvaluator(spec, store, Clock.fixed(Instant.ofEpochMilli(t0 + 2_000L), ZoneOffset.UTC));
        GroupEvaluation eval = evaluator.evaluateGroup("modern-g");
        assertThat(eval).isNotNull();
        assertThat(eval.status()).isEqualTo(StatusCode.OK);
        assertThat(eval.totalLag()).isEqualTo(5L);
        assertThat(eval.partitions().get(0).owner()).isEqualTo("/10.2.3.4");
        assertThat(eval.partitions().get(0).clientId()).isEqualTo("modern-client-1");

        // Offset tombstone removes partition; clearing KIP-848 metadata then removes empty group
        store.deleteConsumerPartition("modern-g", "events", 0);
        assertThat(store.trackedPartitionCount()).isZero();
        store.clearKip848GroupMetadata("modern-g");
        assertThat(store.trackedGroupCount()).isZero();

        // Proactive expiration purges stale group
        store.recordConsumerOffset("g2", "events", 0, 10L, 3L, t0, t0);
        assertThat(store.purgeExpiredGroups(t0 + 15_000L)).containsExactly("g2");
    }
}
