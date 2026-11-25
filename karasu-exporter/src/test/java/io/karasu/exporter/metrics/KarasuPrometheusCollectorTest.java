/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.exporter.metrics;

import io.karasu.core.collector.ClusterManager;
import io.karasu.core.config.ClusterSpec;
import io.karasu.core.snapshot.ClusterMetricsSnapshot;
import io.karasu.core.snapshot.ClusterMetricsSnapshot.DurationHistogramSnapshot;
import io.karasu.core.spi.MetricLabelEnricher;
import io.karasu.exporter.labels.LabelNormalizer;
import io.karasu.protocol.model.ClusterHealthSnapshot;
import io.karasu.protocol.model.ClusterPhase;
import io.karasu.protocol.model.ClusterTargetKey;
import io.karasu.protocol.model.CommitSample;
import io.karasu.protocol.model.GroupEvaluation;
import io.karasu.protocol.model.PartitionEvaluation;
import io.karasu.protocol.model.StatusCode;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies exporter metrics, 3-layer extensible labels, Histogram exposition (#18),
 * uncommitted/incomplete group suppression (#03, #09), scrape-time label isolation (#12),
 * and snapshot staleness degradation (#15).
 */
class KarasuPrometheusCollectorTest {

    private static DurationHistogramSnapshot sampleHistogram() {
        long[] counts = new long[DurationHistogramSnapshot.DEFAULT_BUCKETS.length];
        counts[1] = 3L; // 3 builds <= 0.005s
        return new DurationHistogramSnapshot(DurationHistogramSnapshot.DEFAULT_BUCKETS, counts, 0.009);
    }

    @Test
    void exportsKarasuMetricsHistogramAndSuppressesIncompleteAndUncommittedSeries() throws Exception {
        long nowMs = 1_700_000_000_000L;
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC);

        MetricLabelEnricher spiEnricher = new MetricLabelEnricher() {
            @Override
            public Map<String, String> enrichClusterLabels(ClusterSpec spec) {
                return Map.of("managed_by", "karasu");
            }
        };
        LabelNormalizer normalizer = new LabelNormalizer(
                Map.of("env", "prod"),
                List.of("env", "tenant", "managed_by"),
                8,
                List.of(spiEnricher)
        );
        KarasuPrometheusCollector collector = new KarasuPrometheusCollector(
                new ClusterManager(8),
                normalizer,
                new KarasuPrometheusCollector.ExporterOptions("karasu_kafka", false, true),
                fixedClock
        );

        ClusterTargetKey keyA = new ClusterTargetKey("k8s-east-1", "kafka-order");
        ClusterSpec specA = new ClusterSpec(keyA, List.of("a:9092"), Map.of("tenant", "order"),
                null, null, null, null, null, null, null);
        CommitSample endSample = new CommitSample(88L, 10L, nowMs - 1000L, nowMs - 900L, 12L);
        PartitionEvaluation committedPart = new PartitionEvaluation("orders", 0, "/10.0.0.1", "c1", StatusCode.STALL, endSample, endSample, 12L, 1.0f, true);
        PartitionEvaluation uncommittedPart = new PartitionEvaluation("orders", 1, "/10.0.0.1", "c1", StatusCode.OK, null, null, PartitionEvaluation.UNKNOWN_LAG, 0.0f, false);
        GroupEvaluation geComplete = new GroupEvaluation(keyA, "order-cg", StatusCode.ERR, 1.0f, List.of(committedPart, uncommittedPart), 2, committedPart, 12L, true);
        // Incomplete group (e.g. truncated by limit) must NOT publish lag_sum (#03)
        GroupEvaluation geIncomplete = new GroupEvaluation(keyA, "partial-cg", StatusCode.OK, 1.0f, List.of(committedPart), 1, committedPart, 12L, false);

        ClusterHealthSnapshot healthA = new ClusterHealthSnapshot(
                ClusterPhase.READY, true, true, true, true, true, nowMs / 1000L, 0L, 2, 2, false, 1L, "");
        ClusterMetricsSnapshot snapA = new ClusterMetricsSnapshot(
                keyA, specA, healthA, List.of(geComplete, geIncomplete), Map.of("orders", new long[]{100L}),
                Map.of(), Map.of(), sampleHistogram(), nowMs - 1000L);

        MetricSnapshots snapshots = collector.collectSnapshots(List.of(snapA));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(baos, snapshots);
        String text = baos.toString(StandardCharsets.UTF_8);

        // Complete group + committed partition exported
        assertThat(text).contains("karasu_kafka_consumer_lag_sum{");
        assertThat(text).contains("consumer_group=\"order-cg\"");
        assertThat(text).contains("k8s_cluster=\"k8s-east-1\"");
        assertThat(text).contains("kafka_cluster=\"kafka-order\"");
        assertThat(text).contains("tenant=\"order\"");
        assertThat(text).contains("managed_by=\"karasu\"");

        // Incomplete group's lag_sum suppressed (#03), and uncommitted partition=1 lag suppressed (#09)
        assertThat(text).doesNotContain("karasu_kafka_consumer_lag_sum{consumer_group=\"partial-cg\"");
        assertThat(text).doesNotContain("partition=\"1\"");

        // Genuine Prometheus Histogram output (#18)
        assertThat(text).contains("# TYPE karasu_snapshot_build_duration_seconds histogram");
        assertThat(text).contains("karasu_snapshot_build_duration_seconds_bucket{");
        assertThat(text).contains("le=\"+Inf\"} 3");
        assertThat(text).contains("karasu_snapshot_build_duration_seconds_count{");
        assertThat(text).contains("karasu_snapshot_build_duration_seconds_sum{");
    }

    @Test
    void isolatesSingleClusterMergedLabelLimitViolationDuringScrape() throws Exception {
        // Regression test for finding #12: maxExtraLabels=1, commonLabels={env=prod}, bad cluster adds {tenant=x}
        long nowMs = 1_700_000_000_000L;
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC);

        LabelNormalizer normalizer = new LabelNormalizer(Map.of("env", "prod"), List.of(), 1, List.of());
        KarasuPrometheusCollector collector = new KarasuPrometheusCollector(
                new ClusterManager(1),
                normalizer,
                new KarasuPrometheusCollector.ExporterOptions("karasu_kafka", false, true),
                fixedClock
        );

        ClusterTargetKey okKey = new ClusterTargetKey("k8s-a", "ok-cluster");
        ClusterSpec okSpec = new ClusterSpec(okKey, List.of("a:9092"), Map.of(), null, null, null, null, null, null, null);
        ClusterTargetKey badKey = new ClusterTargetKey("k8s-b", "bad-cluster");
        ClusterSpec badSpec = new ClusterSpec(badKey, List.of("b:9092"), Map.of("tenant", "overflow"), null, null, null, null, null, null, null);

        // Strict pre-validation MUST reject badSpec
        assertThatThrownBy(() -> normalizer.validateMergedClusterSpec(badSpec))
                .isInstanceOf(IllegalArgumentException.class);

        // Even if badSpec reached scrape time (e.g. via dynamic enricher), scrape MUST NOT throw!
        ClusterHealthSnapshot ready = new ClusterHealthSnapshot(
                ClusterPhase.READY, true, true, true, true, true, nowMs / 1000L, 0L, 0, 0, false, 1L, "");
        ClusterMetricsSnapshot snapOk = new ClusterMetricsSnapshot(okKey, okSpec, ready, List.of(), Map.of(), Map.of(), Map.of(), sampleHistogram(), nowMs);
        ClusterMetricsSnapshot snapBad = new ClusterMetricsSnapshot(badKey, badSpec, ready, List.of(), Map.of(), Map.of(), Map.of(), sampleHistogram(), nowMs);

        MetricSnapshots out = collector.collectSnapshots(List.of(snapOk, snapBad));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(baos, out);
        String text = baos.toString(StandardCharsets.UTF_8);
        assertThat(text).contains("kafka_cluster=\"ok-cluster\"");
        assertThat(text).contains("kafka_cluster=\"bad-cluster\"");
    }

    @Test
    void degradesStaleSnapshotExceedingMaxAge() throws Exception {
        // Regression test for finding #15
        long nowMs = 1_700_000_200_000L;
        long staleBuiltAt = nowMs - 120_000L; // 120s old > 60s maxSnapshotAge
        Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC);

        LabelNormalizer normalizer = new LabelNormalizer(Map.of(), List.of(), 8, List.of());
        KarasuPrometheusCollector collector = new KarasuPrometheusCollector(
                new ClusterManager(8),
                normalizer,
                new KarasuPrometheusCollector.ExporterOptions("karasu_kafka", false, true),
                fixedClock
        );

        ClusterTargetKey key = new ClusterTargetKey("k8s-a", "stale-kafka");
        ClusterSpec spec = new ClusterSpec(key, List.of("a:9092"), Map.of(), null, null, null, null, null, null, null);
        CommitSample end = new CommitSample(10L, 1L, staleBuiltAt, staleBuiltAt, 5L);
        PartitionEvaluation pe = new PartitionEvaluation("t", 0, "", "", StatusCode.OK, end, end, 5L, 1.0f, true);
        GroupEvaluation ge = new GroupEvaluation(key, "stale-cg", StatusCode.OK, 1.0f, List.of(pe), 1, pe, 5L, true);
        ClusterHealthSnapshot oldHealthy = new ClusterHealthSnapshot(
                ClusterPhase.READY, true, true, true, true, true, staleBuiltAt / 1000L, 0L, 1, 1, false, 1L, "");

        ClusterMetricsSnapshot staleSnap = new ClusterMetricsSnapshot(
                key, spec, oldHealthy, List.of(ge), Map.of("t", new long[]{15L}), Map.of(), Map.of(), sampleHistogram(), staleBuiltAt);

        MetricSnapshots out = collector.collectSnapshots(List.of(staleSnap));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(baos, out);
        String text = baos.toString(StandardCharsets.UTF_8);

        assertThat(text).contains("karasu_cluster_up{k8s_cluster=\"k8s-a\",kafka_cluster=\"stale-kafka\"} 0.0");
        assertThat(text).contains("karasu_cluster_state{k8s_cluster=\"k8s-a\",kafka_cluster=\"stale-kafka\",state=\"DEGRADED\"} 1.0");
        assertThat(text).doesNotContain("stale-cg");
    }

    @Test
    void rejectsReservedLabelCollisionInCommonLabels() {
        assertThatThrownBy(() -> new LabelNormalizer(Map.of("kafka_cluster", "bad"), List.of(), 8, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
    }
}
