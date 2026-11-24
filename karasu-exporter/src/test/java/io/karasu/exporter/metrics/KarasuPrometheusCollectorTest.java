/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package io.karasu.exporter.metrics;

import io.karasu.core.collector.ClusterManager;
import io.karasu.core.config.ClusterSpec;
import io.karasu.core.snapshot.ClusterMetricsSnapshot;
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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KarasuPrometheusCollectorTest {

    @Test
    void exportsKarasuMetricsWithK8sKafkaAndNormalizedExtraLabelsAndQualityGate() throws Exception {
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
                new KarasuPrometheusCollector.ExporterOptions("karasu_kafka", false, true)
        );

        // Cluster A: READY with lag=12, status=STALL -> ERR
        ClusterTargetKey keyA = new ClusterTargetKey("k8s-east-1", "kafka-order");
        ClusterSpec specA = new ClusterSpec(keyA, List.of("a:9092"), Map.of("tenant", "order"),
                null, null, null, null, null, null, null);
        CommitSample endSample = new CommitSample(88L, 10L, 1700000000000L, 1700000000100L, 12L);
        PartitionEvaluation pe = new PartitionEvaluation("orders", 0, "/10.0.0.1", "c1", StatusCode.STALL, endSample, endSample, 12L, 1.0f);
        GroupEvaluation ge = new GroupEvaluation(keyA, "order-cg", StatusCode.ERR, 1.0f, List.of(pe), 1, pe, 12L);
        ClusterHealthSnapshot healthA = new ClusterHealthSnapshot(
                ClusterPhase.READY, true, true, true, true, true, 1700000000L, 0L, 1, 1, false, 1L, "");
        ClusterMetricsSnapshot snapA = new ClusterMetricsSnapshot(
                keyA, specA, healthA, List.of(ge), Map.of("orders", new long[]{100L}), Map.of(), Map.of(), 0.002, 1700000000000L);

        // Cluster B: DEGRADED (no tenant label!) -> business lag must be suppressed by quality gate, health metrics still exported
        ClusterTargetKey keyB = new ClusterTargetKey("k8s-west-2", "kafka-pay");
        ClusterSpec specB = new ClusterSpec(keyB, List.of("b:9092"), Map.of(),
                null, null, null, null, null, null, null);
        GroupEvaluation geB = new GroupEvaluation(keyB, "stale-cg", StatusCode.OK, 1.0f, List.of(), 0, null, 999L);
        ClusterHealthSnapshot healthB = new ClusterHealthSnapshot(
                ClusterPhase.DEGRADED, false, false, true, false, false, 1699999000L, 50L, 1, 1, false, 1L, "disconnected");
        ClusterMetricsSnapshot snapB = new ClusterMetricsSnapshot(
                keyB, specB, healthB, List.of(geB), Map.of("pay", new long[]{500L}), Map.of(), Map.of(), 0.001, 1700000000000L);

        MetricSnapshots snapshots = collector.collectSnapshots(List.of(snapA, snapB));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(baos, snapshots);
        String text = baos.toString(StandardCharsets.UTF_8);

        // Cluster A business metrics present with k8s_cluster, kafka_cluster, and all 3 extra label layers
        assertThat(text).contains("karasu_kafka_consumer_lag_sum{");
        assertThat(text).contains("k8s_cluster=\"k8s-east-1\"");
        assertThat(text).contains("kafka_cluster=\"kafka-order\"");
        assertThat(text).contains("env=\"prod\"");
        assertThat(text).contains("tenant=\"order\"");
        assertThat(text).contains("managed_by=\"karasu\"");
        assertThat(text).contains("consumer_group=\"order-cg\"");
        assertThat(text).contains("karasu_kafka_topic_partition_status{");
        assertThat(text).contains("karasu_kafka_topic_partition_offset{");

        // Cluster B health metrics exported (up=0, unified empty tenant label), stale business lag suppressed!
        assertThat(text).contains("karasu_cluster_up{");
        assertThat(text).contains("k8s_cluster=\"k8s-west-2\"");
        assertThat(text).contains("kafka_cluster=\"kafka-pay\"");
        assertThat(text).doesNotContain("stale-cg");
    }

    @Test
    void rejectsReservedLabelCollisionInCommonLabels() {
        assertThatThrownBy(() -> new LabelNormalizer(Map.of("kafka_cluster", "bad"), List.of(), 8, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("reserved");
    }
}
